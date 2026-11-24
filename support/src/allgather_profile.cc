#include "allgather_profile.h"
#include "init_trace.h"

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>
#include <vector>

thread_local NitAgStep* nit_ag_step = nullptr;
thread_local NitAgMemory* nit_ag_memory = nullptr;

uint64_t nit_ag_now() {
  timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    std::perror("AllGather profile: clock_gettime");
    std::abort();
  }
  return uint64_t(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}

namespace {
struct Base {
  uint64_t id = 0, start_ns = 0, end_ns = 0, magic = 0;
  uintptr_t bootstrap = 0;
  long tid = syscall(SYS_gettid);
  int rank = 0, nranks = 0, result = -1;
  size_t bytes = 0;
  NitLink link = {};
};

struct Capture {
  std::mutex mutex;
  std::atomic<uint64_t> next_id{1};
  std::string directory;
  std::vector<std::pair<void*, bool>> records;
};

Capture& capture() {
  static auto* value = new Capture;
  return *value;
}

void save();

bool enabled() {
  static bool value = []() {
    const char* directory = std::getenv("NCCL_BOOTSTRAP_PROFILE_DIR");
    if (!directory || !*directory) return false;
    struct stat status;
    if (stat(directory, &status) || !S_ISDIR(status.st_mode)) {
      std::fprintf(stderr, "AllGather profile: directory does not exist: %s\n", directory);
      std::abort();
    }
    capture().directory = directory;
    if (std::atexit(save)) {
      std::fprintf(stderr, "AllGather profile: atexit registration failed\n");
      std::abort();
    }
    return true;
  }();
  return value;
}

void initialize(Base& base, uintptr_t bootstrap, int rank, int nranks, size_t bytes) {
  base.id = capture().next_id.fetch_add(1, std::memory_order_relaxed);
  base.bootstrap = bootstrap;
  base.rank = rank;
  base.nranks = nranks;
  base.bytes = bytes;
  base.link = nit_capture();
}

void retain(void* record, bool preparation) {
  auto& state = capture();
  std::lock_guard<std::mutex> lock(state.mutex);
  state.records.emplace_back(record, preparation);
}

void writeBase(FILE* file, const Base& base, const char* kind) {
  std::fprintf(file,
      "{\"kind\":\"%s\",\"id\":%llu,\"tid\":%ld,\"bootstrap\":%llu,\"magic\":%llu,"
      "\"rank\":%d,\"nranks\":%d,\"bytes\":%zu,\"start_ns\":%llu,\"end_ns\":%llu,"
      "\"result\":%d,\"native_instance\":%llu,\"native_span\":%llu",
      kind, (unsigned long long)base.id, base.tid, (unsigned long long)base.bootstrap,
      (unsigned long long)base.magic,
      base.rank, base.nranks, base.bytes, (unsigned long long)base.start_ns,
      (unsigned long long)base.end_ns, base.result,
      (unsigned long long)base.link.context.instance, (unsigned long long)base.link.span);
}

void writeOperation(FILE* file, const char* name, const NitAgOperation& op) {
  std::fprintf(file,
      ",\"%s\":{\"submit_calls\":%llu,\"submit_nulls\":%llu,\"submit_ns\":%llu,"
      "\"test_calls\":%llu,\"test_ns\":%llu,\"submitted_ns\":%llu,\"completed_ns\":%llu}",
      name, (unsigned long long)op.submit_calls, (unsigned long long)op.submit_nulls,
      (unsigned long long)op.submit_ns, (unsigned long long)op.test_calls,
      (unsigned long long)op.test_ns, (unsigned long long)op.submitted_ns,
      (unsigned long long)op.completed_ns);
}
}  // namespace

struct NitAllGatherProfile::Record : Base {
  NitAgMemory memory[4];
  std::vector<NitAgStep> steps;
};

struct NitAgPreparation::Record : Base {
  const char* name;
};

NitAllGatherProfile::NitAllGatherProfile(int rank, int nranks, int bytes, uintptr_t bootstrap, uint64_t magic) {
  if (!enabled()) return;
  record_ = new Record;
  initialize(*record_, bootstrap, rank, nranks, bytes);
  record_->magic = magic;
  record_->steps.resize(nranks > 0 ? nranks - 1 : 0);
  for (auto& memory : record_->memory) memory.bytes = size_t(nranks) * bytes;
}

NitAllGatherProfile::~NitAllGatherProfile() {
  if (!record_) return;
  if (!record_->end_ns) finish(-1);
  retain(record_, false);
}

void NitAllGatherProfile::start() {
  if (record_) record_->start_ns = nit_ag_now();
}

NitAgMemory* NitAllGatherProfile::memory(int index) {
  return record_ ? &record_->memory[index] : nullptr;
}

NitAgStep* NitAllGatherProfile::beginStep(int index) {
  if (!record_) return nullptr;
  nit_ag_step = &record_->steps[index];
  nit_ag_step->start_ns = nit_ag_now();
  return nit_ag_step;
}

void NitAllGatherProfile::endStep(int result) {
  if (!record_ || !nit_ag_step) return;
  nit_ag_step->end_ns = nit_ag_now();
  nit_ag_step->result = result;
  nit_ag_step = nullptr;
}

void NitAllGatherProfile::finish(int result) {
  if (!record_) return;
  endStep(result);
  record_->end_ns = nit_ag_now();
  record_->result = result;
}

NitAgMemoryScope::NitAgMemoryScope(NitAgMemory* memory)
    : memory_(memory), previous_(nit_ag_memory) {
  nit_ag_memory = memory;
  if (memory_) memory_->start_ns = nit_ag_now();
}

NitAgMemoryScope::~NitAgMemoryScope() {
  if (memory_) memory_->end_ns = nit_ag_now();
  nit_ag_memory = previous_;
}

void NitAgMemoryScope::result(int result) {
  if (memory_) memory_->result = result;
}

NitAgPreparation::NitAgPreparation(const char* name, uintptr_t bootstrap, int rank, int nranks, size_t bytes) {
  if (!enabled()) return;
  record_ = new Record;
  initialize(*record_, bootstrap, rank, nranks, bytes);
  record_->name = name;
  record_->start_ns = nit_ag_now();
}

NitAgPreparation::~NitAgPreparation() {
  if (!record_) return;
  record_->end_ns = nit_ag_now();
  retain(record_, true);
}

void NitAgPreparation::result(int result) {
  if (record_) record_->result = result;
}

namespace {
void save() {
  auto& state = capture();
  std::lock_guard<std::mutex> lock(state.mutex);
  if (state.records.empty()) return;
  char host[256] = {};
  if (gethostname(host, sizeof(host) - 1)) {
    std::perror("AllGather profile: gethostname");
    std::abort();
  }
  std::string path = state.directory + "/allgather." + host + "." + std::to_string(getpid())
      + "." + std::to_string(nit_ag_now()) + ".jsonl";
  FILE* file = std::fopen(path.c_str(), "wx");
  if (!file) {
    std::perror("AllGather profile: open");
    std::abort();
  }
  std::fprintf(file, "{\"kind\":\"metadata\",\"schema\":1,\"clock\":\"CLOCK_MONOTONIC\","
      "\"host\":\"%s\",\"pid\":%d,\"page_size\":%ld}\n", host, getpid(), sysconf(_SC_PAGESIZE));
  for (auto item : state.records) {
    if (item.second) {
      auto* record = static_cast<NitAgPreparation::Record*>(item.first);
      writeBase(file, *record, "preparation");
      std::fprintf(file, ",\"name\":\"%s\"}\n", record->name);
      delete record;
      continue;
    }
    auto* record = static_cast<NitAllGatherProfile::Record*>(item.first);
    writeBase(file, *record, "allgather");
    std::fprintf(file, ",\"memory\":[");
    for (int i = 0; i < 4; ++i) {
      const auto& m = record->memory[i];
      std::fprintf(file,
          "%s{\"start_ns\":%llu,\"end_ns\":%llu,\"bytes\":%llu,\"result\":%d,"
          "\"reg_calls\":%llu,\"reg_ns\":%llu,\"registered_bytes\":%llu,"
          "\"dereg_calls\":%llu,\"dereg_ns\":%llu,\"cache_hits\":%llu,"
          "\"cache_misses\":%llu,\"cache_lock_ns\":%llu}",
          i ? "," : "", (unsigned long long)m.start_ns, (unsigned long long)m.end_ns,
          (unsigned long long)m.bytes, m.result, (unsigned long long)m.reg_calls,
          (unsigned long long)m.reg_ns, (unsigned long long)m.registered_bytes,
          (unsigned long long)m.dereg_calls, (unsigned long long)m.dereg_ns,
          (unsigned long long)m.cache_hits, (unsigned long long)m.cache_misses,
          (unsigned long long)m.cache_lock_ns);
    }
    std::fprintf(file, "],\"steps\":[");
    bool first = true;
    for (const auto& step : record->steps) {
      if (!step.start_ns) continue;
      std::fprintf(file,
          "%s{\"start_ns\":%llu,\"end_ns\":%llu,\"result\":%d,\"loops\":%llu,"
          "\"cq_calls\":%llu,\"cq_empty\":%llu,\"cq_entries\":%llu",
          first ? "" : ",", (unsigned long long)step.start_ns,
          (unsigned long long)step.end_ns, step.result, (unsigned long long)step.loops,
          (unsigned long long)step.cq_calls, (unsigned long long)step.cq_empty,
          (unsigned long long)step.cq_entries);
      writeOperation(file, "send", step.send);
      writeOperation(file, "recv", step.recv);
      std::fprintf(file, "}");
      first = false;
    }
    std::fprintf(file, "]}\n");
    delete record;
  }
  bool failed = std::ferror(file);
  if (std::fclose(file)) failed = true;
  if (failed) {
    std::perror("AllGather profile: write");
    std::abort();
  }
  state.records.clear();
}
}  // namespace
