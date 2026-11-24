#include "ib_connect_profile.h"
#include "init_trace.h"
#include "allgather_profile.h"

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unordered_map>
#include <unistd.h>
#include <vector>

namespace {
constexpr const char* phases[] = {
  "setup", "socket_connect", "send_device_list", "recv_device_list", "local_setup",
  "send_qp_metadata", "recv_qp_metadata", "remote_setup", "send_ready", "cleanup"
};
constexpr const char* operations[] = {
  "subnet_select", "alloc_comm", "comm_init", "stats_init", "socket_init", "socket_connect", "socket_ready",
  "alloc_metadata", "send_device_list", "recv_device_list", "device_init", "alloc_pd", "gid_snapshot",
  "create_mrc_cq", "create_ib_cq", "pkey_lookup", "qps_create", "qp_create", "qp_init", "query_ece",
  "reg_scratchpad", "reg_cts_fifo", "reg_remote_speed", "send_qp_metadata", "recv_qp_metadata",
  "reg_completions", "qps_to_rts", "set_ece", "qp_rtr", "qp_rts", "device_speeds", "lb_weights",
  "send_ready", "cleanup_qps", "provider_create_qp", "provider_modify_init",
  "provider_modify_rtr", "provider_modify_rts", "provider_modify_other"
};
static_assert(sizeof(phases) / sizeof(*phases) == size_t(NitIbPhase::Count));
static_assert(sizeof(operations) / sizeof(*operations) == size_t(NitIbOp::Count));

struct Phase {
  uint64_t wall_ns = 0, active_ns = 0, visits = 0;
};
struct Operation {
  uint64_t calls = 0, ns = 0, min_ns = 0, max_ns = 0, errors = 0, bytes = 0, invalid_offsets = 0;
};
struct Sample {
  NitIbOp operation;
  NitIbPhase phase;
  int index, result;
  uint64_t start_ns, end_ns;
};
struct Capture {
  std::mutex mutex;
  std::string directory;
  uint64_t next_id = 1;
  std::unordered_map<uintptr_t, NitIbConnectPoll::Record*> active;
  std::vector<NitIbConnectPoll::Record*> records;
};
Capture& capture() {
  static auto* value = new Capture;
  return *value;
}
thread_local NitIbConnectPoll::Record* current = nullptr;
void save();

bool enabled() {
  static bool value = []() {
    const char* directory = std::getenv("NCCL_IB_CONNECT_PROFILE_DIR");
    if (!directory || !*directory) return false;
    struct stat status;
    if (stat(directory, &status) || !S_ISDIR(status.st_mode)) {
      std::fprintf(stderr, "IB connect profile: directory does not exist: %s\n", directory);
      std::abort();
    }
    capture().directory = directory;
    if (std::atexit(save)) {
      std::fprintf(stderr, "IB connect profile: atexit registration failed\n");
      std::abort();
    }
    return true;
  }();
  return value;
}
bool sampled(NitIbOp op) {
  return op == NitIbOp::QpCreate || op == NitIbOp::QpInit ||
      op == NitIbOp::QpRtr || op == NitIbOp::QpRts ||
      op == NitIbOp::ProviderCreateQp || op == NitIbOp::ProviderModifyInit ||
      op == NitIbOp::ProviderModifyRtr || op == NitIbOp::ProviderModifyRts ||
      op == NitIbOp::ProviderModifyOther;
}
}  // namespace

struct NitIbConnectPoll::Record {
  std::mutex mutex;
  uint64_t id = 0, start_ns = 0, end_ns = 0, phase_start = 0, active_ns = 0, polls = 0;
  uintptr_t handle = 0;
  long start_tid = 0, end_tid = 0;
  int device = -1, result = -1;
  bool complete = false;
  NitLink link = {};
  NitIbPhase phase = NitIbPhase::Setup;
  Phase phases[size_t(NitIbPhase::Count)];
  Operation operations[size_t(NitIbOp::Count)];
  std::vector<Sample> samples;
};

NitIbConnectPoll::NitIbConnectPoll(uintptr_t handle, int device) {
  const int saved_errno = errno;
  if (enabled()) {
    auto& state = capture();
    {
      std::lock_guard<std::mutex> lock(state.mutex);
      auto found = state.active.find(handle);
      if (found != state.active.end()) {
        record_ = found->second;
      } else {
        record_ = new Record;
        record_->id = state.next_id++;
        record_->handle = handle;
        record_->device = device;
        record_->link = nit_capture();
        record_->start_tid = syscall(SYS_gettid);
        state.records.push_back(record_);
        state.active.emplace(handle, record_);
      }
    }
    record_->mutex.lock();
    previous_ = current;
    current = record_;
    start_ = segment_ = nit_ag_now();
    if (!record_->start_ns) {
      record_->start_ns = record_->phase_start = start_;
      record_->phases[size_t(record_->phase)].visits = 1;
    }
  }
  errno = saved_errno;
}

void NitIbConnectPoll::phase(NitIbPhase next) {
  if (!record_ || next == record_->phase) return;
  const int saved_errno = errno;
  uint64_t now = nit_ag_now();
  auto& previous = record_->phases[size_t(record_->phase)];
  previous.wall_ns += now - record_->phase_start;
  previous.active_ns += now - segment_;
  record_->phase = next;
  record_->phase_start = segment_ = now;
  ++record_->phases[size_t(next)].visits;
  errno = saved_errno;
}

void NitIbConnectPoll::finish(int result, bool connected) {
  if (!record_ || finished_) return;
  const int saved_errno = errno;
  uint64_t now = nit_ag_now();
  record_->phases[size_t(record_->phase)].active_ns += now - segment_;
  record_->active_ns += now - start_;
  ++record_->polls;
  if (result != 0 || connected) {
    record_->phases[size_t(record_->phase)].wall_ns += now - record_->phase_start;
    record_->end_ns = now;
    record_->end_tid = syscall(SYS_gettid);
    record_->result = result;
    record_->complete = true;
  }
  finished_ = true;
  errno = saved_errno;
}

NitIbConnectPoll::~NitIbConnectPoll() {
  if (!record_) return;
  const int saved_errno = errno;
  if (!finished_) finish(-1, false);
  current = previous_;
  bool complete = record_->complete;
  record_->mutex.unlock();
  if (complete) {
    auto& state = capture();
    std::lock_guard<std::mutex> lock(state.mutex);
    auto found = state.active.find(record_->handle);
    if (found != state.active.end() && found->second == record_) state.active.erase(found);
  }
  errno = saved_errno;
}

NitIbConnectOperation::NitIbConnectOperation(NitIbOp operation, int index)
    : record_(current), operation_(operation), index_(index) {
  const int saved_errno = errno;
  if (record_) start_ = nit_ag_now();
  errno = saved_errno;
}

NitIbConnectOperation::~NitIbConnectOperation() {
  if (!record_) return;
  const int saved_errno = errno;
  uint64_t end = nit_ag_now(), elapsed = end - start_;
  auto& op = record_->operations[size_t(operation_)];
  if (!op.calls || elapsed < op.min_ns) op.min_ns = elapsed;
  if (elapsed > op.max_ns) op.max_ns = elapsed;
  ++op.calls;
  op.ns += elapsed;
  op.errors += result_ != 0;
  if (bytes_ >= 0) op.bytes += uint64_t(bytes_);
  else ++op.invalid_offsets;
  if (sampled(operation_)) record_->samples.push_back({operation_, record_->phase, index_, result_, start_, end});
  errno = saved_errno;
}

namespace {
void save() {
  const int saved_errno = errno;
  auto& state = capture();
  std::vector<NitIbConnectPoll::Record*> records;
  {
    std::lock_guard<std::mutex> lock(state.mutex);
    records = state.records;
  }
  if (records.empty()) return;
  char host[256] = {};
  if (gethostname(host, sizeof(host) - 1)) {
    std::perror("IB connect profile: gethostname");
    std::abort();
  }
  std::string path = state.directory + "/ib-connect." + host + "." + std::to_string(getpid())
      + "." + std::to_string(nit_ag_now()) + ".jsonl";
  FILE* file = std::fopen(path.c_str(), "wx");
  if (!file) {
    std::perror("IB connect profile: open");
    std::abort();
  }
  std::fprintf(file, "{\"kind\":\"metadata\",\"schema\":1,\"clock\":\"CLOCK_MONOTONIC\",\"host\":\"%s\",\"pid\":%d}\n", host, getpid());
  for (auto* r : records) {
    std::lock_guard<std::mutex> lock(r->mutex);
    uint64_t end = r->end_ns ? r->end_ns : nit_ag_now();
    std::fprintf(file,
        "{\"kind\":\"ib_connect\",\"id\":%llu,\"handle\":%llu,\"device\":%d,\"start_tid\":%ld,\"end_tid\":%ld,"
        "\"start_ns\":%llu,\"end_ns\":%llu,\"complete\":%s,\"result\":%d,\"polls\":%llu,\"active_ns\":%llu,"
        "\"native_process\":%llu,\"native_span\":%llu,\"native_instance\":%llu,\"owner_pid\":%d,"
        "\"comm_rank\":%d,\"nranks\":%d,\"comm_hash\":%llu,\"phases\":[",
        (unsigned long long)r->id, (unsigned long long)r->handle, r->device, r->start_tid, r->end_tid,
        (unsigned long long)r->start_ns, (unsigned long long)end, r->complete ? "true" : "false", r->result,
        (unsigned long long)r->polls, (unsigned long long)r->active_ns,
        (unsigned long long)r->link.process, (unsigned long long)r->link.span,
        (unsigned long long)r->link.context.instance, r->link.context.owner_pid,
        r->link.context.comm_rank, r->link.context.nranks, (unsigned long long)r->link.context.comm_hash);
    bool first = true;
    for (size_t i = 0; i < size_t(NitIbPhase::Count); ++i) {
      const auto& p = r->phases[i];
      if (!p.visits) continue;
      uint64_t wall = p.wall_ns + (!r->complete && i == size_t(r->phase) ? end - r->phase_start : 0);
      std::fprintf(file, "%s{\"name\":\"%s\",\"wall_ns\":%llu,\"active_ns\":%llu,\"visits\":%llu}",
          first ? "" : ",", phases[i], (unsigned long long)wall, (unsigned long long)p.active_ns, (unsigned long long)p.visits);
      first = false;
    }
    std::fprintf(file, "],\"operations\":[");
    first = true;
    for (size_t i = 0; i < size_t(NitIbOp::Count); ++i) {
      const auto& op = r->operations[i];
      if (!op.calls) continue;
      std::fprintf(file,
          "%s{\"name\":\"%s\",\"calls\":%llu,\"ns\":%llu,\"min_ns\":%llu,\"max_ns\":%llu,"
          "\"errors\":%llu,\"bytes\":%llu,\"invalid_offsets\":%llu}",
          first ? "" : ",", operations[i], (unsigned long long)op.calls, (unsigned long long)op.ns,
          (unsigned long long)op.min_ns, (unsigned long long)op.max_ns, (unsigned long long)op.errors,
          (unsigned long long)op.bytes, (unsigned long long)op.invalid_offsets);
      first = false;
    }
    std::fprintf(file, "],\"qp_calls\":[");
    first = true;
    for (const auto& s : r->samples) {
      std::fprintf(file,
          "%s{\"operation\":\"%s\",\"phase\":\"%s\",\"index\":%d,\"result\":%d,\"start_ns\":%llu,\"end_ns\":%llu}",
          first ? "" : ",", operations[size_t(s.operation)], phases[size_t(s.phase)],
          s.index, s.result, (unsigned long long)s.start_ns, (unsigned long long)s.end_ns);
      first = false;
    }
    std::fprintf(file, "]}\n");
  }
  bool failed = std::ferror(file);
  if (std::fclose(file)) failed = true;
  if (failed) {
    std::fprintf(stderr, "IB connect profile: write failed: %s\n", path.c_str());
    std::abort();
  }
  errno = saved_errno;
}
}  // namespace
