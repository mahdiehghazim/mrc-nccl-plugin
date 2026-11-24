#include "init_trace.h"

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>
#include <vector>

namespace {
uint64_t now() {
  timespec ts;
  if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
    std::perror("NCCL init trace: clock_gettime");
    std::abort();
  }
  return uint64_t(ts.tv_sec) * 1000000000 + ts.tv_nsec;
}

struct Event {
  uint64_t id, parent, cause, cause_process, start, end;
  int cause_pid, result;
  NitContext context;
  char name[96];
};

struct Buffer {
  std::mutex mutex;
  std::vector<Event> completed;
  std::vector<Event> stack;
  long tid = syscall(SYS_gettid);
  FILE* file = nullptr;
  unsigned batches = 0;
};

struct State {
  std::mutex mutex;
  std::vector<Buffer*> buffers;
  std::atomic<uint64_t> next_span{1};
  std::atomic<uint64_t> next_instance{1};
  std::string directory;
  char host[256] = {};
  int pid = getpid();
  uint64_t process = 0;
};

State& state() {
  static State* value = new State;
  return *value;
}

void flush(Buffer& buffer) {
  if (buffer.completed.empty()) return;
  auto& s = state();
  if (!buffer.file) {
    std::string path = s.directory + "/" + s.host + "." + std::to_string(s.pid)
        + "." + std::to_string(buffer.tid) + ".jsonl";
    buffer.file = fopen(path.c_str(), "wx");
    if (!buffer.file) {
      std::perror(("NCCL init trace: " + path).c_str());
      std::abort();
    }
    if (fprintf(buffer.file,
        "{\"type\":\"metadata\",\"schema\":1,\"clock\":\"CLOCK_MONOTONIC\","
        "\"host\":\"%s\",\"pid\":%d,\"tid\":%ld,\"process\":%llu}\n",
        s.host, s.pid, buffer.tid, (unsigned long long)s.process) < 0) {
      std::perror("NCCL init trace: metadata write");
      std::abort();
    }
  }
  for (const auto& e : buffer.completed) {
    if (fprintf(buffer.file,
        "{\"type\":\"span\",\"id\":%llu,\"parent\":%llu,\"cause\":%llu,\"cause_process\":%llu,\"cause_pid\":%d,"
        "\"name\":\"%s\",\"start_ns\":%llu,\"end_ns\":%llu,\"result\":%d,"
        "\"comm\":%llu,\"parent_comm\":%llu,\"comm_hash\":%llu,\"parent_hash\":%llu,"
        "\"instance\":%llu,\"owner_pid\":%d,\"comm_rank\":%d,\"nranks\":%d}\n",
        (unsigned long long)e.id, (unsigned long long)e.parent, (unsigned long long)e.cause,
        (unsigned long long)e.cause_process, e.cause_pid, e.name,
        (unsigned long long)e.start, (unsigned long long)e.end, e.result,
        (unsigned long long)e.context.comm, (unsigned long long)e.context.parent_comm,
        (unsigned long long)e.context.comm_hash, (unsigned long long)e.context.parent_hash,
        (unsigned long long)e.context.instance, e.context.owner_pid,
        e.context.comm_rank, e.context.nranks) < 0) {
      std::perror("NCCL init trace: span write");
      std::abort();
    }
  }
  if (fflush(buffer.file) != 0) {
    std::perror("NCCL init trace: flush");
    std::abort();
  }
  buffer.completed.clear();
  ++buffer.batches;
}

Buffer& buffer() {
  thread_local Buffer* value = []() {
    auto* value = new Buffer;
    value->completed.reserve(4096);
    auto& s = state();
    std::lock_guard<std::mutex> lock(s.mutex);
    s.buffers.push_back(value);
    return value;
  }();
  return *value;
}

void finish() {
  nit_flush();
}
}  // namespace

extern "C" int nit_enabled() {
  const int saved_errno = errno;
  static const int enabled = []() {
    const char* directory = getenv("NCCL_INIT_TRACE_DIR");
    if (!directory || !*directory) return 0;
    struct stat status;
    if (stat(directory, &status) != 0 || !S_ISDIR(status.st_mode)) {
      std::fprintf(stderr, "NCCL init trace: directory does not exist: %s\n", directory);
      std::abort();
    }
    auto& s = state();
    s.directory = directory;
    if (gethostname(s.host, sizeof(s.host) - 1) != 0) {
      std::perror("NCCL init trace: gethostname");
      std::abort();
    }
    s.process = 14695981039346656037ull;
    for (const char* ch = s.host; *ch; ++ch) s.process = (s.process ^ uint8_t(*ch)) * 1099511628211ull;
    s.process ^= now() ^ uint64_t(s.pid);
    if (std::atexit(finish) != 0) {
      std::fprintf(stderr, "NCCL init trace: atexit registration failed\n");
      std::abort();
    }
    return 1;
  }();
  errno = saved_errno;
  return enabled;
}

extern "C" uint64_t nit_begin(const char* name, const NitContext* context, const NitLink* cause) {
  const int saved_errno = errno;
  if (!nit_enabled()) {
    errno = saved_errno;
    return 0;
  }
  auto& b = buffer();
  std::lock_guard<std::mutex> lock(b.mutex);
  Event event = {};
  event.id = state().next_span.fetch_add(1, std::memory_order_relaxed);
  if (!b.stack.empty()) {
    event.parent = b.stack.back().id;
    event.context = b.stack.back().context;
  }
  if (context) event.context = *context;
  if (cause) {
    event.cause = cause->span;
    event.cause_pid = cause->pid;
    event.cause_process = cause->process;
  }
  if (std::strlen(name) >= sizeof(event.name)) {
    std::fprintf(stderr, "NCCL init trace: span name too long: %s\n", name);
    std::abort();
  }
  std::strcpy(event.name, name);
  b.stack.push_back(event);
  b.stack.back().start = now();
  errno = saved_errno;
  return event.id;
}

extern "C" void nit_end(uint64_t id, int result) {
  if (!id) return;
  const int saved_errno = errno;
  const uint64_t end = now();
  auto& b = buffer();
  std::lock_guard<std::mutex> lock(b.mutex);
  if (b.stack.empty() || b.stack.back().id != id) {
    std::fprintf(stderr, "NCCL init trace: unbalanced span stack\n");
    std::abort();
  }
  Event event = b.stack.back();
  b.stack.pop_back();
  event.end = end;
  event.result = result;
  b.completed.push_back(event);
  // Bounded buffering; full-buffer I/O can appear in the enclosing parent's residual.
  if (b.completed.size() >= 65536) flush(b);
  errno = saved_errno;
}

extern "C" NitLink nit_capture() {
  const int saved_errno = errno;
  if (!nit_enabled()) return {};
  auto& b = buffer();
  std::lock_guard<std::mutex> lock(b.mutex);
  NitLink link = {};
  if (!b.stack.empty()) link = {b.stack.back().context, b.stack.back().id, state().process, state().pid};
  errno = saved_errno;
  return link;
}

extern "C" void nit_set_context(const NitContext* context) {
  if (!nit_enabled()) return;
  auto& b = buffer();
  std::lock_guard<std::mutex> lock(b.mutex);
  if (!b.stack.empty()) b.stack.back().context = *context;
}

extern "C" uint64_t nit_next_instance() {
  return nit_enabled() ? state().next_instance.fetch_add(1, std::memory_order_relaxed) : 0;
}

extern "C" void nit_flush() {
  const int saved_errno = errno;
  auto& s = state();
  std::lock_guard<std::mutex> lock(s.mutex);
  for (auto* b : s.buffers) {
    std::lock_guard<std::mutex> buffer_lock(b->mutex);
    flush(*b);
  }
  errno = saved_errno;
}
