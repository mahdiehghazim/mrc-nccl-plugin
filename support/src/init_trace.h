#ifndef NCCL_INIT_TRACE_H_
#define NCCL_INIT_TRACE_H_

#include <stdint.h>

struct NitContext {
  uint64_t comm, parent_comm, comm_hash, parent_hash, instance;
  int owner_pid, comm_rank, nranks;
};

struct NitLink {
  NitContext context;
  uint64_t span, process;
  int pid;
};

extern "C" {
int nit_enabled();
uint64_t nit_begin(const char* name, const NitContext* context, const NitLink* cause);
void nit_end(uint64_t span, int result);
NitLink nit_capture();
void nit_set_context(const NitContext* context);
uint64_t nit_next_instance();
void nit_flush();
}

class NitScope {
 public:
  explicit NitScope(const char* name, const NitContext* context = nullptr, const NitLink* cause = nullptr)
      : span_(nit_begin(name, context, cause)) {}
  ~NitScope() { nit_end(span_, result_); }
  NitScope(const NitScope&) = delete;
  NitScope& operator=(const NitScope&) = delete;
  void result(int result) { result_ = result; }
  void context(const NitContext& context) { if (span_) nit_set_context(&context); }

 private:
  uint64_t span_;
  int result_ = -1;
};

class NitStage {
 public:
  ~NitStage() { nit_end(span_, -1); }
  void begin(const char* name) {
    end();
    span_ = nit_begin(name, nullptr, nullptr);
  }
  void end(int result = 0) {
    nit_end(span_, result);
    span_ = 0;
  }
 private:
  uint64_t span_ = 0;
};

#define NIT_CALL(name, call) ([&]() { \
  NitScope nit_scope_(name); \
  auto nit_result_ = (call); \
  nit_scope_.result(static_cast<int>(nit_result_)); \
  return nit_result_; \
}())

#endif
