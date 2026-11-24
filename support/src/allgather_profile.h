#ifndef NCCL_ALLGATHER_PROFILE_H_
#define NCCL_ALLGATHER_PROFILE_H_

#include <stddef.h>
#include <stdint.h>

struct NitAgOperation {
  uint64_t submit_calls = 0, submit_nulls = 0, submit_ns = 0;
  uint64_t test_calls = 0, test_ns = 0;
  uint64_t submitted_ns = 0, completed_ns = 0;
};

struct NitAgStep {
  uint64_t start_ns = 0, end_ns = 0, loops = 0;
  uint64_t cq_calls = 0, cq_empty = 0, cq_entries = 0;
  NitAgOperation send, recv;
  int result = -1;
};

struct NitAgMemory {
  uint64_t start_ns = 0, end_ns = 0, bytes = 0;
  uint64_t reg_calls = 0, reg_ns = 0, registered_bytes = 0;
  uint64_t dereg_calls = 0, dereg_ns = 0;
  uint64_t cache_hits = 0, cache_misses = 0, cache_lock_ns = 0;
  int result = -1;
};

extern thread_local NitAgStep* nit_ag_step;
extern thread_local NitAgMemory* nit_ag_memory;
uint64_t nit_ag_now();

class NitAllGatherProfile {
 public:
  struct Record;
  NitAllGatherProfile(int rank, int nranks, int bytes, uintptr_t bootstrap, uint64_t magic = 0);
  ~NitAllGatherProfile();
  NitAllGatherProfile(const NitAllGatherProfile&) = delete;
  NitAllGatherProfile& operator=(const NitAllGatherProfile&) = delete;
  void start();
  NitAgMemory* memory(int index);
  NitAgStep* beginStep(int index);
  void endStep(int result);
  void finish(int result);

 private:
  Record* record_ = nullptr;
};

class NitAgMemoryScope {
 public:
  explicit NitAgMemoryScope(NitAgMemory* memory);
  ~NitAgMemoryScope();
  void result(int result);
 private:
  NitAgMemory* memory_;
  NitAgMemory* previous_;
};

class NitAgPreparation {
 public:
  struct Record;
  NitAgPreparation(const char* name, uintptr_t bootstrap, int rank, int nranks, size_t bytes);
  ~NitAgPreparation();
  void result(int result);
 private:
  Record* record_ = nullptr;
};

class NitAgProviderMemoryScope {
 public:
  explicit NitAgProviderMemoryScope(bool deregister, size_t bytes = 0)
      : memory_(nit_ag_memory), deregister_(deregister) {
    if (!memory_) return;
    if (deregister_) ++memory_->dereg_calls;
    else {
      ++memory_->reg_calls;
      memory_->registered_bytes += bytes;
    }
    start_ = nit_ag_now();
  }
  ~NitAgProviderMemoryScope() {
    if (!memory_) return;
    uint64_t elapsed = nit_ag_now() - start_;
    if (deregister_) memory_->dereg_ns += elapsed;
    else memory_->reg_ns += elapsed;
  }
 private:
  NitAgMemory* memory_;
  bool deregister_;
  uint64_t start_ = 0;
};

#define NIT_AG_MEMORY(profile, index, call) ([&]() { \
  NitAgMemoryScope nit_memory_((profile).memory(index)); \
  auto nit_result_ = (call); \
  nit_memory_.result(static_cast<int>(nit_result_)); \
  return nit_result_; \
}())

#define NIT_AG_PREPARE(name, bootstrap, rank, nranks, bytes, call) ([&]() { \
  NitAgPreparation nit_preparation_(name, reinterpret_cast<uintptr_t>(bootstrap), rank, nranks, bytes); \
  auto nit_result_ = (call); \
  nit_preparation_.result(static_cast<int>(nit_result_)); \
  return nit_result_; \
}())

#endif
