#ifndef NCCL_IB_CONNECT_PROFILE_H_
#define NCCL_IB_CONNECT_PROFILE_H_

#include <stdint.h>

enum class NitIbPhase {
  Setup, SocketConnect, SendDeviceList, RecvDeviceList, LocalSetup,
  SendMetadata, RecvMetadata, RemoteSetup, SendReady, Cleanup, Count
};

enum class NitIbOp {
  SubnetSelect, AllocComm, CommInit, StatsInit, SocketInit, SocketConnect, SocketReady,
  AllocMetadata, SendDeviceList, RecvDeviceList, DeviceInit, AllocPd, GidSnapshot,
  CreateMrcCq, CreateIbCq, PkeyLookup, QpsCreate, QpCreate, QpInit, QueryEce,
  RegScratchpad, RegCtsFifo, RegRemoteSpeed, SendMetadata, RecvMetadata,
  RegCompletions, QpsToRts, SetEce, QpRtr, QpRts, DeviceSpeeds, LbWeights,
  SendReady, CleanupQps, ProviderCreateQp, ProviderModifyInit,
  ProviderModifyRtr, ProviderModifyRts, ProviderModifyOther, Count
};

class NitIbConnectPoll {
 public:
  struct Record;
  NitIbConnectPoll(uintptr_t handle, int device);
  ~NitIbConnectPoll();
  NitIbConnectPoll(const NitIbConnectPoll&) = delete;
  NitIbConnectPoll& operator=(const NitIbConnectPoll&) = delete;
  void phase(NitIbPhase phase);
  void finish(int result, bool connected);

 private:
  Record* record_ = nullptr;
  Record* previous_ = nullptr;
  uint64_t start_ = 0, segment_ = 0;
  bool finished_ = false;
};

class NitIbConnectOperation {
 public:
  explicit NitIbConnectOperation(NitIbOp operation, int index = -1);
  ~NitIbConnectOperation();
  void result(int result, int64_t bytes = 0) { result_ = result; bytes_ = bytes; }

 private:
  NitIbConnectPoll::Record* record_;
  NitIbOp operation_;
  int index_, result_ = -1;
  int64_t bytes_ = 0;
  uint64_t start_ = 0;
};

#define NIT_IB_CONNECT_CALL(operation, call) ([&]() { \
  NitIbConnectOperation nit_operation_(NitIbOp::operation); \
  auto nit_result_ = (call); \
  nit_operation_.result(static_cast<int>(nit_result_)); \
  return nit_result_; \
}())

#define NIT_IB_CONNECT_INDEX(operation, index, call) ([&]() { \
  NitIbConnectOperation nit_operation_(NitIbOp::operation, index); \
  auto nit_result_ = (call); \
  nit_operation_.result(static_cast<int>(nit_result_)); \
  return nit_result_; \
}())

#define NIT_IB_CONNECT_SOCKET(operation, offset, call) ([&]() { \
  const int nit_offset_ = (offset); \
  NitIbConnectOperation nit_operation_(NitIbOp::operation); \
  auto nit_result_ = (call); \
  nit_operation_.result(static_cast<int>(nit_result_), int64_t(offset) - nit_offset_); \
  return nit_result_; \
}())

#define NIT_IB_CONNECT_VOID(operation, call) do { \
  NitIbConnectOperation nit_operation_(NitIbOp::operation); \
  (call); \
  nit_operation_.result(0); \
} while (0)

#endif
