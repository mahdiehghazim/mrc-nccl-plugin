#include "transport/net_ib/connect.h"
#include "transport/net_ib/p2p_resiliency.h"
#include "ibvwrap.h"
#include "early_mrc.h"
#include "early_bootstrap.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <set>
#include <vector>

#define DECLARE_FIXTURE(name) decltype(name) fixture_##name
DECLARE_FIXTURE(ncclIbInit);
DECLARE_FIXTURE(ncclIbFinalize);
DECLARE_FIXTURE(wrap_ibv_alloc_pd);
DECLARE_FIXTURE(wrap_ibv_dealloc_pd);
DECLARE_FIXTURE(wrap_ibv_create_cq);
DECLARE_FIXTURE(wrap_ibv_destroy_cq);
DECLARE_FIXTURE(wrap_ibv_reg_mr);
DECLARE_FIXTURE(wrap_ibv_dereg_mr);
DECLARE_FIXTURE(wrap_mrc_create_cq);
DECLARE_FIXTURE(wrap_mrc_destroy_cq);
DECLARE_FIXTURE(wrap_mrc_create_qp);
DECLARE_FIXTURE(wrap_mrc_get_qpn);
DECLARE_FIXTURE(wrap_mrc_modify_qp);
DECLARE_FIXTURE(wrap_mrc_destroy_qp);
DECLARE_FIXTURE(wrap_mrc_post_recv);
DECLARE_FIXTURE(ncclSocketConnect);
DECLARE_FIXTURE(ncclSocketReady);
DECLARE_FIXTURE(ncclSocketProgress);
DECLARE_FIXTURE(ncclSocketListen);
DECLARE_FIXTURE(ncclSocketAccept);
DECLARE_FIXTURE(ncclSocketGetAddr);

#define ncclIbInit fixture_ncclIbInit
#define ncclIbFinalize fixture_ncclIbFinalize
#define wrap_ibv_alloc_pd fixture_wrap_ibv_alloc_pd
#define wrap_ibv_dealloc_pd fixture_wrap_ibv_dealloc_pd
#define wrap_ibv_create_cq fixture_wrap_ibv_create_cq
#define wrap_ibv_destroy_cq fixture_wrap_ibv_destroy_cq
#define wrap_ibv_reg_mr fixture_wrap_ibv_reg_mr
#define wrap_ibv_dereg_mr fixture_wrap_ibv_dereg_mr
#define wrap_mrc_create_cq fixture_wrap_mrc_create_cq
#define wrap_mrc_destroy_cq fixture_wrap_mrc_destroy_cq
#define wrap_mrc_create_qp fixture_wrap_mrc_create_qp
#define wrap_mrc_get_qpn fixture_wrap_mrc_get_qpn
#define wrap_mrc_modify_qp fixture_wrap_mrc_modify_qp
#define wrap_mrc_destroy_qp fixture_wrap_mrc_destroy_qp
#define wrap_mrc_post_recv fixture_wrap_mrc_post_recv
#define ncclSocketConnect fixture_ncclSocketConnect
#define ncclSocketReady fixture_ncclSocketReady
#define ncclSocketProgress fixture_ncclSocketProgress
#define ncclSocketListen fixture_ncclSocketListen
#define ncclSocketAccept fixture_ncclSocketAccept
#define ncclSocketGetAddr fixture_ncclSocketGetAddr
#include "../src/transport/net_ib/connect.cc"

namespace {
struct FakeQp {
  uint32_t qpn;
  ibv_qp_state state;
  uint32_t remote;
};
std::set<void*> live;
int providerCalls, failAt, rtrCalls, socketCalls, nextQpn = 1;
bool failProvider() { return ++providerCalls == failAt; }
template<class T> T* allocate() {
  auto* value = new T{};
  assert(live.insert(value).second);
  return value;
}
template<class T> void release(T* value) {
  assert(live.erase(value) == 1);
  delete value;
}
void checkClean() {
  assert(live.empty());
  for (int d = 0; d < 4; ++d) assert(ncclIbDevs[d].pdRefs == 0);
  assert(socketCalls == 0);
}
}

ncclResult_t fixture_ncclIbInit(void** ctx, uint64_t, ncclNetCommConfig_t* config,
    ncclDebugLogger_t logger, ncclProfilerCallback_t) {
  ncclMrcSetLogger(logger);
  auto* copy = allocate<ncclNetCommConfig_t>();
  *copy = *config;
  *ctx = copy;
  return ncclSuccess;
}
ncclResult_t fixture_ncclIbFinalize(void* ctx) {
  release(static_cast<ncclNetCommConfig_t*>(ctx));
  return ncclSuccess;
}
ncclResult_t fixture_wrap_ibv_alloc_pd(ibv_pd** out, ibv_context* ctx) {
  if (failProvider()) return ncclSystemError;
  *out = allocate<ibv_pd>();
  (*out)->context = ctx;
  return ncclSuccess;
}
ncclResult_t fixture_wrap_ibv_dealloc_pd(ibv_pd* pd) {
  release(pd);
  return ncclSuccess;
}
ncclResult_t fixture_wrap_ibv_create_cq(ibv_cq** out, ibv_context*, int, void*, ibv_comp_channel*, int) {
  if (failProvider()) return ncclSystemError;
  *out = allocate<ibv_cq>();
  return ncclSuccess;
}
ncclResult_t fixture_wrap_ibv_destroy_cq(ibv_cq* cq) {
  release(cq);
  return ncclSuccess;
}
ncclResult_t fixture_wrap_mrc_create_cq(mrc_cq** out, mrc_context*, int, void*, mrc_comp_channel*, int) {
  if (failProvider()) return ncclSystemError;
  *out = reinterpret_cast<mrc_cq*>(allocate<int>());
  return ncclSuccess;
}
ncclResult_t fixture_wrap_mrc_destroy_cq(mrc_cq* cq) {
  release(reinterpret_cast<int*>(cq));
  return ncclSuccess;
}
ncclResult_t fixture_wrap_ibv_reg_mr(ibv_mr** out, ibv_pd* pd, void* addr, size_t length, int) {
  if (failProvider()) return ncclSystemError;
  *out = allocate<ibv_mr>();
  (*out)->pd = pd;
  (*out)->addr = addr;
  (*out)->length = length;
  (*out)->lkey = (*out)->rkey = nextQpn++;
  return ncclSuccess;
}
ncclResult_t fixture_wrap_ibv_dereg_mr(ibv_mr* mr) {
  release(mr);
  return ncclSuccess;
}
ncclResult_t fixture_wrap_mrc_create_qp(mrc_qp** out, mrc_context*, mrc_qp_init_attr*) {
  if (failProvider()) return ncclSystemError;
  auto* qp = allocate<FakeQp>();
  qp->qpn = nextQpn++;
  qp->state = IBV_QPS_RESET;
  *out = reinterpret_cast<mrc_qp*>(qp);
  return ncclSuccess;
}
ncclResult_t fixture_wrap_mrc_get_qpn(mrc_qp* opaque, uint32_t* qpn) {
  if (failProvider()) return ncclSystemError;
  *qpn = reinterpret_cast<FakeQp*>(opaque)->qpn;
  return ncclSuccess;
}
ncclResult_t fixture_wrap_mrc_modify_qp(mrc_qp* opaque, ibv_qp_attr* attr, int, mrc_qp_attr*, int) {
  if (failProvider()) return ncclSystemError;
  auto* qp = reinterpret_cast<FakeQp*>(opaque);
  if (attr->qp_state == IBV_QPS_RTR) {
    assert(qp->state == IBV_QPS_INIT);
    qp->remote = attr->dest_qp_num;
    ++rtrCalls;
  } else if (attr->qp_state == IBV_QPS_RTS) {
    assert(qp->state == IBV_QPS_RTR);
  } else {
    assert(attr->qp_state == IBV_QPS_INIT);
    assert(qp->state == IBV_QPS_RESET);
  }
  qp->state = attr->qp_state;
  return ncclSuccess;
}
ncclResult_t fixture_wrap_mrc_destroy_qp(mrc_qp* qp) {
  release(reinterpret_cast<FakeQp*>(qp));
  return ncclSuccess;
}
ncclResult_t fixture_wrap_mrc_post_recv(mrc_qp* opaque, ibv_recv_wr*, ibv_recv_wr**) {
  assert(reinterpret_cast<FakeQp*>(opaque)->state == IBV_QPS_RTS);
  return ncclSuccess;
}
ncclResult_t fixture_ncclSocketConnect(ncclSocket*) { ++socketCalls; return ncclSystemError; }
ncclResult_t fixture_ncclSocketReady(ncclSocket*, int*) { ++socketCalls; return ncclSystemError; }
ncclResult_t fixture_ncclSocketProgress(int, ncclSocket*, void*, int, int*, int*) {
  ++socketCalls;
  return ncclSystemError;
}
ncclResult_t fixture_ncclSocketListen(ncclSocket*) { ++socketCalls; return ncclSystemError; }
ncclResult_t fixture_ncclSocketAccept(ncclSocket*, ncclSocket*, bool) { ++socketCalls; return ncclSystemError; }
ncclResult_t fixture_ncclSocketGetAddr(ncclSocket*, ncclSocketAddress*) { ++socketCalls; return ncclSystemError; }

int main() {
  ncclNMergedIbDevs = ncclNIbDevs = 4;
  for (int d = 0; d < 4; ++d) {
    auto& merged = ncclIbMergedDevs[d];
    std::snprintf(merged.devName, sizeof(merged.devName), "mlx5_%d_dma", d);
    merged.vProps.ndevs = 1;
    merged.vProps.devs[0] = d;
    auto& dev = ncclIbDevs[d];
    std::strcpy(dev.devName, merged.devName);
    dev.context = reinterpret_cast<ibv_context*>(static_cast<uintptr_t>(d + 1));
    dev.mrcContext = reinterpret_cast<mrc_context*>(static_cast<uintptr_t>(d + 1));
    dev.portNum = 1;
    dev.portAttr.active_mtu = IBV_MTU_4096;
    dev.portAttr.link_layer = dev.gidInfo.link_layer = IBV_LINK_LAYER_ETHERNET;
    dev.gidInfo.localGid.raw[15] = d + 1;
    dev.currSpeed = dev.speed = 400000;
  }
  auto callback = +[](void**, int, void*, int64_t, void*) { return ncclSuccess; };
  ncclMrcSetLogger(earlyMrcLogger);
  assert(ncclIbInitDevices(nullptr, nullptr) == ncclSuccess);
  assert(ncclIbInitDevices(nullptr, callback) == ncclSuccess);
  assert(ncclProfilerFunction == callback);
  assert(ncclIbFinalizeDevices() == ncclSuccess);
  assert(ncclIbFinalizeDevices() == ncclSuccess);
  ncclProfilerFunction = nullptr;
  void *a = nullptr, *b = nullptr;
  EarlyMrcDescriptor da{}, db{};
  assert(ncclMrcEarlyBootstrapPrepareV1("=mlx5_0_dma", 0, 2, -1, &a, &da, sizeof(da)) == 0);
  assert(ncclMrcEarlyBootstrapPrepareV1("=mlx5_2_dma", 1, 2, -1, &b, &db, sizeof(db)) == 0);
  assert(rtrCalls == 0);
  assert(da.send.qpInfo[0].qpn != da.recv.qpInfo[0].qpn);
  auto bad = db;
  bad.send.qpInfo[0].devIndex = NCCL_IB_MAX_DEVS_PER_NIC;
  assert(ncclMrcEarlyBootstrapFinishV1(a, &bad, &db, sizeof(db)) != 0);
  assert(rtrCalls == 0);
  assert(ncclMrcEarlyBootstrapFinishV1(a, &db, &db, sizeof(db)) == 0);
  assert(ncclMrcEarlyBootstrapFinishV1(b, &da, &da, sizeof(da)) == 0);
  const int flowCalls = providerCalls / 2;
  auto* session = static_cast<EarlyMrcSession*>(a);
  assert(session->recv->remCtsFifo.addr == db.send.addr);
  auto* send = static_cast<ncclIbSendComm*>(session->handle.stage.comm);
  assert(send->remCmplsRecords.addr == db.recv.addr);
  for (unsigned q = 0; q < da.nqps; ++q) {
    auto* recvQp = reinterpret_cast<FakeQp*>(session->recv->base.qps[q].mrcQp);
    auto* sendQp = reinterpret_cast<FakeQp*>(send->base.qps[q].mrcQp);
    assert(recvQp->state == IBV_QPS_RTS && recvQp->remote == db.send.qpInfo[q].qpn);
    assert(sendQp->state == IBV_QPS_RTS && sendQp->remote == db.recv.qpInfo[q].qpn);
  }
  char uid[128] = {};
  std::memcpy(uid + 120, "MRCEARLY", 8);
  assert(ncclMrcEarlyBootstrapBindV1(a, uid, sizeof(uid)) == 0);
  void *adoptedSend = nullptr, *adoptedRecv = nullptr;
  int found = 0;
  assert(nitEarlyBootstrapTake(uid, 0, 2, "MRC", &adoptedSend, &adoptedRecv, &found) == 0 && found);
  assert(ncclMrcEarlyBootstrapCloseV1(a, 1) == 0);
  assert(!live.empty());
  assert(ncclIbCloseSend(adoptedSend) == 0);
  assert(ncclIbCloseRecv(adoptedRecv) == 0);
  assert(ncclMrcEarlyBootstrapCloseV1(b, 0) == 0);
  checkClean();

  // Fail each provider operation in local preparation and peer finalization.
  for (int step = 1; step <= flowCalls; ++step) {
    a = nullptr;
    providerCalls = 0;
    failAt = step;
    int result = ncclMrcEarlyBootstrapPrepareV1("mlx5_0_dma", 0, 2, -1, &a, &da, sizeof(da));
    if (result == 0) result = ncclMrcEarlyBootstrapFinishV1(a, &db, &db, sizeof(db));
    assert(result != 0);
    assert(ncclMrcEarlyBootstrapCloseV1(a, 0) == 0);
    checkClean();
  }
  std::printf("PASS: live-QP handoff, peer mapping, no socket progress, %d injected provider failures\n", flowCalls);
}
