#pragma once

#ifdef __cplusplus
extern "C" {
#endif

int ncclMrcEarlyBootstrapDescriptorSizeV1(void);
int ncclMrcEarlyBootstrapPrepareV1(const char* deviceName, int rank, int nranks,
                                  int trafficClass, void** session, void* descriptor, int size);
int ncclMrcEarlyBootstrapFinishV1(void* session, const void* predecessor, const void* successor, int size);
int ncclMrcEarlyBootstrapBindV1(void* session, const void* uniqueId, int size);
int ncclMrcEarlyBootstrapCloseV1(void* session, int requireConsumed);

#ifdef __cplusplus
}
#endif
