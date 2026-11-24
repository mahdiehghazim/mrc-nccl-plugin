#pragma once

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

int nitEarlyBootstrapPublish(const void* uniqueId, int rank, int nranks,
                             const char* netName, void* send, void* recv, void* owner);
int nitEarlyBootstrapTake(const void* uniqueId, int rank, int nranks,
                          const char* netName, void** send, void** recv, int* found);
int nitEarlyBootstrapWithdraw(void* owner, int* consumed);

#ifdef __cplusplus
}
#endif
