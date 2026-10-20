#ifndef NCCL_MRC_BOOTSTRAP_HANDOFF_H_
#define NCCL_MRC_BOOTSTRAP_HANDOFF_H_

#include <cstdint>
#include <string>

// On success the caller owns the validated listening FD; on failure no FD escapes.
bool ncclMrcBootstrapTakeListener(const char* path, const char* runId, int rank,
                                  const char* ip, uint16_t port, int* fd, std::string* error);

struct ncclMrcBootstrapBroker;
struct ncclMrcBootstrapRoute {
  uint8_t run[16];
  uint32_t source;
  uint32_t destination;
};

bool ncclMrcBootstrapMakeRoute(const char* runId, int source, int destination,
                              ncclMrcBootstrapRoute* route, std::string* error);
void ncclMrcBootstrapEncodeRoute(const ncclMrcBootstrapRoute& route, char (&header)[28], bool strided = false);
bool ncclMrcBootstrapRegister(const char* path, const char* runId, int rank, const char* ip,
                             uint16_t port, const char* previousIp, uint64_t token,
                             ncclMrcBootstrapBroker** broker, std::string* error,
                             int first = 0, int stride = 0, int nranks = 0);
// Nonblocking: fd == -1 means pending. A successful transfer gives the caller ownership.
bool ncclMrcBootstrapPoll(ncclMrcBootstrapBroker* broker, int* fd, std::string* error);
// Closing the control connection unregisters the communicator, not the job listeners.
bool ncclMrcBootstrapUnregister(ncclMrcBootstrapBroker* broker, std::string* error);

#endif
