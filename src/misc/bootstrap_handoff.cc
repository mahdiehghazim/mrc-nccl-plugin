#include "bootstrap_handoff.h"

#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#include <vector>

struct ncclMrcBootstrapBroker {
  int control = -1;
  int pending = -1;
  bool strided = false;
  enum { ReceiveFd, SendAck, ReceiveDone, Delivered, Failed } phase = ReceiveFd;
  sockaddr_in local = {}, peer = {};
  std::chrono::steady_clock::time_point deadline;
  ~ncclMrcBootstrapBroker() {
    if (pending >= 0) close(pending);
    if (control >= 0) close(control);
  }
};

namespace {
void require(bool condition, const char* message) {
  if (!condition) throw std::runtime_error(message);
}
void systemCheck(bool condition, const char* operation) {
  if (!condition) throw std::runtime_error(std::string(operation) + ": " + strerror(errno));
}
struct Descriptors {
  std::vector<int> values;
  ~Descriptors() { for (int fd : values) if (fd >= 0) close(fd); }
};
int option(int fd, int name) {
  int value = 0;
  socklen_t size = sizeof(value);
  systemCheck(getsockopt(fd, SOL_SOCKET, name, &value, &size) == 0, "getsockopt");
  require(size == sizeof(value), "unexpected socket option size");
  return value;
}
void validateRun(const std::string& run) {
  require(run.size() == 36, "invalid handoff run UUID");
  for (size_t i = 0; i < run.size(); ++i) {
    bool separator = i == 8 || i == 13 || i == 18 || i == 23;
    require(separator ? run[i] == '-' : ((run[i] >= '0' && run[i] <= '9') || (run[i] >= 'a' && run[i] <= 'f')),
            "invalid handoff run UUID");
  }
}
int openControl(const char* path, const std::string& run, Descriptors& owned) {
  validateRun(run);
  std::string endpoint(path);
  std::string prefix = "/tmp/nccl-bootstrap." + run + ".";
  require(endpoint.size() == prefix.size() + 8 + strlen("/handoff.sock") &&
          endpoint.compare(0, prefix.size(), prefix) == 0 &&
          endpoint.compare(prefix.size() + 8, std::string::npos, "/handoff.sock") == 0, "invalid handoff path");
  for (size_t i = prefix.size(); i < prefix.size() + 8; ++i)
    require((endpoint[i] >= 'a' && endpoint[i] <= 'z') || (endpoint[i] >= '0' && endpoint[i] <= '9') ||
            endpoint[i] == '_', "invalid handoff directory");
  std::string directory = endpoint.substr(0, prefix.size() + 8);
  struct stat metadata;
  systemCheck(lstat(directory.c_str(), &metadata) == 0, "stat handoff directory");
  require(S_ISDIR(metadata.st_mode) && (metadata.st_mode & 0777) == 0700 && metadata.st_uid == geteuid(),
          "handoff directory must be private and owned by this UID");
  systemCheck(lstat(path, &metadata) == 0, "stat handoff endpoint");
  require(S_ISSOCK(metadata.st_mode) && (metadata.st_mode & 0777) == 0600 && metadata.st_uid == geteuid(),
          "handoff endpoint must be a private same-UID socket");
  owned.values.reserve(17);
  int control = socket(AF_UNIX, SOCK_SEQPACKET | SOCK_CLOEXEC, 0);
  systemCheck(control >= 0, "create handoff socket");
  owned.values.push_back(control);
  timeval timeout = {2, 0};
  systemCheck(setsockopt(control, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0, "set handoff receive timeout");
  systemCheck(setsockopt(control, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0, "set handoff send timeout");
  sockaddr_un address = {};
  address.sun_family = AF_UNIX;
  require(endpoint.size() < sizeof(address.sun_path), "handoff path is too long");
  memcpy(address.sun_path, path, endpoint.size() + 1);
  systemCheck(connect(control, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0, "connect handoff helper");
  ucred peer = {};
  socklen_t size = sizeof(peer);
  systemCheck(getsockopt(control, SOL_SOCKET, SO_PEERCRED, &peer, &size) == 0, "query helper UID");
  require(size == sizeof(peer) && peer.uid == geteuid(), "helper UID mismatch");
  return control;
}

// Even malformed responses may carry descriptors: collect them before validating.
bool receivePacket(int control, const char* expected, bool withFd, bool nonblocking, int* fd) {
  Descriptors owned;
  owned.values.reserve(16);
  char payload[256] = {};
  union { cmsghdr alignment; char bytes[CMSG_SPACE(sizeof(int) * 16)]; } ancillary = {};
  iovec vector = {payload, sizeof(payload)};
  msghdr message = {};
  message.msg_iov = &vector;
  message.msg_iovlen = 1;
  message.msg_control = ancillary.bytes;
  message.msg_controllen = sizeof(ancillary.bytes);
  ssize_t received = recvmsg(control, &message, MSG_CMSG_CLOEXEC | (nonblocking ? MSG_DONTWAIT : 0));
  if (received < 0 && nonblocking && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) return false;
  systemCheck(received >= 0, "receive broker response");
  bool unexpected = false;
  int headers = 0;
  for (cmsghdr* header = CMSG_FIRSTHDR(&message); header; header = CMSG_NXTHDR(&message, header)) {
    ++headers;
    if (header->cmsg_level != SOL_SOCKET || header->cmsg_type != SCM_RIGHTS || header->cmsg_len < CMSG_LEN(0)) {
      unexpected = true;
      continue;
    }
    size_t bytes = header->cmsg_len - CMSG_LEN(0);
    for (size_t offset = 0; offset + sizeof(int) <= bytes; offset += sizeof(int)) {
      int value;
      memcpy(&value, CMSG_DATA(header) + offset, sizeof(value));
      owned.values.push_back(value);
    }
    if (bytes != sizeof(int)) unexpected = true;
  }
  if (received >= 5 && (memcmp(payload, "ERR3 ", 5) == 0 || memcmp(payload, "ERR4 ", 5) == 0))
    throw std::runtime_error("broker rejected request: " + std::string(payload + 5, received - 5));
  require(received == static_cast<ssize_t>(strlen(expected)) && memcmp(payload, expected, received) == 0 &&
          !(message.msg_flags & (MSG_TRUNC | MSG_CTRUNC)) && !unexpected &&
          headers == (withFd ? 1 : 0) && owned.values.size() == (withFd ? 1u : 0u), "invalid descriptor response");
  if (withFd) { *fd = owned.values[0]; owned.values[0] = -1; }
  return true;
}

void validateEndpoint(int fd, const sockaddr_in& expected, bool peer) {
  sockaddr_storage actual = {};
  socklen_t size = sizeof(actual);
  int result = peer ? getpeername(fd, reinterpret_cast<sockaddr*>(&actual), &size) :
                      getsockname(fd, reinterpret_cast<sockaddr*>(&actual), &size);
  systemCheck(result == 0, "query handoff socket endpoint");
  auto* ipv4 = reinterpret_cast<sockaddr_in*>(&actual);
  require(size == sizeof(sockaddr_in) && ipv4->sin_family == AF_INET &&
          (peer || ipv4->sin_port == expected.sin_port) && ipv4->sin_addr.s_addr == expected.sin_addr.s_addr,
          "received socket endpoint differs from manifest");
}
} // namespace

bool ncclMrcBootstrapTakeListener(const char* path, const char* runId, int rank,
                                  const char* ip, uint16_t port, int* fd, std::string* error) {
  if (fd) *fd = -1;
  try {
    require(fd && error && path && runId && ip && rank >= 0 && rank < 65536 && port, "invalid handoff arguments");
    std::string run(runId);
    sockaddr_in expected = {};
    expected.sin_family = AF_INET;
    expected.sin_port = htons(port);
    require(inet_pton(AF_INET, ip, &expected.sin_addr) == 1, "invalid listener IPv4 address");

    Descriptors owned;
    int control = openControl(path, run, owned);
    std::string request = "TAKE1 " + run + " " + std::to_string(rank) + " " + ip + " " + std::to_string(port);
    systemCheck(send(control, request.data(), request.size(), MSG_NOSIGNAL) == static_cast<ssize_t>(request.size()),
                "send handoff request");
    int listener = -1;
    receivePacket(control, "FD1", true, false, &listener);
    owned.values.push_back(listener);
    require(option(listener, SO_TYPE) == SOCK_STREAM && option(listener, SO_PROTOCOL) == IPPROTO_TCP &&
            option(listener, SO_ACCEPTCONN) == 1 && option(listener, SO_REUSEADDR) == 0 &&
            option(listener, SO_REUSEPORT) == 0, "descriptor is not an exclusive TCP listener");
    validateEndpoint(listener, expected, false);
    int flags = fcntl(listener, F_GETFL);
    systemCheck(flags >= 0 && fcntl(listener, F_SETFL, flags | O_NONBLOCK) == 0, "set listener nonblocking");
    systemCheck(fcntl(listener, F_SETFD, FD_CLOEXEC) == 0, "set listener close-on-exec");
    systemCheck(send(control, "ACK1", 4, MSG_NOSIGNAL) == 4, "acknowledge listening descriptor");
    receivePacket(control, "DONE1", false, false, nullptr);
    error->clear();
    *fd = listener;
    owned.values[1] = -1;
    return true;
  } catch (const std::runtime_error& failure) {
    if (error) *error = failure.what();
  } catch (const std::bad_alloc&) {
    if (error) *error = "out of memory during listener handoff";
  }
  return false;
}

bool ncclMrcBootstrapMakeRoute(const char* runId, int source, int destination,
                              ncclMrcBootstrapRoute* route, std::string* error) {
  try {
    require(runId && route && error && source >= 0 && source < 65536 && destination >= 0 && destination < 65536,
            "invalid bootstrap route");
    std::string run(runId);
    validateRun(run);
    size_t index = 0;
    for (size_t i = 0; i < run.size();) {
      if (run[i] == '-') { ++i; continue; }
      auto digit = [](char c) { return c <= '9' ? c - '0' : c - 'a' + 10; };
      route->run[index++] = static_cast<uint8_t>((digit(run[i]) << 4) | digit(run[i + 1]));
      i += 2;
    }
    route->source = static_cast<uint32_t>(source);
    route->destination = static_cast<uint32_t>(destination);
    error->clear();
    return true;
  } catch (const std::runtime_error& failure) {
    if (error) *error = failure.what();
  } catch (const std::bad_alloc&) {
    if (error) *error = "out of memory creating route";
  }
  return false;
}

void ncclMrcBootstrapEncodeRoute(const ncclMrcBootstrapRoute& route, char (&header)[28], bool strided) {
  memcpy(header, strided ? "MRC4" : "MRC3", 4);
  memcpy(header + 4, route.run, 16);
  uint32_t source = htonl(route.source), destination = htonl(route.destination);
  memcpy(header + 20, &source, 4);
  memcpy(header + 24, &destination, 4);
}

bool ncclMrcBootstrapRegister(const char* path, const char* runId, int rank, const char* ip,
                             uint16_t port, const char* previousIp, uint64_t token,
                             ncclMrcBootstrapBroker** broker, std::string* error, int first, int stride, int nranks) {
  if (broker) *broker = nullptr;
  try {
    require(path && runId && ip && previousIp && broker && error && port && token && rank >= 0 && rank < 65536,
            "invalid broker registration");
    std::unique_ptr<ncclMrcBootstrapBroker> context(new ncclMrcBootstrapBroker);
    require(stride == 0 || (first >= 0 && stride > 0 && stride <= 65536 && nranks > 0 && nranks <= 65536 &&
            static_cast<int64_t>(first) + static_cast<int64_t>(stride) * (nranks - 1) < 65536 &&
            rank >= first && (rank - first) % stride == 0 && (rank - first) / stride < nranks),
            "invalid strided registration");
    context->strided = stride != 0;
    context->local.sin_family = context->peer.sin_family = AF_INET;
    context->local.sin_port = htons(port);
    require(inet_pton(AF_INET, ip, &context->local.sin_addr) == 1 &&
            inet_pton(AF_INET, previousIp, &context->peer.sin_addr) == 1, "invalid broker IPv4 endpoint");
    Descriptors owned;
    context->control = openControl(path, runId, owned);
    owned.values[0] = -1;
    char tokenString[17];
    snprintf(tokenString, sizeof(tokenString), "%016llx", static_cast<unsigned long long>(token));
    std::string request = std::string(context->strided ? "REG4 " : "REG3 ") + runId + " " + tokenString + " " + std::to_string(rank) +
                          " " + ip + " " + std::to_string(port);
    if (context->strided)
      request += " " + std::to_string(first) + " " + std::to_string(stride) + " " + std::to_string(nranks);
    systemCheck(send(context->control, request.data(), request.size(), MSG_NOSIGNAL) == static_cast<ssize_t>(request.size()),
                "send broker registration");
    receivePacket(context->control, context->strided ? "OK4" : "OK3", false, false, nullptr);
    int flags = fcntl(context->control, F_GETFL);
    systemCheck(flags >= 0 && fcntl(context->control, F_SETFL, flags | O_NONBLOCK) == 0, "set broker control nonblocking");
    context->deadline = std::chrono::steady_clock::now() + std::chrono::seconds(35);
    error->clear();
    *broker = context.release();
    return true;
  } catch (const std::runtime_error& failure) {
    if (error) *error = failure.what();
  } catch (const std::bad_alloc&) {
    if (error) *error = "out of memory registering communicator";
  }
  return false;
}

bool ncclMrcBootstrapPoll(ncclMrcBootstrapBroker* broker, int* fd, std::string* error) {
  if (fd) *fd = -1;
  try {
    require(broker && fd && error, "invalid connected-FD poll");
    require(broker->phase != ncclMrcBootstrapBroker::Delivered && broker->phase != ncclMrcBootstrapBroker::Failed,
            "connected-FD handoff already completed or failed");
    require(std::chrono::steady_clock::now() < broker->deadline, "connected-FD handoff timed out");
    error->clear();
    if (broker->phase == ncclMrcBootstrapBroker::ReceiveFd) {
      if (!receivePacket(broker->control, broker->strided ? "FD4" : "FD3", true, true, &broker->pending)) return true;
      require(option(broker->pending, SO_TYPE) == SOCK_STREAM && option(broker->pending, SO_PROTOCOL) == IPPROTO_TCP &&
              option(broker->pending, SO_ACCEPTCONN) == 0 && option(broker->pending, SO_REUSEADDR) == 0 &&
              option(broker->pending, SO_REUSEPORT) == 0, "descriptor is not an exclusive connected TCP socket");
      validateEndpoint(broker->pending, broker->local, false);
      validateEndpoint(broker->pending, broker->peer, true);
      int flags = fcntl(broker->pending, F_GETFL);
      systemCheck(flags >= 0 && fcntl(broker->pending, F_SETFL, flags | O_NONBLOCK) == 0, "set connected socket nonblocking");
      systemCheck(fcntl(broker->pending, F_SETFD, FD_CLOEXEC) == 0, "set connected socket close-on-exec");
      broker->phase = ncclMrcBootstrapBroker::SendAck;
    }
    if (broker->phase == ncclMrcBootstrapBroker::SendAck) {
      ssize_t sent = send(broker->control, broker->strided ? "ACK4" : "ACK3", 4, MSG_NOSIGNAL | MSG_DONTWAIT);
      if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) return true;
      systemCheck(sent == 4, "acknowledge connected descriptor");
      broker->phase = ncclMrcBootstrapBroker::ReceiveDone;
    }
    if (!receivePacket(broker->control, broker->strided ? "DONE4" : "DONE3", false, true, nullptr)) return true;
    *fd = broker->pending;
    broker->pending = -1;
    broker->phase = ncclMrcBootstrapBroker::Delivered;
    return true;
  } catch (const std::runtime_error& failure) {
    if (error) *error = failure.what();
  } catch (const std::bad_alloc&) {
    if (error) *error = "out of memory adopting connected descriptor";
  }
  if (broker) {
    broker->phase = ncclMrcBootstrapBroker::Failed;
    if (broker->pending >= 0) { close(broker->pending); broker->pending = -1; }
    if (broker->control >= 0) { close(broker->control); broker->control = -1; }
  }
  return false;
}

bool ncclMrcBootstrapUnregister(ncclMrcBootstrapBroker* broker, std::string* error) {
  std::unique_ptr<ncclMrcBootstrapBroker> owned(broker);
  bool success = true;
  if (error) error->clear();
  if (broker) {
    for (int* fd : {&broker->pending, &broker->control}) {
      if (*fd >= 0 && close(*fd) != 0) {
        if (error) *error = std::string("close broker descriptor: ") + strerror(errno);
        success = false;
      }
      *fd = -1;
    }
  }
  return success;
}
