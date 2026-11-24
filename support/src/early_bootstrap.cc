#include "early_bootstrap.h"

#include <array>
#include <cstring>
#include <map>
#include <mutex>
#include <new>
#include <tuple>

namespace {
using Key = std::tuple<std::array<unsigned char, 128>, int, int>;
struct Entry {
  std::array<char, 32> name;
  void* send;
  void* recv;
  void* owner;
  bool consumed;
};
std::mutex mutex;
std::map<Key, Entry> entries;

Key key(const void* uniqueId, int rank, int nranks) {
  std::array<unsigned char, 128> bytes;
  std::memcpy(bytes.data(), uniqueId, bytes.size());
  return {bytes, rank, nranks};
}
}

extern "C" int nitEarlyBootstrapPublish(const void* uniqueId, int rank, int nranks,
                                         const char* netName, void* send, void* recv, void* owner) {
  if (!uniqueId || !netName || std::strlen(netName) >= 32 || !send || !recv || !owner ||
      nranks < 2 || rank < 0 || rank >= nranks) return 1;
  std::lock_guard<std::mutex> lock(mutex);
  for (const auto& entry : entries)
    if (entry.second.owner == owner) return 1;
  Entry entry = {};
  std::strcpy(entry.name.data(), netName);
  entry.send = send;
  entry.recv = recv;
  entry.owner = owner;
  try {
    return entries.emplace(key(uniqueId, rank, nranks), entry).second ? 0 : 1;
  } catch (const std::bad_alloc&) {
    return 2;
  }
}

extern "C" int nitEarlyBootstrapTake(const void* uniqueId, int rank, int nranks,
                                      const char* netName, void** send, void** recv, int* found) {
  if (!uniqueId || !netName || !send || !recv || !found) return 1;
  *found = 0;
  std::lock_guard<std::mutex> lock(mutex);
  auto entry = entries.find(key(uniqueId, rank, nranks));
  if (entry == entries.end()) return 0;
  if (entry->second.consumed || std::strcmp(entry->second.name.data(), netName)) return 1;
  *send = entry->second.send;
  *recv = entry->second.recv;
  entry->second.consumed = true;
  *found = 1;
  return 0;
}

extern "C" int nitEarlyBootstrapWithdraw(void* owner, int* consumed) {
  if (!owner || !consumed) return 1;
  std::lock_guard<std::mutex> lock(mutex);
  for (auto entry = entries.begin(); entry != entries.end(); ++entry) {
    if (entry->second.owner != owner) continue;
    *consumed = entry->second.consumed;
    entries.erase(entry);
    return 0;
  }
  return 1;
}
