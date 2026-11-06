#pragma once

#include "crypto/block/block.h"
#include "MemoCache.h"

namespace schema { struct AccountState; }

// Owned by one block's detector group, never shared across different snapshots.
// The implementation retains immutable shard roots and successful account
// lookups until the group finishes. PImpl avoids IndexData/InterfacesDetector's
// circular header dependency.
class AccountLookupContext {
 public:
  explicit AccountLookupContext(std::vector<td::Ref<vm::Cell>> roots);
  ~AccountLookupContext();

  bool matches(const std::vector<td::Ref<vm::Cell>>& roots) const;
  td::Result<schema::AccountState> lookup(const block::StdAddress& address);
  memo::Stats stats() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
