#pragma once

#include "crypto/block/block.h"
#include "MemoCache.h"

namespace schema { struct AccountState; }
namespace vm { class CellDbReader; }
class ParentAccountCache;
struct ParentAccountView;

// Owned by one block's detector group, never shared across different snapshots.
// The implementation retains immutable shard roots and successful account
// lookups until the group finishes. PImpl avoids IndexData/InterfacesDetector's
// circular header dependency.
class AccountLookupContext {
 public:
  explicit AccountLookupContext(std::vector<td::Ref<vm::Cell>> roots,
      std::shared_ptr<ParentAccountCache> parents = {}, std::shared_ptr<const ParentAccountView> view = {},
      std::shared_ptr<vm::CellDbReader> reader = {});
  ~AccountLookupContext();

  bool matches(const std::vector<td::Ref<vm::Cell>>& roots) const;
  td::Result<schema::AccountState> lookup(const block::StdAddress& address);
  memo::Stats stats() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};
