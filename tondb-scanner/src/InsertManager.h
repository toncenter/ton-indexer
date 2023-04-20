#pragma once
#include "td/actor/actor.h"
#include "IndexData.h"

class InsertManagerInterface: public td::actor::Actor {
public:
  virtual void insert(ParsedBlock block_ds, td::Promise<td::Unit> promise) = 0;
};
