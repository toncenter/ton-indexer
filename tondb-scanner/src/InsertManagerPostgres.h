#pragma once
#include <queue>
#include "InsertManager.h"


class InsertManagerPostgres: public InsertManagerInterface {
private:
  std::queue<std::vector<BlockDataState>> insert_queue_;
  std::queue<td::Promise<td::Unit>> promise_queue_;
public:
  InsertManagerPostgres() {}

  void start_up() override;
  void alarm() override;

  void insert(std::vector<BlockDataState> block_ds, td::Promise<td::Unit> promise) override;
};
