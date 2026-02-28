#include "ShardBatchScanner.h"
#include "StateParser.h"
#include "td/utils/filesystem.h"
#include "td/utils/port/path.h"

ShardStateScanner::ShardStateScanner(td::Ref<vm::Cell> shard_state, schema::MasterchainBlockDataState mc_block_ds,
                                     Options options)
  : shard_state_(shard_state), mc_block_ds_(mc_block_ds), options_(options) {
}

void ShardStateScanner::start_up() {
  shard_state_data_ = std::make_shared<ShardStateData>();
  for (const auto &shard_ds: mc_block_ds_.shard_blocks_) {
    shard_state_data_->shard_states_.push_back(shard_ds.block_state);
  }
  auto config_r = block::ConfigInfo::extract_config(mc_block_ds_.shard_blocks_[0].block_state,
                                                    mc_block_ds_.shard_blocks_[0].handle->id(),
                                                    block::ConfigInfo::needCapabilities |
                                                    block::ConfigInfo::needLibraries);
  if (config_r.is_error()) {
    LOG(ERROR) << "Failed to extract config: " << config_r.move_as_error();
    std::_Exit(2);
    return;
  }
  shard_state_data_->config_ = config_r.move_as_ok();

  if (!tlb::unpack_cell(shard_state_, shard_state_data_->sstate_)) {
    LOG(ERROR) << "Failed to unpack ShardStateUnsplit";
    stop();
    return;
  }

  shard_ = ton::ShardIdFull(block::ShardId(shard_state_data_->sstate_.shard_id.write()));

  auto checkpoint = load_from_checkpoint();
  accounts_dict_ = std::make_unique<vm::AugmentedDictionary>(
    vm::load_cell_slice_ref(shard_state_data_->sstate_.accounts), 256, block::tlb::aug_ShardAccounts
  );
  iterator_ = accounts_dict_->init_iterator(false);
  if (auto flag = iterator_.lookup(checkpoint); flag) {
    LOG(INFO) << "Starting from: " << iterator_.cur_pos().to_hex(256) << " == " << checkpoint.to_hex();
    schedule_next();
  }
}

void ShardStateScanner::batch_parsed(td::Bits256 last_addr, std::vector<InsertData> results) {
  if (results.empty()) {
    batch_inserted(last_addr);
  }
  auto P = td::PromiseCreator::lambda(
    [SelfId = actor_id(this), cnt = results.size(), last_addr](td::Result<td::Unit> R) {
      if (R.is_error()) {
        LOG(ERROR) << "Failed to insert data: " << R.move_as_error();
        return;
      }
      td::actor::send_closure(SelfId, &ShardStateScanner::batch_inserted, last_addr);
    });

  // Insert the results into the database
  td::actor::send_closure(options_.insert_manager_, &PostgreSQLInsertManager::insert_data, std::move(results),
                          std::move(P));
}

void ShardStateScanner::batch_inserted(td::Bits256 last_addr) {
  update_checkpoint(last_addr);
  schedule_next();
}

void ShardStateScanner::schedule_next() {
  if (iterator_.eof()) {
    finished_ = true;
    return;
  }
  std::vector<std::pair<td::Bits256, block::gen::ShardAccount::Record> > batch;
  constexpr size_t max_batch_size = 8192;
  for (; batch.size() < max_batch_size && !iterator_.eof(); iterator_.next()) {
    auto cur_addr = td::Bits256{iterator_.cur_pos()};
    auto shard_account_csr = iterator_.cur_value();
    block::gen::ShardAccount::Record acc_info;
    if (!tlb::csr_unpack(shard_account_csr, acc_info)) {
      LOG(ERROR) << "Failed to unpack ShardAccount for " << cur_addr.to_hex();
      continue;
    }
    batch.emplace_back(cur_addr, std::move(acc_info));
  }
  td::Bits256 last_addr{!iterator_.eof() ? iterator_.cur_pos() : td::Bits256::zero()};
  LOG(INFO) << "Batch of " << batch.size() << " read, it ends with " << last_addr.to_hex();

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this), last_addr](td::Result<std::vector<InsertData>> R) {
      if (R.is_error()) {
          LOG(ERROR) << "Failed to process batch: " << R.move_as_error();
          return;
      }
      td::actor::send_closure(SelfId, &ShardStateScanner::batch_parsed, last_addr, R.move_as_ok());
  });
  td::actor::create_actor<StateBatchParser>("parser", std::move(batch), shard_state_data_, options_, std::move(P)).release();
}

std::string ShardStateScanner::get_checkpoint_file_path() {
  if (options_.working_dir_.empty()) {
    LOG(ERROR) << "No working directory provided, cannot save checkpoint";
    return "";
  }
  auto path = options_.working_dir_ + "/" + std::to_string(options_.seqno_) + "_" + shard_.to_str() + ".checkpoint";
  return path;
}

td::Bits256 ShardStateScanner::load_from_checkpoint() {
  auto path = get_checkpoint_file_path();
  if (!path.empty() && std::filesystem::exists(path)) {
    auto buffer_r = td::read_file(path);
    if (buffer_r.is_error()) {
      LOG(FATAL) << "Failed to read checkpoint from " << path << ": " << buffer_r.move_as_error();
    }
    auto buffer = buffer_r.move_as_ok();
    if (buffer.size() != 64) {
      LOG(FATAL) << "Invalid checkpoint size: " << buffer.size() << ", expected 64 bytes";
    }
    td::Bits256 checkpoint;
    if (!checkpoint.from_hex(td::Slice(buffer))) {
      LOG(FATAL) << "Failed to decode checkpoint from hex: " << td::Slice(buffer);
    }
    LOG(INFO) << "Read checkpoint for seqno "
              << options_.seqno_
              << " and shard"
              << shard_.to_str() << ": "
              << checkpoint.to_hex();
    return checkpoint;
  } else {
    LOG(INFO) << "No checkpoint found for shard " << shard_.to_str() << ", starting from zero address";
  }
  return td::Bits256::zero();
}

void ShardStateScanner::update_checkpoint(td::Bits256 up_to_addr) {
  auto checkpoint_file_path = get_checkpoint_file_path();
  if (!checkpoint_file_path.empty()) {
    auto S = td::write_file(checkpoint_file_path, up_to_addr.to_hex());
    if (S.is_error()) {
      LOG(ERROR) << "Failed to write checkpoint to " << checkpoint_file_path;
    }
  }
}
