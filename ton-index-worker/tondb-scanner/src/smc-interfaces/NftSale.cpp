#include "NftSale.h"
#include "convert-utils.h"
#include "execute-smc.h"


GetGemsNftFixPriceSale::GetGemsNftFixPriceSale(block::StdAddress address, 
                       td::Ref<vm::Cell> code_cell,
                       td::Ref<vm::Cell> data_cell, 
                       AllShardStates shard_states,
                       std::shared_ptr<block::ConfigInfo> config,
                       td::Promise<Result> promise) :
  address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)),
  shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)) {}

void GetGemsNftFixPriceSale::start_up() {
  if (code_cell_.is_null() || data_cell_.is_null()) {
    promise_.set_error(td::Status::Error("Code or data null"));
    stop();
    return;
  }

  auto stack_r = execute_smc_method<11>(address_, code_cell_, data_cell_, config_, "get_sale_data", {},
            {vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, 
            vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, 
            vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_int});


  if (stack_r.is_error()) {
    promise_.set_error(stack_r.move_as_error());
    stop();
    return;
  }
  auto stack = stack_r.move_as_ok();

  Result data;
  data.address = address_;
  if (stack[0].as_int()->to_long() != 0x46495850) {
    promise_.set_error(td::Status::Error("get_sale_data: invalid magic"));
    stop();
    return;
  }
  data.is_complete = stack[1].as_int()->to_long() != 0;
  data.created_at = stack[2].as_int()->to_long();
  auto marketplace_address = convert::to_std_address(stack[3].as_slice());
  if (marketplace_address.is_error()) {
    promise_.set_error(marketplace_address.move_as_error_prefix("marketplace address parsing failed: "));
    stop();
    return;
  }
  data.marketplace_address = marketplace_address.move_as_ok();
  auto nft_address = convert::to_std_address(stack[4].as_slice());
  if (nft_address.is_error()) {
    promise_.set_error(nft_address.move_as_error_prefix("nft address parsing failed: "));
    stop();
    return;
  }
  data.nft_address = nft_address.move_as_ok();
  
  auto nft_owner_address_cs = stack[5].as_slice();
  if (nft_owner_address_cs->size() == 2 && nft_owner_address_cs->prefetch_ulong(2) == 0) {
    data.nft_owner_address = std::nullopt;
  } else {
    auto nft_owner_address = convert::to_std_address(stack[5].as_slice());
    if (nft_owner_address.is_error()) {
      promise_.set_error(nft_owner_address.move_as_error_prefix("nft owner address parsing failed: "));
      stop();
      return;
    }
    data.nft_owner_address = nft_owner_address.move_as_ok();
  }

  data.full_price = stack[6].as_int();
  auto marketplace_fee_address = convert::to_std_address(stack[7].as_slice());
  if (marketplace_fee_address.is_error()) {
    promise_.set_error(marketplace_fee_address.move_as_error_prefix("marketplace fee address parsing failed: "));
    stop();
    return;
  }
  data.marketplace_fee_address = marketplace_fee_address.move_as_ok();
  data.marketplace_fee = stack[8].as_int();
  auto royalty_address = convert::to_std_address(stack[9].as_slice());
  if (royalty_address.is_error()) {
    promise_.set_error(royalty_address.move_as_error_prefix("royalty address parsing failed: "));
    stop();
    return;
  }
  data.royalty_address = royalty_address.move_as_ok();
  data.royalty_amount = stack[10].as_int();

  promise_.set_value(std::move(data));
  stop();
}

GetGemsNftAuction::GetGemsNftAuction(block::StdAddress address, 
                       td::Ref<vm::Cell> code_cell,
                       td::Ref<vm::Cell> data_cell, 
                       AllShardStates shard_states,
                       std::shared_ptr<block::ConfigInfo> config,
                       td::Promise<Result> promise) :
  address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)),
  shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)) {}

void GetGemsNftAuction::start_up() {
  if (code_cell_.is_null() || data_cell_.is_null()) {
    promise_.set_error(td::Status::Error("Code or data null"));
    stop();
    return;
  }

  auto stack_r = execute_smc_method<20>(address_, code_cell_, data_cell_, config_, "get_sale_data", {},
            {vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice,
            vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice,
            vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int,
            vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, 
            vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int});

  if (stack_r.is_error()) {
    promise_.set_error(stack_r.move_as_error());
    stop();
    return;
  }
  auto stack = stack_r.move_as_ok();

  Result data;
  data.address = address_;
  if (stack[0].as_int()->to_long() != 0x415543) {
    promise_.set_error(td::Status::Error("get_sale_data: invalid magic"));
    stop();
    return;
  }
  data.end = stack[1].as_int()->to_long() != 0;
  data.end_time = stack[2].as_int()->to_long();
  auto marketplace_address = convert::to_std_address(stack[3].as_slice());
  if (marketplace_address.is_error()) {
    promise_.set_error(marketplace_address.move_as_error_prefix("marketplace address parsing failed: "));
    stop();
    return;
  }
  data.mp_addr = marketplace_address.move_as_ok();
  auto nft_address = convert::to_std_address(stack[4].as_slice());
  if (nft_address.is_error()) {
    promise_.set_error(nft_address.move_as_error_prefix("nft address parsing failed: "));
    stop();
    return;
  }
  data.nft_addr = nft_address.move_as_ok();
  auto nft_owner_address_cs = stack[5].as_slice();
  if (nft_owner_address_cs->size() == 2 && nft_owner_address_cs->prefetch_ulong(2) == 0) {
    data.nft_owner = std::nullopt;
  } else {
    auto nft_owner_address = convert::to_std_address(stack[5].as_slice());
    if (nft_owner_address.is_error()) {
      promise_.set_error(nft_owner_address.move_as_error_prefix("nft owner address parsing failed: "));
      stop();
      return;
    }
    data.nft_owner = nft_owner_address.move_as_ok();
  }
  data.last_bid = stack[6].as_int();
  auto last_member_address_cs = stack[7].as_slice();
  if (last_member_address_cs->size() == 2 && last_member_address_cs->prefetch_ulong(2) == 0) {
    data.last_member = std::nullopt;
  } else {
    auto last_member_address = convert::to_std_address(stack[7].as_slice());
    if (last_member_address.is_error()) {
      promise_.set_error(last_member_address.move_as_error_prefix("last member address parsing failed: "));
      stop();
      return;
    }
    data.last_member = last_member_address.move_as_ok();
  }
  data.min_step = stack[8].as_int()->to_long();
  auto mp_fee_address = convert::to_std_address(stack[9].as_slice());
  if (mp_fee_address.is_error()) {
    promise_.set_error(mp_fee_address.move_as_error_prefix("marketplace fee address parsing failed: "));
    stop();
    return;
  }
  data.mp_fee_addr = mp_fee_address.move_as_ok();
  data.mp_fee_factor = stack[10].as_int()->to_long();
  data.mp_fee_base = stack[11].as_int()->to_long();
  auto royalty_fee_address = convert::to_std_address(stack[12].as_slice());
  if (royalty_fee_address.is_error()) {
    promise_.set_error(royalty_fee_address.move_as_error_prefix("royalty fee address parsing failed: "));
    stop();
    return;
  }
  data.royalty_fee_addr = royalty_fee_address.move_as_ok();
  data.royalty_fee_factor = stack[13].as_int()->to_long();
  data.royalty_fee_base = stack[14].as_int()->to_long();
  data.max_bid = stack[15].as_int();
  data.min_bid = stack[16].as_int();
  data.created_at = stack[17].as_int()->to_long();
  data.last_bid_at = stack[18].as_int()->to_long();
  data.is_canceled = stack[19].as_int()->to_long() != 0;

  promise_.set_value(std::move(data));
  stop();
}