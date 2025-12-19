#include "NftSaleV4.h"
#include "convert-utils.h"
#include "execute-smc.h"


GetGemsNftFixPriceSaleV4::GetGemsNftFixPriceSaleV4(block::StdAddress address,
                       td::Ref<vm::Cell> code_cell,
                       td::Ref<vm::Cell> data_cell,
                       AllShardStates shard_states,
                       std::shared_ptr<block::ConfigInfo> config,
                       td::Promise<Result> promise) :
  address_(std::move(address)), code_cell_(std::move(code_cell)), data_cell_(std::move(data_cell)),
  shard_states_(std::move(shard_states)), config_(std::move(config)), promise_(std::move(promise)) {}

void GetGemsNftFixPriceSaleV4::start_up() {
  if (code_cell_.is_null() || data_cell_.is_null()) {
    promise_.set_error(td::Status::Error("Code or data null"));
    stop();
    return;
  }

  auto stack_r = execute_smc_method_nullable<13>(address_, code_cell_, data_cell_, config_, "get_fix_price_data_v4", {},
            {vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice,
            vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_int,
            vm::StackEntry::Type::t_slice, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_slice,
            vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int, vm::StackEntry::Type::t_int,
            nullable(vm::StackEntry::Type::t_cell)});
  if (stack_r.is_error()) {
    auto err = stack_r.move_as_error();

    promise_.set_error(std::move(err));
    stop();
    return;
  }
  auto stack = stack_r.move_as_ok();
  Result data;
  data.address = address_;
  data.is_complete = stack[0].as_int()->to_long() != 0;
  data.created_at = stack[1].as_int()->to_long();

  auto marketplace_address = convert::to_std_address(stack[2].as_slice());
  if (marketplace_address.is_error()) {
    promise_.set_error(marketplace_address.move_as_error_prefix("marketplace address parsing failed: "));
    stop();
    return;
  }
  data.marketplace_address = marketplace_address.move_as_ok();

  auto nft_address = convert::to_std_address(stack[3].as_slice());
  if (nft_address.is_error()) {
    promise_.set_error(nft_address.move_as_error_prefix("nft address parsing failed: "));
    stop();
    return;
  }
  data.nft_address = nft_address.move_as_ok();

  auto nft_owner_address_cs = stack[4].as_slice();
  if (nft_owner_address_cs->size() == 2 && nft_owner_address_cs->prefetch_ulong(2) == 0) {
    data.nft_owner_address = std::nullopt;
  } else {
    auto nft_owner_address = convert::to_std_address(stack[4].as_slice());
    if (nft_owner_address.is_error()) {
      promise_.set_error(nft_owner_address.move_as_error_prefix("nft owner address parsing failed: "));
      stop();
      return;
    }
    data.nft_owner_address = nft_owner_address.move_as_ok();
  }

  data.full_price = stack[5].as_int();

  auto marketplace_fee_address = convert::to_std_address(stack[6].as_slice());
  if (marketplace_fee_address.is_error()) {
    promise_.set_error(marketplace_fee_address.move_as_error_prefix("marketplace fee address parsing failed: "));
    stop();
    return;
  }
  data.marketplace_fee_address = marketplace_fee_address.move_as_ok();
  data.marketplace_fee = stack[7].as_int();

  auto royalty_address = convert::to_std_address(stack[8].as_slice());
  if (royalty_address.is_error()) {
    promise_.set_error(royalty_address.move_as_error_prefix("royalty address parsing failed: "));
    stop();
    return;
  }
  data.royalty_address = royalty_address.move_as_ok();
  data.royalty_amount = stack[9].as_int();

  data.sold_at = stack[10].as_int()->to_long();
  data.sold_query_id = stack[11].as_int()->to_long();

  data.jetton_price_dict = {};
  if (stack[12].is_cell()) {
    try {
      auto jetton_dict_cell = stack[12].as_cell();
      vm::Dictionary jetton_dict{jetton_dict_cell, 256};
      auto it = jetton_dict.begin();
      while (!it.eof()) {
        auto key_data = it.cur_pos();
        ton::WorkchainId workchain_id = 0;
        ton::StdSmcAddress addr_bytes{key_data};
        auto addr = block::StdAddress(workchain_id, addr_bytes);

        auto value_cs = it.cur_value();
        if (value_cs.is_null()) {
          promise_.set_error(td::Status::Error("Invalid jetton dict value entry"));
          stop();
          return;
        }
        auto bd = vm::CellBuilder().append_cellslice(value_cs.write()).finalize();
        auto price = block::tlb::t_VarUInteger_16.as_integer_skip(value_cs.write());
        if (price.not_null()) {
          std::ostringstream addr_stream;
          addr_stream << addr.workchain << ":" << addr.addr.to_hex();
          data.jetton_price_dict[addr_stream.str()] = price->to_dec_string();
        } else {
          if (value_cs.is_null()) {
            promise_.set_error(td::Status::Error("Invalid jetton dict value entry"));
            stop();
            return;
          }
        }
        ++it;
      }
    }
    catch (vm::VmError& e) {
      LOG(WARNING) << "Failed to parse jetton_price_dict: " << e.get_msg();
      promise_.set_error(td::Status::Error("Invalid jetton dict"));
      stop();
      return;
    }
  }

  promise_.set_value(std::move(data));
  stop();
}
