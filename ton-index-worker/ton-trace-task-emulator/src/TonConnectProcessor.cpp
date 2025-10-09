#include "TonConnectProcessor.h"
#include <td/utils/base64.h>
#include <crypto/vm/boc.h>
#include <crypto/vm/cellslice.h>
#include "smc-interfaces/FetchAccountFromShard.h"

void TonConnectProcessor::start_up() {
  LOG(INFO) << "Processing TonConnect task " << tonconnect_task_.id << " for address " << tonconnect_task_.from;
  
  std::vector<td::Ref<vm::Cell>> shard_states;
  for (const auto& shard_state : mc_data_state_.shard_blocks_) {
    shard_states.push_back(shard_state.block_state);
  }

  auto P = td::PromiseCreator::lambda([SelfId = actor_id(this)](td::Result<schema::AccountState> R) {
    if (R.is_error()) {
      td::actor::send_closure(SelfId, &TonConnectProcessor::error, R.move_as_error_prefix("Failed to fetch account state: "));
      return;
    }
    auto account_state = R.move_as_ok();
    td::actor::send_closure(SelfId, &TonConnectProcessor::got_account_state, std::move(account_state));
  });
  auto addr = block::StdAddress::parse(tonconnect_task_.from);
  if (addr.is_error()) {
    error(td::Status::Error("Invalid address format: " + tonconnect_task_.from));
    return;
  }

  td::actor::create_actor<FetchAccountFromShardV2>("FetchAccountFromShardV2",
    std::move(shard_states), addr.move_as_ok(), std::move(P)).release();
}

void TonConnectProcessor::error(td::Status error) {
  LOG(ERROR) << "Error processing TonConnect task " << tonconnect_task_.id << ": " << error;
  
  promise_.set_error(std::move(error));
  stop();
}

void TonConnectProcessor::got_account_state(schema::AccountState account_state) {
  if (!account_state.code_hash.has_value() || !account_state.data_hash.has_value()) {
    error(td::Status::Error("Account state no code or no data"));
    return;
  }
  // Step 1: Detect wallet type
  auto wallet_type = detect_wallet_type(account_state.code_hash.value());
  if (wallet_type == WALLET_UNKNOWN) {
    error(td::Status::Error("Account is not a known wallet"));
    return;
  }

  auto external_message_body = td::Result<td::Ref<vm::Cell>>();
  switch (wallet_type) {
    case WALLET_V3R1:
    case WALLET_V3R2:
      external_message_body = compose_message_body_v3(account_state);
      break;
    case WALLET_V4R1:
    case WALLET_V4R2:
      external_message_body = compose_message_body_v4(account_state);
      break;
    case WALLET_V5R1:
      external_message_body = compose_message_body_v5(account_state);
      break;
    default:
      error(td::Status::Error("Unsupported wallet type: " + std::to_string(wallet_type)));
      return;
  }

  if (external_message_body.is_error()) {
    error(external_message_body.move_as_error_prefix("Failed to compose external message: "));
    return;
  }

  auto ext_msg = compose_external_message(external_message_body.move_as_ok());
  if (ext_msg.is_error()) {
    error(ext_msg.move_as_error_prefix("Failed to compose external message: "));
    return;
  }
  
  auto boc_result = serialize_cell_to_boc(ext_msg.move_as_ok());
  if (boc_result.is_error()) {
    error(boc_result.move_as_error_prefix("Failed to serialize external message to BOC: "));
    return;
  }
  
  TraceTask trace_task;
  trace_task.id = tonconnect_task_.id;
  trace_task.boc = boc_result.move_as_ok();
  trace_task.ignore_chksig = true;
  trace_task.detect_interfaces = tonconnect_task_.detect_interfaces;
  trace_task.include_code_data = tonconnect_task_.include_code_data;
  trace_task.mc_block_seqno = tonconnect_task_.mc_block_seqno;

  LOG(INFO) << "Successfully converted TonConnect task " << tonconnect_task_.id << " to regular TraceTask";

  promise_.set_value(std::move(trace_task));
  stop();
}

WalletType TonConnectProcessor::detect_wallet_type(const td::Bits256& code_hash) {
  auto code_hash_b64 = td::base64_encode(code_hash.as_slice());

  if (code_hash_b64 == "oM/CxIruFqJx8s/AtzgtgXVs7LEBfQd/qqs7tgL2how=") {
    return WALLET_V1R1;
  }
  if (code_hash_b64 == "1JAvzJ+tdGmPqONTIgpo2g3PcuMryy657gQhfBfTBiw=") {
    return WALLET_V1R2;
  }
  if (code_hash_b64 == "WHzHie/xyE9G7DeX5F/ICaFP9a4k8eDHpqmcydyQYf8=") {
    return WALLET_V1R3;
  }
  if (code_hash_b64 == "XJpeaMEI4YchoHxC+ZVr+zmtd+xtYktgxXbsiO7mUyk=") {
    return WALLET_V2R1;
  }
  if (code_hash_b64 == "/pUw0yQ4Uwg+8u8LTCkIwKv2+hwx6iQ6rKpb+MfXU/E=") {
    return WALLET_V2R2;
  }
  if (code_hash_b64 == "thBBpYp5gLlG6PueGY48kE0keZ/6NldOpCUcQaVm9YE=") {
    return WALLET_V3R1;
  }
  if (code_hash_b64 == "hNr6RJ+Ypph3ibojI1gHK8D3bcRSQAKl0JGLmnXS1Zk=") {
    return WALLET_V3R2;
  }
  if (code_hash_b64 == "ZN1UgFUixb6KnbWc6gEFzPDQh4bKeb64y3nogKjXMi0=") {
    return WALLET_V4R1;
  }
  if (code_hash_b64 == "/rX/aCDi/w2Ug+fg1iyBfYRniftK5YDIeIZtlZ2r1cA=") {
    return WALLET_V4R2;
  }
  if (code_hash_b64 == "IINLe3KxEhR+Gy+0V7hOdNGjDwT3N9T2KmaOlVLSty8=") {
    return WALLET_V5R1;
  }
  
  LOG(WARNING) << "Unknown wallet type for code hash: " << code_hash_b64;
  return WALLET_UNKNOWN;
}

td::Result<td::Ref<vm::Cell>> TonConnectProcessor::message_to_cell(const TonConnectMessage& out_msg) {
  TRY_RESULT(src_addr, block::StdAddress::parse(tonconnect_task_.from));
  
  td::Ref<vm::Cell> int_msg;
  block::gen::Message::Record message;
  block::gen::CommonMsgInfo::Record_int_msg_info msg_info;
  msg_info.ihr_disabled = true;
  msg_info.bounced = false;
  {
    block::gen::MsgAddressInt::Record_addr_std src;
    src.anycast = vm::CellBuilder().store_zeroes(1).as_cellslice_ref();
    src.workchain_id = src_addr.workchain;
    src.address = src_addr.addr;
    if (!tlb::csr_pack(msg_info.src, src)) {
      return td::Status::Error("Failed to pack MsgAddressInt for source");
    }
  }
  {
    TRY_RESULT(dest_addr, block::StdAddress::parse(out_msg.address));
    block::gen::MsgAddressInt::Record_addr_std dest;
    dest.anycast = vm::CellBuilder().store_zeroes(1).as_cellslice_ref();
    dest.workchain_id = dest_addr.workchain;
    dest.address = dest_addr.addr;
    if (!tlb::csr_pack(msg_info.dest, dest)) {
      return td::Status::Error("Failed to pack MsgAddressInt for destination");
    }
    msg_info.bounce = dest_addr.bounceable;
  }
  {
    auto amount = std::stoull(out_msg.amount);
    block::CurrencyCollection cc{amount};
    if (!cc.pack_to(msg_info.value)) {
      return td::Status::Error("Failed to pack CurrencyCollection for value");
    }
  }
  {
    vm::CellBuilder cb;
    block::tlb::t_Grams.store_integer_value(cb, td::BigInt256(0));
    msg_info.fwd_fee = cb.as_cellslice_ref();
  }
  {
    vm::CellBuilder cb;
    block::tlb::t_VarUInteger_16.store_integer_value(cb, td::BigInt256(0));
    msg_info.extra_flags = cb.as_cellslice_ref();
  }
  
  msg_info.created_lt = 0;
  msg_info.created_at = static_cast<uint32_t>(td::Timestamp::now().at_unix());
  if (!tlb::csr_pack(message.info, msg_info)) {
    return td::Status::Error("Failed to pack CommonMsgInfo");
  }
  if (out_msg.state_init.has_value()) {
    TRY_RESULT_PREFIX(state_init_boc, td::base64_decode(td::Slice(out_msg.state_init.value())),
                "Failed to decode state init BOC: ");
    TRY_RESULT_PREFIX(state_init_cell, vm::std_boc_deserialize(state_init_boc),
                "Failed to deserialize state init cell: ");
    message.init = vm::CellBuilder()
                        .store_ones(1)
                        .store_ones(1)
                        .store_ref(state_init_cell)
                        .as_cellslice_ref();
  } else {
    message.init = vm::CellBuilder()
                        .store_zeroes(1)
                        .as_cellslice_ref();
  }
  if (out_msg.payload.has_value()) {
    TRY_RESULT_PREFIX(payload_boc, td::base64_decode(td::Slice(out_msg.payload.value())),
                "Failed to decode payload BOC: ");
    TRY_RESULT_PREFIX(payload_cell, vm::std_boc_deserialize(payload_boc),
                "Failed to deserialize payload cell: ");
    message.body = vm::CellBuilder().store_ones(1).store_ref(payload_cell).as_cellslice_ref();
  } else {
    message.body = vm::CellBuilder().store_zeroes(1).as_cellslice_ref();
  }

  tlb::type_pack_cell(int_msg, block::gen::t_Message_Any, message);
  return int_msg;
}

td::Result<td::Ref<vm::Cell>> TonConnectProcessor::compose_message_body_v3(const schema::AccountState& account_state) {
  auto cs = vm::load_cell_slice(account_state.data);
  auto seqno = static_cast<td::uint32>(cs.fetch_ulong(32));
  auto wallet_id = static_cast<td::uint32>(cs.fetch_ulong(32));

  TRY_RESULT(src_addr, block::StdAddress::parse(tonconnect_task_.from));

  vm::CellBuilder cb;
  uint32_t valid_until = td::Timestamp::now().at_unix() + 300; // 5 minutes
  auto dummy_signature = std::string(64, '\0'); // Placeholder for signature
  cb.store_bytes(td::Slice(dummy_signature))
    .store_long(wallet_id, 32)
    .store_long(valid_until, 32)
    .store_long(seqno, 32);
  for (const auto &out_msg : tonconnect_task_.messages) {
    TRY_RESULT(int_msg, message_to_cell(out_msg));
    const int32_t send_mode = 3;
    cb.store_long(send_mode, 8).store_ref(int_msg);
  }
  
  return cb.finalize();
}

td::Result<td::Ref<vm::Cell>> TonConnectProcessor::compose_external_message(td::Ref<vm::Cell> body) {
  block::gen::Message::Record message;
  /*info*/ {
    block::gen::CommonMsgInfo::Record_ext_in_msg_info info;
    /* src */
    tlb::csr_pack(info.src, block::gen::MsgAddressExt::Record_addr_none{});
    /* dest */ {
      auto addr = block::StdAddress::parse(tonconnect_task_.from).move_as_ok();
      block::gen::MsgAddressInt::Record_addr_std dest;
      dest.anycast = vm::CellBuilder().store_zeroes(1).as_cellslice_ref();
      dest.workchain_id = addr.workchain;
      dest.address = addr.addr;

      tlb::csr_pack(info.dest, dest);
    }
    /* import_fee */ {
      vm::CellBuilder cb;
      block::tlb::t_Grams.store_integer_value(cb, td::BigInt256(0));
      info.import_fee = cb.as_cellslice_ref();
    }

    tlb::csr_pack(message.info, info);
  }
  /* init */ {
    message.init = vm::CellBuilder().store_zeroes(1).as_cellslice_ref();
  }
  /* body */ {
    message.body = vm::CellBuilder().store_zeroes(1).append_cellslice(vm::load_cell_slice_ref(body)).as_cellslice_ref();
  }

  td::Ref<vm::Cell> res;
  tlb::type_pack_cell(res, block::gen::t_Message_Any, message);
  return res;
}

template<class T>
inline long long to_seconds(T x) {
    if      (x >= 1'000'000'000'000'000'000LL) return x / 1'000'000'000;  // ns→s
    else if (x >= 1'000'000'000'000'000LL)     return x / 1'000'000;      // µs→s
    else if (x >= 1'000'000'000'000LL)         return x / 1'000;          // ms→s
    else                                       return x;                  // s
}

td::Result<td::Ref<vm::Cell>> TonConnectProcessor::compose_message_body_v4(const schema::AccountState& account_state) {
  auto cs = vm::load_cell_slice(account_state.data);
  auto seqno = static_cast<td::uint32>(cs.fetch_ulong(32));
  auto wallet_id = static_cast<td::uint32>(cs.fetch_ulong(32));

  vm::CellBuilder cb;
  auto valid_until = tonconnect_task_.valid_until.value_or(td::Timestamp::now().at_unix() + 300);
  valid_until = to_seconds(valid_until);
  auto dummy_signature = std::string(64, '\0'); // Placeholder for signature
  cb.store_bytes(td::Slice(dummy_signature))
    .store_long(wallet_id, 32)
    .store_long(valid_until, 32)
    .store_long(seqno, 32)
    .store_long(0, 8); // The only difference with wallet-v3
  for (const auto &out_msg : tonconnect_task_.messages) {
    TRY_RESULT(int_msg, message_to_cell(out_msg));
    const int32_t send_mode = 3;
    cb.store_long(send_mode, 8).store_ref(int_msg);
  }
  
  return cb.finalize();
}

td::Result<td::Ref<vm::Cell>> TonConnectProcessor::compose_message_body_v5(const schema::AccountState& account_state) {
  // Read v5 persistent state: is_signature_allowed(1), seqno(32), wallet_id(32), ...
  auto cs = vm::load_cell_slice(account_state.data);
  cs.advance(1); // skip is_signature_allowed
  auto seqno     = static_cast<td::uint32>(cs.fetch_ulong(32));
  auto wallet_id = static_cast<td::uint32>(cs.fetch_ulong(32));

  const uint32_t EXTERNAL_SIGNED = 0x7369676E; // 'sign'
  const uint32_t ACTION_SEND_MSG = 0x0ec3c86d; // action_send_msg
  const uint8_t  SEND_MODE       = 3;
  auto valid_until = tonconnect_task_.valid_until.value_or(td::Timestamp::now().at_unix() + 300);
  valid_until = to_seconds(valid_until);

  if (tonconnect_task_.messages.empty())
    return td::Status::Error("No messages to send");
  if (tonconnect_task_.messages.size() > 255)
    return td::Status::Error("Too many messages for v5 (>255)");

  // Build OutList (root = last node). Each node layout must be:
  // bits: [action_send_msg prefix (32)] [mode (8)]
  // refs: [0]=prev OutList, [1]=out_msg
  vm::CellBuilder empty_cb;
  auto outlist_prev = empty_cb.finalize(); // out_list_empty

  for (const auto& out_msg : tonconnect_task_.messages) {
    TRY_RESULT(int_msg, message_to_cell(out_msg));
    const int32_t send_mode = 3;

    vm::CellBuilder node;
    node.store_long(ACTION_SEND_MSG, 32); // 0x0ec3c86d
    node.store_long(SEND_MODE, 8);        // mode:(##8)
    node.store_ref(outlist_prev);         // prev:^(OutList n) — FIRST ref (verify_c5_actions walks this)
    node.store_ref(int_msg);         // out_msg:^(MessageRelaxed Any)
    outlist_prev = node.finalize();       // new root
  }
  auto outlist_root = outlist_prev;

  // Compose ExternalMsg body (v5)
  vm::CellBuilder cb;
  cb.store_long(EXTERNAL_SIGNED, 32);  // external_signed
  cb.store_long(wallet_id, 32);
  cb.store_long(valid_until, 32);
  cb.store_long(seqno, 32);

  // inner: out_actions:(Maybe OutList), has_other_actions:(##1)=0
  cb.store_long(1, 1);                 // out_actions present
  cb.store_ref(outlist_root);          // ^OutList
  cb.store_long(0, 1);                 // has_other_actions = 0 (no extended actions)

  // signature: bits512 (placeholder; to be replaced with real Ed25519 signature)
  cb.store_bytes(td::Slice(std::string(64, '\0')));

  return cb.finalize();
}

td::Result<std::string> TonConnectProcessor::serialize_cell_to_boc(td::Ref<vm::Cell> cell) {
  if (cell.is_null()) {
    return td::Status::Error("Cell is null");
  }
  
  auto boc_result = vm::std_boc_serialize(cell);
  if (boc_result.is_error()) {
    return boc_result.move_as_error_prefix("Failed to serialize cell to BOC: ");
  }
  
  auto boc_bytes = boc_result.move_as_ok();
  return td::base64_encode(boc_bytes);
}
