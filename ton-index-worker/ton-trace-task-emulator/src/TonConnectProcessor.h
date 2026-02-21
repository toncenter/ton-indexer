#pragma once
#include <td/actor/actor.h>
#include <validator/impl/external-message.hpp>
#include <emulator/transaction-emulator.h>
#include <msgpack.hpp>
#include "IndexData.h"

struct TonConnectMessage {
    std::string address;
    std::string amount;
    std::optional<std::string> payload;
    std::optional<std::string> state_init;
    // std::optional<std::map<std::string, std::string>> extra_currency;

    MSGPACK_DEFINE(address, amount, payload, state_init/*, extra_currency*/);
};

struct TonConnectTraceTask {
  std::string id;
  std::string from;
  std::optional<uint64_t> valid_until;
  std::vector<TonConnectMessage> messages;
  bool detect_interfaces;
  bool include_code_data;
  std::optional<uint32_t> mc_block_seqno;

  MSGPACK_DEFINE(id, from, valid_until, messages, detect_interfaces, include_code_data, mc_block_seqno);
};

struct TraceTask {
  std::string id;
  std::string boc;
  bool ignore_chksig;
  bool detect_interfaces;
  bool include_code_data;
  std::optional<uint32_t> mc_block_seqno;

  MSGPACK_DEFINE(id, boc, ignore_chksig, detect_interfaces, include_code_data, mc_block_seqno);
};

struct TaskEnvelope {
    std::string     type; // "trace" | "tonconnect"
    msgpack::object task; // hold as generic object; convert after switch
    MSGPACK_DEFINE_MAP(type, task);
};

enum WalletType {
  WALLET_V1R1 = 0,
  WALLET_V1R2 = 1,
  WALLET_V1R3 = 2,
  WALLET_V2R1 = 3,
  WALLET_V2R2 = 4,
  WALLET_V3R1 = 5,
  WALLET_V3R2 = 6,
  WALLET_V4R1 = 7,
  WALLET_V4R2 = 8,
  WALLET_V5R1 = 9,
  WALLET_UNKNOWN = -1
};

class TonConnectProcessor: public td::actor::Actor {
public:
  TonConnectProcessor(TonConnectTraceTask tonconnect_task, const schema::MasterchainBlockDataState& mc_data_state, td::Promise<TraceTask> promise)
    : tonconnect_task_(std::move(tonconnect_task)), mc_data_state_(mc_data_state), promise_(std::move(promise)) {}

  // Convert TonConnect task to regular TraceTask
  void process_tonconnect_task(const TonConnectTraceTask& tonconnect_task, 
                            const schema::MasterchainBlockDataState& mc_data_state);

  void start_up() override;
private:
  TonConnectTraceTask tonconnect_task_;
  schema::MasterchainBlockDataState mc_data_state_;
  td::Promise<TraceTask> promise_;

  void error(td::Status error);
  void got_account_state(schema::AccountState account_state);

  td::Result<td::Ref<vm::Cell>> message_to_cell(const TonConnectMessage& message);

  // Wallet detection and external message composition methods
  WalletType detect_wallet_type(const td::Bits256& code_hash);
  td::Result<td::Ref<vm::Cell>> compose_message_body_v3(const schema::AccountState& account_state);
  td::Result<td::Ref<vm::Cell>> compose_message_body_v4(const schema::AccountState& account_state);
  td::Result<td::Ref<vm::Cell>> compose_message_body_v5(const schema::AccountState& account_state);
  td::Result<td::Ref<vm::Cell>> compose_external_message(td::Ref<vm::Cell> body);

  // Helper method to serialize cell to BOC
  td::Result<std::string> serialize_cell_to_boc(td::Ref<vm::Cell> cell);
};
