#include <atomic>
#include <array>
#include <iostream>
#include "DbScanner.h"
#include "crypto/block/block.h"
#include "validator/impl/shard.hpp"
#include "td/utils/port/path.h"
#include "td/utils/port/signals.h"
#include "vm/cp0.h"

class AccountsProbe final : public td::actor::Actor {
  td::actor::ActorOwn<DbScanner> scanner_;
  std::shared_ptr<std::atomic_bool> done_;
  int& result_;
  std::string secondary_;
 public:
  AccountsProbe(std::shared_ptr<std::atomic_bool> done,int& result,std::string secondary)
      : done_(std::move(done)),result_(result),secondary_(std::move(secondary)) {}
  void start_up() override {
    scanner_=td::actor::create_actor<DbScanner>("accounts_scanner","/var/ton-work/db",dbs_secondary,
        std::optional<std::string>(secondary_),3600.0f,true);
    td::connect(td::PromiseCreator::lambda([self=actor_id(this)](td::Result<td::Unit> r){
      td::actor::send_closure(self,&AccountsProbe::finished,std::move(r));
    }),run());
  }
  td::Result<std::array<uint64_t,4>> count(td::Ref<vm::Cell> root) {
    block::gen::ShardStateUnsplit::Record state;
    if(!tlb::unpack_cell(root,state)) return td::Status::Error("Invalid shard state");
    vm::AugmentedDictionary accounts(vm::load_cell_slice_ref(state.accounts),256,block::tlb::aug_ShardAccounts);
    std::array<uint64_t,4> counts{};
    bool ok=accounts.check_for_each_extra([&](td::Ref<vm::CellSlice> value,td::Ref<vm::CellSlice>,td::ConstBitPtr,int n)->bool {
      if(n!=256) return false;
      block::gen::ShardAccount::Record entry;
      if(!tlb::csr_unpack(value,entry)) return false;
      block::gen::Account::Record_account account;
      if(!tlb::unpack_cell(entry.account,account)) {
        block::gen::Account::Record_account_none none;
        if(!tlb::unpack_cell(entry.account,none)) return false;
        ++counts[3]; return true;
      }
      block::gen::AccountStorage::Record storage;
      if(!tlb::csr_unpack(account.storage,storage)) return false;
      switch(block::gen::t_AccountState.get_tag(storage.state.write())) {
        case block::gen::AccountState::account_uninit: ++counts[0]; break;
        case block::gen::AccountState::account_active: ++counts[1]; break;
        case block::gen::AccountState::account_frozen: ++counts[2]; break;
        default: return false;
      }
      return counts[0]+counts[1]+counts[2]+counts[3]<1000000;
    });
    if(!ok) return td::Status::Error("Invalid account dictionary or baseline scan limit exceeded");
    return counts;
  }
  td::actor::Task<td::Unit> run() {
    co_await td::actor::ask(scanner_,&DbScanner::get_last_mc_seqno);
    auto reader=co_await td::actor::ask(scanner_,&DbScanner::get_cell_db_reader);
    for(unsigned seqno:{2u,3u,1001u}) {
      auto handle=co_await td::actor::ask(scanner_,&DbScanner::get_mc_block_handle,seqno);
      auto mc=co_await td::actor::ask(scanner_,&DbScanner::fetch_block_by_id,handle->id());
      auto state=co_await ton::validator::MasterchainStateQ::fetch(handle->id(),{},mc.block_state);
      CO_TRY_BOOL(state->get_seqno()==seqno && state->get_global_id()==-239);
      auto mc_counts=count(mc.block_state); if(mc_counts.is_error()) co_return mc_counts.move_as_error();
      auto total=mc_counts.move_as_ok();
      std::cout<<"ACCOUNTS_STATE {\"anchor_mc\":"<<seqno<<",\"workchain\":-1,\"state_hash\":\""<<mc.block_state->get_hash().to_hex()<<"\",\"uninit\":"<<total[0]<<",\"active\":"<<total[1]<<",\"frozen\":"<<total[2]<<"}"<<std::endl;
      for(const auto& descriptor:state->get_shards()) {
        auto id=descriptor->top_block_id();
        td::Ref<vm::Cell> root;
        if(id.seqno()==0) {
          auto r=reader->load_cell(id.root_hash.as_slice());if(r.is_error())co_return r.move_as_error();root=r.move_as_ok();
        } else {
          auto shard=co_await td::actor::ask(scanner_,&DbScanner::fetch_block_by_id,id);root=shard.block_state;
        }
        auto r=count(root);if(r.is_error())co_return r.move_as_error();auto c=r.move_as_ok();
        for(unsigned i=0;i<4;++i)total[i]+=c[i];
        std::cout<<"ACCOUNTS_STATE {\"anchor_mc\":"<<seqno<<",\"block_id\":\""<<id.to_str()<<"\",\"state_hash\":\""<<root->get_hash().to_hex()<<"\",\"uninit\":"<<c[0]<<",\"active\":"<<c[1]<<",\"frozen\":"<<c[2]<<"}"<<std::endl;
      }
      std::cout<<"ACCOUNTS_RESULT {\"mc_seqno\":"<<seqno<<",\"gen_utime\":"<<state->get_unix_time()<<",\"mc_state_hash\":\""<<mc.block_state->get_hash().to_hex()<<"\",\"uninit\":"<<total[0]<<",\"active\":"<<total[1]<<",\"frozen\":"<<total[2]<<",\"nonexist\":"<<total[3]<<"}"<<std::endl;
    }
    co_return td::Unit{};
  }
  void finished(td::Result<td::Unit> r) {
    result_=r.is_ok()?0:1;if(r.is_error())LOG(ERROR)<<r.error();
    scanner_.reset();done_->store(true);stop();
  }
};
int main(int argc,char**argv) {
  td::set_default_failure_signal_handler().ensure();SET_VERBOSITY_LEVEL(verbosity_INFO);CHECK(vm::init_op_cp0() && argc==2);
  std::string secondary=argv[1];td::mkpath(secondary+"/").ensure();
  int result=1;auto done=std::make_shared<std::atomic_bool>(false);
  td::actor::Scheduler scheduler({td::actor::Scheduler::NodeInfo{4,2}});td::actor::ActorOwn<AccountsProbe> probe;
  scheduler.run_in_context([&]{probe=td::actor::create_actor<AccountsProbe>("accounts_probe",done,result,secondary);});
  while(!done->load()&&scheduler.run(1)){}
  scheduler.run_in_context([&]{probe.reset();td::actor::SchedulerContext::get().stop();});scheduler.run();return result;
}
