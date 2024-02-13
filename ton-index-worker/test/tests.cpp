#include "td/utils/port/signals.h"
#include "td/utils/OptionParser.h"
#include "td/utils/format.h"
#include "td/utils/logging.h"
#include "td/utils/check.h"

#include "crypto/vm/cp0.h"

#include "td/utils/tests.h"
#include "td/actor/actor.h"
#include "td/utils/base64.h"
// #include "InterfaceDetector.hpp"




// TEST(TonDbScanner, JettonWalletDetector) {
//   td::actor::Scheduler scheduler({1});
//   auto watcher = td::create_shared_destructor([] { td::actor::SchedulerContext::get()->stop(); });

//   block::StdAddress addr(std::string("EQDKC7jQ_tIJuYyrWfI4FIAN-hFHakG3GrATpOiqBVtsGOd5"));
//   auto code_cell = vm::std_boc_deserialize(td::base64_decode(td::Slice("te6cckECEgEAAzEAART/APSkE/S88sgLAQIBYgIDAgLLBAUAG6D2BdqJofQB9IH0gahhAgEgBgcCAWILDAIBSAgJAfH4Hpn/0AfSAQ+AH2omh9AH0gfSBqGCibUKkVY4L5cWCUYX/5cWEqGiE4KhAJqgoB5CgCfQEsZ4sA54tmZJFkZYCJegB6AGWAZJB8gDg6ZGWBZQPl/+ToAn0gegIY/QAQa6ThAHlxYjvADGRlgqgEZ4s4fQEL5bWJ5kCgC3QgxwCSXwTgAdDTAwFxsJUTXwPwEuD6QPpAMfoAMXHXIfoAMfoAMALTHyGCEA+KfqW6lTE0WfAP4CGCEBeNRRm6ljFERAPwEOA1ghBZXwe8upNZ8BHgXwSED/LwgAEV+kQwcLry4U2ACughAXjUUZyMsfGcs/UAf6AiLPFlAGzxYl+gJQA88WyVAFzCORcpFx4lAIqBOgggiYloCqAIIImJaAoKAUvPLixQTJgED7ABAjyFAE+gJYzxYBzxbMye1UAgEgDQ4AgUgCDXIe1E0PoA+kD6QNQwBNMfIYIQF41FGboCghB73ZfeuhKx8uLF0z8x+gAwE6BQI8hQBPoCWM8WAc8WzMntVIA/c7UTQ+gD6QPpA1DAI0z/6AFFRoAX6QPpAU1vHBVRzbXBUIBNUFAPIUAT6AljPFgHPFszJIsjLARL0APQAywDJ+QBwdMjLAsoHy//J0FANxwUcsfLiwwr6AFGooYIImJaAggiYloAStgihggiYloCgGKEn4w8l1wsBwwAjgDxARAOM7UTQ+gD6QPpA1DAH0z/6APpA9AQwUWKhUkrHBfLiwSjC//LiwoIImJaAqgAXoBe88uLDghB73ZfeyMsfyz9QBfoCIc8WUAPPFvQAyXGAGMjLBSTPFnD6AstqzMmAQPsAQBPIUAT6AljPFgHPFszJ7VSAAcFJ5oBihghBzYtCcyMsfUjDLP1j6AlAHzxZQB88WyXGAEMjLBSTPFlAG+gIVy2oUzMlx+wAQJBAjAA4QSRA4N18EAHbCALCOIYIQ1TJ223CAEMjLBVAIzxZQBPoCFstqEssfEss/yXL7AJM1bCHiA8hQBPoCWM8WAc8WzMntVLp4DOo=")).move_as_ok()).move_as_ok();
//   auto data_cell = vm::std_boc_deserialize(td::base64_decode(td::Slice("te6cckECEwEAA3sAAY0xKctoASFZDXpO6Q6ZgXHilrBvG9KSTVMUJk1CMXwYaoCc9JirAC61IQRl0/la95t27xhIpjxZt32vl1QQVF2UgTNuvD18YAEBFP8A9KQT9LzyyAsCAgFiAwQCAssFBgAboPYF2omh9AH0gfSBqGECASAHCAIBYgwNAgFICQoB8fgemf/QB9IBD4AfaiaH0AfSB9IGoYKJtQqRVjgvlxYJRhf/lxYSoaITgqEAmqCgHkKAJ9ASxniwDni2ZkkWRlgIl6AHoAZYBkkHyAODpkZYFlA+X/5OgCfSB6Ahj9ABBrpOEAeXFiO8AMZGWCqARnizh9AQvltYnmQLALdCDHAJJfBOAB0NMDAXGwlRNfA/AS4PpA+kAx+gAxcdch+gAx+gAwAtMfIYIQD4p+pbqVMTRZ8A/gIYIQF41FGbqWMUREA/AQ4DWCEFlfB7y6k1nwEeBfBIQP8vCAARX6RDBwuvLhTYAK6CEBeNRRnIyx8Zyz9QB/oCIs8WUAbPFiX6AlADzxbJUAXMI5FykXHiUAioE6CCCJiWgKoAggiYloCgoBS88uLFBMmAQPsAECPIUAT6AljPFgHPFszJ7VQCASAODwCBSAINch7UTQ+gD6QPpA1DAE0x8hghAXjUUZugKCEHvdl966ErHy4sXTPzH6ADAToFAjyFAE+gJYzxYBzxbMye1UgD9ztRND6APpA+kDUMAjTP/oAUVGgBfpA+kBTW8cFVHNtcFQgE1QUA8hQBPoCWM8WAc8WzMkiyMsBEvQA9ADLAMn5AHB0yMsCygfL/8nQUA3HBRyx8uLDCvoAUaihggiYloCCCJiWgBK2CKGCCJiWgKAYoSfjDyXXCwHDACOAQERIA4ztRND6APpA+kDUMAfTP/oA+kD0BDBRYqFSSscF8uLBKML/8uLCggiYloCqABegF7zy4sOCEHvdl97Iyx/LP1AF+gIhzxZQA88W9ADJcYAYyMsFJM8WcPoCy2rMyYBA+wBAE8hQBPoCWM8WAc8WzMntVIABwUnmgGKGCEHNi0JzIyx9SMMs/WPoCUAfPFlAHzxbJcYAQyMsFJM8WUAb6AhXLahTMyXH7ABAkECMADhBJEDg3XwQAdsIAsI4hghDVMnbbcIAQyMsFUAjPFlAE+gIWy2oSyx8Syz/JcvsAkzVsIeIDyFAE+gJYzxYBzxbMye1U8/HTGA==")).move_as_ok()).move_as_ok();
//   auto P = td::PromiseCreator::lambda([](td::Result<JettonWalletData> R) {
//     CHECK(R.is_ok());
//     LOG(INFO) << R.move_as_ok().jetton;
//   });
//   scheduler.run_in_context([&] { 
//     td::actor::ActorOwn<JettonWalletDetector> jetton_wallet_detector = td::actor::create_actor<JettonWalletDetector>("insertmanager");
//     td::actor::send_closure(jetton_wallet_detector, &JettonWalletDetector::detect, block::StdAddress(), code_cell, data_cell, 0, std::move(P)); 
//     watcher.reset();
//   });

//   scheduler.run();
  
// }



