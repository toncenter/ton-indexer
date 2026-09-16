#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "AbiTestSupport.h"

TEST_CASE("smoke: ton-abi scaffold links and runs") {
  ton_abi::ContractABI abi;
  abi.contract_name = "Smoke";
  CHECK(abi.contract_name == "Smoke");

  auto r = ton_abi::load_abi_from_json("not json");
  CHECK(r.is_error());
}
