// Exercise TON crypto with the same gRPC dependency graph as the executor.
#include "ExecutorServer.h"
#include "Ed25519.h"
#include "td/utils/misc.h"

int main() {
  std::string error;
  auto server = tvm_executor::ExecutorServer::start("127.0.0.1:0", [](tvm_executor::Execution call) {
    call.complete({tvm_executor::Code::Ok, {}, {}});
  }, &error);
  CHECK(server != nullptr);
  // RFC 8032 test 1: an empty message and a valid Ed25519 signature.
  auto key_bytes = td::hex_decode("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a").move_as_ok();
  auto signature = td::hex_decode("e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155"
                                  "5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b").move_as_ok();
  auto key = td::Ed25519::PublicKey::from_slice(key_bytes).move_as_ok();
  key.verify_signature("", signature).ensure();
  signature[0] ^= 1;
  CHECK(key.verify_signature("", signature).is_error());
  server->shutdown();
}
