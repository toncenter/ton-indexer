#include <iostream>

#include "tl-utils/tl-utils.hpp"
#include "auto/tl/ton_api_json.h"
#include "auto/tl/tonlib_api_json.h"

#include "td/utils/OptionParser.h"
#include "td/utils/filesystem.h"
#include "td/utils/port/path.h"
#include "td/utils/port/signals.h"

#include "tonlib/Client.h"

using namespace tonlib;

auto sync_send = [](auto& client, auto query) {
  using ReturnTypePtr = typename std::decay_t<decltype(*query)>::ReturnType;
  using ReturnType = typename ReturnTypePtr::element_type;
  client.send({1, std::move(query)});
  while (true) {
    auto response = client.receive(100);
    if (response.object && response.id != 0) {
      CHECK(response.id == 1);
      if (response.object->get_id() == tonlib_api::error::ID) {
        auto error = tonlib_api::move_object_as<tonlib_api::error>(response.object);
        return td::Result<ReturnTypePtr>(td::Status::Error(error->code_, error->message_));
      }
      return td::Result<ReturnTypePtr>(tonlib_api::move_object_as<ReturnType>(response.object));
    }
  }
};
auto static_send = [](auto query) {
  using ReturnTypePtr = typename std::decay_t<decltype(*query)>::ReturnType;
  using ReturnType = typename ReturnTypePtr::element_type;
  auto response = Client::execute({1, std::move(query)});
  if (response.object->get_id() == tonlib_api::error::ID) {
    auto error = tonlib_api::move_object_as<tonlib_api::error>(response.object);
    return td::Result<ReturnTypePtr>(td::Status::Error(error->code_, error->message_));
  }
  return td::Result<ReturnTypePtr>(tonlib_api::move_object_as<ReturnType>(response.object));
};

using tonlib_api::make_object;

void sync(Client& client) {
  sync_send(client, make_object<tonlib_api::sync>()).ensure();
}


int main(int argc, char* argv[]) {
    td::set_default_failure_signal_handler();
    using tonlib_api::make_object;

    td::OptionParser p;
    std::string global_config_path;
    std::string global_config_str;
    bool reset_keystore_dir = false;
    std::string keystore_dir = "/tmp/ton_keystore";

    p.add_checked_option('C', "global-config", "file to read global config", [&](td::Slice fname) {
        TRY_RESULT(str, td::read_file_str(fname.str()));
        global_config_str = std::move(str);
        LOG(INFO) << global_config_str;
        return td::Status::OK();
    });
    p.add_option('f', "force", "reser keystore dir", [&]() { reset_keystore_dir = true; });
    p.run(argc, argv).ensure();

    if (reset_keystore_dir) {
      td::rmrf(keystore_dir).ignore();
    }
    td::mkdir(keystore_dir).ensure();
    

    LOG(INFO) << "It Works!";

    td::mkdir(keystore_dir).ensure();

    SET_VERBOSITY_LEVEL(VERBOSITY_NAME(INFO));
    static_send(make_object<tonlib_api::setLogTagVerbosityLevel>("tonlib_query", 4)).ensure();
    auto tags = static_send(make_object<tonlib_api::getLogTags>()).move_as_ok()->tags_;
    for (auto& tag : tags) {
        static_send(make_object<tonlib_api::setLogTagVerbosityLevel>(tag, 4)).ensure();
    }

    Client client;
    {
        auto info = sync_send(client, make_object<tonlib_api::init>(make_object<tonlib_api::options>(
                                      make_object<tonlib_api::config>(global_config_str, "", false, false),
                                      make_object<tonlib_api::keyStoreTypeDirectory>(keystore_dir))))
                        .move_as_ok();
        LOG(INFO) << "Init done!";
    }

    sync(client);

    LOG(INFO) << "Exit!";

    return 0;
}
