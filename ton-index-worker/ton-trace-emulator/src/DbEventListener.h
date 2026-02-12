#pragma once
#include "td/actor/actor.h"
#include "tl-utils/common-utils.hpp"
#include "ton/ton-tl.hpp"
#include "td/utils/overloaded.h"
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>


class TraceEmulatorScheduler;


class DbEventListener : public td::actor::Actor {
  public:
    DbEventListener(std::string path, td::actor::ActorId<TraceEmulatorScheduler> consumer)
        : path_(std::move(path)), consumer_(consumer) {}

    void start_up() override;

    void alarm() override;

  private:
    std::string path_;
    td::actor::ActorId<TraceEmulatorScheduler> consumer_;
    std::string buffer_;
    int fd_{-1};

    bool ensure_open();

    void process_buffer();
};