#pragma once
#include <cstddef>
#include <functional>
#include <string>
#include "td/actor/actor.h"
#include "auto/tl/ton_api.h"


class DbEventListener : public td::actor::Actor {
  public:
    using EventHandler = std::function<void(ton::tl_object_ptr<ton::ton_api::db_Event>)>;

    DbEventListener(std::string path, EventHandler handler)
        : path_(std::move(path)), handler_(std::move(handler)) {}

    void start_up() override;

    void alarm() override;

  private:
    std::string path_;
    EventHandler handler_;
    std::string buffer_;
    int fd_{-1};
    bool open_error_logged_{false};
    bool reader_conflict_logged_{false};

    bool ensure_open();
    void close_fd();
    bool fifo_recreated() const;

    std::size_t process_buffer(std::size_t max_events);
};
