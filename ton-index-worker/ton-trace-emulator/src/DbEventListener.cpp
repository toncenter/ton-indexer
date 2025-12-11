#include "DbEventListener.h"
#include "TraceScheduler.h"


void DbEventListener::start_up() {
    alarm_timestamp() = td::Timestamp::now();
}

void DbEventListener::alarm() {
#if TD_PORT_POSIX
    if (!ensure_open()) {
        return;
    }

    char chunk[4096];
    while (true) {
        auto bytes_read = ::read(fd_, chunk, sizeof(chunk));
        if (bytes_read > 0) {
            buffer_.append(chunk, static_cast<size_t>(bytes_read));
            process_buffer();
            continue;
        }

        if (bytes_read == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(3));
            continue;
        }

        if (errno == EINTR) {
            continue;
        }

        LOG(ERROR) << "Failed to read DB events FIFO '" << path_ << "': "
                    << td::Status::PosixError(errno, "read failed");
        ::close(fd_);
        fd_ = -1;
        alarm_timestamp() = td::Timestamp::in(0.1);
        return;
    }
#else
    stop();
#endif
}

bool DbEventListener::ensure_open() {
    if (fd_ >= 0) {
        return true;
    }
    fd_ = ::open(path_.c_str(), O_RDONLY | O_NONBLOCK);
    if (fd_ < 0) {
        LOG(ERROR) << "Failed to open DB events FIFO '" << path_ << "': "
                    << td::Status::PosixError(errno, "open failed");
        alarm_timestamp() = td::Timestamp::in(0.5);
        return false;
    }
    auto flags = fcntl(fd_, F_GETFL, 0);
    if (flags >= 0) {
        fcntl(fd_, F_SETFL, flags & ~O_NONBLOCK);
    }
    return true;
}

void DbEventListener::process_buffer() {
    while (true) {
        td::Slice slice(buffer_);
        auto parsed = ton::fetch_tl_prefix<ton::ton_api::db_Event>(slice, true);
        if (parsed.is_error()) {
            auto status = parsed.move_as_error();
            auto consumed = buffer_.size() - slice.size();
            if (consumed == 0) {
                // likely incomplete message, wait for more data
                break;
            }
            LOG(ERROR) << "Failed to parse DB event: " << status;
            buffer_.erase(0, consumed);
            continue;
        }

        auto consumed = buffer_.size() - slice.size();
        buffer_.erase(0, consumed);
        td::actor::send_closure(consumer_, &TraceEmulatorScheduler::handle_db_event, parsed.move_as_ok());
    }
}