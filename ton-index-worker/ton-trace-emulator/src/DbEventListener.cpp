#include "DbEventListener.h"
#include "TraceScheduler.h"

namespace {
constexpr std::size_t kReadChunkSize = 4096;
constexpr std::size_t kMaxReadChunksPerAlarm = 64;
constexpr std::size_t kMaxEventsPerAlarm = 1024;
constexpr double kBusyPollDelaySec = 0.001;
constexpr double kIdlePollDelaySec = 0.05;
}  // namespace


void DbEventListener::start_up() {
    alarm_timestamp() = td::Timestamp::now();
}

void DbEventListener::alarm() {
#if TD_PORT_POSIX
    if (!ensure_open()) {
        return;
    }

    char chunk[kReadChunkSize];
    std::size_t chunks_read = 0;
    std::size_t events_processed = process_buffer(kMaxEventsPerAlarm);
    bool made_progress = events_processed > 0;

    while (chunks_read < kMaxReadChunksPerAlarm && events_processed < kMaxEventsPerAlarm) {
        auto bytes_read = ::read(fd_, chunk, sizeof(chunk));
        if (bytes_read > 0) {
            made_progress = true;
            chunks_read++;
            buffer_.append(chunk, static_cast<size_t>(bytes_read));
            events_processed += process_buffer(kMaxEventsPerAlarm - events_processed);
            continue;
        }

        if (bytes_read == 0) {
            break;
        }

        if (errno == EINTR) {
            continue;
        }

        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            break;
        }

        LOG(ERROR) << "Failed to read DB events FIFO '" << path_ << "': "
                    << td::Status::PosixError(errno, "read failed");
        ::close(fd_);
        fd_ = -1;
        alarm_timestamp() = td::Timestamp::in(0.1);
        return;
    }

    alarm_timestamp() = td::Timestamp::in(made_progress ? kBusyPollDelaySec : kIdlePollDelaySec);
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
    return true;
}

std::size_t DbEventListener::process_buffer(std::size_t max_events) {
    std::size_t processed = 0;
    while (processed < max_events) {
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
        processed++;
    }
    return processed;
}
