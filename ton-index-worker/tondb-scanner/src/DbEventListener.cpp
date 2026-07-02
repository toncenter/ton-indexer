#include "DbEventListener.h"
#include "tl-utils/common-utils.hpp"

#if TD_PORT_POSIX
#include <errno.h>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

namespace {
constexpr std::size_t kReadChunkSize = 4096;
constexpr std::size_t kMaxReadChunksPerAlarm = 64;
constexpr std::size_t kMaxEventsPerAlarm = 1024;
constexpr double kBusyPollDelaySec = 0.001;
constexpr double kIdlePollDelaySec = 0.05;
constexpr double kOpenRetryDelaySec = 0.5;
constexpr double kReaderConflictRetryDelaySec = 5.0;
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
            // no writer at the moment; reopen if the FIFO was recreated on disk
            if (fifo_recreated()) {
                LOG(WARNING) << "DB events FIFO '" << path_ << "' was recreated, reopening";
                close_fd();
                alarm_timestamp() = td::Timestamp::in(kOpenRetryDelaySec);
                return;
            }
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
        close_fd();
        alarm_timestamp() = td::Timestamp::in(kOpenRetryDelaySec);
        return;
    }

    alarm_timestamp() = td::Timestamp::in(made_progress ? kBusyPollDelaySec : kIdlePollDelaySec);
#else
    stop();
#endif
}

#if TD_PORT_POSIX
bool DbEventListener::ensure_open() {
    if (fd_ >= 0) {
        return true;
    }
    if (mkfifo(path_.c_str(), 0660) != 0 && errno != EEXIST) {
        if (!open_error_logged_) {
            LOG(ERROR) << "Failed to create DB events FIFO '" << path_ << "': "
                        << td::Status::PosixError(errno, "mkfifo failed");
            open_error_logged_ = true;
        }
        alarm_timestamp() = td::Timestamp::in(kOpenRetryDelaySec);
        return false;
    }
    int fd = ::open(path_.c_str(), O_RDONLY | O_NONBLOCK | O_CLOEXEC);
    if (fd < 0) {
        if (!open_error_logged_) {
            LOG(ERROR) << "Failed to open DB events FIFO '" << path_ << "': "
                        << td::Status::PosixError(errno, "open failed");
            open_error_logged_ = true;
        }
        alarm_timestamp() = td::Timestamp::in(kOpenRetryDelaySec);
        return false;
    }
    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISFIFO(st.st_mode)) {
        if (!open_error_logged_) {
            LOG(ERROR) << "DB events path '" << path_ << "' is not a FIFO";
            open_error_logged_ = true;
        }
        ::close(fd);
        alarm_timestamp() = td::Timestamp::in(kReaderConflictRetryDelaySec);
        return false;
    }
    // exclusive reader lock: with two readers each would get a random half of the byte
    // stream, so a second instance must stay on polling instead of corrupting both
    if (flock(fd, LOCK_EX | LOCK_NB) != 0) {
        ::close(fd);
        if (!reader_conflict_logged_) {
            LOG(WARNING) << "DB events FIFO '" << path_ << "' already has another reader; not consuming events";
            reader_conflict_logged_ = true;
        }
        alarm_timestamp() = td::Timestamp::in(kReaderConflictRetryDelaySec);
        return false;
    }
    if (reader_conflict_logged_) {
        LOG(WARNING) << "DB events FIFO '" << path_ << "' reader lock acquired, consuming events";
        reader_conflict_logged_ = false;
    }
    open_error_logged_ = false;
    fd_ = fd;
    return true;
}

void DbEventListener::close_fd() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    buffer_.clear();
}

bool DbEventListener::fifo_recreated() const {
    struct stat fd_st, path_st;
    if (fstat(fd_, &fd_st) != 0 || stat(path_.c_str(), &path_st) != 0) {
        return true;
    }
    return fd_st.st_ino != path_st.st_ino || fd_st.st_dev != path_st.st_dev;
}
#else
bool DbEventListener::ensure_open() {
    return false;
}

void DbEventListener::close_fd() {
}

bool DbEventListener::fifo_recreated() const {
    return false;
}
#endif

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
        handler_(parsed.move_as_ok());
        processed++;
    }
    return processed;
}
