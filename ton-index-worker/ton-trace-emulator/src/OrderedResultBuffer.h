#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <utility>

template <class T>
class OrderedResultBuffer {
public:
    struct Entry {
        std::uint32_t seqno;
        T value;
    };

    void reset(std::uint32_t next_seqno) {
        values_.clear();
        next_seqno_ = next_seqno;
    }

    bool insert(std::uint32_t seqno, T value) {
        if (seqno < next_seqno_) {
            return false;
        }
        return values_.emplace(seqno, std::move(value)).second;
    }

    std::optional<Entry> take_next() {
        auto it = values_.find(next_seqno_);
        if (it == values_.end()) {
            return std::nullopt;
        }

        Entry result{
            .seqno = next_seqno_,
            .value = std::move(it->second),
        };
        values_.erase(it);
        ++next_seqno_;
        return result;
    }

private:
    std::uint32_t next_seqno_{0};
    std::map<std::uint32_t, T> values_;
};
