#pragma once

#include <string>
#include <vector>
#include <set>
#include "td/utils/crypto.h"

struct Interface {
    std::string name;
    std::set<unsigned> methods;

    static unsigned calc_method_id(const std::string& method_name) {
        return (td::crc16(td::Slice(method_name)) & 0xffff) | 0x10000;
    }
};

extern const std::vector<Interface> g_interfaces;
