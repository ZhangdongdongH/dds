/*    Copyright 2014 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */


#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include <stdio.h>
#include <string>
#include <map>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <boost/algorithm/string.hpp>
#include "mongo/util/text.h"
#include "mongo/util/log.h"
#include "mongo/util/concurrency/rwlock.h"
#include "mongo/util/net/publicip_privateiprange.h"

namespace mongo {

    // eg: 192.168.1.100,192.168.1.100/24
    bool PublicIpPrivateIpRange::parseFromString(const std::string& line) {

        std::vector<std::string> keys;
        boost::split(keys, line, boost::is_any_of(":"));

        if(!keys.empty()) {
            if(keys[0] == "publicIpPrivateRange") {
                if(keys[1] == "001") {
                    return parseFromString001(keys);
                }
            }
        }

        return false;
    }

    bool PublicIpPrivateIpRange::parseFromString(std::vector<std::string>& keys) {
        if(!keys.empty()) {
            if(keys[0] == "publicIpPrivateRange") {
                if(keys[1] == "001") {
                    return parseFromString001(keys);
                }
            }
        }

        return false;
    }

    bool PublicIpPrivateIpRange::parseFromString001(std::vector<std::string>& keys) {

        if(keys.empty()) {
            return false;
        }

        if(keys.size()<3) {
            return false;
        }

        if(!(keys[0] == "publicIpPrivateRange" && keys[1] == "001")) {
            return false;
        }

        if(keys.size()<4) {
            reset();
            return true;
        }


        if(keys[3] == "") {
            reset();
            return true;
        }
        std::map<std::string, std::string> tmpPublicIpMap;
        std::map<uint32_t, IpRange> tmpRangeMap;

        parsePublicIp(keys[2], tmpPublicIpMap);
        parsePrivateIpRange(keys[3], tmpRangeMap);

        // swap to update
        rwlock lk(_lock, true);
        publicIpMap.swap(tmpPublicIpMap);
        privateIpRangeMap.swap(tmpRangeMap);

        return true;
    }

    bool PublicIpPrivateIpRange::parsePrivateIpRange(std::string & range, std::map<uint32_t, IpRange> & tmpRangeMap) {
        // parse each item
        std::multimap<uint32_t, IpRange> tmpRangeMultiMap;
        std::vector<std::string> items = StringSplitter::split(range, ",");

        for (std::vector<std::string>::iterator it = items.begin(); it != items.end(); it++ ) {
            IpRange range;
            if (!parseItem(*it, range)) {
                return false;
            }
            tmpRangeMultiMap.insert(std::make_pair(range.min, range));
        }


        // merge overlapped items

        IpRange last;

        for (std::multimap<uint32_t, IpRange>::iterator mit = tmpRangeMultiMap.begin(); mit != tmpRangeMultiMap.end(); mit++ ) {
            if (last.invalid()) {
                last = mit->second;
            } else {
                if (last.max >= mit->second.min) {
                    last.max = std::max(last.max, mit->second.max);
                } else {
                    tmpRangeMap.insert(std::make_pair(last.min, last));
                    last = mit->second;
                }
            }
        }

        if (!last.invalid()) {
            tmpRangeMap.insert(std::make_pair(last.min, last));
        }

        return true;
    }

    void PublicIpPrivateIpRange::parsePublicIp(std::string publicIp, std::map<std::string, std::string> & tmpPublicIpMap) {

        if(publicIp.empty()) {
            tmpPublicIpMap.clear();
            return;
        }

        std::vector<std::string> items = StringSplitter::split(publicIp, ",");
        for(auto & item : items) {
            std::vector<std::string> key_value = StringSplitter::split(item, "=");
            if(key_value.size() != 2) {
                continue;
            }
            boost::trim(key_value[0]);
            boost::trim(key_value[1]);
            tmpPublicIpMap.insert(std::make_pair(key_value[0], key_value[1]));
        }


    }
    bool PublicIpPrivateIpRange::isInPrivateIpRange(const std::string& ipstr)
    {
        uint32_t ipval = 0;
        if (!addrToUint(ipstr, ipval)) {
            return false;
        }
        return isInPrivateIpRange(ipval);
    }

    bool PublicIpPrivateIpRange::isPublicIp(const std::string& ipstr) {

        if(!isPublicIpUsed()) {
            return false;
        }

        uint32_t ipval = 0;
        if (!addrToUint(ipstr, ipval)) {
            return false;
        }

        return !isInPrivateIpRange(ipval);
    }

    int PublicIpPrivateIpRange::rangeSize() {
        rwlock lk(_lock, false);
        return privateIpRangeMap.size();
    }

    bool PublicIpPrivateIpRange::isInPrivateIpRange(const uint32_t ip) {
        rwlock lk(_lock, false);

        if (privateIpRangeMap.empty()) {
            return false;
        }

        std::map<uint32_t, IpRange>::iterator it = privateIpRangeMap.lower_bound(ip);
        if (it != privateIpRangeMap.end() && it->second.include(ip)) {
            return true;
        }

        if (it != privateIpRangeMap.begin()) {
            it--;
            if (it->second.include(ip)) {
                return true;
            }
        }

        return false;
    }



    // valid format
    // 192.168.1.100 or 192.168.1.100/24  or 192.168.1.100-192.168.1.200
    // rds rule: 0.0.0.0/0 means all ips
    bool PublicIpPrivateIpRange::parseItem(const std::string& raw, IpRange& range) {
        std::string item = raw;
        boost::trim(item);
          
        if (item.find("/") != std::string::npos) {
            std::vector<std::string> fields = StringSplitter::split(item, "/");
            if (fields.size() != 2) {
               return false;
            }

            uint32_t val = 0;
            if (!addrToUint(fields[0], val)) {
                return false;
            }

            int mask = atoi(fields[1].c_str());
            if (mask < 0 || mask > 32) {
                return false;
            }

            if (mask == 0) {
                range.min = 0;
                range.max = 0xFFFFFFFF;
            } else {
                range.min = (val & (0xFFFFFFFF << (32 - mask)));
                range.max = (val | ((1 << (32 - mask)) - 1));
            }
        } else if (item.find("-") != std::string::npos) {
            std::vector<std::string> fields = StringSplitter::split(item, "-");
            if (fields.size() != 2) {
               return false;
            }

            if (!addrToUint(fields[0], range.min) ||
               !addrToUint(fields[1], range.max) ||
               range.min > range.max) {
                return false;
            }
        } else {
            if (!addrToUint(item, range.min)) {
                return false;
            }
            range.max = range.min;
        }

        return true;
    }

    std::string PublicIpPrivateIpRange::toString() {
        std::stringstream ss;
        rwlock lk(_lock, false);
        ss << " publice Ip: [ ";
        for (auto it = publicIpMap.begin(); it != publicIpMap.end(); it++ ) {
            ss << it->first << "=" << it->second << " ";
        }
        ss << "]. privateIpRange: ";
        for (std::map<uint32_t, IpRange>::iterator it = privateIpRangeMap.begin(); it != privateIpRangeMap.end(); it++ ) {
            std::string min_ipstr;
            std::string max_ipstr;
            uintToAddr(it->second.min, min_ipstr);
            uintToAddr(it->second.max, max_ipstr);
            ss << "[" << min_ipstr << ":" << max_ipstr << "] ";
        }
        return ss.str();
    }

    bool PublicIpPrivateIpRange::addrToUint(const std::string& addr, uint32_t& ipval) {
        struct in_addr inaddr;
        int ret = inet_aton(addr.c_str(), &inaddr);
        if (!ret) {
            return false;
        }

        ipval = ntohl(inaddr.s_addr);
        return true;
    }

    void PublicIpPrivateIpRange::uintToAddr(uint32_t ipval, std::string& addr) {
        uint32_t nipval = htonl(ipval);
        char str[64] = {0};
        unsigned char *bytes = (unsigned char *)&nipval;
        sprintf(str, "%d.%d.%d.%d", bytes[0], bytes[1], bytes[2], bytes[3]);
        addr.assign(str);
    }

}
