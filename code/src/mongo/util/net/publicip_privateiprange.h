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


#pragma once

#include <vector>

#include "mongo/util/concurrency/rwlock.h"
#include "mongo/util/net/whitelist.h"

namespace mongo {
    
    /* 
     * PublicIpPrivateIpRange is a comma separated string
     * suppoted format
     * 1. single ip, 192.168.1.100
     * 2. netmask, 192.168.1.100/24
     * 3. net range, 192.168.1.100-192.168.1.200
     */
    class PublicIpPrivateIpRange {
    public:
        PublicIpPrivateIpRange():_lock("publicIpPrivateIpRangeMutex") {}
        bool parseFromString(const std::string& line);
        bool parseFromString(std::vector<std::string>& keys);
        bool parseFromString001(std::vector<std::string>& keys);

        bool isInPrivateIpRange(const uint32_t ip);
        bool isInPrivateIpRange(const std::string& ipstr);
        bool isPublicIp(const std::string& ipstr);
        void parsePublicIp(std::string publicIp, std::map<std::string, std::string> & tmpPublicIpMap);
        bool parsePrivateIpRange(std::string & range, std::map<uint32_t, IpRange> & tmpRangeMap);
        std::string& getPublicIp(const std::string & privateIp) {
            return publicIpMap[privateIp];
        }


        std::map<std::string, std::string> getPublicIp() {
            rwlock lk(_lock, false);
            return publicIpMap;
        }
        bool isPublicIpUsed() {
            return (!getPublicIp().empty());
        }
        void reset() {
            rwlock lk(_lock, true);
            publicIpMap.clear();
            privateIpRangeMap.clear();
        }
        int rangeSize();
        std::string toString();


    private:
        bool addrToUint(const std::string& addr, uint32_t& ipval);
        void uintToAddr(uint32_t ipval, std::string& addr);
        bool parseItem(const std::string& item, IpRange& range);

    private:
        // key is private ip and value is publicip
        std::map<std::string, std::string> publicIpMap;
        std::map<uint32_t, IpRange> privateIpRangeMap;
        RWLock _lock;


    };

}

