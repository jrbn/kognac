/*
 * Copyright 2016 Jacopo Urbani
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
**/

#ifndef LRUCACHE_H_
#define LRUCACHE_H_

#include <kognac/factory.h>
#include <kognac/hashfunctions.h>
#include <kognac/utils.h>
#include <kognac/consts.h>

#include <sparsehash/sparse_hash_map>
#include <sparsehash/dense_hash_map>
#include <boost/log/trivial.hpp>
#include <list>
#include <string>
#include <cstring>

const char EMPTY_KEY[2] = { '0', '0' };
const char DELETED_KEY[2] = { 127, 127 };

struct eqstr {
    bool operator()(const char* s1, const char* s2) const {

        if (s1 == s2) {
            return true;
        }

        if (s1 == EMPTY_KEY || s1 == DELETED_KEY || s2 == EMPTY_KEY || s2 == DELETED_KEY) {
            return false;
        }

        if (s1 && s2 && s1[0] == s2[0] && s1[1] == s2[1]) {
            int l = Utils::decode_short(s1, 0);
            return Utils::compare(s1, 2, 2 + l, s2, 2, 2 + l) == 0;
        }
        return false;
    }
};

struct hashstr {
    std::size_t operator()(const char *k) const {
        return Hashes::dbj2s(k + 2, Utils::decode_short(k, 0));
    }
};

typedef google::sparse_hash_map<const char *, long, hashstr, eqstr> CompressedByteArrayToNumberMap;
typedef google::dense_hash_map<const char *, long, hashstr, eqstr> ByteArrayToNumberMap;
typedef google::dense_hash_map<string, long> GStringToNumberMap;

class LRUByteArraySet {
private:
    google::dense_hash_map<const char *, std::list<char*>::iterator, hashstr, eqstr> map;
    std::list<char *> list;
    const int maxSize;
    PreallocatedArraysFactory<char> factory;
public:

    LRUByteArraySet(int maxSize, int maxKeySize) : maxSize(maxSize), factory(maxKeySize, 2, maxSize) {
        map.set_empty_key(EMPTY_KEY);
        map.set_deleted_key(DELETED_KEY);
    }

    bool put(const char *key) {
        //Check if the element exists
        google::dense_hash_map<const char *, std::list<char*>::iterator, hashstr, eqstr>::iterator itr = map.find((char*) key);
        if (itr != map.end()) {
            std::list<char*>::iterator elItr = itr->second;
            list.splice(list.end(), list, elItr);
            return false;
        }

        char *term;
        if (map.size() == maxSize) {
            char *el = list.front();
            map.erase(el);
            list.pop_front();
            term = el;
        } else {
            term = factory.get();
        }

        int sizeKey = Utils::decode_short(key);
        memcpy(term, key, sizeKey + 2);
        list.push_back(term);
        std::list<char*>::iterator itrLastEl = list.end();
        itrLastEl--;
        map.insert(std::make_pair(term, itrLastEl));
        return true;
    }

//    string toString() {
//        string output = string("Map size: ") + to_string(map.size()) + string(" List size: ") + to_string(list.size());
//        for(std::list<char *>::iterator itr = list.begin(); itr != list.end(); ++itr) {
//            cout << string((*itr)+2,1) << endl;
//        }
//
//
//        for(google::dense_hash_map<const char *, std::list<char*>::iterator, hashstr, eqstr>::iterator itr = map.begin(); itr != map.end(); ++itr) {
//            cout << string("MAP ") << string(itr->first + 2, 1) << string("-") << string(*(itr->second) + 2, 1) << endl;
//        }
//
//        return output;
//    }

    ~LRUByteArraySet() {
        for (std::list<char*>::iterator itr = list.begin(); itr != list.end(); ++itr) {
            factory.release(*itr);
        }
    }
};

#endif /* LRUCACHE_H_ */
