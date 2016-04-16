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

#ifndef LRUSET_H_
#define LRUSET_H_

#include <iostream>
#include <boost/log/trivial.hpp>

class LRUSet {
private:
    char **cache;
    int *sizeCacheElements;

    int *hashCache;
    const int size;
public:
    LRUSet(int size) :
        size(size) {
        cache = new char*[size];
        sizeCacheElements = new int[size];
        hashCache = new int[size];
        for (int i = 0; i < size; ++i) {
            cache[i] = NULL;
            sizeCacheElements[i] = 0;
        }
    }

    bool exists(const char *term, int s) {
        int hash = abs(Hashes::dbj2s(term, s));
        int idx = hash % size;
        if (cache[idx] != NULL && hashCache[idx] == hash
                && sizeCacheElements[idx] >= s + 2
                && Utils::decode_short(cache[idx], 0) == s) {
            return memcmp(cache[idx] + 2, term, s - 2) == 0;
        }
        return false;
    }

    bool exists(const char *term) {
        int s = Utils::decode_short((char*) term);
        int hash = abs(Hashes::dbj2s(term + 2, s));
        int idx = hash % size;
        if (cache[idx] != NULL && hashCache[idx] == hash
                && sizeCacheElements[idx] >= s + 2) {
            int s1 = Utils::decode_short(cache[idx]);
            if (s == s1) {
                return memcmp(cache[idx] + 2, term + 2, s) == 0;
            }
        }
        return false;
    }

    void add(const char* term) {
        int s = Utils::decode_short((char*) term, 0);
        int hash = abs(Hashes::dbj2s(term + 2, s));
        int idx = hash % size;

        if (cache[idx] == NULL) {
            cache[idx] = new char[MAX_TERM_SIZE];
        }

        if (sizeCacheElements[idx] < s + 2) {
            delete[] cache[idx];
            cache[idx] = new char[s + 2];
            sizeCacheElements[idx] = s + 2;
        }

        memcpy(cache[idx], term, s + 2);
        hashCache[idx] = hash;
    }

    void add(const char* term, const int s) {
        int hash = abs(Hashes::dbj2s(term, s));
        int idx = hash % size;

        if (cache[idx] == NULL) {
            cache[idx] = new char[s + 2];
            sizeCacheElements[idx] = s + 2;
        }

        if (sizeCacheElements[idx] < s + 2) {
            delete[] cache[idx];
            cache[idx] = new char[s + 2];
            sizeCacheElements[idx] = s + 2;
        }

        Utils::encode_short(cache[idx], 0, s);
        memcpy(cache[idx] + 2, term, s);
        hashCache[idx] = hash;
    }

    ~LRUSet() {
        for (int i = 0; i < size; ++i) {
            if (cache[i] != NULL)
                delete[] cache[i];
        }
        delete[] cache;
        delete[] sizeCacheElements;
        delete[] hashCache;
    }
};

#endif /* LRUSET_H_ */
