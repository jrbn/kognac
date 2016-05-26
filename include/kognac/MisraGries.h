/*
 * Copyright 2016 Sourav Dutta
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

// ---------------------------------
// Implements Misra-Gries algorithm
// to find the top-k frequent items
//  :sdutta
// ---------------------------------

#ifndef MISRAGRIES_H_
#define MISRAGRIES_H_

#include <map>
#include <string>
#include <unordered_map>
#include <vector>
#include <cstring>

#include <google/dense_hash_map>
#include <kognac/hashfunctions.h>

using namespace std;

/*** ALL STUFF NECESSARY TO IMPLEMENT A HEAP ***/
#define STR_POOL_EL_SIZE 16384
const char MG_EMPTY_KEY[2] = { '0', '0' };
const char MG_DELETED_KEY[2] = { 127, 127 };

typedef std::pair<unsigned short, const char*> HashMapKey;
struct MG_eqstr {
    bool operator()(const HashMapKey &key1, const HashMapKey &key2) const {
        if (key1.second == key2.second) {
            return true;
        }
        if (key1.second == MG_EMPTY_KEY || key1.second == MG_DELETED_KEY
                || key2.second == MG_EMPTY_KEY
                || key2.second == MG_DELETED_KEY) {
            return false;
        }
        if (key1.first == key2.first) {
            return memcmp(key1.second, key2.second, key1.first) == 0;
        }
        return false;
    }
};

struct MG_hashstr {
    std::size_t operator()(const HashMapKey &key) const {
        //Simplified hashing
        if (key.first > 20) {
            return Hashes::dbj2s(key.second, 10) ^ Hashes::dbj2s(key.second + key.first - 10, 10);
        } else {
            return Hashes::dbj2s(key.second, key.first);
        }
    }
};

typedef google::dense_hash_map<HashMapKey, long*, MG_hashstr, MG_eqstr> HashMap;
typedef std::vector<std::pair<long, HashMapKey> > CounterVector;

typedef map<string, long> StringToNumberMap;

struct MG_CountSorter {
    const std::pair<long, HashMapKey> *vector;

    MG_CountSorter(const std::pair<long, HashMapKey> *v) : vector(v) {
    }

    bool operator()(const size_t &el1, const size_t &el2) const {
        return vector[el1].first < vector[el2].first;
    }
};

class MG {
private:
    // Maximum size of the heap as defined earlier
    const unsigned long heapSize;

    // Additional data structures during the adding of the heap
    HashMap lookupMap;
    std::pair<long, HashMapKey> *counterVector;
    std::vector<size_t> idxCounterVector;
    char* stringPool;

    void addElem(const char *key, unsigned int len);

public:
    // Constructor
    MG(const unsigned long);

    // Adds an element into the structure
    void add (const char*, unsigned int);

    // Returns the map structure
    const StringToNumberMap getHeapElements(void) const;

    std::vector<string> getPositiveTerms() const;

    // Merges two MG heaps together and stores the result in this
    void merge(StringToNumberMap&);

    ~MG() {
        delete[] stringPool;
        delete[] counterVector;
    }
};

#endif  // MISRAGRIES_H_
