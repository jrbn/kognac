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

#ifndef HASHTABLE_H_
#define HASHTABLE_H_

#include <kognac/utils.h>

#include <string>
#include <iostream>
#include <math.h>

using namespace std;

class Hashtable {
private:
    const size_t size;
    long *table;
    long (*hash)(const char*, const int);
public:
    Hashtable(const size_t size, long (*hash)(const char*, const int));

    long add(const char *el, const int l) {
        long hashcode = hash(el, l);
        size_t idx = abs((long)(hashcode % size));
        table[idx]++;
        return hashcode;
    }

    long get(const char *el, const int l) {
        size_t idx = abs((long)(hash(el, l) % size));
        return table[idx];
    }

    long get(const string &el) {
        size_t idx = abs((long)(hash(el.c_str(), el.size()) % size));
        return table[idx];
    }

    long get(size_t idx) {
        return table[idx];
    }

    void merge(Hashtable *ht) {
        for (size_t i = 0; i < size; ++i) {
            table[i] += ht->table[i];
        }
    }

    long getThreshold(size_t highestN) {
        return Utils::quickSelect(table, size, highestN);
    }

    long getTotalCount() {
        long count = 0;
        for (size_t i = 0; i < size; ++i) {
            count += table[i];
        }
        return count;
    }

    ~Hashtable() {
        delete[] table;
    }
};

#endif
