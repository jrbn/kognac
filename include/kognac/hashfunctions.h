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

#ifndef HASHFUNCTIONS_H_
#define HASHFUNCTIONS_H_

#include <kognac/murmurhash3.h>

#include <boost/functional/hash.hpp>

#include <cstring>

using namespace std;

class Hashes {
public:
    static long murmur3_56(const char* s, const int size) {
        char output[16];
        MurmurHash3_x64_128(s, size, 0, output);
        long number = output[0];
        number += output[1] << 8;
        number += output[2] << 16;
        number += output[3] << 24;
        number += (long)output[8] << 32;
        number += (long)output[9] << 40;
        number += (long)output[10] << 48;
        return number & 0xFFFFFFFFFFFFFFl;
    }

    static int murmur3s(const char* s, const int size) {
        int out;
        MurmurHash3_x86_32(s, size, 0, &out);
        return out;
    }

    static int murmur3(const char* s) {
        int out;
        MurmurHash3_x86_32(s, strlen(s), 0, &out);
        return out;
    }

    static int fnv1a(const char *s) {
        int hval = 0;
        while (*s) {
            hval ^= (int) * s++;
            hval *= 16777619;
        }
        return hval;
    }

    static long fnv1a_56(const char *s, int size) {
        long hval = 0;
        int i = 0;
        while (i < size) {
            hval ^= (int) s[i++];
            hval *= 16777619;
        }
        return hval & 0xFFFFFFFFFFFFFFl;
    }
    static int fnv1as(const char *s, const int size) {
        int hval = 0;
        int i = 0;
        while (i < size) {
            hval ^= (int) s[i++];
            hval *= 16777619;
        }
        return hval;
    }

    static int dbj2(const char* s) {
        unsigned long hash = 5381;
        int c;
        while ((c = *s++))
            hash = ((hash << 5) + hash) + c;
        return hash;
    }

    static int dbj2s(const char* s, const int size) {
        unsigned long hash = 5381;
        int i = 0;
        while (i < size) {
            hash = ((hash << 5) + hash) + s[i++];
        }
        return hash;
    }

    static long dbj2s_56(const char* s, const int size) {
        unsigned long hash = 5381;
        int i = 0;
        while (i < size) {
            hash = ((hash << 5) + hash) + s[i++];
        }
        return hash & 0xFFFFFFFFFFFFFFl;
    }

    static long getCodeWithDefaultFunction(const char *term, const int size) {
        return Hashes::murmur3_56(term, size);
    }

    static size_t hashArray(const uint64_t *array, const size_t size) {
        size_t seed = 0;
        for (size_t i = 0; i < size; ++i) {
            boost::hash_combine(seed, array[i]);
        }
        return seed;
    }
};

#endif /* HASHFUNCTIONS_H_ */
