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

#ifndef FLA_H
#define FLA_H

#include <cmath>
#include <vector>

using namespace std;

#define FLAJETCOS 0.77531

class FlajoletMartin {
private:

    long key1, key2, key3;

    //Taken from http://graphics.stanford.edu/~seander/bithacks.html#ZerosOnRightMultLookup
    static int deBruijnAlgo(const unsigned int v) {
        int r;           // result goes here
        static const int MultiplyDeBruijnBitPosition[32] = {
            0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8,
            31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9
        };
        r = MultiplyDeBruijnBitPosition[((uint32_t)((v & -v) * 0x077CB531U)) >> 27];
        return r;
    }

public:

    FlajoletMartin() {
        key1 = key2 = key3 = 0;
    }

    static int posleastSignificantOne(const long i) {
        unsigned int n = (unsigned int)i;
        if (n == 0) {
            n = (unsigned int)(i >> 32);
            int result = deBruijnAlgo(n);
            if (result == 0) {
                result = 64;
            } else {
                result += 32;
            }
            return result;
        } else {
            return deBruijnAlgo(n);
        }
    }

    static int posFirstZero(long n) {
        int i = 0;
        for (; i < 64 && n & 1; ++i)
            n >>= 1;
        return i;
    }

    void addElement(const long el1, const long el2, const long el3) {
        int p1 = posleastSignificantOne(el1);
        int p2 = posleastSignificantOne(el2);
        int p3 = posleastSignificantOne(el3);
        key1 |= (long)1 << p1;
        key2 |= (long)1 << p2;
        key3 |= (long)1 << p3;
    }

    long estimateCardinality() {
        int pos1 = posFirstZero(key1);
        int pos2 = posFirstZero(key2);
        int pos3 = posFirstZero(key3);
        int avg = (pos1 + pos2 + pos3) / 3;
        return (long) (double)pow(2, avg) / FLAJETCOS * 3;
    }
};

#endif
