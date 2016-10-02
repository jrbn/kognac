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

#ifndef TRIPLE_H_
#define TRIPLE_H_

#include <limits>
#include <functional>

class LZ4Reader;
class LZ4Writer;
class MultiDiskLZ4Writer;
class MultiDiskLZ4Reader;

typedef struct Triple {
    long s, p, o, count;

    Triple(long s, long p, long o) {
        this->s = s;
        this->p = p;
        this->o = o;
        this->count = 0;
    }

    Triple() {
        s = p = o = count = 0;
    }

    bool less(const Triple &t) const {
        if (s < t.s) {
            return true;
        } else if (s == t.s) {
            if (p < t.p) {
                return true;
            } else if (p == t.p) {
                return o < t.o;
            }
        }
        return false;
    }

    bool greater(const Triple &t) const {
        if (s > t.s) {
            return true;
        } else if (s == t.s) {
            if (p > t.p) {
                return true;
            } else if (p == t.p) {
                return o > t.o;
            }
        }
        return false;
    }

    void readFrom(LZ4Reader *reader);

    void readFrom(int part, MultiDiskLZ4Reader *reader);

    void writeTo(LZ4Writer *writer);

    void writeTo(int part, MultiDiskLZ4Writer *writer);

    static bool sLess(const Triple &t1, const Triple &t2) {
        return t1.less(t2);
    }

} Triple;

const Triple minv(std::numeric_limits<long>::min(),
        std::numeric_limits<long>::min(), std::numeric_limits<long>::min());
const Triple maxv(std::numeric_limits<long>::max(),
        std::numeric_limits<long>::max(), std::numeric_limits<long>::max());

struct cmp: std::less<Triple> {
    bool operator ()(const Triple& a, const Triple& b) const {
        if (a.s < b.s) {
            return true;
        } else if (a.s == b.s) {
            if (a.p < b.p) {
                return true;
            } else if (a.p == b.p) {
                return a.o < b.o;
            }
        }
        return false;
    }

    Triple min_value() const {
        return minv;
    }

    Triple max_value() const {
        return maxv;
    }
};

class TripleWriter {
    public:
        virtual void write(const long t1, const long t2, const long t3) = 0;
        virtual void write(const long t1, const long t2, const long t3, const long count) = 0;
        virtual ~TripleWriter() {
        }
};

#endif /* TRIPLE_H_ */
