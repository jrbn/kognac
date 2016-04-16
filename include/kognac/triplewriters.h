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

#ifndef _TRIPLEWRITERS_H
#define _TRIPLEWRITERS_H

#include <kognac/lz4io.h>

#include <vector>
#include <string>

class SimpleTripleWriter: public TripleWriter {
private:
    LZ4Writer *writer;
public:
    SimpleTripleWriter(string dir, string prefixFile, bool quad);

    void write(const long t1, const long t2, const long t3);

    void write(const long t1, const long t2, const long t3, const long count);

    ~SimpleTripleWriter() {
        delete writer;
    }
};

class SortedTripleWriter: public TripleWriter {
private:
    const int fileSize;
    vector<Triple> buffer;
    int buffersCurrentSize;
    int idLastWrittenFile;
    string dir;
    string prefixFile;

public:
    SortedTripleWriter(string dir, string prefixFile, int fileSize);

    void write(const long t1, const long t2, const long t3);

    void write(const long t1, const long t2, const long t3, const long count);

    ~SortedTripleWriter();
};

#endif
