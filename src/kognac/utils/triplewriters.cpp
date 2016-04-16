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

#include <kognac/triplewriters.h>
#include <kognac/sorter.h>

SimpleTripleWriter::SimpleTripleWriter(string dir, string prefixFile, bool quad) {
    writer = new LZ4Writer(dir + string("/") + prefixFile);
    writer->writeByte(quad);
}

void SimpleTripleWriter::write(const long t1, const long t2, const long t3) {
    writer->writeLong(t1);
    writer->writeLong(t2);
    writer->writeLong(t3);
}

void SimpleTripleWriter::write(const long t1, const long t2, const long t3, const long count) {
    writer->writeLong(t1);
    writer->writeLong(t2);
    writer->writeLong(t3);
    writer->writeLong(count);
}

SortedTripleWriter::SortedTripleWriter(string dir, string prefixFile,
                                       int fileSize) :
    fileSize(fileSize) {
    buffersCurrentSize = 0;
    idLastWrittenFile = -1;
    this->dir = dir;
    this->prefixFile = prefixFile;
    buffer.clear();
}

void SortedTripleWriter::write(const long t1, const long t2, const long t3) {
    write(t1, t2, t3, 0);
}

void SortedTripleWriter::write(const long t1, const long t2, const long t3, const long count) {
    Triple t;
    t.s = t1;
    t.p = t2;
    t.o = t3;
    t.count = count;
    buffer.push_back(t);
    buffersCurrentSize++;

    if (buffersCurrentSize == fileSize) {
        idLastWrittenFile++;
        string fileName = dir + string("/") + prefixFile
                          + to_string(idLastWrittenFile);
        Sorter::sortBufferAndWriteToFile(buffer, fileName);
        buffersCurrentSize = 0;
        buffer.clear();
    }
}

SortedTripleWriter::~SortedTripleWriter() {
    if (buffersCurrentSize > 0) {
        idLastWrittenFile++;
        string fileName = dir + string("/") + prefixFile
                          + to_string(idLastWrittenFile);
        Sorter::sortBufferAndWriteToFile(buffer, fileName);
    }
}
