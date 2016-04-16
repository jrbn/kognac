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

#include <kognac/stringscol.h>

#include <cstring>

void StringCollection::clear() {
    currentIdx = 0;
    currentPos = 0;
}

void StringCollection::deallocate() {
    for (int i = 0; i < pool.size(); ++i) {
        delete[] pool[i];
    }
    pool.clear();
}

const char *StringCollection::addNew(const char *text, int size) {
    if ((segmentSize - currentPos) < size) {
        //Create new segment.
        if (currentIdx == pool.size() - 1) {
            char *newsegment = new char[segmentSize];
            pool.push_back(newsegment);
        }
        currentIdx++;
        currentPos = 0;
    }
    char *segment = pool[currentIdx];
    int startPos = currentPos;
    memcpy(segment + currentPos, text, size);
//  segment[currentPos + size] = '\0';
    currentPos += size;
    return segment + startPos;
}

long StringCollection::allocatedBytes() {
    return (long)pool.size() * (long)segmentSize;
}

long StringCollection::occupiedBytes() {
    return (long)currentIdx * segmentSize + currentPos;
}

StringCollection::~StringCollection() {
    deallocate();
}
