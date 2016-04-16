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

#ifndef STRINGSCOL_H_
#define STRINGSCOL_H_

#include <vector>

class StringCollection {
private:
    const int segmentSize;
    int currentPos;
    int currentIdx;
    std::vector<char *> pool;

public:
    StringCollection(int segmentSize) :
        segmentSize(segmentSize) {
        currentPos = 0;
        currentIdx = 0;
        char *newsegment = new char[segmentSize];
        pool.push_back(newsegment);
    }

    void clear();

    void deallocate();

    const char *addNew(const char *text, int size);

    long allocatedBytes();

    long occupiedBytes();

    ~StringCollection();
};

#endif /* STRINGSCOL_H_ */
