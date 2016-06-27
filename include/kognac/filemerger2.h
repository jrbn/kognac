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

#ifndef FILEMERGER2_H_
#define FILEMERGER2_H_

#include <kognac/filemerger.h>
#include <kognac/multimergedisklz4reader.h>

using namespace std;

template<class K>
class FileMerger2 : public FileMerger<K> {
private:
    int len;
    int start;
    MultiMergeDiskLZ4Reader *reader;

public:
    FileMerger2(MultiMergeDiskLZ4Reader *reader, int start, int len) {
        FileMerger<K>::nfiles = 0;
        FileMerger<K>::files = NULL;
        FileMerger<K>::elementsRead = 0;
        this->reader = reader;
        this->start = start;
        this->len = len;

        for (int i = 0; i < len; ++i) {
            //Read the first element and put it in the queue
            if (!reader->isEOF(start + i)) {
                QueueEl<K> el;
                el.key.readFrom(start + i, reader);
                el.fileIdx = i;
                FileMerger<K>::queue.push(el);
                FileMerger<K>::elementsRead++;
            }
        }
        FileMerger<K>::nextFileToRead = -1;
    }

    K get() {
        if (FileMerger<K>::nextFileToRead != -1) {
            QueueEl<K> el;
            el.key.readFrom(start + FileMerger<K>::nextFileToRead, reader);
            el.fileIdx = FileMerger<K>::nextFileToRead;
            FileMerger<K>::queue.push(el);
            FileMerger<K>::elementsRead++;
        }

        //Get the first triple
        QueueEl<K> el = FileMerger<K>::queue.top();
        FileMerger<K>::queue.pop();
        K out = el.key;

        //Replace the current element with a new one from the same file
        if (!reader->isEOF(start + el.fileIdx)) {
            FileMerger<K>::nextFileToRead = el.fileIdx;
        } else {
            FileMerger<K>::nextFileToRead = -1;
        }
        return out;
    }

    ~FileMerger2() {
    }
};

#endif
