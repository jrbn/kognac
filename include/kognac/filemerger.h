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

#ifndef FILEMERGER_H_
#define FILEMERGER_H_

#include <kognac/lz4io.h>
#include <kognac/triple.h>

#include <boost/log/trivial.hpp>

#include <string>
#include <queue>
#include <vector>

using namespace std;

template<class K>
struct QueueEl {
    K key;
    int fileIdx;
};

template<class K>
struct QueueElCmp {
    bool operator()(const QueueEl<K> &t1, const QueueEl<K> &t2) const {
        return t1.key.greater(t2.key);
    }
};

template<class K>
class FileMerger {
protected:
    priority_queue<QueueEl<K>, vector<QueueEl<K> >, QueueElCmp<K> > queue;
    LZ4Reader **files;
    int nfiles;
    int nextFileToRead;
    long elementsRead;

    FileMerger() {}

public:
    FileMerger(vector<string> fn) {
        //Open the files
        files = new LZ4Reader*[fn.size()];
        nfiles = fn.size();
        elementsRead = 0;
        for (int i = 0; i < fn.size(); ++i) {
            files[i] = new LZ4Reader(fn[i]);
            //Read the first element and put it in the queue
            if (!files[i]->isEof()) {
                QueueEl<K> el;
                el.key.readFrom(files[i]);
                el.fileIdx = i;
                queue.push(el);
                elementsRead++;
            }
        }
        nextFileToRead = -1;
    }

    bool isEmpty() {
        return queue.empty() && nextFileToRead == -1;
    }

    K get() {
        if (nextFileToRead != -1) {
            QueueEl<K> el;
            el.key.readFrom(files[nextFileToRead]);
            el.fileIdx = nextFileToRead;
            queue.push(el);
            elementsRead++;
        }

        //Get the first triple
        QueueEl<K> el = queue.top();
        queue.pop();
        K out = el.key;

        //Replace the current element with a new one from the same file
        if (!files[el.fileIdx]->isEof()) {
            nextFileToRead = el.fileIdx;
        } else {
            nextFileToRead = -1;
        }

        return out;
    }

    virtual ~FileMerger() {
        for (int i = 0; i < nfiles; ++i) {
            delete files[i];
        }
        if (files != NULL)
            delete[] files;
    }
};

#endif /* FILEMERGER_H_ */
