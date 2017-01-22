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
#include <boost/filesystem.hpp>

#include <string>
#include <queue>
#include <vector>

using namespace std;
namespace fs = boost::filesystem;

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
        std::vector<int> extensions; //mark the current extension
        std::vector<string> input;

        FileMerger() : deletePreviousExt(false) {}

    private:
        const bool deletePreviousExt;

        //Changes LZ4Reader **files
        int loadFileWithExtension(string prefix, int part, int ext) {
            string filename = prefix + "." + to_string(ext);
            if (fs::exists(filename)) {
                files[part] = new LZ4Reader(filename);
                return ext;
            } else {
                return -1;
            }
        }

    public:
        FileMerger(vector<string> fn,
                bool considerExtensions = false,
                bool deletePreviousExt = false) :
            deletePreviousExt(deletePreviousExt) {
                //Open the files
                files = new LZ4Reader*[fn.size()];
                nfiles = fn.size();
                elementsRead = 0;
                this->input = fn;

                for (int i = 0; i < fn.size(); ++i) {
                    if (considerExtensions) {
                        const int loadedExt = loadFileWithExtension(fn[i], i, 0);
                        extensions.push_back(loadedExt);
                    } else {
                        files[i] = new LZ4Reader(fn[i]);
                        extensions.push_back(-1);
                    }

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
                if (extensions[el.fileIdx] != -1) {
                    while (true) {
                        const int currentExt = extensions[el.fileIdx];
                        delete files[el.fileIdx];
                        if (deletePreviousExt) {
                            string filename = input[el.fileIdx] + "." + to_string(currentExt);
                            fs::remove(filename);
                        }
                        const int nextExt = loadFileWithExtension(input[el.fileIdx], el.fileIdx, currentExt + 1);
                        if (nextExt == -1) {
                            nextFileToRead = -1;
                            extensions[el.fileIdx] = -1;
                            files[el.fileIdx] = NULL;
                            break;
                        } else {
                            nextFileToRead = el.fileIdx;
                            extensions[el.fileIdx] = nextExt;
                            if (!files[el.fileIdx]->isEof()) {
                                break;
                            }
                        }
                    }
                } else {
                    nextFileToRead = -1;
                }
            }

            return out;
        }

        virtual ~FileMerger() {
            for (int i = 0; i < nfiles; ++i) {
                if (files[i]) {
                    delete files[i];
                    if (deletePreviousExt) {
                        string filename = input[i] + "." + to_string(extensions[i]);
                        fs::remove(filename);
                    }
                }
            }
            if (files != NULL)
                delete[] files;
        }
};

#endif /* FILEMERGER_H_ */
