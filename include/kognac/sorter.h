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

#ifndef SORTER_H_
#define SORTER_H_

#include <kognac/utils.h>
#include <kognac/lz4io.h>

#include <string>
#include <vector>

using namespace std;

class Sorter {
private:
    static void sort(vector<string> &inputFiles, int filesPerMerge,
                     string prefixOutputFiles);

    static void sortUnsortedFiles(vector<string> &inputFiles, string dir,
                                  string prefixOutputFiles, int fileSize);

public:
    static void sortBufferAndWriteToFile(vector<Triple> &vector,
                                         string fileOutput);

    static void mergeSort(string inputDir, int nThreads, bool initialSorting,
                          int recordsInitialMemorySort, int filesPerMerge);

    template<class K>
    static vector<string> sortFiles(vector<string> inputFiles,
                                    string prefixOutputFile) {
        long maxSizeToSort = max((long) (BLOCK_SUPPORT_BUFFER_COMPR * 2),
                                 (long) (Utils::getSystemMemory() * 0.70));
        int sizeEl = sizeof(K);
        long currentSize = 0;
        int idxFile = 0;

        vector<K> inmemoryContainer;
        vector<string> output;
        for (vector<string>::iterator itr = inputFiles.begin(); itr != inputFiles.end();
                itr++) {
            LZ4Reader reader(*itr);
            while (!reader.isEof()) {
                K el;
                el.readFrom(&reader);
                if (currentSize + sizeEl > maxSizeToSort) {
                    std::sort(inmemoryContainer.begin(), inmemoryContainer.end(), K::less);
                    string outputFile = prefixOutputFile + "." + to_string(idxFile++);
                    LZ4Writer writer(outputFile);
                    for (typename vector<K>::iterator itr = inmemoryContainer.begin(); itr !=
                            inmemoryContainer.end(); ++itr) {
                        itr->writeTo(&writer);
                    }
                    currentSize = 0;
                    inmemoryContainer.clear();
                    output.push_back(outputFile);
                }

                inmemoryContainer.push_back(el);
                currentSize += sizeEl;
            }
        }

        if (inmemoryContainer.size() > 0) {
            std::sort(inmemoryContainer.begin(), inmemoryContainer.end(), K::less);
            string outputFile = prefixOutputFile + "." + to_string(idxFile++);
            LZ4Writer writer(outputFile);
            for (typename vector<K>::iterator itr = inmemoryContainer.begin(); itr !=
                    inmemoryContainer.end(); ++itr) {
                itr->writeTo(&writer);
            }
            inmemoryContainer.clear();
            output.push_back(outputFile);
        }

        return output;
    }

};

#endif /* SORTER_H_ */
