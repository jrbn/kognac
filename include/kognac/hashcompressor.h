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

#ifndef trident_hashcompressor_h
#define trident_hashcompressor_h

#include <kognac/compressor.h>
#include <kognac/lz4io.h>

struct DictPair {
    long key;
    const char *value;

    void readFrom(LZ4Reader *reader) {
        key = reader->parseLong();
        int l = 0;
        value = reader->parseString(l);
    }

    bool greater(const DictPair &p) const {
        return key > p.key;
    }
};

class HashCompressor {

private:
    void writeTermOnTmpFile(const long key, const char *term, const int l, LZ4Writer **writers, const int nwriters);

    void compressAndExtractDictionaries(int partitionId, vector<FileInfo> &input, string *tmpDictFiles, int ndicts, string *permDirs, int nperms, int signaturePerms, long *output);

    void encodeTerm(char *supportTerm, const char *term, const int l);

    void mergeDictionaries(string tmpDir, string **files, int dictID, int nfiles, string outputFile, long maxSizeToSort);

    void inmemorysort(string **inputFiles, int dictID, int nFiles, string outputFile, int &noutputFiles, const long maxSizeToSort);

    void sortAndDumpToFile(vector<std::pair<long, const char*>> *terms, string outputFile);

    void mergeFiles(string tmpPrefixFile, int nInputFiles, string outputFile);

    static bool inmemoryPairLess(std::pair<long, const char*> p1, std::pair<long, const char*> p2);

public:
    long compress(string inputDir, string tmpDir, string *permDirs, int nperms,
                  int signaturePerm, string *nameDicts, int ndicts, int threads,
                  int readThreads);
};

#endif
