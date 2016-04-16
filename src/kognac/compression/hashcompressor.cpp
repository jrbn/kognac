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

#include <kognac/hashcompressor.h>
#include <kognac/hashmap.h>
#include <kognac/lz4io.h>
#include <kognac/stringscol.h>
#include <kognac/hashfunctions.h>
#include <kognac/triplewriters.h>
#include <kognac/filemerger.h>

#include <string>
#include <boost/filesystem.hpp>

using namespace std;
namespace fs = boost::filesystem;

long HashCompressor::compress(string inputDir, string tmpDir, string *permDirs,
                              int nperms, int signaturePerms,
                              string *nameDicts, int ndicts, int maxThreads,
                              int readThreads) {

    vector<FileInfo> *chunks = Compressor::splitInputInChunks(inputDir, maxThreads);
    string **tmpDictFiles = new string*[maxThreads];
    for (int i = 0; i < maxThreads; ++i) {
        tmpDictFiles[i] = new string[ndicts];
        for (int j = 0; j < ndicts; ++j) {
            tmpDictFiles[i][j] = tmpDir + string("/dict-") + to_string(i) + to_string(j);
        }
    }

    /***** Compress the triples and extract (in parallel) all the terms from the input *****/
    boost::thread *threads = new boost::thread[maxThreads - 1];
    int chunksToProcess = 0;
    long *outputs = new long[maxThreads];
    for (int i = 0; i < maxThreads; ++i)
        outputs[i] = 0;

    while (chunksToProcess < maxThreads) {
        for (int i = 1; i < readThreads && (chunksToProcess + i) < maxThreads;
                ++i) {
            /***** Compress the triples *****/
            threads[i - 1] = boost::thread(
                                 boost::bind(&HashCompressor::compressAndExtractDictionaries,
                                             this, chunksToProcess + i,
                                             chunks[chunksToProcess + i],
                                             tmpDictFiles[chunksToProcess + i],
                                             ndicts, permDirs,
                                             nperms, signaturePerms,
                                             outputs + chunksToProcess + i));
        }
        compressAndExtractDictionaries(chunksToProcess, chunks[chunksToProcess],
                                       tmpDictFiles[chunksToProcess], ndicts, permDirs, nperms,
                                       signaturePerms, outputs + chunksToProcess);
        for (int i = 1; i < readThreads; ++i) {
            threads[i - 1].join();
        }
        chunksToProcess += readThreads;
    }
    delete[] threads;
    long total = 0;
    for (int i = 0; i < maxThreads; ++i) {
        total += outputs[i];
    }
    delete[] outputs;

    /***** Then I merge the dictionaries entries looking for conflicts *****/
    long maxMemAllocate = max((long) (BLOCK_SUPPORT_BUFFER_COMPR * 2), (long) (Utils::getSystemMemory() * 0.70 / ndicts));
    threads = new boost::thread[ndicts - 1];
    for (int i = 1; i < ndicts; ++i) {
        threads[i - 1] = boost::thread(boost::bind(&HashCompressor::mergeDictionaries, this, tmpDir, tmpDictFiles, i, maxThreads, nameDicts[i], maxMemAllocate));
    }
    mergeDictionaries(tmpDir, tmpDictFiles, 0, maxThreads, nameDicts[0], maxMemAllocate);
    for (int i = 1; i < ndicts; ++i) {
        threads[i - 1].join();
    }

    delete[] threads;
    for (int i = 0; i < maxThreads; ++i) {
        for (int j = 0; j < ndicts; ++j)
            fs::remove(fs::path(tmpDictFiles[i][j]));
        delete[] tmpDictFiles[i];
    }
    delete[] tmpDictFiles;

    delete[] chunks;
    return total;
}

void HashCompressor::mergeDictionaries(string tmpDir, string **files, int dictID, int nfiles, string outputFile, long maxSizeToSort) {
    //First create a temporary directory
    string dir = tmpDir + string("/tmp-sorting-pool-") + to_string(dictID);
    fs::create_directories(fs::path(dir));

    //Do an immemory sort first
    int nsortedFiles = 0;
    string tmpPrefixFile = dir + string("/tmp-file-");
    inmemorysort(files, dictID, nfiles, tmpPrefixFile, nsortedFiles, maxSizeToSort);

    //Do a merge sort
    mergeFiles(tmpPrefixFile, nsortedFiles, outputFile);

    fs::remove_all(fs::path(dir));
}

void HashCompressor::mergeFiles(string tmpPrefixFile, int nInputFiles, string outputFile) {
    vector<string> inputFiles;
    for (int  i = 0; i < nInputFiles; ++i) {
        inputFiles.push_back(tmpPrefixFile + string(".") + to_string(i));
    }
    FileMerger<DictPair> merger(inputFiles);

    long previousKey = -1;
    char *previousTerm = new char[MAX_TERM_SIZE + 2];
    int sPreviousTerm = 0;
    Utils::encode_short(previousTerm, 0, 0);

    LZ4Writer output(outputFile);
    while (!merger.isEmpty()) {
        DictPair t = merger.get();
        if (t.key != previousKey) {
            output.writeLong(t.key);
            int sTerm = Utils::decode_short(t.value);
            output.writeString(t.value, sTerm + 2);
            previousKey = t.key;
            sPreviousTerm = sTerm;
            memcpy(previousTerm, t.value, sTerm + 2);
        } else {
            //Check for conflicts
            int sTerm = Utils::decode_short(t.value);
            if (sTerm == sPreviousTerm && memcmp(t.value + 2, previousTerm + 2, sPreviousTerm - 2) != 0) {
                //Conflict!
                BOOST_LOG_TRIVIAL(warning) << "There is a conflict! This case is not implemented because it should be extremely rare. If it happens, we block the computation and return ERROR";
                exit(1);
            }
        }
    }
    delete[] previousTerm;
}

void HashCompressor::inmemorysort(string **inputFiles, int dictID, int nFiles, string outputFile, int &noutputFiles, const long maxSizeToSort) {

    noutputFiles = 0;
    StringCollection supportCollection(BLOCK_SUPPORT_BUFFER_COMPR);
    long bytesAllocated = 0;
    vector<std::pair<long, const char*>> terms;

    for (int i = 0; i < nFiles; ++i) {
        //Read the file
        string fileName = inputFiles[i][dictID];
        //Process the file
        if (fs::exists(fs::path(fileName))) {
            LZ4Reader *fis = new LZ4Reader(fileName);
            while (!fis->isEof()) {
                long key = fis->parseLong();
                int lenStr = 0;
                const char *t = fis->parseString(lenStr);

                if ((bytesAllocated + (sizeof(pair<long, const char*>) * terms.size()))
                        >= maxSizeToSort) {
                    string ofile = outputFile + string(".")
                                   + to_string(noutputFiles++);
                    sortAndDumpToFile(&terms, ofile);
                    terms.clear();
                    supportCollection.clear();
                    bytesAllocated = 0;
                }
                std::pair<long, const char*> pair = std::make_pair(key, supportCollection.addNew(t, lenStr));
                terms.push_back(pair);
                bytesAllocated += lenStr;
            }
            delete fis;
            fs::remove(fileName);
        }
    }

    if (terms.size() > 0) {
        sortAndDumpToFile(&terms, outputFile + string(".") + to_string(noutputFiles++));
    }
}

bool HashCompressor::inmemoryPairLess(std::pair<long, const char*> p1, std::pair<long, const char*> p2) {
    return p1.first < p2.first;
}

bool te(const char *p1, const char *p2) {
    int s1 = Utils::decode_short(p1);
    int s2 = Utils::decode_short(p2);
    if (s1 == s2) {
        return memcmp(p1 + 2, p2 + 2, s1 - 2);
    }
    return false;
}

void HashCompressor::sortAndDumpToFile(vector<std::pair<long, const char*>> *terms, string outputFile) {
    std::sort(terms->begin(), terms->end(), HashCompressor::inmemoryPairLess);
    LZ4Writer *outputSegment = new LZ4Writer(outputFile);
    const char *prevTerm = NULL;
    for (vector<std::pair<long, const char*>>::iterator itr = terms->begin(); itr != terms->end();
            ++itr) {
        if (prevTerm == NULL
                || !te(prevTerm, itr->second)) {
            outputSegment->writeLong(itr->first);
            outputSegment->writeString(itr->second, Utils::decode_short(itr->second) + 2);
            prevTerm = itr->second;
        }
    }
    delete outputSegment;
}

void HashCompressor::encodeTerm(char *supportTerm, const char *term, const int l) {
    Utils::encode_short(supportTerm, 0, l);
    memcpy(supportTerm + 2, term, l);
}

void HashCompressor::writeTermOnTmpFile(const long key, const char *term, const int l, LZ4Writer **writers, const int nwriters) {
    int part = key % nwriters;
    writers[part]->writeLong(key);
    writers[part]->writeString(term, l);
}

void HashCompressor::compressAndExtractDictionaries(int partitionId, vector<FileInfo> &input, string *tmpDictEntries, int ndicts, string *permDirs, int nperms, int signaturePerms, long *output) {
    LZ4Writer **writers = new LZ4Writer*[ndicts];
    for (int i  = 0; i < ndicts; ++i) {
        writers[i] = new LZ4Writer(tmpDictEntries[i]);
    }
    SimpleTripleWriter **permWriters = new SimpleTripleWriter*[nperms];
    for (int i = 0; i < nperms; ++i) {
        permWriters[i] = new SimpleTripleWriter(permDirs[i],
                                                string("input-") + to_string(partitionId), false);
    }

    LRUByteArraySet lrucache(10000, MAX_TERM_SIZE + 2);
    char *supportTerm = new char[MAX_TERM_SIZE + 2];
    long count = 0;
    long countNotValid = 0;
    int detailPerms[6];
    Compressor::parsePermutationSignature(signaturePerms, detailPerms);
    for (int i = 0; i < input.size(); ++i) {
        FileReader reader(input[i]);
        while (reader.parseTriple()) {
            if (reader.isTripleValid()) {
                count++;
                //Write subject predicate object on both spo and ops, and add data structures
                int l = 0;
                const char *t = reader.getCurrentS(l);
                long ks = Hashes::getCodeWithDefaultFunction(t, l);
                encodeTerm(supportTerm, t, l);
                if (lrucache.put(supportTerm)) {
                    writeTermOnTmpFile(ks, supportTerm, l + 2, writers, ndicts);
                }

                t = reader.getCurrentP(l);
                long kp = Hashes::getCodeWithDefaultFunction(t, l);
                encodeTerm(supportTerm, t, l);
                if (lrucache.put(supportTerm)) {
                    writeTermOnTmpFile(kp, supportTerm, l + 2, writers, ndicts);
                }

                t = reader.getCurrentO(l);
                long ko = Hashes::getCodeWithDefaultFunction(t, l);
                encodeTerm(supportTerm, t, l);
                if (lrucache.put(supportTerm)) {
                    writeTermOnTmpFile(ko, supportTerm, l + 2, writers, ndicts);
                }

                for (int i = 0; i < nperms; ++i) {
                    switch (detailPerms[i]) {
                    case IDX_SPO:
                        permWriters[i]->write(ks, kp, ko);
                        break;
                    case IDX_OPS:
                        permWriters[i]->write(ko, kp, ks);
                        break;
                    case IDX_SOP:
                        permWriters[i]->write(ks, ko, kp);
                        break;
                    case IDX_OSP:
                        permWriters[i]->write(ko, ks, kp);
                        break;
                    case IDX_PSO:
                        permWriters[i]->write(kp, ks, ko);
                        break;
                    case IDX_POS:
                        permWriters[i]->write(kp, ko, ks);
                        break;
                    }
                }
            } else {
                countNotValid++;
            }
        }
    }

    for (int i = 0; i < ndicts; ++i) {
        delete writers[i];
    }
    delete[] writers;

    for (int i = 0; i < nperms; ++i) {
        delete permWriters[i];
    }
    delete[] permWriters;

    delete[] supportTerm;
    *output = count;
    BOOST_LOG_TRIVIAL(debug) << "Compressed triples: " << count << " not valid: " << countNotValid;
}
