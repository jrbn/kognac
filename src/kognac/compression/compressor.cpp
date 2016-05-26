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

#include <kognac/compressor.h>
#include <kognac/filereader.h>
#include <kognac/schemaextractor.h>
#include <kognac/triplewriters.h>
#include <kognac/utils.h>
#include <kognac/hashfunctions.h>
#include <kognac/factory.h>
#include <kognac/lruset.h>
#include <kognac/flajolet.h>
#include <kognac/stringscol.h>
#include <kognac/MisraGries.h>
#include <kognac/sorter.h>
#include <kognac/filemerger.h>

#include <iostream>
#include <utility>
#include <cstdlib>

#include <boost/lexical_cast.hpp>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>
#include <boost/thread.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/chrono.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/lexical_cast.hpp>

namespace fs = boost::filesystem;
namespace timens = boost::chrono;
using namespace std;

bool lessTermFrequenciesDesc(const std::pair<string, long> &p1,
                             const std::pair<string, long> &p2) {
    return p1.second > p2.second || (p1.second == p2.second && p1.first > p2.first);
}

bool sampledTermsSorter1(const std::pair<string, size_t> &p1,
                         const std::pair<string, size_t> &p2) {
    /*    int l1 = Utils::decode_short(p1.first);
        int l2 = Utils::decode_short(p2.first);
        int ret = memcmp(p1.first + 2, p2.first + 2, min(l1, l2));
        if (ret == 0) {
            return (l1 - l2) < 0;
        } else {
            return ret < 0;
        }*/
    return p1.first < p2.first;
}

bool sampledTermsSorter2(const std::pair<string, size_t> &p1,
                         const std::pair<string, size_t> &p2) {
    return p1.second > p2.second;
}

bool cmpSampledTerms(const char* s1, const char* s2) {
    if (s1 == s2) {
        return true;
    }
    if (s1 && s2 && s1[0] == s2[0] && s1[1] == s2[1]) {
        int l = Utils::decode_short(s1, 0);
        return Utils::compare(s1, 2, 2 + l, s2, 2, 2 + l) == 0;
    }
    return false;
}

struct cmpInfoFiles {
    bool operator()(FileInfo i, FileInfo j) {
        return (i.size > j.size);
    }
} cmpInfoFiles;

Compressor::Compressor(string input, string kbPath) :
    input(input), kbPath(kbPath) {
    tmpFileNames = NULL;
    finalMap = new ByteArrayToNumberMap();
    poolForMap = new StringCollection(64 * 1024 * 1024);
    totalCount = 0;
    nTerms = 0;
    dictFileNames = NULL;
    uncommonDictFileNames = NULL;
}

void Compressor::addPermutation(const int permutation, int &output) {
    output |= 1 << permutation;
}

void Compressor::parsePermutationSignature(int signature, int *output) {
    int p = 0, idx = 0;
    while (signature) {
        if (signature & 1) {
            output[idx++] = p;
        }
        signature = signature >> 1;
        p++;
    }
}

void Compressor::uncompressTriples(ParamsUncompressTriples params) {
    vector<FileInfo> &files = params.files;
    Hashtable *table1 = params.table1;
    Hashtable *table2 = params.table2;
    Hashtable *table3 = params.table3;
    string outFile = params.outFile;
    SchemaExtractor *extractor = params.extractor;
    long *distinctValues = params.distinctValues;
    std::vector<string> *resultsMGS = params.resultsMGS;
    size_t sizeHeap = params.sizeHeap;

    LZ4Writer out(outFile);
    long count = 0;
    long countNotValid = 0;
    const char *supportBuffer = NULL;

    char *supportBuffers[3];
    if (extractor != NULL) {
        supportBuffers[0] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[1] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[2] = new char[MAX_TERM_SIZE + 2];
    }

    MG *heap = NULL;
    if (resultsMGS != NULL) {
        heap = new MG(sizeHeap);
    }

    FlajoletMartin estimator;

    for (int i = 0; i < files.size(); ++i) {
        FileReader reader(files[i]);
        while (reader.parseTriple()) {
            if (reader.isTripleValid()) {
                count++;

                if (heap != NULL && count % 5000 == 0) {
                    //Dump the heap
                    std::vector<string> lt = heap->getPositiveTerms();
                    std::copy(lt.begin(), lt.end(), std::back_inserter(*resultsMGS));
                }

                int length;
                supportBuffer = reader.getCurrentS(length);

                if (heap != NULL) {
                    heap->add(supportBuffer, length);
                }

                if (extractor != NULL) {
                    Utils::encode_short(supportBuffers[0], length);
                    memcpy(supportBuffers[0] + 2, supportBuffer, length);
                }
                long h1 = table1->add(supportBuffer, length);
                long h2 = table2->add(supportBuffer, length);
                long h3 = table3->add(supportBuffer, length);
                out.writeByte(0);
                estimator.addElement(h1, h2, h3);

                //This is an hack to save memcpy...
                out.writeVLong(length + 2);
                out.writeShort(length);
                out.writeRawArray(supportBuffer, length);

                supportBuffer = reader.getCurrentP(length);

                if (heap != NULL) {
                    heap->add(supportBuffer, length);
                }

                if (extractor != NULL) {
                    Utils::encode_short(supportBuffers[1], length);
                    memcpy(supportBuffers[1] + 2, supportBuffer, length);
                }
                h1 = table1->add(supportBuffer, length);
                h2 = table2->add(supportBuffer, length);
                h3 = table3->add(supportBuffer, length);
                out.writeByte(0);
                estimator.addElement(h1, h2, h3);

                out.writeVLong(length + 2);
                out.writeShort(length);
                out.writeRawArray(supportBuffer, length);

                supportBuffer = reader.getCurrentO(length);

                if (heap != NULL) {
                    heap->add(supportBuffer, length);
                }

                if (extractor != NULL) {
                    Utils::encode_short(supportBuffers[2], length);
                    memcpy(supportBuffers[2] + 2, supportBuffer, length);
                    extractor->extractSchema(supportBuffers);
                }
                h1 = table1->add(supportBuffer, length);
                h2 = table2->add(supportBuffer, length);
                h3 = table3->add(supportBuffer, length);
                out.writeByte(0);
                estimator.addElement(h1, h2, h3);

                out.writeVLong(length + 2);
                out.writeShort(length);
                out.writeRawArray(supportBuffer, length);
            } else {
                countNotValid++;
            }
        }
    }

    *distinctValues = estimator.estimateCardinality();

    if (extractor != NULL) {
        delete[] supportBuffers[0];
        delete[] supportBuffers[1];
        delete[] supportBuffers[2];
    }

    if (heap != NULL) {
        std::vector<string> lt = heap->getPositiveTerms();
        std::copy(lt.begin(), lt.end(), std::back_inserter(*resultsMGS));
        delete heap;
    }

    BOOST_LOG_TRIVIAL(debug) << "Parsed triples: " << count << " not valid: " << countNotValid;
}

void Compressor::sampleTerm(const char *term, int sizeTerm, int sampleArg,
                            int dictPartitions, GStringToNumberMap *map/*,
                            LRUSet *duplicateCache, LZ4Writer **dictFile*/) {
    if (abs(random() % 10000) < sampleArg) {
        GStringToNumberMap::iterator itr = map->find(string(term + 2, sizeTerm - 2));
        if (itr != map->end()) {
            itr->second = itr->second + 1;
        } else {
            //char *newTerm = new char[sizeTerm];
            //memcpy(newTerm, (const char*) term, sizeTerm);
            map->insert(std::make_pair(string(term + 2, sizeTerm - 2), 1));
        }
    } else {
        /*if (!duplicateCache->exists(term)) {
            AnnotatedTerm t;
            t.term = term;
            t.size = sizeTerm;
            t.tripleIdAndPosition = -1;

            //Which partition?
            int partition = Utils::getPartition(term + 2, sizeTerm - 2,
                                                dictPartitions);
            //Add it into the file
            t.writeTo(dictFile[partition]);
            //Add it into the map
            duplicateCache->add(term);
        }*/
    }
}

#ifdef COUNTSKETCH

void Compressor :: uncompressTriplesForMGCS (vector<FileInfo> &files, MG *heap, CountSketch *cs, string outFile,
        SchemaExtractor *extractor, long *distinctValues) {
    LZ4Writer out(outFile);
    long count = 0;
    long countNotValid = 0;
    const char *supportBuffer = NULL;

    char *supportBuffers[3];
    if (extractor != NULL) {
        supportBuffers[0] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[1] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[2] = new char[MAX_TERM_SIZE + 2];
    }

    FlajoletMartin estimator;

    for (int i = 0; i < files.size(); ++i) {
        FileReader reader(files[i]);
        while (reader.parseTriple()) {
            if (reader.isTripleValid()) {
                count++;
                int length;

                // Read S
                supportBuffer = reader.getCurrentS(length);

                if (extractor != NULL) {
                    Utils::encode_short(supportBuffers[0], length);
                    memcpy(supportBuffers[0] + 2, supportBuffer, length);
                }
                // Add to Misra-Gries heap
                heap->add(supportBuffer, length);

                // Add to Count Sketch and get the 3 keys for estimation
                long h1, h2, h3;
                cs->add(supportBuffer, length, h1, h2, h3);


                out.writeByte(0);
                estimator.addElement(h1, h2, h3);

                //This is an hack to save memcpy...
                out.writeVLong(length + 2);
                out.writeShort(length);
                out.writeRawArray(supportBuffer, length);


                // Read P
                supportBuffer = reader.getCurrentP(length);
                if (extractor != NULL) {
                    Utils::encode_short(supportBuffers[1], length);
                    memcpy(supportBuffers[1] + 2, supportBuffer, length);
                }

                // Add to Misra-Gries heap
                heap->add(supportBuffer, length);

                // Add to COunt Sketch and get the 3 keys for estimation
                cs->add(supportBuffer, length, h1, h2, h3);

                out.writeByte(0);
                estimator.addElement(h1, h2, h3);

                out.writeVLong(length + 2);
                out.writeShort(length);
                out.writeRawArray(supportBuffer, length);


                // Read O
                supportBuffer = reader.getCurrentO(length);
                if (extractor != NULL) {
                    Utils::encode_short(supportBuffers[2], length);
                    memcpy(supportBuffers[2] + 2, supportBuffer, length);
                    extractor->extractSchema(supportBuffers);
                }

                // Add to Misra-Gries heap
                heap->add(supportBuffer, length);

                // Add to COunt Sketch and get the 3 keys for estimation
                cs->add(supportBuffer, length, h1, h2, h3);

                out.writeByte(0);
                estimator.addElement(h1, h2, h3);

                out.writeVLong(length + 2);
                out.writeShort(length);
                out.writeRawArray(supportBuffer, length);
            } else {
                countNotValid++;
            }
        }
    }

    *distinctValues = estimator.estimateCardinality();

    if (extractor != NULL) {
        delete[] supportBuffers[0];
        delete[] supportBuffers[1];
        delete[] supportBuffers[2];
    }

    BOOST_LOG_TRIVIAL(debug) << "Parsed triples: " << count << " not valid: " << countNotValid;
}

void Compressor :: extractTermsForMGCS (ParamsExtractCommonTermProcedure params, const set<string>& freq, const CountSketch *cs) {
    string inputFile = params.inputFile;
    GStringToNumberMap *map = params.map;
    int dictPartitions = params.dictPartitions;
    string *dictFileName = params.dictFileName;
    //int maxMapSize = params.maxMapSize;

    long tripleId = params.idProcess;
    int pos = 0;
    int parallelProcesses = params.parallelProcesses;
    string *udictFileName = params.singleTerms;
    const bool copyHashes = params.copyHashes;

    LZ4Reader reader(inputFile);
    map->set_empty_key(EMPTY_KEY);
    map->set_deleted_key(DELETED_KEY);

    //Create an array of files.
    LZ4Writer **dictFile = new LZ4Writer*[dictPartitions];
    LZ4Writer **udictFile = new LZ4Writer*[dictPartitions];
    for (int i = 0; i < dictPartitions; ++i) {
        dictFile[i] = new LZ4Writer(dictFileName[i]);
        udictFile[i] = new LZ4Writer(udictFileName[i]);
    }

    unsigned long countInfrequent = 0, countFrequent = 0;


    char *prevEntries[3];
    int sPrevEntries[3];
    if (copyHashes) {
        prevEntries[0] = new char[MAX_TERM_SIZE + 2];
        prevEntries[1] = new char[MAX_TERM_SIZE + 2];
    } else {
        prevEntries[0] = prevEntries[1] = NULL;
    }

    while (!reader.isEof()) {
        int sizeTerm = 0;
        reader.parseByte(); //Ignore it. Should always be 0
        const char *term = reader.parseString(sizeTerm);

        if (copyHashes) {
            if (pos != 2) {
                memcpy(prevEntries[pos], term, sizeTerm);
                sPrevEntries[pos] = sizeTerm;
            } else {
                prevEntries[2] = (char*)term;
                sPrevEntries[2] = sizeTerm;

                extractTermForMGCS(prevEntries[0], sPrevEntries[0], countFrequent, countInfrequent,
                                   dictPartitions, copyHashes, tripleId, 0, prevEntries,
                                   sPrevEntries, dictFile, udictFile, freq, cs);

                extractTermForMGCS(prevEntries[1], sPrevEntries[1], countFrequent, countInfrequent,
                                   dictPartitions, copyHashes, tripleId, 1, prevEntries,
                                   sPrevEntries, dictFile, udictFile, freq, cs);

                extractTermForMGCS(term, sizeTerm, countFrequent, countInfrequent, dictPartitions,
                                   copyHashes, tripleId, pos, prevEntries, sPrevEntries, dictFile,
                                   udictFile, freq, cs);
            }
        } else {
            extractTermForMGCS(term, sizeTerm, countFrequent, countInfrequent, dictPartitions,
                               copyHashes, tripleId, pos, prevEntries, sPrevEntries, dictFile,
                               udictFile, freq, cs);
        }

        pos = (pos + 1) % 3;
        if (pos == 0) {
            tripleId += parallelProcesses;
        }
    }

    BOOST_LOG_TRIVIAL(debug) << "Hashtable size after extraction " << map->size() << ". Frequent terms " << countFrequent << " infrequent " << countInfrequent;

    if (copyHashes) {
        delete[] prevEntries[0];
        delete[] prevEntries[1];
    }

    for (int i = 0; i < dictPartitions; ++i) {
        delete dictFile[i];
        delete udictFile[i];
    }

    delete[] dictFile;
    delete[] udictFile;
}


void Compressor :: extractTermForMGCS(const char *term, const int sizeTerm, unsigned long& countFreq, unsigned long& countInfrequent,
                                      const int dictPartition, const bool copyHashes, const long tripleId, const int pos,
                                      char **prevEntries, int *sPrevEntries, LZ4Writer **dictFile, LZ4Writer **udictFile,
                                      const set<string>& freq, const CountSketch *cs) {
    char *trm = new char[sizeTerm - 1];
    memcpy(trm, term + 2, sizeTerm - 2);
    trm[sizeTerm - 2] = '\0';
    string str(trm);

    if (freq.find(str) == freq.end()) { // Term infrequent
        countInfrequent++;
        int partition = Utils::getPartition(term + 2, sizeTerm - 2, dictPartition);
        AnnotatedTerm t;
        t.size = sizeTerm;
        t.term = term;
        t.tripleIdAndPosition = (long) (tripleId << 2) + (pos & 0x3);

        if (!copyHashes) {
            //Add it into the file
            t.useHashes = false;
        } else {
            //Output the three pairs
            t.useHashes = true;
            if (pos == 0) {
                long hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                long hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashp;
                t.hashT2 = hasho;
            } else if (pos == 1) {
                long hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                long hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hasho;
            } else {
                //pos = 2
                long hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                long hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hashp;
            }
        }

        t.writeTo(udictFile[partition]);
    }
}
#endif

void Compressor::uncompressAndSampleTriples(vector<FileInfo> &files,
        string outFile, string *dictFileName, int dictPartitions, int sampleArg,
        GStringToNumberMap *map, SchemaExtractor *extractor) {
    //Triples
    LZ4Writer out(outFile);
    map->set_empty_key(EMPTY_KEY);

    char *supportBuffers[3];
    if (extractor != NULL) {
        supportBuffers[0] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[1] = new char[MAX_TERM_SIZE + 2];
        supportBuffers[2] = new char[MAX_TERM_SIZE + 2];
    }

    long count = 0;
    long countNotValid = 0;
    const char *supportBuffer = NULL;
    char *supportBuffer2 = new char[MAX_TERM_SIZE];
    for (int i = 0; i < files.size(); ++i) {
        FileReader reader(files[i]);
        while (reader.parseTriple()) {
            if (reader.isTripleValid()) {
                count++;

                int length;
                supportBuffer = reader.getCurrentS(length);
                Utils::encode_short(supportBuffer2, length);
                memcpy(supportBuffer2 + 2, supportBuffer, length);

                out.writeByte(0);
                out.writeString(supportBuffer2, length + 2);

                sampleTerm(supportBuffer2, length + 2, sampleArg,
                           dictPartitions, map/*, &duplicateCache, dictFile*/);

                if (extractor != NULL) {
                    memcpy(supportBuffers[0], supportBuffer2, length + 2);
                }

                supportBuffer = reader.getCurrentP(length);
                Utils::encode_short(supportBuffer2, length);
                memcpy(supportBuffer2 + 2, supportBuffer, length);

                out.writeByte(0);
                out.writeString(supportBuffer2, length + 2);

                sampleTerm(supportBuffer2, length + 2, sampleArg,
                           dictPartitions, map/*, &duplicateCache, dictFile*/);

                if (extractor != NULL) {
                    memcpy(supportBuffers[1], supportBuffer2, length + 2);
                }


                supportBuffer = reader.getCurrentO(length);
                Utils::encode_short(supportBuffer2, length);
                memcpy(supportBuffer2 + 2, supportBuffer, length);

                out.writeByte(0);
                out.writeString(supportBuffer2, length + 2);

                sampleTerm(supportBuffer2, length + 2, sampleArg,
                           dictPartitions, map/*, &duplicateCache, dictFile*/);

                if (extractor != NULL) {
                    memcpy(supportBuffers[2], supportBuffer2, length + 2);
                    extractor->extractSchema(supportBuffers);
                }

            } else {
                countNotValid++;
            }
        }
    }

    if (extractor != NULL) {
        delete[] supportBuffers[0];
        delete[] supportBuffers[1];
        delete[] supportBuffers[2];
    }

    delete[] supportBuffer2;
    BOOST_LOG_TRIVIAL(debug) << "Parsed triples: " << count << " not valid: " << countNotValid;
}

void Compressor::extractUncommonTerm(const char *term, const int sizeTerm,
                                     ByteArrayToNumberMap *map,
                                     LZ4Writer **udictFile,
                                     const long tripleId,
                                     const int pos,
                                     const int partitions,
                                     const bool copyHashes,
                                     char **prevEntries, int *sPrevEntries) {

    if (map->find(term) == map->end()) {
        const int partition = Utils::getPartition(term + 2, sizeTerm - 2,
                              partitions);
        AnnotatedTerm t;
        t.size = sizeTerm;
        t.term = term;
        t.tripleIdAndPosition = (long) (tripleId << 2) + (pos & 0x3);

        if (!copyHashes) {
            //Add it into the file
            t.useHashes = false;
        } else {
            //Output the three pairs
            t.useHashes = true;
            if (pos == 0) {
                long hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                long hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashp;
                t.hashT2 = hasho;
            } else if (pos == 1) {
                long hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                long hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hasho;
            } else { //pos = 2
                long hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                long hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hashp;
            }
        }
        t.writeTo(udictFile[partition]);
    }
}

unsigned long Compressor::getEstimatedFrequency(const string &e) const {
    long v1 = table1 ? table1->get(e.c_str(), e.size()) : 0;
    long v2 = table2 ? table2->get(e.c_str(), e.size()) : 0;
    long v3 = table3 ? table3->get(e.c_str(), e.size()) : 0;
    return min(v1, min(v2, v3));
}

void Compressor::extractCommonTerm(const char* term, const int sizeTerm,
                                   long &countFrequent,
                                   const long thresholdForUncommon, Hashtable *table1,
                                   Hashtable *table2, Hashtable *table3,
                                   const int dictPartitions,
                                   long &minValueToBeAdded,
                                   const long maxMapSize,  GStringToNumberMap *map,

                                   std::priority_queue<std::pair<string, long>,
                                   std::vector<std::pair<string, long> >,
                                   priorityQueueOrder> &queue) {
    long v1, v2, v3;
    bool v2Checked = false;
    bool v3Checked = false;

    bool valueHighEnough = false;
    bool termInfrequent = false;
    v1 = table1->get(term + 2, sizeTerm - 2);
    long minValue = -1;
    if (v1 < thresholdForUncommon) {
        termInfrequent = true;
    } else {
        v2 = table2->get(term + 2, sizeTerm - 2);
        v2Checked = true;
        if (v2 < thresholdForUncommon) {
            termInfrequent = true;
        } else {
            v3 = table3->get(term + 2, sizeTerm - 2);
            v3Checked = true;
            if (v3 < thresholdForUncommon) {
                termInfrequent = true;
            } else {
                minValue = min(v1, min(v2, v3));
                valueHighEnough = minValue > minValueToBeAdded;
            }
        }
    }

    /* if (termInfrequent) {
        countInfrequent++;
        int partition = Utils::getPartition(term + 2, sizeTerm - 2,
                                            dictPartitions);
        AnnotatedTerm t;
        t.size = sizeTerm;
        t.term = term;
        t.tripleIdAndPosition = (long) (tripleId << 2) + (pos & 0x3);

        if (!copyHashes) {
            //Add it into the file
            t.useHashes = false;
        } else {
            //Output the three pairs
            t.useHashes = true;
            if (pos == 0) {
                long hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                long hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashp;
                t.hashT2 = hasho;
            } else if (pos == 1) {
                long hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                long hasho = Hashes::murmur3_56(prevEntries[2] + 2, sPrevEntries[2] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hasho;
            } else { //pos = 2
                long hashs = Hashes::murmur3_56(prevEntries[0] + 2, sPrevEntries[0] - 2);
                long hashp = Hashes::murmur3_56(prevEntries[1] + 2, sPrevEntries[1] - 2);
                t.hashT1 = hashs;
                t.hashT2 = hashp;
            }
        }
        t.writeTo(udictFile[partition]);
    } else {*/
    countFrequent++;
    bool mapTooSmall = map->size() < maxMapSize;
    if ((mapTooSmall || valueHighEnough)
            && map->find(string(term + 2, sizeTerm - 2)) == map->end()) {
        //Create copy
        //char *newTerm = new char[sizeTerm];
        //memcpy(newTerm, term, sizeTerm);

        std::pair<string, long> pair = std::make_pair(string(term + 2, sizeTerm - 2),
                                       minValue);
        map->insert(pair);
        queue.push(pair);
        if (map->size() > maxMapSize) {
            //Replace term and minCount with values to be added
            std::pair<string, long> elToRemove = queue.top();
            queue.pop();
            map->erase(elToRemove.first);
            /*//Insert value into the dictionary
            if (!duplicateCache.exists(elToRemove.first)) {
                //Which partition?
                int partition = Utils::getPartition(
                                    elToRemove.first + 2,
                                    Utils::decode_short(elToRemove.first),
                                    dictPartitions);

                AnnotatedTerm t;
                t.size = Utils::decode_short((char*) elToRemove.first) + 2;
                t.term = elToRemove.first;
                t.tripleIdAndPosition = -1;
                t.writeTo(dictFile[partition]);

                //Add it into the map
                duplicateCache.add(elToRemove.first);
            }*/
            minValueToBeAdded = queue.top().second;
        }
    }/* else {
        //Copy the term in a file so that later it can be inserted in the dictionaries
        if (!duplicateCache.exists(term + 2, sizeTerm - 2)) {
            //Which partition?
            int partition = Utils::getPartition(term + 2,
                                                sizeTerm - 2, dictPartitions);

            AnnotatedTerm t;
            t.size = sizeTerm;
            t.term = term;
            t.tripleIdAndPosition = -1;
            t.writeTo(dictFile[partition]);

            //Add it into the map
            duplicateCache.add(term + 2, sizeTerm - 2);
        }
    }
    }*/
}

void Compressor::extractUncommonTerms(const int dictPartitions, string inputFile,
                                      const bool copyHashes, const int idProcess,
                                      const int parallelProcesses,
                                      string *udictFileName,
                                      const bool splitByHash) {

    //Either one or the other. Both are not supported in extractUncommonTerm
    assert(!splitByHash || dictPartitions == 1);

    int partitions = dictPartitions;
    if (splitByHash)
        partitions = partitions * parallelProcesses;

    LZ4Writer **udictFile = new LZ4Writer*[partitions];
    if (splitByHash) {
        string prefixFile;
        for (int i = 0; i < partitions; ++i) {
            const int modHash =  i % parallelProcesses;
            if (modHash == 0) {
                prefixFile = udictFileName[i / parallelProcesses];
            }
            udictFile[i] = new LZ4Writer(prefixFile + string(".") +
                                         to_string(modHash));
        }
    } else {
        for (int i = 0; i < partitions; ++i) {
            udictFile[i] = new LZ4Writer(udictFileName[i]);
        }
    }

    LZ4Reader reader(inputFile);
    char *prevEntries[3];
    int sPrevEntries[3];
    if (copyHashes) {
        prevEntries[0] = new char[MAX_TERM_SIZE + 2];
        prevEntries[1] = new char[MAX_TERM_SIZE + 2];
    } else {
        prevEntries[0] = prevEntries[1] = NULL;
    }
    long tripleId = idProcess;
    int pos = 0;

    while (!reader.isEof()) {
        int sizeTerm = 0;
        int flag = reader.parseByte(); //Ignore it. Should always be 0
        if (flag != 0) {
            BOOST_LOG_TRIVIAL(error) << "Flag should always be zero!";
            throw 10;
        }

        const char *term = reader.parseString(sizeTerm);
        if (copyHashes) {
            if (pos != 2) {
                memcpy(prevEntries[pos], term, sizeTerm);
                sPrevEntries[pos] = sizeTerm;
            } else {
                prevEntries[2] = (char*)term;
                sPrevEntries[2] = sizeTerm;

                extractUncommonTerm(prevEntries[0], sPrevEntries[0], finalMap,
                                    udictFile, tripleId, 0,
                                    (splitByHash) ? parallelProcesses : dictPartitions, copyHashes,
                                    prevEntries, sPrevEntries);

                extractUncommonTerm(prevEntries[1], sPrevEntries[1], finalMap,
                                    udictFile, tripleId, 1,
                                    (splitByHash) ? parallelProcesses : dictPartitions, copyHashes,
                                    prevEntries, sPrevEntries);

                extractUncommonTerm(term, sizeTerm, finalMap,
                                    udictFile, tripleId, pos,
                                    (splitByHash) ? parallelProcesses : dictPartitions, copyHashes,
                                    prevEntries, sPrevEntries);
            }
        } else {
            extractUncommonTerm(term, sizeTerm, finalMap,
                                udictFile, tripleId, pos,
                                (splitByHash) ? parallelProcesses : dictPartitions, copyHashes,
                                prevEntries, sPrevEntries);
        }

        pos = (pos + 1) % 3;
        if (pos == 0) {
            tripleId += parallelProcesses;
        }
    }

    if (copyHashes) {
        delete[] prevEntries[0];
        delete[] prevEntries[1];
    }

    for (int i = 0; i < partitions; ++i) {
        delete udictFile[i];
    }
    delete[] udictFile;
}

void Compressor::extractCommonTerms(ParamsExtractCommonTermProcedure params) {

    string inputFile = params.inputFile;
    Hashtable **tables = params.tables;
    GStringToNumberMap *map = params.map;
    int dictPartitions = params.dictPartitions;
    //string *dictFileName = params.dictFileName;
    int maxMapSize = params.maxMapSize;

    int pos = 0;
    //int parallelProcesses = params.parallelProcesses;
    //string *udictFileName = params.singleTerms;
    int thresholdForUncommon = params.thresholdForUncommon;
    //const bool copyHashes = params.copyHashes;

    Hashtable *table1 = tables[0];
    Hashtable *table2 = tables[1];
    Hashtable *table3 = tables[2];

    LZ4Reader reader(inputFile);
    map->set_empty_key(EMPTY_KEY);
    map->set_deleted_key(DELETED_KEY);

    long minValueToBeAdded = 0;
    std::priority_queue<std::pair<string, long>,
        std::vector<std::pair<string, long> >, priorityQueueOrder> queue;

    long countFrequent = 0;

    while (!reader.isEof()) {
        int sizeTerm = 0;
        reader.parseByte(); //Ignore it. Should always be 0
        const char *term = reader.parseString(sizeTerm);

        extractCommonTerm(term, sizeTerm, countFrequent,
                          thresholdForUncommon, table1, table2, table3,
                          dictPartitions, minValueToBeAdded,
                          maxMapSize, map, queue);

        pos = (pos + 1) % 3;
    }

    BOOST_LOG_TRIVIAL(debug) << "Hashtable size after extraction " << map->size() << ". Frequent terms " << countFrequent;

}

void Compressor::mergeCommonTermsMaps(ByteArrayToNumberMap *finalMap,
                                      GStringToNumberMap *maps, int nmaps) {
    char supportTerm[MAX_TERM_SIZE];
    for (int i = 0; i < nmaps; i++) {
        for (GStringToNumberMap::iterator itr = maps[i].begin();
                itr != maps[i].end(); ++itr) {
            Utils::encode_short(supportTerm, itr->first.size());
            memcpy(supportTerm + 2, itr->first.c_str(), itr->first.size());

            ByteArrayToNumberMap::iterator foundValue = finalMap->find(supportTerm);
            if (foundValue == finalMap->end()) {
                const char *newkey = poolForMap->addNew(supportTerm,
                                                        Utils::decode_short(supportTerm) + 2);
                finalMap->insert(std::make_pair(newkey, itr->second));
            }
        }
        maps[i].clear();
    }
}

bool comparePairs(std::pair<const char *, long> i,
                  std::pair<const char *, long> j) {
    if (i.second > j.second) {
        return true;
    } else if (i.second == j.second) {
        int s1 = Utils::decode_short((char*) i.first);
        int s2 = Utils::decode_short((char*) j.first);
        return Utils::compare(i.first, 2, s1 + 2, j.first, 2, s2 + 2) > 0;
    } else {
        return false;
    }
}

void Compressor::assignNumbersToCommonTermsMap(ByteArrayToNumberMap *map,
        long *counters, LZ4Writer **writers, LZ4Writer **invWriters,
        int ndictionaries, bool preserveMapping) {
    std::vector<std::pair<const char *, long> > pairs;
    for (ByteArrayToNumberMap::iterator itr = map->begin(); itr != map->end();
            ++itr) {
        std::pair<const char *, long> pair;
        pair.first = itr->first;
        pair.second = itr->second;
        pairs.push_back(pair);

#ifdef DEBUG
        /*        const char* text = SchemaExtractor::support.addNew(itr->first, Utils::decode_short(itr->first) + 2);
                long hash = Hashes::murmur3_56(itr->first + 2, Utils::decode_short(itr->first));
                SchemaExtractor::properties.insert(make_pair(hash, text));*/
#endif
    }
    std::sort(pairs.begin(), pairs.end(), &comparePairs);

    int counterIdx = 0;
    for (int i = 0; i < pairs.size(); ++i) {
        nTerm key;
        ByteArrayToNumberMap::iterator itr = map->find(pairs[i].first);
        int size = Utils::decode_short(pairs[i].first) + 2;
        int part = Utils::getPartition(pairs[i].first + 2,
                                       Utils::decode_short(pairs[i].first), ndictionaries);

        if (preserveMapping) {
            key = counters[part];
            counters[part] += ndictionaries;
        } else {
            key = counters[counterIdx];
            counters[counterIdx] += ndictionaries;
            counterIdx = (counterIdx + 1) % ndictionaries;
        }
        writers[part]->writeLong(key);
        writers[part]->writeString(pairs[i].first, size);
        itr->second = key;

        //This is needed for the smart compression
        if (invWriters != NULL) {
            int part2 = key % ndictionaries;
            invWriters[part2]->writeLong(key);
            invWriters[part2]->writeString(pairs[i].first, size);
        }
    }
}

void Compressor::newCompressTriples(ParamsNewCompressProcedure params) {
    //Read the file in input
    string in = params.inNames[params.part];
    fs::path pFile(in);
    fs::path pNewFile = pFile;
    pNewFile.replace_extension(to_string(params.itrN));
    string newFile = pNewFile.string();
    long compressedTriples = 0;
    long compressedTerms = 0;
    long uncompressedTerms = 0;
    LZ4Reader *uncommonTermsReader = NULL;

    long nextTripleId = -1;
    int nextPos = -1;
    long nextTerm = -1;
    if (params.uncommonTermsFile != NULL) {
        uncommonTermsReader = new LZ4Reader(*(params.uncommonTermsFile));
        if (!uncommonTermsReader->isEof()) {
            long tripleId = uncommonTermsReader->parseLong();
            nextTripleId = tripleId >> 2;
            nextPos = tripleId & 0x3;
            nextTerm = uncommonTermsReader->parseLong();
        } else {
            BOOST_LOG_TRIVIAL(warning) << "The file " << *(params.uncommonTermsFile) << " is empty";
        }
    } else {
        BOOST_LOG_TRIVIAL(debug) << "No uncommon file is provided";
    }

    long currentTripleId = params.part;
    int increment = params.parallelProcesses;

    if (fs::exists(pFile) && fs::file_size(pFile) > 0) {
        LZ4Reader r(pFile.string());
        LZ4Writer w(newFile);

        long triple[3];
        char *tTriple = new char[MAX_TERM_SIZE * 3];
        bool valid[3];

        SimpleTripleWriter **permWriters = new SimpleTripleWriter*[params.nperms];
        const int nperms = params.nperms;
        int detailPerms[6];
        Compressor::parsePermutationSignature(params.signaturePerms, detailPerms);
        for (int i = 0; i < nperms; ++i) {
            permWriters[i] = new SimpleTripleWriter(params.permDirs[i],
                                                    params.prefixOutputFile + to_string(params.part), false);
        }
        while (!r.isEof()) {
            for (int i = 0; i < 3; ++i) {
                valid[i] = false;
                int flag = r.parseByte();
                if (flag == 1) {
                    //convert number
                    triple[i] = r.parseLong();
                    valid[i] = true;
                } else {
                    //Match the text against the hashmap
                    int size;
                    const char *tTerm = r.parseString(size);

                    if (currentTripleId == nextTripleId && nextPos == i) {
                        triple[i] = nextTerm;
                        valid[i] = true;
                        if (!uncommonTermsReader->isEof()) {
                            long tripleId = uncommonTermsReader->parseLong();
                            nextTripleId = tripleId >> 2;
                            nextPos = tripleId & 0x3;
                            nextTerm = uncommonTermsReader->parseLong();
                        }
                        compressedTerms++;
                    } else {
                        bool ok = false;
                        //Check the hashmap
                        if (params.commonMap != NULL) {
                            ByteArrayToNumberMap::iterator itr =
                                params.commonMap->find(tTerm);
                            if (itr != params.commonMap->end()) {
                                triple[i] = itr->second;
                                valid[i] = true;
                                ok = true;
                                compressedTerms++;
                            }
                        }

                        if (!ok) {
                            CompressedByteArrayToNumberMap::iterator itr2 =
                                params.map->find(tTerm);
                            if (itr2 != params.map->end()) {
                                triple[i] = itr2->second;
                                valid[i] = true;
                                compressedTerms++;
                            } else {
                                memcpy(tTriple + MAX_TERM_SIZE * i, tTerm,
                                       size);
                                uncompressedTerms++;
                            }
                        }
                    }
                }
            }

            if (valid[0] && valid[1] && valid[2]) {
                for (int i = 0; i < nperms; ++i) {
                    switch (detailPerms[i]) {
                    case IDX_SPO:
                        permWriters[i]->write(triple[0], triple[1], triple[2]);
                        break;
                    case IDX_OPS:
                        permWriters[i]->write(triple[2], triple[1], triple[0]);
                        break;
                    case IDX_SOP:
                        permWriters[i]->write(triple[0], triple[2], triple[1]);
                        break;
                    case IDX_OSP:
                        permWriters[i]->write(triple[2], triple[0], triple[1]);
                        break;
                    case IDX_PSO:
                        permWriters[i]->write(triple[1], triple[0], triple[2]);
                        break;
                    case IDX_POS:
                        permWriters[i]->write(triple[1], triple[2], triple[0]);
                        break;
                    }
                }
                /*switch (nperms) {
                case 1:
                    permWriters[0]->write(triple[0], triple[1], triple[2]);
                    break;
                case 2:
                    permWriters[0]->write(triple[0], triple[1], triple[2]);
                    permWriters[1]->write(triple[2], triple[1], triple[0]);
                    break;
                case 3:
                    permWriters[0]->write(triple[0], triple[1], triple[2]);
                    permWriters[1]->write(triple[2], triple[1], triple[0]);
                    permWriters[2]->write(triple[1], triple[2], triple[0]);
                    break;
                case 4:
                    permWriters[0]->write(triple[0], triple[1], triple[2]);
                    permWriters[1]->write(triple[2], triple[1], triple[0]);
                    permWriters[2]->write(triple[0], triple[2], triple[1]);
                    permWriters[3]->write(triple[2], triple[0], triple[1]);
                    break;
                case 6:
                    permWriters[0]->write(triple[0], triple[1], triple[2]);
                    permWriters[1]->write(triple[2], triple[1], triple[0]);
                    permWriters[2]->write(triple[0], triple[2], triple[1]);
                    permWriters[3]->write(triple[2], triple[0], triple[1]);
                    permWriters[5]->write(triple[1], triple[0], triple[2]);
                    permWriters[4]->write(triple[1], triple[2], triple[0]);
                    break;
                }*/
                compressedTriples++;
            } else {
                //Write it into the file
                for (int i = 0; i < 3; ++i) {
                    if (valid[i]) {
                        w.writeByte(1);
                        w.writeLong((long) triple[i]);
                    } else {
                        w.writeByte(0);
                        char *t = tTriple + MAX_TERM_SIZE * i;
                        w.writeString(t, Utils::decode_short(t) + 2);
                    }
                }
            }

            currentTripleId += increment;
        }

        for (int i = 0; i < nperms; ++i) {
            delete permWriters[i];
        }
        delete[] permWriters;

        if (uncommonTermsReader != NULL) {
            if (!(uncommonTermsReader->isEof())) {
                BOOST_LOG_TRIVIAL(error) << "There are still elements to read in the uncommon file";
            }
            delete uncommonTermsReader;
        }
        delete[] tTriple;
    } else {
        BOOST_LOG_TRIVIAL(warning) << "The file " << in << " does not exist or is empty";
    }

    BOOST_LOG_TRIVIAL(debug) << "Compressed triples " << compressedTriples << " compressed terms " << compressedTerms << " uncompressed terms " << uncompressedTerms;

    //Delete the input file and replace it with a new one
    fs::remove(pFile);
    params.inNames[params.part] = newFile;
}

bool Compressor::isSplittable(string path) {
    if (boost::algorithm::ends_with(path, ".gz")
            || boost::algorithm::ends_with(path, ".bz2")) {
        return false;
    } else {
        return true;
    }
}

vector<FileInfo> *Compressor::splitInputInChunks(const string &input, int nchunks) {
    /*** Get list all files ***/
    fs::path pInput(input);
    vector<FileInfo> infoAllFiles;
    long totalSize = 0;
    if (fs::is_directory(pInput)) {
        fs::directory_iterator end;
        for (fs::directory_iterator dir_iter(input); dir_iter != end;
                ++dir_iter) {
            if (dir_iter->path().filename().string()[0] != '.') {
                long fileSize = fs::file_size(dir_iter->path());
                totalSize += fileSize;
                FileInfo i;
                i.size = fileSize;
                i.start = 0;
                i.path = dir_iter->path().string();
                i.splittable = isSplittable(dir_iter->path().string());
                infoAllFiles.push_back(i);
            }
        }
    } else {
        long fileSize = fs::file_size(input);
        totalSize += fileSize;
        FileInfo i;
        i.size = fileSize;
        i.start = 0;
        i.path = input;
        i.splittable = isSplittable(input);
        infoAllFiles.push_back(i);
    }

    BOOST_LOG_TRIVIAL(info) << "Going to parse " << infoAllFiles.size() << " files. Total size in bytes: " << totalSize << " bytes";

    /*** Sort the input files by size, and split the files through the multiple processors ***/
    std::sort(infoAllFiles.begin(), infoAllFiles.end(), cmpInfoFiles);
    vector<FileInfo> *files = new vector<FileInfo> [nchunks];
    long splitTargetSize = totalSize / nchunks;
    int processedFiles = 0;
    int currentSplit = 0;
    long splitSize = 0;
    while (processedFiles < infoAllFiles.size()) {
        FileInfo f = infoAllFiles[processedFiles++];
        if (!f.splittable) {
            splitSize += f.size;
            files[currentSplit].push_back(f);
            if (splitSize >= splitTargetSize
                    && currentSplit < nchunks - 1) {
                currentSplit++;
                splitSize = 0;
            }
        } else {
            long assignedFileSize = 0;
            while (assignedFileSize < f.size) {
                long sizeToCopy;
                if (currentSplit == nchunks - 1) {
                    sizeToCopy = f.size - assignedFileSize;
                } else {
                    sizeToCopy = min(f.size - assignedFileSize,
                                     splitTargetSize - splitSize);
                }

                //Copy inside the split
                FileInfo splitF;
                splitF.path = f.path;
                splitF.start = assignedFileSize;
                splitF.splittable = true;
                splitF.size = sizeToCopy;
                files[currentSplit].push_back(splitF);

                splitSize += sizeToCopy;
                assignedFileSize += sizeToCopy;
                if (splitSize >= splitTargetSize
                        && currentSplit < nchunks - 1) {
                    currentSplit++;
                    splitSize = 0;
                }
            }
        }
    }
    infoAllFiles.clear();

    for (int i = 0; i < nchunks; ++i) {
        long totalSize = 0;
        for (vector<FileInfo>::iterator itr = files[i].begin();
                itr < files[i].end(); ++itr) {
            totalSize += itr->size;
        }
        BOOST_LOG_TRIVIAL(debug) << "Files in split " << i << ": " << files[i].size() << " size " << totalSize;
    }
    return files;
}

//J: This procedure is commented because we concluded this approach was not worth to test it (too slow)
void Compressor::do_mcgs() {
//    // Fix size of MG heap for calculating memory
//    const unsigned long partitionHeapSize = MGCS_HEAP_SIZE;  // Input parameter for the number of top-k elements required
//
//    // Each element in the Heap stores an element and a count
//    const unsigned long memForHeap = (partitionHeapSize * maxReadingThreads) * (MAX_TERM_SIZE + 2 + sizeof(long));
//    BOOST_LOG_TRIVIAL(debug) << "Total Heap size (across all partitions) = " << memForHeap << " bytes.";
//
//    // Calculate size of the CS Hash Tables
//    long memForCS = (long)(Utils::getSystemMemory() * 0.8) - memForHeap;
//
//    long numHashTabs = MGCS_HASH_TABLES;
//    long numHashBuck = (long)(memForCS / numHashTabs);  // TODO If prime then better
//    BOOST_LOG_TRIVIAL(debug) << "Count Sketch with " << numHashTabs << " hash tables with " << numHashBuck << " buckets each. Total memory = " << memForCS << " bytes.";
//
//
//    // Creat global Count-Sketch data structure
//    CountSketch *cs = new CountSketch(numHashTabs, numHashBuck);
//
//
//    // Uncompress the input triples in a parallel fashion
//    MG **MGheaps = new MG*[parallelProcesses];
//
//    long *distinctValues = new long[parallelProcesses];
//    memset(distinctValues, 0, sizeof(long)*parallelProcesses);
//
//    boost::thread *threads = new boost::thread[parallelProcesses - 1];
//    SchemaExtractor *extractors = new SchemaExtractor[parallelProcesses - 1];
//
//    int chunksToProcess = 0;
//    while (chunksToProcess < parallelProcesses) {
//        for (int i = 1; ( (i < maxReadingThreads) && ((chunksToProcess + i) < parallelProcesses) ); i++) {
//            MGheaps[chunksToProcess + i] = new MG (partitionHeapSize);  // Create the Misra-Gries structure for each thread
//
//            tmpFileNames[chunksToProcess + i] = kbPath + string("/tmp-") + boost::lexical_cast<string>(chunksToProcess + i);
//
//            threads[i - 1] = boost::thread(boost::bind(&Compressor::uncompressTriplesForMGCS, this,
//                                           files[chunksToProcess + i], MGheaps[chunksToProcess + i], cs,
//                                           tmpFileNames[chunksToProcess + i], copyHashes ? extractors + i - 1 : NULL,
//                                           distinctValues + i + chunksToProcess));
//        }
//
//        MGheaps[chunksToProcess] = new MG (partitionHeapSize);
//        tmpFileNames[chunksToProcess] = kbPath + string("/tmp-") + to_string(chunksToProcess);
//
//        uncompressTriplesForMGCS(files[chunksToProcess], MGheaps[chunksToProcess], cs, tmpFileNames[chunksToProcess],
//                                 copyHashes ? extractors : NULL, distinctValues + chunksToProcess);
//
//        for (int i = 1; i < maxReadingThreads; i++) {
//            threads[i - 1].join();
//        }
//
//        // Merge the heaps of MG obtained as above
//        BOOST_LOG_TRIVIAL(debug) << "Merging the MG heaps...";
//
//        for (int i = 0; i < maxReadingThreads; i++) {
//            if ((chunksToProcess + i) != 0) {
//                StringToNumberMap partitionOutput = (MGheaps[chunksToProcess + i])->getHeapElements();
//                MGheaps[0]->merge(partitionOutput);
//
//                delete MGheaps[chunksToProcess + i];
//            }
//        }
//
//        chunksToProcess += maxReadingThreads;  // Increment chunk value to process next chunk of data
//    }
//
//    // Merge the schema extractors
//    if (copyHashes) {
//        BOOST_LOG_TRIVIAL(debug) << "Merge the extracted schema";
//        for (int i = 0; i < parallelProcesses - 1; ++i) {
//            schemaExtrator->merge(extractors[i]);
//        }
//        BOOST_LOG_TRIVIAL(debug) << "Prepare the schema...";
//        schemaExtrator->prepare();
//        BOOST_LOG_TRIVIAL(debug) << "... done";
//    }
//    delete[] extractors;
//
//
//    // Obtain the top-k elements in MGheaps[0]
//    set<string> freqElements;
//    const StringToNumberMap top_k = MGheaps[0]->getHeapElements();
//    StringToNumberMap::const_iterator it = top_k.begin();
//
//    for (; it != top_k.end(); it++) {
//        freqElements.insert(it->first);
//    }
//
//
//    // Write frequent and infrequent items in dictionary
//    /*** Extract the common URIs ***/
//    ParamsExtractCommonTermProcedure params;
//    params.tables = NULL;
//    params.dictPartitions = dictPartitions;
//    params.maxMapSize = sampleArg;
//    params.parallelProcesses = parallelProcesses;
//    params.copyHashes = copyHashes;
//
//    BOOST_LOG_TRIVIAL(debug) << "Extract the common terms";
//    for (int i = 1; i < parallelProcesses; ++i) {
//        params.inputFile = tmpFileNames[i];
//        params.map = &commonTermsMaps[i];
//        params.dictFileName = dictFileNames[i];
//        params.idProcess = i;
//        params.singleTerms = uncommonDictFileNames[i];
//        threads[i - 1] = boost::thread(
//                             boost::bind(&Compressor::extractTermsForMGCS,
//                                         this, params, freqElements, cs));
//    }
//    params.inputFile = tmpFileNames[0];
//    params.map = &commonTermsMaps[0];
//    params.dictFileName = dictFileNames[0];
//    params.idProcess = 0;
//    params.singleTerms = uncommonDictFileNames[0];
//    extractTermsForMGCS(params, freqElements, cs);
//    for (int i = 1; i < parallelProcesses; ++i) {
//        threads[i - 1].join();
//    }
//
//    // Insert the frequent elements in the finalMap structure
//    BOOST_LOG_TRIVIAL(debug) << "Frequent Terms " << freqElements.size();
//    finalMap->set_empty_key(EMPTY_KEY);
//    finalMap->set_deleted_key(DELETED_KEY);
//
//    set<string>::iterator it1 = freqElements.begin();
//    for (; it1 != freqElements.end(); it1++) {
//        unsigned long frq = cs->GetFreqEstimate(*it1);
//
//        char supportArray[MAX_TERM_SIZE];
//        memcpy(supportArray + 2, it1->c_str(), it1->size() + 1);
//        Utils::encode_short(supportArray, it1->size());
//        const char *newkey = poolForMap->addNew(supportArray, it1->size() + 2);
//        finalMap->insert(std::make_pair(newkey, frq));
//    }
//
//    // Delete the heaps and the other data structures
//    BOOST_LOG_TRIVIAL(debug) << "Delete some datastructures";
//    delete[] distinctValues;
//    delete[] MGheaps;
//    delete[] threads;
//
}

void Compressor::parse(int dictPartitions, int sampleMethod, int sampleArg,
                       int sampleArg2, int parallelProcesses, int maxReadingThreads,
                       bool copyHashes, SchemaExtractor *schemaExtrator,
                       const bool splitUncommonByHash, bool onlySample) {
    tmpFileNames = new string[parallelProcesses];
    vector<FileInfo> *files = splitInputInChunks(input, parallelProcesses);

    /*** Set name dictionary files ***/
    dictFileNames = new string*[parallelProcesses];
    uncommonDictFileNames = new string*[parallelProcesses];
    for (int i = 0; i < parallelProcesses; ++i) {
        string *df = new string[dictPartitions];
        string *df2 = new string[dictPartitions];
        for (int j = 0; j < dictPartitions; ++j) {
            df[j] = kbPath + string("/dict-file-") + to_string(i) + string("-")
                    + to_string(j);
            df2[j] = kbPath + string("/udict-file-") + to_string(i)
                     + string("-") + to_string(j);
        }
        dictFileNames[i] = df;
        uncommonDictFileNames[i] = df2;
    }

    SchemaExtractor *extractors = new SchemaExtractor[maxReadingThreads];

#ifdef DEBUG
    //SchemaExtractor::initMap();
#endif

    timens::system_clock::time_point start = timens::system_clock::now();
    GStringToNumberMap *commonTermsMaps =
        new GStringToNumberMap[parallelProcesses];
    if (sampleMethod == PARSE_COUNTMIN) {
        do_countmin(dictPartitions, sampleArg, parallelProcesses,
                    maxReadingThreads, copyHashes,
                    extractors, files, commonTermsMaps, false);
    } else if (sampleMethod == PARSE_COUNTMIN_MGCS) {
        do_countmin(dictPartitions, sampleArg, parallelProcesses,
                    maxReadingThreads, copyHashes,
                    extractors, files, commonTermsMaps, true);
    } else if (sampleMethod == PARSE_MGCS) {
        BOOST_LOG_TRIVIAL(error) << "No longer supported";
        throw 10;
        do_mcgs();
    } else { //PARSE_SAMPLE
        do_sample(dictPartitions, sampleArg, sampleArg2, maxReadingThreads,
                  copyHashes, parallelProcesses, extractors, files,
                  commonTermsMaps);
    }

    boost::chrono::duration<double> sec = boost::chrono::system_clock::now() - start;
    BOOST_LOG_TRIVIAL(debug) << "Time heavy hitters detection = " << sec.count() * 1000 << " ms";

    /*** Merge the schema extractors ***/
    if (copyHashes) {
        BOOST_LOG_TRIVIAL(debug) << "Merge the extracted schema";
        for (int i = 0; i < maxReadingThreads; ++i) {
            schemaExtrator->merge(extractors[i]);
        }
        if (!onlySample) {
            BOOST_LOG_TRIVIAL(debug) << "Prepare the schema...";
            schemaExtrator->prepare();
            BOOST_LOG_TRIVIAL(debug) << "... done";
        }
    }
    delete[] extractors;

    if (!onlySample) {
        /*** Extract the uncommon terms ***/
        BOOST_LOG_TRIVIAL(debug) << "Extract the uncommon terms";
        boost::thread *threads = new boost::thread[parallelProcesses];
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1] = boost::thread(
                                 boost::bind(&Compressor::extractUncommonTerms, this,
                                             dictPartitions, tmpFileNames[i], copyHashes,
                                             i, parallelProcesses,
                                             uncommonDictFileNames[i],
                                             splitUncommonByHash));
        }
        extractUncommonTerms(dictPartitions, tmpFileNames[0], copyHashes, 0,
                             parallelProcesses,
                             uncommonDictFileNames[0],
                             splitUncommonByHash);
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1].join();
        }
        BOOST_LOG_TRIVIAL(debug) << "Finished the extraction of the uncommon terms";
        delete[] threads;
    }
    delete[] commonTermsMaps;
    delete[] files;
}

unsigned int Compressor::getThresholdForUncommon(
    const int parallelProcesses,
    const int sizeHashTable,
    const int sampleArg,
    long *distinctValues,
    Hashtable **tables1,
    Hashtable **tables2,
    Hashtable **tables3) {
    nTerms = 0;
    for (int i = 0; i < parallelProcesses; ++i) {
        nTerms = max(nTerms, distinctValues[i]);
    }
    BOOST_LOG_TRIVIAL(debug) << "Estimated number of terms per partition: " << nTerms;
    long termsPerBlock = max((long)1, (long)(nTerms / sizeHashTable)); //Terms per block
    long tu1 = max((long) 1, tables1[0]->getThreshold(sizeHashTable - sampleArg));
    long tu2 = max((long) 1, tables2[0]->getThreshold(sizeHashTable - sampleArg));
    long tu3 = max((long) 1, tables3[0]->getThreshold(sizeHashTable - sampleArg));
    return max(4 * termsPerBlock, min(min(tu1, tu2), tu3));
}

void Compressor::do_countmin_secondpass(const int dictPartitions,
                                        const int sampleArg,
                                        const int parallelProcesses,
                                        bool copyHashes,
                                        const unsigned int sizeHashTable,
                                        Hashtable **tables1,
                                        Hashtable **tables2,
                                        Hashtable **tables3,
                                        long *distinctValues,
                                        GStringToNumberMap *commonTermsMaps) {

    /*** Calculate the threshold value to identify uncommon terms ***/
    totalCount = tables1[0]->getTotalCount() / 3;
    unsigned int thresholdForUncommon = getThresholdForUncommon(
                                            parallelProcesses, sizeHashTable,
                                            sampleArg, distinctValues,
                                            tables1, tables2, tables3);
    BOOST_LOG_TRIVIAL(debug) << "Threshold to mark elements as uncommon: " <<
                             thresholdForUncommon;

    /*** Extract the common URIs ***/
    Hashtable *tables[3];
    tables[0] = tables1[0];
    tables[1] = tables2[0];
    tables[2] = tables3[0];
    ParamsExtractCommonTermProcedure params;
    params.tables = tables;
    params.dictPartitions = dictPartitions;
    params.maxMapSize = sampleArg;
    params.parallelProcesses = parallelProcesses;
    params.thresholdForUncommon = thresholdForUncommon;
    params.copyHashes = copyHashes;
    boost::thread *threads = new boost::thread[parallelProcesses - 1];

    BOOST_LOG_TRIVIAL(debug) << "Extract the common terms";
    for (int i = 1; i < parallelProcesses; ++i) {
        params.inputFile = tmpFileNames[i];
        params.map = &commonTermsMaps[i];
        params.dictFileName = dictFileNames[i];
        params.idProcess = i;
        params.singleTerms = uncommonDictFileNames[i];
        threads[i - 1] = boost::thread(
                             boost::bind(&Compressor::extractCommonTerms, this, params));
    }
    params.inputFile = tmpFileNames[0];
    params.map = &commonTermsMaps[0];
    params.dictFileName = dictFileNames[0];
    params.idProcess = 0;
    params.singleTerms = uncommonDictFileNames[0];
    extractCommonTerms(params);
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1].join();
    }
    delete[] threads;
}

void Compressor::do_countmin(const int dictPartitions, const int sampleArg,
                             const int parallelProcesses, const int maxReadingThreads,
                             const bool copyHashes, SchemaExtractor *extractors,
                             vector<FileInfo> *files,
                             GStringToNumberMap *commonTermsMaps,
                             bool usemisgra) {
    /*** Uncompress the triples in parallel ***/
    Hashtable **tables1 = new Hashtable*[parallelProcesses];
    Hashtable **tables2 = new Hashtable*[parallelProcesses];
    Hashtable **tables3 = new Hashtable*[parallelProcesses];
    long *distinctValues = new long[parallelProcesses];
    memset(distinctValues, 0, sizeof(long)*parallelProcesses);

    boost::thread *threads = new boost::thread[parallelProcesses - 1];

    /*** If we intend to use Misra to store the popular terms, then we must init
     * it ***/
    vector<string> *resultsMGS = NULL;
    if (usemisgra) {
        resultsMGS = new vector<string>[parallelProcesses];
    }

    /*** Calculate size of the hash table ***/
    long nBytesInput = Utils::getNBytes(input);
    bool isInputCompressed = Utils::isCompressed(input);
    long maxSize;
    if (!isInputCompressed) {
        maxSize = nBytesInput / 1000;
    } else {
        maxSize = nBytesInput / 25;
    }
    BOOST_LOG_TRIVIAL(debug) << "Size Input: " << nBytesInput <<
                             " bytes. Max table size=" << maxSize;
    long memForHashTables = (long)(Utils::getSystemMemory() * 0.6)
                            / (1 + maxReadingThreads) / 3;
    //Divided numer hash tables
    const unsigned int sizeHashTable = std::min((long)maxSize,
                                       (long)std::max((unsigned int)1000000,
                                               (unsigned int)(memForHashTables / sizeof(long))));
    BOOST_LOG_TRIVIAL(debug) << "Size hash table " << sizeHashTable;

    int chunksToProcess = 0;

    ParamsUncompressTriples params;
    //Set only global params
    params.sizeHeap = sampleArg;

    while (chunksToProcess < parallelProcesses) {
        for (int i = 1;
                i < maxReadingThreads
                && (chunksToProcess + i) < parallelProcesses;
                ++i) {
            tables1[chunksToProcess + i] = new Hashtable(sizeHashTable,
                    &Hashes::dbj2s_56);
            tables2[chunksToProcess + i] = new Hashtable(sizeHashTable,
                    &Hashes::fnv1a_56);
            tables3[chunksToProcess + i] = new Hashtable(sizeHashTable,
                    &Hashes::murmur3_56);
            tmpFileNames[chunksToProcess + i] = kbPath + string("/tmp-")
                                                + boost::lexical_cast<string>(
                                                    chunksToProcess + i);

            params.files = files[chunksToProcess + i];
            params.table1 = tables1[chunksToProcess + i];
            params.table2 = tables2[chunksToProcess + i];
            params.table3 = tables3[chunksToProcess + i];
            params.outFile = tmpFileNames[chunksToProcess + i];
            params.extractor = copyHashes ? extractors + i : NULL;
            params.distinctValues = distinctValues + i + chunksToProcess;
            params.resultsMGS = usemisgra ? &resultsMGS[chunksToProcess + i] : NULL;
            threads[i - 1] = boost::thread(
                                 boost::bind(&Compressor::uncompressTriples, this,
                                             params));
        }
        tables1[chunksToProcess] = new Hashtable(sizeHashTable,
                &Hashes::dbj2s_56);
        tables2[chunksToProcess] = new Hashtable(sizeHashTable,
                &Hashes::fnv1a_56);
        tables3[chunksToProcess] = new Hashtable(sizeHashTable,
                &Hashes::murmur3_56);
        tmpFileNames[chunksToProcess] = kbPath + string("/tmp-")
                                        + to_string(chunksToProcess);

        params.files = files[chunksToProcess];
        params.table1 = tables1[chunksToProcess];
        params.table2 = tables2[chunksToProcess];
        params.table3 = tables3[chunksToProcess];
        params.outFile = tmpFileNames[chunksToProcess];
        params.extractor = copyHashes ? extractors : NULL;
        params.distinctValues = distinctValues + chunksToProcess;
        params.resultsMGS = usemisgra ? &resultsMGS[chunksToProcess] : NULL;
        uncompressTriples(params);

        for (int i = 1; i < maxReadingThreads; ++i) {
            threads[i - 1].join();
        }

        //Merging the tables
        BOOST_LOG_TRIVIAL(debug) << "Merging the tables...";

        for (int i = 0; i < maxReadingThreads; ++i) {
            if ((chunksToProcess + i) != 0) {
                BOOST_LOG_TRIVIAL(debug) << "Merge table " << (chunksToProcess + i);
                tables1[0]->merge(tables1[chunksToProcess + i]);
                tables2[0]->merge(tables2[chunksToProcess + i]);
                tables3[0]->merge(tables3[chunksToProcess + i]);
                delete tables1[chunksToProcess + i];
                delete tables2[chunksToProcess + i];
                delete tables3[chunksToProcess + i];
            }
        }

        chunksToProcess += maxReadingThreads;
    }

    /*** If misra-gries is not active, then we must perform another pass to
     * extract the strings of the common terms. Otherwise, MGS gives it to
     * us ***/
    if (!usemisgra) {
        do_countmin_secondpass(dictPartitions, sampleArg, parallelProcesses,
                               copyHashes, sizeHashTable, tables1, tables2,
                               tables3, distinctValues, commonTermsMaps);
    } else {
        /*** Determine a minimum threshold value from the count_min tables to
         * mark the element has common ***/
        long minFreq = getThresholdForUncommon(
                           parallelProcesses, sizeHashTable,
                           sampleArg, distinctValues,
                           tables1, tables2, tables3);
        BOOST_LOG_TRIVIAL(debug) << "The minimum frequency required is " << minFreq;

        /*** Then go through all elements and take the frequency from the
         * count_min tables ***/
        std::vector<std::pair<string, long>> listFrequentTerms;
        for (int i = 0; i < parallelProcesses; ++i) {
            //const StringToNumberMap& mapElements = mgs[i]->getHeapElements();
            for (vector<string>::const_iterator itr = resultsMGS[i].begin();
                    itr != resultsMGS[i].end(); ++itr) {
                //Get the frequency and add it
                long v1 = tables1[0]->get(*itr);
                long v2 = tables2[0]->get(*itr);
                long v3 = tables3[0]->get(*itr);
                long freq = min(v1, min(v2, v3));
                if (freq >= minFreq)
                    listFrequentTerms.push_back(make_pair(*itr, freq));
            }
        }

        /*** Sort the list and keep the highest n ***/
        BOOST_LOG_TRIVIAL(debug) << "There are " << listFrequentTerms.size() << " potential frequent terms";
        std::sort(listFrequentTerms.begin(), listFrequentTerms.end(),
                  lessTermFrequenciesDesc);

        /*** Copy the first n terms in the map ***/
        char supportBuffer[MAX_TERM_SIZE];
        finalMap->set_empty_key(EMPTY_KEY);
        finalMap->set_deleted_key(DELETED_KEY);
        for (int i = 0; finalMap->size() < sampleArg && i < listFrequentTerms.size(); ++i) {
            std::pair<string, long> pair = listFrequentTerms[i];
            if (i == 0 || pair.first != listFrequentTerms[i - 1].first) {
                memcpy(supportBuffer + 2, pair.first.c_str(), pair.first.size());
                Utils::encode_short(supportBuffer, 0, pair.first.size());
                const char *newkey = poolForMap->addNew(supportBuffer,
                                                        pair.first.size() + 2);
                finalMap->insert(std::make_pair(newkey, pair.second));
            }
        }

        /*** delete msgs data structures ***/
        delete[] resultsMGS;
    }

    /*** Delete the hashtables ***/
    BOOST_LOG_TRIVIAL(debug) << "Delete some datastructures";
    table1 = std::shared_ptr<Hashtable>(tables1[0]);
    table2 = std::shared_ptr<Hashtable>(tables2[0]);
    table3 = std::shared_ptr<Hashtable>(tables3[0]);
    delete[] distinctValues;
    delete[] tables1;
    delete[] tables2;
    delete[] tables3;

    /*** Merge the hashmaps with the common terms ***/
    if (!usemisgra) {
        finalMap->set_empty_key(EMPTY_KEY);
        finalMap->set_deleted_key(DELETED_KEY);
        BOOST_LOG_TRIVIAL(debug) << "Merge the local common term maps";
        mergeCommonTermsMaps(finalMap, commonTermsMaps, parallelProcesses);
    }
    BOOST_LOG_TRIVIAL(debug) << "Size hashtable with common terms " << finalMap->size();

    delete[] threads;
}

void Compressor::do_sample(const int dictPartitions, const int sampleArg,
                           const int sampleArg2,
                           const int maxReadingThreads,
                           bool copyHashes,
                           const int parallelProcesses,
                           SchemaExtractor *extractors, vector<FileInfo> *files,
                           GStringToNumberMap *commonTermsMaps) {
    //Perform only a sample
    int chunksToProcess = 0;
    boost::thread *threads = new boost::thread[maxReadingThreads - 1];
    while (chunksToProcess < parallelProcesses) {
        for (int i = 1;
                i < maxReadingThreads
                && (chunksToProcess + i) < parallelProcesses;
                ++i) {
            tmpFileNames[chunksToProcess + i] = kbPath + string("/tmp-")
                                                + boost::lexical_cast<string>(chunksToProcess + i);
            threads[i - 1] = boost::thread(
                                 boost::bind(&Compressor::uncompressAndSampleTriples,
                                             this,
                                             files[chunksToProcess + i],
                                             tmpFileNames[chunksToProcess + i],
                                             dictFileNames[chunksToProcess + i],
                                             dictPartitions,
                                             sampleArg2,
                                             &commonTermsMaps[chunksToProcess + i],
                                             copyHashes ? extractors + i : NULL));
        }
        tmpFileNames[chunksToProcess] = kbPath + string("/tmp-" + to_string(chunksToProcess));
        uncompressAndSampleTriples(files[chunksToProcess],
                                   tmpFileNames[chunksToProcess],
                                   dictFileNames[chunksToProcess],
                                   dictPartitions,
                                   sampleArg2,
                                   &commonTermsMaps[chunksToProcess],
                                   copyHashes ? extractors : NULL);
        for (int i = 1; i < maxReadingThreads; ++i) {
            threads[i - 1].join();
        }
        chunksToProcess += maxReadingThreads;
    }
    //This is just a rough estimate
    totalCount = commonTermsMaps[0].size() * 100 / sampleArg2 / 3 * 4
                 * dictPartitions;
    delete[] threads;

    /*** Collect all terms in the sample in one vector ***/
    std::vector<std::pair<string, size_t>> sampledTerms;
    for (int i = 0; i < parallelProcesses; ++i) {
        //Get all elements in the map
        for (GStringToNumberMap::iterator itr = commonTermsMaps[i].begin();
                itr != commonTermsMaps[i].end(); ++itr) {
            sampledTerms.push_back(make_pair(itr->first, itr->second));
        }
        commonTermsMaps[i].clear();
    }

    /*** Sort all the pairs by term ***/
    BOOST_LOG_TRIVIAL(debug) << "Sorting the sample of " << sampledTerms.size();
    std::sort(sampledTerms.begin(), sampledTerms.end(), &sampledTermsSorter1);

    /*** Merge sample ***/
    BOOST_LOG_TRIVIAL(debug) << "Merging sorted results";
    std::vector<std::pair<string, size_t>> sampledTermsUniq;
    size_t i = 0;
    for (std::vector<std::pair<string, size_t>>::iterator
            itr = sampledTerms.begin(); itr != sampledTerms.end();  ++itr) {
        if (i == 0 || itr->first != sampledTermsUniq.back().first) {
            // Add a new pair
            sampledTermsUniq.push_back(make_pair(itr->first, itr->second));
        } else if (i != 0) {
            //Increment the previous
            sampledTermsUniq.back().second += itr->second;
        }
        i++;
    }

    /*** Sort by descending order ***/
    std::sort(sampledTermsUniq.begin(), sampledTermsUniq.end(),
              &sampledTermsSorter2);

    /*** Pick the top n and copy them in finalMap ***/
    BOOST_LOG_TRIVIAL(debug) << "Copy in the hashmap the top k. The sorted sample contains " << sampledTermsUniq.size();
    finalMap->set_empty_key(EMPTY_KEY);
    finalMap->set_deleted_key(DELETED_KEY);
    char supportTerm[MAX_TERM_SIZE];
    for (int i = 0; i < sampleArg && i < sampledTermsUniq.size(); ++i) {
        Utils::encode_short(supportTerm, sampledTermsUniq[i].first.size());
        memcpy(supportTerm + 2, sampledTermsUniq[i].first.c_str(),
               sampledTermsUniq[i].first.size());
        const char *newkey = poolForMap->addNew(supportTerm,
                                                sampledTermsUniq[i].first.size() + 2);
        finalMap->insert(make_pair(newkey, sampledTermsUniq[i].second));
    }

    BOOST_LOG_TRIVIAL(debug) << "Size hashtable with common terms " << finalMap->size();
}

bool Compressor::areFilesToCompress(int parallelProcesses, string *tmpFileNames) {
    for (int i = 0; i < parallelProcesses; ++i) {
        fs::path pFile(tmpFileNames[i]);
        if (fs::exists(pFile) && fs::file_size(pFile) > 0) {
            return true;
        }
    }
    return false;
}

void Compressor::sortAndDumpToFile2(vector<TriplePair> &pairs,
                                    string outputFile) {
    std::sort(pairs.begin(), pairs.end(), TriplePair::sLess);
    LZ4Writer outputSegment(outputFile);
    for (vector<TriplePair>::iterator itr = pairs.begin(); itr != pairs.end();
            ++itr) {
        itr->writeTo(&outputSegment);
    }
}

void Compressor::sortAndDumpToFile(vector<AnnotatedTerm> &terms, string outputFile,
                                   bool removeDuplicates) {
    if (removeDuplicates) {
        throw 10; //I removed the code below to check for duplicates
    }
    BOOST_LOG_TRIVIAL(debug) << "Sorting and writing to file " << outputFile << " " << terms.size() << " elements. Removedupl=" << removeDuplicates;
    std::sort(terms.begin(), terms.end(), AnnotatedTerm::sLess);
    BOOST_LOG_TRIVIAL(debug) << "Finished sorting";
    LZ4Writer *outputSegment = new LZ4Writer(outputFile);
    //const char *prevTerm = NULL;
    //int sizePrevTerm = 0;
    long countOutput = 0;
    for (vector<AnnotatedTerm>::iterator itr = terms.begin(); itr != terms.end();
            ++itr) {
        //if (!removeDuplicates || prevTerm == NULL
        //        || !itr->equals(prevTerm, sizePrevTerm)) {
        itr->writeTo(outputSegment);
        //prevTerm = itr->term;
        //sizePrevTerm = itr->size;
        countOutput++;
        //}
    }
    delete outputSegment;
    BOOST_LOG_TRIVIAL(debug) << "Written sorted elements: " << countOutput;
}

void Compressor::immemorysort(string **inputFiles,
                              int parallelProcesses, string outputFile, int *noutputFiles,
                              bool removeDuplicates,
                              const long maxSizeToSort, bool sample) {
    timens::system_clock::time_point start = timens::system_clock::now();

    //Split maxSizeToSort in n threads
    const long maxMemPerThread = maxSizeToSort / parallelProcesses;
    boost::thread *threads = new boost::thread[parallelProcesses - 1];
    for (int i = 1; i < parallelProcesses; ++i) {
        string fileName = inputFiles[i][0];
        if (fs::exists(fs::path(fileName))) {
            threads[i - 1] = boost::thread(
                                 boost::bind(
                                     &Compressor::inmemorysort_seq,
                                     this, fileName, i, parallelProcesses,
                                     maxMemPerThread,
                                     removeDuplicates, outputFile,
                                     sample));
        }
    }
    string fileName = inputFiles[0][0];
    if (fs::exists(fs::path(fileName))) {
        inmemorysort_seq(fileName, 0, parallelProcesses,
                         maxMemPerThread,
                         removeDuplicates, outputFile,
                         sample);
    }
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1].join();
    }
    delete[] threads;

    //Collect all files
    std::vector<string> files;
    boost::filesystem::path parentDir =
        boost::filesystem::path(outputFile).parent_path();
    string prefix =
        boost::filesystem::path(outputFile).filename().string() + string(".");
    for (boost::filesystem::directory_iterator itr(parentDir);
            itr != boost::filesystem::directory_iterator(); ++itr) {
        if (boost::filesystem::is_regular_file(itr->path())) {
            if (boost::starts_with(itr->path().filename().string(), prefix)) {
                files.push_back(itr->path().string());
            }
        }
    }

    for (int i = 0; i < files.size(); ++i) {
        string renamedFile = files[i] + "-old";
        boost::filesystem::rename(boost::filesystem::path(files[i]),
                                  boost::filesystem::path(renamedFile));
        files[i] = renamedFile;
    }
    for (int i = 0; i < files.size(); ++i) {
        string of = outputFile + string(".") + to_string(i);
        boost::filesystem::rename(boost::filesystem::path(files[i]),
                                  boost::filesystem::path(of));
    }
    *noutputFiles = files.size();

    boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
                                          - start;
    BOOST_LOG_TRIVIAL(debug) << "Total sorting time = " << sec.count() * 1000
                             << " ms";
}

void Compressor::inmemorysort_seq(const string inputFile,
                                  int idx,
                                  const int incrIdx,
                                  const long maxMemPerThread,
                                  bool removeDuplicates,
                                  string outputFile,
                                  bool sample) {

    vector<AnnotatedTerm> terms;
    vector<string> outputfiles;
    StringCollection supportCollection(BLOCK_SUPPORT_BUFFER_COMPR);
    LZ4Reader *fis = new LZ4Reader(inputFile);
    long bytesAllocated = 0;

    LZ4Writer *sampleFile = NULL;
    if (sample) {
        sampleFile = new LZ4Writer(outputFile + "-" + to_string(idx) + "-sample");
    }

    BOOST_LOG_TRIVIAL(debug) << "Start immemory_seq method";

    while (!fis->isEof()) {
        AnnotatedTerm t;
        t.readFrom(fis);
        //if (map == NULL || map->find(t.term) == map->end()) {
        const int r = rand() % 100; //from 0 to 99
        if (r < 1 && sampleFile) {
            sampleFile->writeString(t.term, t.size);
        }

        if ((bytesAllocated + (sizeof(AnnotatedTerm) * terms.size() * 2))
                >= maxMemPerThread) {
            string ofile = outputFile + string(".") + to_string(idx);
            idx += incrIdx;
            sortAndDumpToFile(terms, ofile, removeDuplicates);
            outputfiles.push_back(ofile);
            terms.clear();
            supportCollection.clear();
            bytesAllocated = 0;
        }

        t.term = supportCollection.addNew((char *) t.term, t.size);
        terms.push_back(t);
        bytesAllocated += t.size;
        /*} else {
            throw 10;
        }*/
    }

    if (terms.size() > 0) {
        string ofile = outputFile + string(".") + to_string(idx);
        sortAndDumpToFile(terms, ofile, removeDuplicates);
        outputfiles.push_back(ofile);
    }

    delete fis;
    fs::remove(inputFile);

    if (sampleFile != NULL) {
        delete sampleFile;
    }
}

/*void Compressor::sampleTuples(string input, std::vector<string> *output) {
    LZ4Reader reader(input);
    while (!reader.isEof()) {
        AnnotatedTerm t;
        t.readFrom(&reader);
        const int r = rand() % 100; //from 0 to 99
        if (r < 1) { //Sample 1%
            output->push_back(string(t.term + 2, t.size - 2));
        }
    }
}*/

bool _sampleLess(const char *c1, const char *c2) {
    int l1 = Utils::decode_short(c1);
    int l2 = Utils::decode_short(c2);
    int ret = memcmp(c1 + 2, c2 + 2, min(l1, l2));
    if (ret == 0) {
        return (l1 - l2) < 0;
    } else {
        return ret < 0;
    }
}



std::vector<string> Compressor::getPartitionBoundaries(const string kbdir,
        const int partitions) {
    /*//Sample the elements
    std::vector<std::vector<string>> samples(inputFiles.size());
    std::vector<boost::thread> threads(inputFiles.size());
    for (int i = 0; i < inputFiles.size(); ++i) {
        threads[i] = boost::thread(boost::bind(&Compressor::sampleTuples, inputFiles[i],
                                               &samples[i]));
    }
    for (int i = 0; i < threads.size(); ++i) {
        threads[i].join();
    }

    //Contains all strings
    std::vector<string> sample;
    for (auto s = samples.begin(); s != samples.end(); ++s) {
        std::copy(s->begin(), s->end(), std::back_inserter(sample));
    }
    assert(sample.size() >= partitions);
    std::sort(sample.begin(), sample.end());*/

    //Read all sample strings
    std::vector<const char *> sample;
    StringCollection col(10 * 1024 * 1024);
    fs::directory_iterator end;
    for (fs::directory_iterator dir_iter(kbdir); dir_iter != end;
            ++dir_iter) {
        if (boost::ends_with(dir_iter->path().filename().string(), "-sample")) {
            //Read the content
            {
                LZ4Reader r(dir_iter->path().string());
                while (!r.isEof()) {
                    int size;
                    const char *s = r.parseString(size);
                    const char *news = col.addNew(s, size);
                    sample.push_back(news);
                }
            }
            fs::remove(dir_iter->path().string());
        }
    }

    //Sort the sample
    std::sort(sample.begin(), sample.end(), &_sampleLess);

    std::vector<string> output;
    size_t sizePartition = sample.size() / partitions;
    BOOST_LOG_TRIVIAL(debug) << "sample.size()=" << sample.size()
                             <<  " sizePartition=" << sizePartition
                             << " remainder=" << (sample.size() % partitions);
    //sizePartition = sizePartition * (partitions + 1) / partitions;
    for (size_t i = 0; i < sample.size(); ++i) {
        if ((i + 1) % sizePartition == 0 && output.size() < partitions - 1) {
            //Add element in the partition
            size_t len = Utils::decode_short(sample[i]);
            string s = string(sample[i] + 2, len);
            output.push_back(s);
        }
    }
    return output;
}

void Compressor::sortRangePartitionedTuples(const string inputFile,
        const string outputFile,
        const std::vector<string> *boundaries) {
    int idx = 0;
    LZ4Writer *output = new LZ4Writer(outputFile + string(".") + to_string(idx));
    //Read the input file, and range-partition its content
    LZ4Reader reader(inputFile);
    string bound;
    bool isLast;
    if (boundaries->size() > 0) {
        bound = boundaries->at(idx);
        isLast = false;
    } else {
        isLast = true;
    }
    long counter = 0;
    while (!reader.isEof()) {
        AnnotatedTerm t;
        t.readFrom(&reader);
        assert(t.tripleIdAndPosition != -1);
        string term = string(t.term + 2, t.size - 2);
        if (!isLast && term > bound) {
            do {
                delete output;
                idx++;
                output = new LZ4Writer(outputFile + string(".") + to_string(idx));
                if (idx < boundaries->size()) {
                    bound = boundaries->at(idx);
                } else {
                    isLast = true;
                }
                //Check condition
                if (term <= bound) {
                    break;
                }
            } while (!isLast);
        }
        t.writeTo(output);
        counter++;
    }
    BOOST_LOG_TRIVIAL(debug) << "Partitioned " << counter << " terms.";
    delete output;
    //fs::remove(fs::path(inputFile));
}

void Compressor::rangePartitionFiles(int maxThreads, vector<string> *inputFiles,
                                     std::vector<string> &outputFiles,
                                     const std::vector<string> &boundaries) {
    assert(inputFiles != NULL);
    assert(inputFiles->size() > 0);
    std::vector<boost::thread> threads(maxThreads);
    string inputPrefix = inputFiles->at(0);
    std::size_t idx = inputPrefix.rfind("-u");
    assert(idx != std::string::npos);
    inputPrefix = inputPrefix.substr(0, idx);

    int i = 0;
    int threadsRunning = 0;
    while (i < inputFiles->size()) {
        threadsRunning = 0;
        int idx = i;
        while (idx < inputFiles->size() && threadsRunning < maxThreads) {
            string outputFile = inputFiles->at(idx) + "-ranged-" + to_string(idx);
            outputFiles.push_back(outputFile);
            threads[threadsRunning] = boost::thread(boost::bind(&Compressor::sortRangePartitionedTuples,
                                                    inputFiles->at(idx),
                                                    outputFile,
                                                    &boundaries));
            idx++;
            threadsRunning++;
        }
        for (int i = 0; i < threadsRunning; ++i) {
            threads[i].join();
        }
        i += threadsRunning;
    }
}

void Compressor::sortPartition(std::vector<string> *inputFiles, string dictfile,
                               string outputfile, int part, uint64_t *counter, long maxMem) {
    std::vector<string> filesToSort;
    for (int i = 0; i < inputFiles->size(); ++i) {
        string s = inputFiles->at(i);
        string fileToAdd = s + string(".") + to_string(part);
        if (fs::exists(fs::path(fileToAdd)))
            filesToSort.push_back(fileToAdd);
    }
    string outputFile = outputfile + "tmp";

    StringCollection col(128 * 1024 * 1024);
    std::vector<AnnotatedTerm> tuples;
    std::vector<string> sortedFiles;
    long bytesAllocated = 0;
    int idx = 0;
    //Load all the files until I fill main memory.
    for (int i = 0; i < filesToSort.size(); ++i) {
        string file = filesToSort[i];
        LZ4Reader r(file);
        while (!r.isEof()) {
            AnnotatedTerm t;
            t.readFrom(&r);
            if ((bytesAllocated + (sizeof(AnnotatedTerm) * 2 * tuples.size()))
                    >= maxMem) {
                string ofile = outputFile + string(".") + to_string(idx);
                idx++;
                sortAndDumpToFile(tuples, ofile, false);
                sortedFiles.push_back(ofile);
                tuples.clear();
                col.clear();
                bytesAllocated = 0;
            }

            t.term = col.addNew((char *) t.term, t.size);
            tuples.push_back(t);
            bytesAllocated += t.size;
        }
        fs::remove(file);
    }

    if (idx == 0) {
        //All data fit in main memory. Do not need to write it down
        BOOST_LOG_TRIVIAL(debug) << "All terms (" << tuples.size() << ") fit in main memory";
        std::sort(tuples.begin(), tuples.end(), AnnotatedTerm::sLess);

        //The following code is replicated below.
        long counterTerms = -1;
        long counterPairs = 0;
        {
            LZ4Writer writer(outputfile);
            LZ4Writer dictWriter(dictfile);

            //Write the output
            char *previousTerm = new char[MAX_TERM_SIZE + 2];
            Utils::encode_short(previousTerm, 0);
            for (size_t i = 0; i < tuples.size(); ++i) {
                AnnotatedTerm t = tuples[i];
                if (!t.equals(previousTerm)) {
                    counterTerms++;
                    memcpy(previousTerm, t.term, t.size);
                    dictWriter.writeLong(counterTerms);
                    dictWriter.writeString(t.term, t.size);
                }
                //Write the output
                counterPairs++;
                assert(t.tripleIdAndPosition != -1);
                writer.writeLong(counterTerms);
                writer.writeLong(t.tripleIdAndPosition);
            }
            delete[] previousTerm;
        }

        *counter = counterTerms + 1;
        BOOST_LOG_TRIVIAL(debug) << "Partition " << part << " contains " << counterPairs << " tuples " << (counterTerms + 1) << " terms";
        for (auto f : sortedFiles) {
            fs::remove(fs::path(f));
        }
    } else {
        if (tuples.size() > 0) {
            string ofile = outputFile + string(".") + to_string(idx);
            sortAndDumpToFile(tuples, ofile, false);
            sortedFiles.push_back(ofile);
        }

        BOOST_LOG_TRIVIAL(debug) << "Merge " << sortedFiles.size() << " files in order to sort the partition";

        //Create a file
        long counterTerms = -1;
        long counterPairs = 0;
        char *previousTerm = new char[MAX_TERM_SIZE + 2];

        std::unique_ptr<LZ4Writer> writer(new LZ4Writer(outputfile));
        std::unique_ptr<LZ4Writer> dictWriter(new LZ4Writer(dictfile));

        //Write the output
        Utils::encode_short(previousTerm, 0);
        //Sort the files
        FileMerger<AnnotatedTerm> merger(sortedFiles);
        while (!merger.isEmpty()) {
            AnnotatedTerm t = merger.get();
            if (!t.equals(previousTerm)) {
                counterTerms++;
                memcpy(previousTerm, t.term, t.size);
                dictWriter->writeLong(counterTerms);
                dictWriter->writeString(t.term, t.size);
            }
            //Write the output
            counterPairs++;
            assert(t.tripleIdAndPosition != -1);
            writer->writeLong(counterTerms);
            writer->writeLong(t.tripleIdAndPosition);
        }
        delete[] previousTerm;

        *counter = counterTerms + 1;
        BOOST_LOG_TRIVIAL(debug) << "Partition " << part << " contains " << counterPairs << " tuples " << (counterTerms + 1) << " terms";
        for (auto f : sortedFiles) {
            fs::remove(fs::path(f));
        }
    }
}

void Compressor::sortPartitionsAndAssignCounters(std::vector<string> &inputFiles,
        string dictfile,
        string outputfile, int partitions,
        long & counter, int parallelProcesses) {

    std::vector<boost::thread> threads(partitions);
    std::vector<string> outputfiles;
    std::vector<uint64_t> counters(partitions);
    long maxMem = max((long) 128 * 1024 * 1024,
                      (long) (Utils::getSystemMemory() * 0.7)) / partitions;
    for (int i = 0; i < partitions; ++i) {
        string out = inputFiles[0];
        auto idx = out.find("-u");
        out = out.substr(0, idx + 2);
        out += string("-ranged-") + to_string(i);
        string dictpartfile = dictfile + string(".") + to_string(i);
        BOOST_LOG_TRIVIAL(debug) << "Sorting partition " << i;

        threads[i] = boost::thread(boost::bind(Compressor::sortPartition,
                                               &inputFiles, dictpartfile,
                                               out, i, &counters[i], maxMem));


        /*Compressor::sortPartition(&inputFiles, dictpartfile, out, i,
                                  &counters[i], maxMem);*/
        outputfiles.push_back(out);
    }
    for (int i = 0; i < partitions; ++i) {
        threads[i].join();
    }

    //Re-read the sorted tuples and write by tripleID
    long startCounter = counter;
    for (int i = 0; i < partitions; ++i) {
        string infile = outputfiles[i];
        string outfile = outputfile + string(".") + to_string(i);
        threads[i] = boost::thread(boost::bind(
                                       &Compressor::assignCountersAndPartByTripleID,
                                       startCounter, infile,
                                       outfile, parallelProcesses));
        startCounter += counters[i];
    }
    for (int i = 0; i < partitions; ++i) {
        threads[i].join();
    }
}

void Compressor::assignCountersAndPartByTripleID(long startCounter,
        string infile, string outfile, int parallelProcesses) {
    LZ4Reader r(infile);

    LZ4Writer **outputs = new LZ4Writer*[parallelProcesses];
    for (int i = 0; i < parallelProcesses; ++i) {
        outputs[i] = new LZ4Writer(outfile + string(".") + to_string(i));
    }

    while (!r.isEof()) {
        const long c = r.parseLong();
        const long tid = r.parseLong();
        const  int idx = (long) (tid >> 2) % parallelProcesses;
        outputs[idx]->writeLong(tid);
        outputs[idx]->writeLong(c + startCounter);
    }

    for (int i = 0; i < parallelProcesses; ++i) {
        delete outputs[i];
    }
    delete[] outputs;
    fs::remove(infile);

}

void Compressor::mergeNotPopularEntries(vector<string> *inputFiles,
                                        string dictOutput,
                                        string outputFile1, string outputFile2,
                                        long * startCounter, int increment,
                                        int parallelProcesses) {

    //Sample one file: Get boundaries for parallelProcesses range partitions
    assert(inputFiles->size() > 0);
    fs::path p = fs::path(inputFiles->at(0)).parent_path();
    const std::vector<string> boundaries = getPartitionBoundaries(p.string(),
                                           parallelProcesses);
    assert(boundaries.size() == parallelProcesses - 1);

    //Range-partitions all the files in the input collection
    std::vector<string> rangePartitionedFiles;
    BOOST_LOG_TRIVIAL(debug) << "Range-partitions the files...";
    rangePartitionFiles(parallelProcesses, inputFiles,
                        rangePartitionedFiles, boundaries);

    //Collect all ranged-partitions files by partition and globally sort them.
    BOOST_LOG_TRIVIAL(debug) << "Sort and assign the counters to the files...";
    sortPartitionsAndAssignCounters(rangePartitionedFiles,
                                    dictOutput,
                                    outputFile2,
                                    boundaries.size() + 1,
                                    *startCounter, parallelProcesses);

    /*FileMerger<AnnotatedTerm> merger(*inputFiles);
    char *previousTerm = new char[MAX_TERM_SIZE + 2];
    Utils::encode_short(previousTerm, 0);
    long nextCounter = *startCounter;
    long currentCounter = nextCounter;

    LZ4Writer output1(outputFile1);
    LZ4Writer **output2 = new LZ4Writer*[parallelProcesses];
    for (int i = 0; i < parallelProcesses; ++i) {
        output2[i] = new LZ4Writer(outputFile2 + string(".") + to_string(i));
    }

    while (!merger.isEmpty()) {
        AnnotatedTerm t = merger.get();
        if (!t.equals(previousTerm)) {
            //Write a new entry in the global file
            currentCounter = nextCounter;
            globalDictOutput->writeLong(currentCounter);
            globalDictOutput->writeString(t.term, t.size);
            nextCounter += increment;
            memcpy(previousTerm, t.term, t.size);
        } else if (t.tripleIdAndPosition == -1) {
            continue;
        }

        if (t.tripleIdAndPosition == -1) {
            //Write it in output1
            output1.writeLong(currentCounter);
            output1.writeString(t.term, t.size);
        } else {
            //Write in output2
            int idx = (long) (t.tripleIdAndPosition >> 2) % parallelProcesses;
            output2[idx]->writeLong(t.tripleIdAndPosition);
            output2[idx]->writeLong(currentCounter);
        }
    }

    *startCounter = nextCounter;

    for (int i = 0; i < parallelProcesses; ++i) {
        delete output2[i];
    }
    delete[] output2;
    delete[] previousTerm;*/
}

void Compressor::sortByTripleID(vector<string> *inputFiles, string outputFile,
                                const long maxMemory) {
    //First sort the input files in chunks of x elements
    int idx = 0;
    vector<string> filesToMerge;
    {
        vector<TriplePair> pairs;

        for (int i = 0; i < inputFiles->size(); ++i) {
            //Read the file
            string fileName = (*inputFiles)[i];
            //Process the file
            LZ4Reader *fis = new LZ4Reader(fileName);
            while (!fis->isEof()) {
                if (sizeof(TriplePair) * pairs.size() >= maxMemory) {
                    string file = outputFile + string(".") + to_string(idx++);
                    sortAndDumpToFile2(pairs, file);
                    filesToMerge.push_back(file);
                    pairs.clear();
                }

                TriplePair tp;
                tp.readFrom(fis);
                pairs.push_back(tp);
            }
            delete fis;
            fs::remove(fileName);
        }

        if (pairs.size() > 0) {
            string file = outputFile + string(".") + to_string(idx++);
            sortAndDumpToFile2(pairs, file);
            filesToMerge.push_back(file);
        }
        pairs.clear();
    }

    //Then do a merge sort and write down the results on outputFile
    FileMerger<TriplePair> merger(filesToMerge);
    LZ4Writer writer(outputFile);
    while (!merger.isEmpty()) {
        TriplePair tp = merger.get();
        writer.writeLong(tp.tripleIdAndPosition);
        writer.writeLong(tp.term);
    }

    //Remove the input files
    for (int i = 0; i < filesToMerge.size(); ++i) {
        fs::remove(filesToMerge[i]);
    }
}

void Compressor::compressTriples(const int parallelProcesses, const int ndicts,
                                 string * permDirs, int nperms, int signaturePerms, vector<string> &notSoUncommonFiles,
                                 vector<string> &finalUncommonFiles, string * tmpFileNames,
                                 StringCollection * poolForMap, ByteArrayToNumberMap * finalMap) {
    /*** Compress the triples ***/
    LZ4Reader **dictFiles = new LZ4Reader*[ndicts];
    for (int i = 0; i < ndicts; ++i) {
        if (fs::exists(fs::status(fs::path(notSoUncommonFiles[i])))) {
            dictFiles[i] = new LZ4Reader(notSoUncommonFiles[i]);
        } else {
            dictFiles[i] = NULL;
        }
    }
    int iter = 0;
    int dictFileProcessed = 0;
    unsigned long maxMemorySize =
        calculateSizeHashmapCompression();
    BOOST_LOG_TRIVIAL(debug) << "Max hashmap size: " << maxMemorySize << " bytes. Initial size of the common map=" << finalMap->size() << " entries.";

    CompressedByteArrayToNumberMap uncommonMap;
    while (areFilesToCompress(parallelProcesses, tmpFileNames)) {
        string prefixOutputFile = "input-" + to_string(iter);

        //Put new terms in the finalMap
        int idx = 0;
        while (poolForMap->allocatedBytes() + uncommonMap.size() * 20
                < maxMemorySize && dictFileProcessed < ndicts) {
            LZ4Reader *dictFile = dictFiles[idx];
            if (dictFile != NULL && !dictFile->isEof()) {
                long compressedTerm = dictFile->parseLong();
                int sizeTerm;
                const char *term = dictFile->parseString(sizeTerm);
                if (uncommonMap.find(term) == uncommonMap.end()) {
                    const char *newTerm = poolForMap->addNew((char*) term,
                                          sizeTerm);
                    uncommonMap.insert(
                        std::make_pair(newTerm, compressedTerm));
                } else {
                    BOOST_LOG_TRIVIAL(error) << "This should not happen! Term " << term
                                             << " was already being inserted";
                }
            } else {
                BOOST_LOG_TRIVIAL(debug) << "Finished putting in the hashmap the elements in file " << notSoUncommonFiles[idx];
                if (dictFile != NULL) {
                    delete dictFile;
                    fs::remove(fs::path(notSoUncommonFiles[idx]));
                    dictFiles[idx] = NULL;
                }
                dictFileProcessed++;
                if (dictFileProcessed == ndicts) {
                    break;
                }
            }
            idx = (idx + 1) % ndicts;
        }

        BOOST_LOG_TRIVIAL(debug) << "Start compression threads... uncommon map size " << uncommonMap.size();
        boost::thread *threads = new boost::thread[parallelProcesses - 1];
        ParamsNewCompressProcedure p;
        p.permDirs = permDirs;
        p.nperms = nperms;
        p.signaturePerms = signaturePerms;
        p.prefixOutputFile = prefixOutputFile;
        p.itrN = iter;
        p.inNames = tmpFileNames;
        p.commonMap = iter == 0 ? finalMap : NULL;
        p.map = &uncommonMap;
        p.parallelProcesses = parallelProcesses;

        for (int i = 1; i < parallelProcesses; ++i) {
            p.part = i;
            p.uncommonTermsFile = iter == 0 ? &finalUncommonFiles[i] : NULL;
            threads[i - 1] = boost::thread(
                                 boost::bind(&Compressor::newCompressTriples, this, p));
        }
        p.part = 0;
        p.uncommonTermsFile = iter == 0 ? &finalUncommonFiles[0] : NULL;
        newCompressTriples(p);
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1].join();
        }
        delete[] threads;

        //Clean the map
        finalMap->clear();
        uncommonMap.clear();
        poolForMap->clear();

        //New iteration!
        iter++;
    }

    delete[] dictFiles;
}

void Compressor::sortFilesByTripleSource(string kbPath,
        const int parallelProcesses,
        const int ndicts, vector<string> uncommonFiles,
        vector<string> &outputFiles) {
    /*** Sort the files which contain the triple source ***/
    BOOST_LOG_TRIVIAL(debug) << "Sort uncommon triples by triple id";
    vector<vector<string>> inputFinalSorting(parallelProcesses);

    assert(uncommonFiles.size() == 1);
    auto pdir = fs::path(uncommonFiles[0]).parent_path();
    string nameFile = fs::path(uncommonFiles[0]).filename().string();
    fs::directory_iterator ei;
    for (fs::directory_iterator diter(pdir); diter != ei; ++diter) {
        if (fs::is_regular_file(diter->status())) {
            auto pfile = diter->path();
            if (boost::starts_with(pfile.filename().string(), nameFile)) {
                if (pfile.has_extension()) {
                    string ext = pfile.extension().string();
                    int idx;
                    stringstream(ext.substr(1, ext.length() - 1)) >> idx;
                    inputFinalSorting[idx].push_back(diter->path().string());
                }
            }
        }
    }

    for (int i = 0; i < parallelProcesses; ++i) {
        outputFiles.push_back(kbPath + string("/listUncommonTerms") + to_string(i));
    }

    boost::thread *threads = new boost::thread[parallelProcesses - 1];
    const long maxMem = max((long) MIN_MEM_SORT_TRIPLES,
                            (long) (Utils::getSystemMemory() * 0.7) / parallelProcesses);
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1] = boost::thread(
                             boost::bind(&Compressor::sortByTripleID, this,
                                         &inputFinalSorting[i], outputFiles[i], maxMem));
    }
    sortByTripleID(&inputFinalSorting[0], outputFiles[0], maxMem);
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1].join();
    }
    delete[] threads;
}

void Compressor::sortDictionaryEntriesByText(string **input, const int ndicts,
        const int parallelProcesses, string * prefixOutputFiles,
        int *noutputfiles, ByteArrayToNumberMap * map, bool filterDuplicates,
        bool sample) {
    long maxMemAllocate = max((long) (BLOCK_SUPPORT_BUFFER_COMPR * 2),
                              (long) (Utils::getSystemMemory() * 0.70 / ndicts));
    BOOST_LOG_TRIVIAL(debug) << "Sorting dictionary entries for partitions";
    boost::thread *threads = new boost::thread[ndicts - 1];

    BOOST_LOG_TRIVIAL(debug) << "Max memory to use to sort inmemory a number of terms: " << maxMemAllocate << " bytes";
    immemorysort(input, parallelProcesses, prefixOutputFiles[0],
                 &noutputfiles[0], filterDuplicates, maxMemAllocate, sample);
    delete[] threads;
    BOOST_LOG_TRIVIAL(debug) << "...done";
}

void Compressor::compress(string * permDirs, int nperms, int signaturePerms,
                          string * dictionaries,
                          int ndicts, int parallelProcesses) {

    /*** Sort the infrequent terms ***/
    int *nsortedFiles = new int[ndicts];
    BOOST_LOG_TRIVIAL(debug) << "Sorting common dictionary entries for partitions";
    sortDictionaryEntriesByText(dictFileNames, ndicts, parallelProcesses,
                                dictionaries, nsortedFiles, finalMap, true,
                                false);
    BOOST_LOG_TRIVIAL(debug) << "...done";

    /*** Sort the very infrequent terms ***/
    int *nsortedFiles2 = new int[ndicts];
    BOOST_LOG_TRIVIAL(debug) << "Sorting uncommon dictionary entries for partitions";
    string *uncommonDictionaries = new string[ndicts];
    for (int i = 0; i < ndicts; ++i) {
        uncommonDictionaries[i] = dictionaries[i] + string("-u");
    }
    sortDictionaryEntriesByText(uncommonDictFileNames, ndicts,
                                parallelProcesses, uncommonDictionaries,
                                nsortedFiles2, NULL, false, true);
    BOOST_LOG_TRIVIAL(debug) << "...done";

    /*** Deallocate the dictionary files ***/
    for (int i = 0; i < parallelProcesses; ++i) {
        delete[] dictFileNames[i];
        delete[] uncommonDictFileNames[i];
    }
    delete[] dictFileNames;
    delete[] uncommonDictFileNames;

    /*** Create the final dictionaries to be written and initialize the
     * counters and other data structures ***/
    LZ4Writer **writers = new LZ4Writer*[ndicts];
    long *counters = new long[ndicts];
    vector<vector<string> > filesToBeMerged;
    vector<string> notSoUncommonFiles;
    vector<string> uncommonFiles;

    for (int i = 0; i < ndicts; ++i) {
        vector<string> files;
        for (int j = 0; j < nsortedFiles[i]; ++j) {
            files.push_back(dictionaries[i] + string(".") + to_string(j));
        }
        for (int j = 0; j < nsortedFiles2[i]; ++j) {
            files.push_back(uncommonDictionaries[i] + string(".") + to_string(j));
        }
        filesToBeMerged.push_back(files);
        writers[i] = new LZ4Writer(dictionaries[i]);
        counters[i] = i;
        notSoUncommonFiles.push_back(dictionaries[i] + string("-np1"));
        uncommonFiles.push_back(dictionaries[i] + string("-np2"));
    }
    delete[] nsortedFiles;
    delete[] nsortedFiles2;
    delete[] uncommonDictionaries;

    /*** Assign a number to the popular entries ***/
    BOOST_LOG_TRIVIAL(debug) << "Assign a number to " << finalMap->size() <<
                             " popular terms in the dictionary";
    assignNumbersToCommonTermsMap(finalMap, counters, writers, NULL, ndicts, true);

    /*** Assign a number to the other entries. Split them into two files.
     * The ones that must be loaded into the hashmap, and the ones used for the merge join ***/
    BOOST_LOG_TRIVIAL(debug) << "Merge (and assign counters) of dictionary entries";
    /*boost::thread *threads = new boost::thread[ndicts - 1];
    for (int i = 1; i < ndicts; ++i) {
        threads[i - 1] = boost::thread(
                             boost::bind(&Compressor::mergeNotPopularEntries, this,
                                         &filesToBeMerged[i], writers[i], notSoUncommonFiles[i],
                                         uncommonFiles[i], &counters[i], ndicts,
                                         parallelProcesses));
    }*/
    if (ndicts > 1) {
        BOOST_LOG_TRIVIAL(error) << "The current version of the code supports only one dictionary partition";
        throw 10;
    }
    if (!filesToBeMerged[0].empty()) {
        mergeNotPopularEntries(&filesToBeMerged[0], dictionaries[0],
                               notSoUncommonFiles[0], uncommonFiles[0], &counters[0], ndicts,
                               parallelProcesses);
    }
    /*for (int i = 1; i < ndicts; ++i) {
        threads[i - 1].join();
    }
    delete[] threads;*/
    BOOST_LOG_TRIVIAL(debug) << "... done";

    /*** Remove unused data structures ***/
    for (int i = 0; i < ndicts; ++i) {
        delete writers[i];
        vector<string> filesToBeRemoved = filesToBeMerged[i];
        for (int j = 0; j < filesToBeRemoved.size(); ++j) {
            fs::remove(fs::path(filesToBeRemoved[j]));
        }
    }

    /*** Sort files by triple source ***/
    vector<string> sortedFiles;
    sortFilesByTripleSource(kbPath, parallelProcesses, ndicts, uncommonFiles, sortedFiles);

    /*** Compress the triples ***/
    compressTriples(parallelProcesses, ndicts, permDirs, nperms, signaturePerms,
                    notSoUncommonFiles, sortedFiles, tmpFileNames,
                    poolForMap, finalMap);

    /*** Clean up remaining datastructures ***/
    delete[] counters;
    for (int i = 0; i < parallelProcesses; ++i) {
        fs::remove(tmpFileNames[i]);
        fs::remove(sortedFiles[i]);
    }
    delete[] tmpFileNames;
    delete poolForMap;
    delete finalMap;
    poolForMap = NULL;
    finalMap = NULL;
}

bool stringComparator(string stringA, string stringB) {
    const char *ac = stringA.c_str();
    const char *bc = stringB.c_str();
    for (int i = 0; ac[i] != '\0' && bc[i] != '\0'; i++) {
        if (ac[i] != bc[i]) {
            return ac[i] < bc[i];
        }
    }
    return stringA.size() < stringB.size();
}

Compressor::~Compressor() {
    if (finalMap != NULL)
        delete finalMap;
    if (poolForMap != NULL)
        delete poolForMap;
}

unsigned long Compressor::calculateSizeHashmapCompression() {
    long memoryAvailable = Utils::getSystemMemory() * 0.70;
    return memoryAvailable;
}

unsigned long Compressor::calculateMaxEntriesHashmapCompression() {
    long memoryAvailable = min((int)(Utils::getSystemMemory() / 3 / 50), 90000000);
    return memoryAvailable;
}

bool _lessExtensions(const string &a, const string &b) {
    string ea = fs::path(a).extension().string();
    string eb = fs::path(b).extension().string();

    int idxa, idxb;
    std::istringstream ss1(ea.substr(1, ea.length() - 1));
    ss1 >> idxa;
    std::istringstream ss2(eb.substr(1, eb.length() - 1));
    ss2 >> idxb;
    return idxa < idxb;
}

std::vector<string> Compressor::getAllDictFiles(string prefixDict) {
    std::vector<string> output;

    fs::directory_iterator end;
    fs::path parentDir = fs::path(prefixDict).parent_path();
    string filename = fs::path(prefixDict).filename().string();
    for (fs::directory_iterator dir_iter(parentDir); dir_iter != end;
            ++dir_iter) {
        if (boost::starts_with(dir_iter->path().filename().string(), filename)
                && dir_iter->path().has_extension()) {
            output.push_back(dir_iter->path().string());
        }
    }

    std::sort(output.begin(), output.end(), _lessExtensions);
    return output;
}
