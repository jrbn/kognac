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
    //vector<FileInfo> &files = params.files;
    DiskReader *filesreader = params.reader;
    DiskLZ4Writer *fileswriter = params.writer;
    const int idwriter = params.idwriter;
    Hashtable *table1 = params.table1;
    Hashtable *table2 = params.table2;
    Hashtable *table3 = params.table3;
    //string outFile = params.outFile;
    SchemaExtractor *extractor = params.extractor;
    long *distinctValues = params.distinctValues;
    std::vector<string> *resultsMGS = params.resultsMGS;
    size_t sizeHeap = params.sizeHeap;

    //LZ4Writer out(outFile);
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

    size_t sizebuffer = 0;
    bool gzipped = false;
    char *buffer = filesreader->getfile(sizebuffer, gzipped);
    while (buffer != NULL) {
        FileReader reader(buffer, sizebuffer, gzipped);
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
                fileswriter->writeByte(idwriter, 0);
                estimator.addElement(h1, h2, h3);

                //This is an hack to save memcpy...
                fileswriter->writeVLong(idwriter, length + 2);
                fileswriter->writeShort(idwriter, length);
                fileswriter->writeRawArray(idwriter, supportBuffer, length);

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
                fileswriter->writeByte(idwriter, 0);
                estimator.addElement(h1, h2, h3);

                fileswriter->writeVLong(idwriter, length + 2);
                fileswriter->writeShort(idwriter, length);
                fileswriter->writeRawArray(idwriter, supportBuffer, length);

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
                fileswriter->writeByte(idwriter, 0);
                estimator.addElement(h1, h2, h3);

                fileswriter->writeVLong(idwriter, length + 2);
                fileswriter->writeShort(idwriter, length);
                fileswriter->writeRawArray(idwriter, supportBuffer, length);
            } else {
                countNotValid++;
            }
        }
        //Get next file
        filesreader->releasefile(buffer);
        buffer = filesreader->getfile(sizebuffer, gzipped);
    }

    fileswriter->setTerminated(idwriter);
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
                                     const int idwriter,
                                     DiskLZ4Writer *writer,
                                     //LZ4Writer **udictFile,
                                     const long tripleId,
                                     const int pos,
                                     const int partitions,
                                     const bool copyHashes,
                                     char **prevEntries, int *sPrevEntries) {

    if (map->find(term) == map->end()) {
        //const int partition = Utils::getPartition(term + 2, sizeTerm - 2,
        //                      partitions);
        if (!copyHashes) {
            //Use the simpler data structure
            SimplifiedAnnotatedTerm t;
            t.size = sizeTerm - 2;
            t.term = term + 2;
            t.tripleIdAndPosition = (long) (tripleId << 2) + (pos & 0x3);
            t.writeTo(idwriter, writer);
        } else {
            AnnotatedTerm t;
            t.size = sizeTerm;
            t.term = term;
            t.tripleIdAndPosition = (long) (tripleId << 2) + (pos & 0x3);
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
            t.writeTo(idwriter, writer);
        }
    } else {
        //What happen here?
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
    countFrequent++;
    bool mapTooSmall = map->size() < maxMapSize;
    if ((mapTooSmall || valueHighEnough)
            && map->find(string(term + 2, sizeTerm - 2)) == map->end()) {
        std::pair<string, long> pair = std::make_pair(string(term + 2, sizeTerm - 2),
                                       minValue);
        map->insert(pair);
        queue.push(pair);
        if (map->size() > maxMapSize) {
            //Replace term and minCount with values to be added
            std::pair<string, long> elToRemove = queue.top();
            queue.pop();
            map->erase(elToRemove.first);
            minValueToBeAdded = queue.top().second;
        }
    }
}

void Compressor::extractUncommonTerms(const int dictPartitions,
                                      DiskLZ4Reader *reader,
                                      const int inputFileId,
                                      const bool copyHashes, const int idProcess,
                                      const int parallelProcesses,
                                      DiskLZ4Writer *writer,
                                      //string *udictFileName,
                                      const bool splitByHash) {

    //Either one or the other. Both are not supported in extractUncommonTerm
    assert(!splitByHash || dictPartitions == 1);
    assert(!splitByHash);// should not be invoked anymore

    int partitions = dictPartitions;
    if (splitByHash)
        partitions = partitions * parallelProcesses;

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

    while (!reader->isEOF(inputFileId)) {
        int sizeTerm = 0;
        int flag = reader->readByte(inputFileId); //Ignore it. Should always be 0
        if (flag != 0) {
            BOOST_LOG_TRIVIAL(error) << "Flag should always be zero!";
            throw 10;
        }

        const char *term = reader->readString(inputFileId, sizeTerm);
        if (copyHashes) {
            if (pos != 2) {
                memcpy(prevEntries[pos], term, sizeTerm);
                sPrevEntries[pos] = sizeTerm;
            } else {
                prevEntries[2] = (char*)term;
                sPrevEntries[2] = sizeTerm;

                extractUncommonTerm(prevEntries[0], sPrevEntries[0], finalMap,
                                    inputFileId,
                                    writer, tripleId, 0,
                                    (splitByHash) ? parallelProcesses : dictPartitions, copyHashes,
                                    prevEntries, sPrevEntries);

                extractUncommonTerm(prevEntries[1], sPrevEntries[1], finalMap,
                                    inputFileId,
                                    writer, tripleId, 1,
                                    (splitByHash) ? parallelProcesses : dictPartitions, copyHashes,
                                    prevEntries, sPrevEntries);

                extractUncommonTerm(term, sizeTerm, finalMap,
                                    inputFileId,
                                    writer, tripleId, pos,
                                    (splitByHash) ? parallelProcesses : dictPartitions, copyHashes,
                                    prevEntries, sPrevEntries);
            }
        } else {
            extractUncommonTerm(term, sizeTerm, finalMap,
                                inputFileId,
                                writer, tripleId, pos,
                                (splitByHash) ? parallelProcesses : dictPartitions, copyHashes,
                                prevEntries, sPrevEntries);
        }

        pos = (pos + 1) % 3;
        if (pos == 0) {
            tripleId += parallelProcesses;
        }
    }

    writer->setTerminated(inputFileId);

    if (copyHashes) {
        delete[] prevEntries[0];
        delete[] prevEntries[1];
    }

    /*for (int i = 0; i < partitions; ++i) {
        delete udictFile[i];
    }
    delete[] udictFile;*/
}

void Compressor::extractCommonTerms(ParamsExtractCommonTermProcedure params) {

    //string inputFile = params.inputFile;
    DiskLZ4Reader *reader = params.reader;
    const int idReader = params.idReader;
    Hashtable **tables = params.tables;
    GStringToNumberMap *map = params.map;
    int dictPartitions = params.dictPartitions;
    int maxMapSize = params.maxMapSize;

    int pos = 0;
    int thresholdForUncommon = params.thresholdForUncommon;

    Hashtable *table1 = tables[0];
    Hashtable *table2 = tables[1];
    Hashtable *table3 = tables[2];

    //LZ4Reader reader(inputFile);
    map->set_empty_key(EMPTY_KEY);
    map->set_deleted_key(DELETED_KEY);

    long minValueToBeAdded = 0;
    std::priority_queue<std::pair<string, long>,
        std::vector<std::pair<string, long> >, priorityQueueOrder> queue;

    long countFrequent = 0;

    while (!reader->isEOF(idReader)) {
        int sizeTerm = 0;
        int flag = reader->readByte(idReader); //Ignore it. Should always be 0
        assert(flag == 0);
        const char *term = reader->readString(idReader, sizeTerm);
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
    long compressedTriples = 0;
    long compressedTerms = 0;
    long uncompressedTerms = 0;
    DiskLZ4Reader *uncommonTermsReader = params.readerUncommonTerms;
    DiskLZ4Reader *r = params.reader;
    const int idReader = params.idReader;

    const int nperms = params.nperms;
    MultiDiskLZ4Writer *writer = params.writer;
    const int startIdxWriter = params.idxWriter;
    int detailPerms[6];
    Compressor::parsePermutationSignature(params.signaturePerms, detailPerms);

    long nextTripleId = -1;
    int nextPos = -1;
    long nextTerm = -1;

    if (!uncommonTermsReader->isEOF(idReader)) {
        long tripleId = uncommonTermsReader->readLong(idReader);
        nextTripleId = tripleId >> 2;
        nextPos = tripleId & 0x3;
        nextTerm = uncommonTermsReader->readLong(idReader);
    } else {
        BOOST_LOG_TRIVIAL(debug) << "No uncommon file is provided";
    }

    long currentTripleId = params.part;
    int increment = params.parallelProcesses;

    long triple[3];
    char *tTriple = new char[MAX_TERM_SIZE * 3];
    bool valid[3];

    /*
    SimpleTripleWriter **permWriters = new SimpleTripleWriter*[params.nperms];
    const int nperms = params.nperms;
    for (int i = 0; i < nperms; ++i) {
        permWriters[i] = new SimpleTripleWriter(params.permDirs[i],
                                                params.prefixOutputFile + to_string(params.part), false);
    }*/

    //This byte is written by the SimpleTripleWriter
    for (int i = 0; i < nperms; ++i) {
        writer->writeByte(startIdxWriter + i, 0);
    }

    while (!r->isEOF(idReader)) {
        for (int i = 0; i < 3; ++i) {
            valid[i] = false;
            int flag = r->readByte(idReader);
            if (flag == 1) {
                //convert number
                triple[i] = r->readLong(idReader);
                valid[i] = true;
            } else {
                //Match the text against the hashmap
                int size;
                const char *tTerm = r->readString(idReader, size);

                if (currentTripleId == nextTripleId && nextPos == i) {
                    triple[i] = nextTerm;
                    valid[i] = true;
                    if (!uncommonTermsReader->isEOF(idReader)) {
                        long tripleId = uncommonTermsReader->readLong(idReader);
                        nextTripleId = tripleId >> 2;
                        nextPos = tripleId & 0x3;
                        nextTerm = uncommonTermsReader->readLong(idReader);
                    } else {
                        //BOOST_LOG_TRIVIAL(debug) << "File " << idReader << " is finished";
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
                    assert(ok);
                }
            }
        }

        if (valid[0] && valid[1] && valid[2]) {
            for (int i = 0; i < nperms; ++i) {
                switch (detailPerms[i]) {
                case IDX_SPO:
                    writer->writeLong(startIdxWriter + i, triple[0]);
                    writer->writeLong(startIdxWriter + i, triple[1]);
                    writer->writeLong(startIdxWriter + i, triple[2]);
                    break;
                case IDX_OPS:
                    writer->writeLong(startIdxWriter + i, triple[2]);
                    writer->writeLong(startIdxWriter + i, triple[1]);
                    writer->writeLong(startIdxWriter + i, triple[0]);
                    break;
                case IDX_SOP:
                    writer->writeLong(startIdxWriter + i, triple[0]);
                    writer->writeLong(startIdxWriter + i, triple[2]);
                    writer->writeLong(startIdxWriter + i, triple[1]);
                    break;
                case IDX_OSP:
                    writer->writeLong(startIdxWriter + i, triple[2]);
                    writer->writeLong(startIdxWriter + i, triple[0]);
                    writer->writeLong(startIdxWriter + i, triple[1]);
                    break;
                case IDX_PSO:
                    writer->writeLong(startIdxWriter + i, triple[1]);
                    writer->writeLong(startIdxWriter + i, triple[0]);
                    writer->writeLong(startIdxWriter + i, triple[2]);
                    break;
                case IDX_POS:
                    writer->writeLong(startIdxWriter + i, triple[1]);
                    writer->writeLong(startIdxWriter + i, triple[2]);
                    writer->writeLong(startIdxWriter + i, triple[0]);
                    break;
                }
            }
            compressedTriples++;
        } else {
            throw 10; //should never happen
        }
        currentTripleId += increment;
    }

    for (int i = 0; i < nperms; ++i) {
        writer->setTerminated(startIdxWriter + i);
    }

    if (uncommonTermsReader != NULL) {
        if (!(uncommonTermsReader->isEOF(idReader))) {
            BOOST_LOG_TRIVIAL(error) << "There are still elements to read in the uncommon file";
            throw 10;
        }
    }
    delete[] tTriple;

    BOOST_LOG_TRIVIAL(debug) << "Compressed triples " << compressedTriples << " compressed terms " << compressedTerms << " uncompressed terms " << uncompressedTerms;
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
    vector<FileInfo> *files = splitInputInChunks(input, maxReadingThreads);

    /*** Set name dictionary files ***/
    dictFileNames = new string*[maxReadingThreads];
    uncommonDictFileNames = new string*[maxReadingThreads];
    for (int i = 0; i < maxReadingThreads ; ++i) {
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

    SchemaExtractor *extractors = new SchemaExtractor[parallelProcesses];

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
        DiskLZ4Reader **readers = new DiskLZ4Reader*[maxReadingThreads];
        DiskLZ4Writer **writers = new DiskLZ4Writer*[maxReadingThreads];
        for (int i = 0; i < maxReadingThreads; ++i) {
            readers[i] = new DiskLZ4Reader(tmpFileNames[i],
                                           parallelProcesses / maxReadingThreads, 3);
            writers[i] = new DiskLZ4Writer(uncommonDictFileNames[i][0],
                                           parallelProcesses / maxReadingThreads, 3);
        }

        boost::thread *threads = new boost::thread[parallelProcesses];
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1] = boost::thread(
                                 boost::bind(&Compressor::extractUncommonTerms, this,
                                             dictPartitions, readers[i % maxReadingThreads],
                                             i / maxReadingThreads,
                                             copyHashes,
                                             i, parallelProcesses,
                                             writers[i % maxReadingThreads],
                                             //uncommonDictFileNames[i],
                                             splitUncommonByHash));
        }
        extractUncommonTerms(dictPartitions, readers[0], 0, copyHashes, 0,
                             parallelProcesses,
                             writers[0],
                             //uncommonDictFileNames[0],
                             splitUncommonByHash);
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1].join();
        }

        for (int i = 0; i < maxReadingThreads; ++i) {
            delete readers[i];
            delete writers[i];
        }
        delete[] readers;
        delete[] writers;

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
                                        const int maxReadingThreads,
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

    //Init the DiskReaders
    DiskLZ4Reader **readers = new DiskLZ4Reader*[maxReadingThreads];
    for (int i = 0; i < maxReadingThreads; ++i) {
        readers[i] = new DiskLZ4Reader(tmpFileNames[i],
                                       parallelProcesses / maxReadingThreads, 3);
    }

    BOOST_LOG_TRIVIAL(debug) << "Extract the common terms";
    for (int i = 1; i < parallelProcesses; ++i) {
        //params.inputFile = tmpFileNames[i];
        params.reader = readers[i % maxReadingThreads];
        params.idReader = i / maxReadingThreads;
        params.map = &commonTermsMaps[i];
        params.dictFileName = dictFileNames[i];
        params.idProcess = i;
        params.singleTerms = uncommonDictFileNames[i];
        threads[i - 1] = boost::thread(
                             boost::bind(&Compressor::extractCommonTerms, this, params));
    }
    //params.inputFile = tmpFileNames[0];
    params.reader = readers[0];
    params.idReader = 0;
    params.map = &commonTermsMaps[0];
    params.dictFileName = dictFileNames[0];
    params.idProcess = 0;
    params.singleTerms = uncommonDictFileNames[0];
    extractCommonTerms(params);
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1].join();
    }
    for (int i = 0; i < maxReadingThreads; ++i)
        delete readers[i];
    delete[] readers;
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
    long memForHashTables = (long)(Utils::getSystemMemory() * 0.5)
                            / (1 + parallelProcesses) / 3;
    //Divided numer hash tables
    const unsigned int sizeHashTable = std::min((long)maxSize,
                                       (long)std::max((unsigned int)1000000,
                                               (unsigned int)(memForHashTables / sizeof(long))));
    BOOST_LOG_TRIVIAL(debug) << "Size hash table " << sizeHashTable;

    if (parallelProcesses % maxReadingThreads != 0) {
        BOOST_LOG_TRIVIAL(error) << "The maximum number of threads must be a multiplier of the reading threads";
        throw 10;
    }


    //Set up the output file names
    //std::vector<std::vector<string>> blocksOutputFiles;
    //blocksOutputFiles.resize(maxReadingThreads);
    for (int i = 0; i < maxReadingThreads; ++i) {
        tmpFileNames[i] = kbPath + string("/tmp-") + boost::lexical_cast<string>(i);
        //blocksOutputFiles[i % maxReadingThreads].push_back(tmpFileNames[i]);
    }

    DiskReader **readers = new DiskReader*[maxReadingThreads];
    boost::thread *threadReaders = new boost::thread[maxReadingThreads];
    DiskLZ4Writer **writers = new DiskLZ4Writer*[maxReadingThreads];
    for (int i = 0; i < maxReadingThreads; ++i) {
        readers[i] = new DiskReader(max(2, (int)(parallelProcesses / maxReadingThreads) * 2), &files[i]);
        threadReaders[i] = boost::thread(boost::bind(&DiskReader::run, readers[i]));
        writers[i] = new DiskLZ4Writer(tmpFileNames[i], parallelProcesses / maxReadingThreads, 3);
    }

    ParamsUncompressTriples params;
    //Set only global params
    params.sizeHeap = sampleArg;

    for (int i = 1; i < parallelProcesses; ++i) {
        tables1[i] = new Hashtable(sizeHashTable,
                                   &Hashes::dbj2s_56);
        tables2[i] = new Hashtable(sizeHashTable,
                                   &Hashes::fnv1a_56);
        tables3[i] = new Hashtable(sizeHashTable,
                                   &Hashes::murmur3_56);


        //params.files = files[i];
        params.reader = readers[i % maxReadingThreads];
        params.table1 = tables1[i];
        params.table2 = tables2[i];
        params.table3 = tables3[i];
        params.writer = writers[i % maxReadingThreads];
        params.idwriter = i / maxReadingThreads;
        params.extractor = copyHashes ? extractors + i : NULL;
        params.distinctValues = distinctValues + i;
        params.resultsMGS = usemisgra ? &resultsMGS[i] : NULL;
        threads[i - 1] = boost::thread(
                             boost::bind(&Compressor::uncompressTriples, this,
                                         params));
    }
    tables1[0] = new Hashtable(sizeHashTable,
                               &Hashes::dbj2s_56);
    tables2[0] = new Hashtable(sizeHashTable,
                               &Hashes::fnv1a_56);
    tables3[0] = new Hashtable(sizeHashTable,
                               &Hashes::murmur3_56);
    //params.files = files[0];
    params.reader = readers[0];
    params.table1 = tables1[0];
    params.table2 = tables2[0];
    params.table3 = tables3[0];
    params.writer = writers[0];
    params.idwriter = 0;
    params.extractor = copyHashes ? extractors : NULL;
    params.distinctValues = distinctValues;
    params.resultsMGS = usemisgra ? &resultsMGS[0] : NULL;
    uncompressTriples(params);

    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1].join();
    }
    for (int i = 0; i < maxReadingThreads; ++i) {
        delete readers[i];
        delete writers[i];
    }
    delete[] readers;
    delete[] writers;
    delete[] threadReaders;

    //Merging the tables
    BOOST_LOG_TRIVIAL(debug) << "Merging the tables...";

    for (int i = 0; i < parallelProcesses; ++i) {
        if (i != 0) {
            BOOST_LOG_TRIVIAL(debug) << "Merge table " << (i);
            tables1[0]->merge(tables1[i]);
            tables2[0]->merge(tables2[i]);
            tables3[0]->merge(tables3[i]);
            delete tables1[i];
            delete tables2[i];
            delete tables3[i];
        }
    }

    /*** If misra-gries is not active, then we must perform another pass to
     * extract the strings of the common terms. Otherwise, MGS gives it to
     * us ***/
    if (!usemisgra) {
        do_countmin_secondpass(dictPartitions, sampleArg, maxReadingThreads,
                               parallelProcesses,
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

void Compressor::sortAndDumpToFile(vector<SimplifiedAnnotatedTerm> &terms,
                                   string outputFile,
                                   bool removeDuplicates) {
    if (removeDuplicates) {
        throw 10; //I removed the code below to check for duplicates
    }
    //BOOST_LOG_TRIVIAL(debug) << "Sorting and writing to file " << outputFile << " " << terms.size() << " elements. Removedupl=" << removeDuplicates;
    std::sort(terms.begin(), terms.end(), SimplifiedAnnotatedTerm::sless);
    //BOOST_LOG_TRIVIAL(debug) << "Finished sorting";
    LZ4Writer *outputSegment = new LZ4Writer(outputFile);
    //const char *prevTerm = NULL;
    //int sizePrevTerm = 0;
    long countOutput = 0;
    for (vector<SimplifiedAnnotatedTerm>::iterator itr = terms.begin();
            itr != terms.end();
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
    //BOOST_LOG_TRIVIAL(debug) << "Written sorted elements: " << countOutput;
}

void Compressor::sortAndDumpToFile(vector<SimplifiedAnnotatedTerm> &terms,
                                   DiskLZ4Writer *writer,
                                   const int id) {
    boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
    std::sort(terms.begin(), terms.end(), SimplifiedAnnotatedTerm::sless);
    boost::chrono::duration<double> dur = boost::chrono::system_clock::now() - start;
    //BOOST_LOG_TRIVIAL(debug) << "Time sorting " << terms.size() << " elements was " << dur.count() << "sec.";
    start = boost::chrono::system_clock::now();
    writer->writeLong(id, terms.size());
    for (vector<SimplifiedAnnotatedTerm>::iterator itr = terms.begin();
            itr != terms.end();
            ++itr) {
        itr->writeTo(id, writer);
    }
    dur = boost::chrono::system_clock::now() - start;
    //BOOST_LOG_TRIVIAL(debug) << "Time dumping " << terms.size() << " terms on the writing buffer " << dur.count() << "sec.";

}

void Compressor::immemorysort(string **inputFiles,
                              int maxReadingThreads,
                              int parallelProcesses,
                              string outputFile, //int *noutputFiles,
                              bool removeDuplicates,
                              const long maxSizeToSort, bool sample) {
    timens::system_clock::time_point start = timens::system_clock::now();

    //Split maxSizeToSort in n threads
    const long maxMemPerThread = maxSizeToSort / parallelProcesses;

    DiskLZ4Reader **readers = new DiskLZ4Reader*[maxReadingThreads];
    memset(readers, 0, sizeof(DiskLZ4Reader*)*maxReadingThreads);
    bool empty = true;
    for (int i = 0; i < maxReadingThreads; ++i) {
        if (fs::exists(inputFiles[i][0])) {
            readers[i] = new DiskLZ4Reader(inputFiles[i][0],
                                           parallelProcesses / maxReadingThreads,
                                           3);
            empty = false;
        }
    }
    DiskLZ4Writer **writers = new DiskLZ4Writer*[maxReadingThreads];
    for (int i = 0; i < maxReadingThreads; ++i) {
        writers[i] = new DiskLZ4Writer(outputFile + string(".") + to_string(i),
                                       parallelProcesses / maxReadingThreads,
                                       3);
    }

    std::vector<std::vector<string>> chunks;
    chunks.resize(maxReadingThreads);
    MultiDiskLZ4Writer **sampleWriters = new MultiDiskLZ4Writer*[maxReadingThreads];
    for (int i = 0; i < parallelProcesses; ++i) {
        chunks[i % maxReadingThreads].push_back(
            outputFile + "-" + to_string(i) + "-sample");
    }
    for (int i = 0; i < maxReadingThreads; ++i) {
        sampleWriters[i] = new MultiDiskLZ4Writer(chunks[i], 3, 3);
    }

    if (!empty) {
        boost::thread *threads = new boost::thread[parallelProcesses - 1];
        for (int i = 1; i < parallelProcesses; ++i) {
            DiskLZ4Reader *reader = readers[i % maxReadingThreads];
            DiskLZ4Writer *writer = writers[i % maxReadingThreads];
            MultiDiskLZ4Writer *sampleWriter = sampleWriters[i % maxReadingThreads];
            if (reader) {
                threads[i - 1] = boost::thread(
                                     boost::bind(
                                         &Compressor::inmemorysort_seq,
                                         reader,
                                         writer,
                                         sampleWriter,
                                         i / maxReadingThreads,
                                         i,
                                         maxMemPerThread,
                                         removeDuplicates,
                                         sample));
            }
        }
        DiskLZ4Reader *reader = readers[0];
        DiskLZ4Writer *writer = writers[0];
        MultiDiskLZ4Writer *sampleWriter = sampleWriters[0];
        if (reader) {
            inmemorysort_seq(reader, writer,
                             sampleWriter, 0, 0,
                             maxMemPerThread,
                             removeDuplicates,
                             sample);
        }
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1].join();
        }
        delete[] threads;
    }
    for (int i = 0; i < maxReadingThreads; ++i) {
        if (readers[i])
            delete readers[i];
        delete writers[i];
        delete sampleWriters[i];
    }
    delete[] readers;
    delete[] writers;
    delete[] sampleWriters;

    /*    //Collect all files
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
        *noutputFiles = files.size();*/

    //boost::chrono::duration<double> sec = boost::chrono::system_clock::now()
    //                                      - start;
    //BOOST_LOG_TRIVIAL(debug) << "Total sorting time = " << sec.count() * 1000
    //                         << " ms";
}

void Compressor::inmemorysort_seq(DiskLZ4Reader *reader,
                                  DiskLZ4Writer *writer,
                                  MultiDiskLZ4Writer *sampleWriter,
                                  const int idReader,
                                  int idx,
                                  const long maxMemPerThread,
                                  bool removeDuplicates,
                                  bool sample) {

    vector<SimplifiedAnnotatedTerm> terms;
    //vector<string> outputfiles;
    StringCollection supportCollection(BLOCK_SUPPORT_BUFFER_COMPR);
    long bytesAllocated = 0;

    //BOOST_LOG_TRIVIAL(debug) << "Start immemory_seq method. MaxMemPerThread=" << maxMemPerThread;

    //long count = 0;
    int sampleCount = 0;
    int sampleAdded = 0;
    while (!reader->isEOF(idReader)) {
        SimplifiedAnnotatedTerm t;
        t.readFrom(idReader, reader);
        if (sample) {
            if (sampleCount % 100 == 0) {
                sampleWriter->writeString(idReader, t.term, t.size);
                sampleAdded++;
                sampleCount = 0;
            }
            sampleCount++;
        }

        if ((bytesAllocated + (sizeof(AnnotatedTerm) * terms.size() * 2))
                >= maxMemPerThread) {
            sortAndDumpToFile(terms, writer, idReader);
            terms.clear();
            supportCollection.clear();
            bytesAllocated = 0;
        }

        t.term = supportCollection.addNew((char *) t.term, t.size);
        terms.push_back(t);
        bytesAllocated += t.size;
    }

    if (terms.size() > 0) {
        sortAndDumpToFile(terms, writer, idReader);
    }
    writer->setTerminated(idReader);
    sampleWriter->setTerminated(idReader);
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

bool _sampleLess(const std::pair<const char *, int> &c1,
                 const std::pair<const char *, int> &c2) {
    int ret = memcmp(c1.first, c2.first, min(c1.second, c2.second));
    if (ret == 0) {
        return (c1.second - c2.second) < 0;
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
    std::vector<std::pair<const char *, int>> sample;
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
                    sample.push_back(std::make_pair(news, size));
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
            string s = string(sample[i].first, sample[i].second);
            output.push_back(s);
        }
    }
    return output;
}

void Compressor::sortRangePartitionedTuples(DiskLZ4Reader *reader,
        int idReader,
        const string outputFile,
        const std::vector<string> *boundaries) {
    int idx = 0; //partition within the same file
    int idxFile = 0; //multiple sorted files in the stream
    LZ4Writer *output = NULL;

    //Read the input file, and range-partition its content
    string bound;
    bool isLast;
    if (boundaries->size() > 0) {
        bound = boundaries->at(idx);
        isLast = false;
    } else {
        isLast = true;
    }
    long counter = 0;
    long countFile = 0;
    while (!reader->isEOF(idReader)) {
        if (countFile == 0) {
            countFile = reader->readLong(idReader);
            assert(countFile > 0);
            idx = 0;
            //Create a new file
            delete output;
            output = new LZ4Writer(outputFile + "-" +
                                   to_string(idxFile) +
                                   string(".") +
                                   to_string(idx));
            if (boundaries->size() > 0) {
                bound = boundaries->at(idx);
                isLast = false;
            } else {
                isLast = true;
            }
            idxFile++;
        }

        SimplifiedAnnotatedTerm t;
        t.readFrom(idReader, reader);
        assert(t.tripleIdAndPosition != -1);
        string term = string(t.term, t.size);
        if (!isLast && term > bound) {
            do {
                delete output;
                idx++;
                output = new LZ4Writer(outputFile + "-" +
                                       to_string(idxFile) +
                                       string(".") +
                                       to_string(idx));
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
        countFile--;
    }
    BOOST_LOG_TRIVIAL(debug) << "Partitioned " << counter << " terms.";
    delete output;
}

void Compressor::rangePartitionFiles(int readThreads, int maxThreads,
                                     string prefixInputFiles,
                                     const std::vector<string> &boundaries) {
    DiskLZ4Reader **readers = new DiskLZ4Reader*[readThreads];
    std::vector<string> infiles;
    for (int i = 0; i < readThreads; ++i) {
        string infile = prefixInputFiles + string(".")
                        + to_string(i);
        readers[i] = new DiskLZ4Reader(infile,
                                       maxThreads / readThreads,
                                       3);
        infiles.push_back(infile);
    }

    std::vector<boost::thread> threads(maxThreads);
    for (int i = 1; i < maxThreads; ++i) {
        DiskLZ4Reader *reader = readers[i % readThreads];
        string outputFile = prefixInputFiles + string("-range-") + to_string(i);
        threads[i] =
            boost::thread(boost::bind(&Compressor::sortRangePartitionedTuples,
                                      reader,
                                      i / readThreads,
                                      outputFile,
                                      &boundaries));
    }
    string outputFile = prefixInputFiles + string("-range-") + to_string(0);
    sortRangePartitionedTuples(readers[0],
                               0,
                               outputFile,
                               &boundaries);
    for (int i = 1; i < maxThreads; ++i) {
        threads[i].join();
    }
    for (int i = 0; i < readThreads; ++i) {
        delete readers[i];
    }
    for (int i = 0; i < infiles.size(); ++i) {
        fs::remove(infiles[i]);
        fs::remove(infiles[i] + ".idx");
    }
    delete[] readers;
}

void Compressor::sortPartition(string prefixInputFiles, string dictfile,
                               DiskLZ4Writer *writer,
                               int idWriter,
                               string prefixIntFiles,
                               int part, uint64_t *counter, long maxMem) {
    std::vector<string> filesToSort;

    fs::path parentDir = fs::path(prefixInputFiles).parent_path();
    fs::directory_iterator ei;
    for (fs::directory_iterator diter(parentDir); diter != ei; ++diter) {
        if (fs::is_regular_file(diter->status())) {
            auto pfile = diter->path();
            if (boost::algorithm::contains(pfile.string(), "range")) {
                if (pfile.has_extension()) {
                    string ext = pfile.extension().string();
                    if (ext == string(".") + to_string(part)) {
                        if (fs::file_size(pfile.string()) > 0) {
                            filesToSort.push_back(pfile.string());
                        } else {
                            fs::remove(pfile.string());
                        }
                    }
                }
            }
        }
    }

    string outputFile = prefixIntFiles + "tmp";

    //Keep all the prefixes stored in a map to increase the size of URIs we can
    //keep in main memory
    StringCollection colprefixes(4 * 1024 * 1024);
    ByteArraySet prefixset;
    prefixset.set_empty_key(EMPTY_KEY);

    StringCollection col(128 * 1024 * 1024);
    std::vector<SimplifiedAnnotatedTerm> tuples;
    std::vector<string> sortedFiles;
    long bytesAllocated = 0;
    int idx = 0;
    std::unique_ptr<char[]> tmpprefix = std::unique_ptr<char[]>(new char[MAX_TERM_SIZE]);

    //Load all the files until I fill main memory.
    for (int i = 0; i < filesToSort.size(); ++i) {
        string file = filesToSort[i];
        std::unique_ptr<LZ4Reader> r = std::unique_ptr<LZ4Reader>(new LZ4Reader(file));
        while (!r->isEof()) {
            SimplifiedAnnotatedTerm t;
            t.readFrom(r.get());
            assert(t.prefix == NULL);
            if ((bytesAllocated +
                    (sizeof(SimplifiedAnnotatedTerm) * tuples.size()))
                    >= maxMem) {
            //if (bytesAllocated > 100000) {
                BOOST_LOG_TRIVIAL(debug) << "Dumping file " << idx << " with "
                                         << tuples.size() << " tuples ...";
                string ofile = outputFile + string(".") + to_string(idx);
                idx++;
                sortAndDumpToFile(tuples, ofile, false);
                sortedFiles.push_back(ofile);
                tuples.clear();
                col.clear();
                bytesAllocated = 0;
            }

            //Check if I can compress the prefix of the string
            int sizeprefix = 0;
            const char *prefix = t.getPrefix(sizeprefix);
            if (sizeprefix > 4) {
                //Check if the prefix exists in the map
                Utils::encode_short(tmpprefix.get(), sizeprefix);
                memcpy(tmpprefix.get() + 2, prefix, sizeprefix);
                auto itr = prefixset.find((const char*)tmpprefix.get());
                if (itr == prefixset.end()) {
                    t.prefix = prefix;
                    t.term = col.addNew((char*) t.term +  sizeprefix,
                                        t.size - sizeprefix);
                    t.size = t.size - sizeprefix;
                    const char *prefixtoadd = colprefixes.addNew(tmpprefix.get(),
                                              sizeprefix + 2);
                    prefixset.insert(prefixtoadd);
                } else {
                    t.prefix = *itr;
                    t.term = col.addNew((char*) t.term +  sizeprefix,
                                        t.size - sizeprefix);
                    t.size = t.size - sizeprefix;
                }

            } else {
                t.prefix = NULL;
                t.term = col.addNew((char *) t.term, t.size);
            }

            tuples.push_back(t);
            bytesAllocated += t.size;
        }
        fs::remove(file);
    }
    BOOST_LOG_TRIVIAL(debug) << "Number of prefixes " << prefixset.size();

    if (idx == 0) {
        //All data fit in main memory. Do not need to write it down
        BOOST_LOG_TRIVIAL(debug) << "All terms (" << tuples.size() << ") fit in main memory";
        std::sort(tuples.begin(), tuples.end(), SimplifiedAnnotatedTerm::sless);

        //The following code is replicated below.
        long counterTerms = -1;
        long counterPairs = 0;
        {
            //LZ4Writer writer(outputfile);
            LZ4Writer dictWriter(dictfile);

            //Write the output
            const char *prevPrefix = NULL;
            char *previousTerm = new char[MAX_TERM_SIZE];
            int previousTermSize = 0;

            for (size_t i = 0; i < tuples.size(); ++i) {
                SimplifiedAnnotatedTerm t = tuples[i];
                if (!t.equals(previousTerm, previousTermSize, prevPrefix)) {
                    counterTerms++;

                    memcpy(previousTerm, t.term, t.size);
                    prevPrefix = t.prefix;
                    previousTermSize = t.size;

                    dictWriter.writeLong(counterTerms);
                    if (t.prefix == NULL) {
                        dictWriter.writeString(t.term, t.size);
                    } else {
                        //Write also the prefix of the string
                        //auto itr = prefixmap2.find(t.prefixid);
                        //assert(itr != prefixmap2.end());
                        int lenprefix = Utils::decode_short(t.prefix);
                        long len = lenprefix + t.size;
                        dictWriter.writeVLong(len);
                        dictWriter.writeRawArray(t.prefix + 2, lenprefix);
                        dictWriter.writeRawArray(t.term, t.size);

                    }
                }
                //Write the output
                counterPairs++;
                assert(t.tripleIdAndPosition != -1);
                writer->writeLong(idWriter, counterTerms);
                writer->writeLong(idWriter, t.tripleIdAndPosition);
            }
            delete[] previousTerm;
        }

        *counter = counterTerms + 1;
        BOOST_LOG_TRIVIAL(debug) << "Partition " << part << " contains " <<
                                 counterPairs << " tuples " <<
                                 (counterTerms + 1) << " terms";
        for (auto f : sortedFiles) {
            fs::remove(fs::path(f));
        }
    } else {
        if (tuples.size() > 0) {
            string ofile = outputFile + string(".") + to_string(idx);
            sortAndDumpToFile(tuples, ofile, false);
            sortedFiles.push_back(ofile);
        }

        BOOST_LOG_TRIVIAL(debug) << "Merge " << sortedFiles.size()
                                 << " files in order to sort the partition";

        while (sortedFiles.size() >= 4) {
            //Add files to the batch
            std::vector<string> batchFiles;
            batchFiles.push_back(sortedFiles[0]);
            batchFiles.push_back(sortedFiles[1]);
            batchFiles.push_back(sortedFiles[2]);

            //Create output file
            string ofile = outputFile + string(".") + to_string(++idx);
            LZ4Writer writer(ofile);

            //Merge batch of files
            FileMerger<SimplifiedAnnotatedTerm> merger(batchFiles);
            while (!merger.isEmpty()) {
                SimplifiedAnnotatedTerm t = merger.get();
                t.writeTo(&writer);
            }

            //Remove them
            sortedFiles.push_back(ofile);
            for (auto f : batchFiles) {
                fs::remove(fs::path(f));
                sortedFiles.erase(sortedFiles.begin());
            }
        }
        BOOST_LOG_TRIVIAL(debug) << "Final merge";

        //Create a file
        std::unique_ptr<LZ4Writer> dictWriter(new LZ4Writer(dictfile));

        const char *prevPrefix = NULL;
        char *previousTerm = new char[MAX_TERM_SIZE];
        int previousTermSize = 0;
        long counterTerms = -1;
        long counterPairs = 0;
        //Sort the files
        std::unique_ptr<FileMerger<SimplifiedAnnotatedTerm>> merger =
                    std::unique_ptr<FileMerger<SimplifiedAnnotatedTerm>>(
                        new FileMerger<SimplifiedAnnotatedTerm>(sortedFiles));
        while (!merger->isEmpty()) {
            SimplifiedAnnotatedTerm t = merger->get();
            if (!t.equals(previousTerm, previousTermSize, prevPrefix)) {
                counterTerms++;

                memcpy(previousTerm, t.term, t.size);
                prevPrefix = t.prefix;
                previousTermSize = t.size;

                dictWriter->writeLong(counterTerms);
                if (t.prefix == NULL) {
                    dictWriter->writeString(t.term, t.size);
                } else {
                    int lenprefix = Utils::decode_short(t.prefix);
                    long len = lenprefix + t.size;
                    dictWriter->writeVLong(len);
                    dictWriter->writeRawArray(t.prefix + 2, lenprefix);
                    dictWriter->writeRawArray(t.term, t.size);

                }
            }

            //Write the output
            counterPairs++;
            assert(t.tripleIdAndPosition != -1);
            writer->writeLong(idWriter, counterTerms);
            writer->writeLong(idWriter, t.tripleIdAndPosition);
        }

        delete[] previousTerm;
        *counter = counterTerms + 1;
        BOOST_LOG_TRIVIAL(debug) << "Partition " << part << " contains "
                                 << counterPairs << " tuples "
                                 << (counterTerms + 1) << " terms";

        for (auto f : sortedFiles) {
            fs::remove(fs::path(f));
        }
    }
    writer->setTerminated(idWriter);
}

void Compressor::sortPartitionsAndAssignCounters(string prefixInputFile,
        string dictfile,
        string outputfile, int partitions,
        long & counter, int parallelProcesses, int maxReadingThreads) {

    std::vector<boost::thread> threads(partitions);
    std::vector<string> outputfiles;
    std::vector<uint64_t> counters(partitions);
    long maxMem = max((long) 128 * 1024 * 1024,
                      (long) (Utils::getSystemMemory() * 0.7)) / partitions;
    BOOST_LOG_TRIVIAL(debug) << "Max memory per thread " << maxMem;

    DiskLZ4Writer **writers = new DiskLZ4Writer*[maxReadingThreads];
    for (int i = 0; i < maxReadingThreads; ++i) {
        string out = prefixInputFile + "-sortedpart-" + to_string(i);
        writers[i] = new DiskLZ4Writer(out, partitions / maxReadingThreads, 3);
        outputfiles.push_back(out);
    }

    for (int i = 0; i < partitions; ++i) {
        string dictpartfile = dictfile + string(".") + to_string(i);
        threads[i] = boost::thread(boost::bind(Compressor::sortPartition,
                                               prefixInputFile, dictpartfile,
                                               writers[i % maxReadingThreads],
                                               i / maxReadingThreads,
                                               outputfiles[i % maxReadingThreads] + to_string(i),
                                               i, &counters[i], maxMem));
    }
    for (int i = 0; i < partitions; ++i) {
        threads[i].join();
    }

    for (int i = 0; i < maxReadingThreads; ++i) {
        delete writers[i];
    }
    delete[] writers;
    BOOST_LOG_TRIVIAL(debug) << "Finished sorting partitions";

    //Re-read the sorted tuples and write by tripleID
    DiskLZ4Reader **readers = new DiskLZ4Reader*[maxReadingThreads];
    for (int i = 0; i < maxReadingThreads; ++i) {
        readers[i] = new DiskLZ4Reader(outputfiles[i],
                                       partitions / maxReadingThreads,
                                       3);
    }

    long startCounter = counter;
    for (int i = 0; i < partitions; ++i) {
        string outfile = outputfile + string(".") + to_string(i);
        threads[i] = boost::thread(boost::bind(
                                       &Compressor::assignCountersAndPartByTripleID,
                                       startCounter, readers[i % maxReadingThreads],
                                       i / maxReadingThreads,
                                       outfile, parallelProcesses));
        startCounter += counters[i];
    }
    for (int i = 0; i < partitions; ++i) {
        threads[i].join();
    }

    for (int i = 0; i < maxReadingThreads; ++i) {
        delete readers[i];
        fs::remove(outputfiles[i]);
        fs::remove(outputfiles[i] + ".idx");
    }
    delete [] readers;
}

void Compressor::assignCountersAndPartByTripleID(long startCounter,
        DiskLZ4Reader *r, int idReader, string outfile, int parallelProcesses) {
    LZ4Writer **outputs = new LZ4Writer*[parallelProcesses];
    for (int i = 0; i < parallelProcesses; ++i) {
        outputs[i] = new LZ4Writer(outfile + string(".") + to_string(i));
    }

    while (!r->isEOF(idReader)) {
        const long c = r->readLong(idReader);
        const long tid = r->readLong(idReader);
        const  int idx = (long) (tid >> 2) % parallelProcesses;
        outputs[idx]->writeLong(tid);
        outputs[idx]->writeLong(c + startCounter);
    }

    for (int i = 0; i < parallelProcesses; ++i) {
        delete outputs[i];
    }
    delete[] outputs;
}

void Compressor::mergeNotPopularEntries(string prefixInputFile,
                                        string dictOutput,
                                        string outputFile2,
                                        long * startCounter, int increment,
                                        int parallelProcesses,
                                        int maxReadingThreads) {

    //Sample one file: Get boundaries for parallelProcesses range partitions
    fs::path p = fs::path(prefixInputFile).parent_path();
    const std::vector<string> boundaries = getPartitionBoundaries(p.string(),
                                           parallelProcesses);
    assert(boundaries.size() == parallelProcesses - 1);

    //Range-partitions all the files in the input collection
    BOOST_LOG_TRIVIAL(debug) << "Range-partitions the files...";
    rangePartitionFiles(maxReadingThreads, parallelProcesses, prefixInputFile,
                        boundaries);

    //Collect all ranged-partitions files by partition and globally sort them.
    BOOST_LOG_TRIVIAL(debug) << "Sort and assign the counters to the files...";
    sortPartitionsAndAssignCounters(prefixInputFile,
                                    dictOutput,
                                    outputFile2,
                                    boundaries.size() + 1,
                                    *startCounter, parallelProcesses,
                                    maxReadingThreads);
}

void Compressor::sortByTripleID(vector<string> *inputFiles,
                                DiskLZ4Writer *writer,
                                const int idWriter,
                                string tmpfileprefix,
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
                    string file = tmpfileprefix + string(".") + to_string(idx++);
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
            string file = tmpfileprefix + string(".") + to_string(idx++);
            sortAndDumpToFile2(pairs, file);
            filesToMerge.push_back(file);
        }
        pairs.clear();
    }

    //Then do a merge sort and write down the results on outputFile
    FileMerger<TriplePair> merger(filesToMerge);
    while (!merger.isEmpty()) {
        TriplePair tp = merger.get();
        //BOOST_LOG_TRIVIAL(debug) << "IDWriter " << idWriter << " " << (tp.tripleIdAndPosition >> 2) << " " << (tp.tripleIdAndPosition & 0x3) << " " << tp.term;
        writer->writeLong(idWriter, tp.tripleIdAndPosition);
        writer->writeLong(idWriter, tp.term);
    }
    writer->setTerminated(idWriter);

    //Remove the input files
    for (int i = 0; i < filesToMerge.size(); ++i) {
        fs::remove(filesToMerge[i]);
    }
}

void Compressor::compressTriples(const int maxReadingThreads,
                                 const int parallelProcesses,
                                 const int ndicts,
                                 string * permDirs, int nperms,
                                 int signaturePerms, vector<string> &notSoUncommonFiles,
                                 vector<string> &finalUncommonFiles,
                                 string * tmpFileNames,
                                 StringCollection * poolForMap,
                                 ByteArrayToNumberMap * finalMap) {

    BOOST_LOG_TRIVIAL(debug) << "Start compression threads... ";
    /*** Compress the triples ***/
    int iter = 0;
    while (areFilesToCompress(parallelProcesses, tmpFileNames)) {
        string prefixOutputFile = "input-" + to_string(iter);

        DiskLZ4Reader **readers = new DiskLZ4Reader*[maxReadingThreads];
        for (int i = 0; i < maxReadingThreads; ++i) {
            readers[i] = new DiskLZ4Reader(tmpFileNames[i],
                                           parallelProcesses / maxReadingThreads,
                                           3);
        }
        DiskLZ4Reader **uncommonReaders = new DiskLZ4Reader*[maxReadingThreads];
        for (int i = 0; i < maxReadingThreads; ++i) {
            uncommonReaders[i] = new DiskLZ4Reader(finalUncommonFiles[i],
                                                   parallelProcesses / maxReadingThreads,
                                                   3);
        }

        //Set up the output
        std::vector<std::vector<string>> chunks;
        chunks.resize(maxReadingThreads);
        //Set up the output files
        for (int i = 0; i < parallelProcesses; ++i) {
            for (int j = 0; j < nperms; ++j) {
                string file = permDirs[j] + string("/") + prefixOutputFile + to_string(i);
                chunks[i % maxReadingThreads].push_back(file);
            }
        }

        MultiDiskLZ4Writer **writers = new MultiDiskLZ4Writer*[maxReadingThreads];
        for (int i = 0; i < maxReadingThreads; ++i) {
            writers[i] = new MultiDiskLZ4Writer(chunks[i], 3, 3);
        }

        boost::thread *threads = new boost::thread[parallelProcesses - 1];
        ParamsNewCompressProcedure p;
        p.nperms = nperms;
        p.signaturePerms = signaturePerms;
        p.commonMap = iter == 0 ? finalMap : NULL;
        p.parallelProcesses = parallelProcesses;
        for (int i = 1; i < parallelProcesses; ++i) {
            p.part = i;
            p.idReader = i / maxReadingThreads;
            p.reader = readers[i % maxReadingThreads];
            p.readerUncommonTerms = uncommonReaders[i % maxReadingThreads];
            p.writer = writers[i % maxReadingThreads];
            p.idxWriter = (i / maxReadingThreads) * nperms;
            threads[i - 1] = boost::thread(
                                 boost::bind(&Compressor::newCompressTriples,
                                             this, p));
        }
        p.idReader = 0;
        p.reader = readers[0];
        p.part = 0;
        p.writer = writers[0];
        p.idxWriter = 0;
        p.readerUncommonTerms = uncommonReaders[0];
        newCompressTriples(p);
        for (int i = 1; i < parallelProcesses; ++i) {
            threads[i - 1].join();
        }
        delete[] threads;

        for (int i = 0; i < maxReadingThreads; ++i) {
            delete readers[i];
            fs::remove(tmpFileNames[i]);
            fs::remove(tmpFileNames[i] + ".idx");
            delete uncommonReaders[i];
            delete writers[i];
        }
        delete[] readers;
        delete[] writers;

        //New iteration!
        finalMap->clear();
        iter++;
    }
}

void Compressor::sortFilesByTripleSource(string kbPath,
        const int maxReadingThreads,
        const int parallelProcesses,
        const int ndicts, vector<string> uncommonFiles,
        vector<string> &outputFiles) {

    /*** Sort the files which contain the triple source ***/
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

    DiskLZ4Writer **writers = new DiskLZ4Writer*[maxReadingThreads];
    for (int i = 0; i < maxReadingThreads; ++i) {
        outputFiles.push_back(kbPath + string("/listUncommonTerms") + to_string(i));
        writers[i] = new DiskLZ4Writer(outputFiles.back(),
                                       parallelProcesses / maxReadingThreads,
                                       3);
    }

    boost::thread *threads = new boost::thread[parallelProcesses - 1];
    const long maxMem = max((long) MIN_MEM_SORT_TRIPLES,
                            (long) (Utils::getSystemMemory() * 0.7) / parallelProcesses);
    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1] = boost::thread(
                             boost::bind(&Compressor::sortByTripleID, this,
                                         &inputFinalSorting[i],
                                         writers[i % maxReadingThreads],
                                         i / maxReadingThreads,
                                         kbPath + string("/listUncommonTerms-tmp") + to_string(i),
                                         maxMem));
    }
    sortByTripleID(&inputFinalSorting[0], writers[0], 0,
                   kbPath + string("/listUncommonTerms-tmp") + to_string(0),
                   maxMem);

    for (int i = 1; i < parallelProcesses; ++i) {
        threads[i - 1].join();
    }
    delete[] threads;

    for (int i = 0; i < maxReadingThreads; ++i) {
        delete writers[i];
    }
    delete[] writers;
}

void Compressor::sortDictionaryEntriesByText(string **input,
        const int ndicts,
        const int maxReadingThreads,
        const int parallelProcesses,
        string * prefixOutputFiles,
        //int *noutputfiles,
        ByteArrayToNumberMap * map,
        bool filterDuplicates,
        bool sample) {
    long maxMemAllocate = max((long) (BLOCK_SUPPORT_BUFFER_COMPR * 2),
                              (long) (Utils::getSystemMemory() * 0.70 / ndicts));
    BOOST_LOG_TRIVIAL(debug) << "Max memory to use to sort inmemory a number of terms: " << maxMemAllocate << " bytes";
    immemorysort(input, maxReadingThreads, parallelProcesses, prefixOutputFiles[0],
                 filterDuplicates, maxMemAllocate, sample);
}

void Compressor::compress(string * permDirs, int nperms, int signaturePerms,
                          string * dictionaries,
                          int ndicts,
                          int parallelProcesses,
                          int maxReadingThreads) {

    /*** Sort the infrequent terms ***/
    BOOST_LOG_TRIVIAL(debug) << "Sorting uncommon dictionary entries for partitions";
    string *uncommonDictionaries = new string[ndicts];
    for (int i = 0; i < ndicts; ++i) {
        uncommonDictionaries[i] = dictionaries[i] + string("-u");
    }
    sortDictionaryEntriesByText(uncommonDictFileNames,
                                ndicts,
                                maxReadingThreads,
                                parallelProcesses,
                                uncommonDictionaries,
                                NULL,
                                false,
                                true);
    BOOST_LOG_TRIVIAL(debug) << "...done";

    /*** Deallocate the dictionary files ***/
    for (int i = 0; i < maxReadingThreads; ++i) {
        delete[] dictFileNames[i];
        fs::remove(fs::path(uncommonDictFileNames[i][0]));
        fs::remove(fs::path(uncommonDictFileNames[i][0] + string(".idx")));
        delete[] uncommonDictFileNames[i];
    }
    delete[] dictFileNames;
    delete[] uncommonDictFileNames;

    /*** Create the final dictionaries to be written and initialize the
     * counters and other data structures ***/
    LZ4Writer **writers = new LZ4Writer*[ndicts];
    long *counters = new long[ndicts];
    vector<string> notSoUncommonFiles;
    vector<string> uncommonFiles;

    for (int i = 0; i < ndicts; ++i) {
        writers[i] = new LZ4Writer(dictionaries[i]);
        counters[i] = i;
        notSoUncommonFiles.push_back(dictionaries[i] + string("-np1"));
        uncommonFiles.push_back(dictionaries[i] + string("-np2"));
    }

    /*** Assign a number to the popular entries ***/
    BOOST_LOG_TRIVIAL(debug) << "Assign a number to " << finalMap->size() <<
                             " popular terms in the dictionary";
    assignNumbersToCommonTermsMap(finalMap, counters, writers, NULL, ndicts, true);

    /*** Assign a number to the other entries. Split them into two files.
     * The ones that must be loaded into the hashmap, and the ones used for
     * the merge join ***/
    BOOST_LOG_TRIVIAL(debug) << "Merge (and assign counters) of dictionary entries";
    if (ndicts > 1) {
        BOOST_LOG_TRIVIAL(error) << "The current version of the code supports only one dictionary partition";
        throw 10;
    }
    mergeNotPopularEntries(uncommonDictionaries[0], dictionaries[0],
                           uncommonFiles[0], &counters[0], ndicts,
                           parallelProcesses, maxReadingThreads);
    BOOST_LOG_TRIVIAL(debug) << "... done";

    /*** Remove unused data structures ***/
    for (int i = 0; i < ndicts; ++i) {
        delete writers[i];
        //vector<string> filesToBeRemoved = filesToBeMerged[i];
        //for (int j = 0; j < filesToBeRemoved.size(); ++j) {
        //    fs::remove(fs::path(filesToBeRemoved[j]));
        //}
    }
    delete[] uncommonDictionaries;

    /*** Sort files by triple source ***/
    BOOST_LOG_TRIVIAL(debug) << "Sort uncommon triples by triple id";
    vector<string> sortedFiles;
    sortFilesByTripleSource(kbPath, maxReadingThreads, parallelProcesses,
                            ndicts, uncommonFiles, sortedFiles);
    BOOST_LOG_TRIVIAL(debug) << "... done";

    /*** Compress the triples ***/
    compressTriples(maxReadingThreads, parallelProcesses, ndicts,
                    permDirs, nperms, signaturePerms,
                    notSoUncommonFiles, sortedFiles, tmpFileNames,
                    poolForMap, finalMap);
    BOOST_LOG_TRIVIAL(debug) << "... done";

    /*** Clean up remaining datastructures ***/
    delete[] counters;
    for (int i = 0; i < maxReadingThreads; ++i) {
        fs::remove(sortedFiles[i]);
        fs::remove(sortedFiles[i] + ".idx");
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
