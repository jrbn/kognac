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

#include <kognac/filereader.h>
#include <kognac/consts.h>

#include <fstream>
#include <iostream>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/iostreams/filter/gzip.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/log/trivial.hpp>
#include <climits>

namespace io = boost::iostreams;
namespace fs = boost::filesystem;
namespace timens = boost::chrono;

string GZIP_EXTENSION = string(".gz");

FileReader::FileReader(char *buffer, size_t sizebuffer, bool gzipped) :
    byteArray(true), rawByteArray(buffer),
    sizeByteArray(sizebuffer), compressed(gzipped) {
    if (compressed) {
        //Decompress the stream
        io::filtering_ostream os;
        os.push(io::gzip_decompressor());
        os.push(io::back_inserter(uncompressedByteArray));
        io::write(os, buffer, sizebuffer);
    }
    currentIdx = 0;
}

FileReader::FileReader(FileInfo sFile) :
    byteArray(false), compressed(!sFile.splittable), rawFile(sFile.path,
            ios_base::in | ios_base::binary) {
    //First check the extension to identify what kind of file it is.
    fs::path p(sFile.path);
    if (p.has_extension() && p.extension() == GZIP_EXTENSION) {
        compressedFile.push(io::gzip_decompressor());
        compressedFile.push(rawFile);
    }

    if (sFile.splittable) {
        end = sFile.start + sFile.size;
    } else {
        end = LONG_MAX;
    }

    //If start != 0 then move to first '\n'
    if (sFile.start > 0) {
        rawFile.seekg(sFile.start);
//Seek to the first '\n'
        while (!rawFile.eof() && rawFile.get() != '\n') {
        };
    }
    countBytes = rawFile.tellg();
    tripleValid = false;

    startS = startP = startO = NULL;
    lengthS = lengthP = lengthO = 0;
}

bool FileReader::parseTriple() {
    bool ok = false;
    if (byteArray) {
        if (compressed) {
            if (currentIdx < uncompressedByteArray.size()) {
                size_t e = currentIdx + 1;
                while (e < uncompressedByteArray.size()
                       && uncompressedByteArray[e] != '\n') {
                    e++;
                }
                tripleValid = parseLine(&uncompressedByteArray[currentIdx],
                                        e - currentIdx);
                currentIdx = e;
                return true;
            } else {
                tripleValid = false;
                return false;
            }
        } else {
            if (currentIdx < sizeByteArray) {
                //read a line
                size_t e = currentIdx + 1;
                while (e < sizeByteArray && rawByteArray[e] != '\n') {
                    e++;
                }
                tripleValid = parseLine(rawByteArray + currentIdx, e - currentIdx);
                currentIdx = e;
                return true;
            } else {
                tripleValid = false;
                return false;
            }
        }
    } else {
        if (compressed) {
            ok = (bool) std::getline(compressedFile, currentLine);
        } else {
            ok = countBytes <= end && std::getline(rawFile, currentLine);
            if (ok) {
                countBytes = rawFile.tellg();
            }
        }

        if (ok) {
            if (currentLine.size() == 0 || currentLine.at(0) == '#') {
                return parseTriple();
            }
            tripleValid = parseLine(currentLine.c_str(), (int)currentLine.size());
            return true;
        }
        tripleValid = false;
        return false;
    }
}

const char *FileReader::getCurrentS(int &length) {
    length = lengthS;
    return startS;
}

const char *FileReader::getCurrentP(int &length) {
    length = lengthP;
    return startP;
}

const char *FileReader::getCurrentO(int &length) {
    length = lengthO;
    return startO;
}

bool FileReader::isTripleValid() {
    return tripleValid;
}

void FileReader::checkRange(const char *pointer, const char* start,
                            const char *end) {
    if (pointer == NULL || pointer <= (start + 1) || pointer > end) {
        throw ex;
    }
}

bool FileReader::parseLine(const char *line, const int sizeLine) {

    try {
        const char* endLine = line + sizeLine;

        // Parse subject
        const char *endS;
        startS = line;
        if (line[0] == '<') {
            endS = strchr(line, '>') + 1;
        } else { // Is a bnode
            endS = strchr(line, ' ');
        }
        checkRange(endS, startS, endLine);
        lengthS = (int)(endS - startS);

        //Parse predicate. Skip one space
        startP = line + lengthS + 1;
        const char *endP = strchr(startP, '>');
        checkRange(endP, startP, endLine);
        lengthP = (int)(endP + 1 - startP);

        // Parse object
        startO = startP + lengthP + 1;
        const char *endO = NULL;
        if (startO[0] == '<') { // URI
            endO = strchr(startO, '>') + 1;
        } else if (startO[0] == '"') { // Literal
            //Copy until the end of the string and remove character
            endO = strrchr(startO, '.')  - 1;
        } else { // Bnode
            endO = strchr(startO, ' ');
        }
        checkRange(endO, startO, endLine);
        lengthO = (int)(endO - startO);

        if (lengthS > 0 && lengthS < (MAX_TERM_SIZE - 1) && lengthP > 0
                && lengthP < (MAX_TERM_SIZE - 1) && lengthO > 0
                && lengthO < (MAX_TERM_SIZE - 1)) {
            return true;
        } else {
            BOOST_LOG_TRIVIAL(error) << "The triple was not parsed correctly: " << lengthS << " " << lengthP << " " << lengthO;
            return false;
        }

    } catch (std::exception &e) {
        BOOST_LOG_TRIVIAL(error) << "Failed parsing line: " + string(line, sizeLine);
    }
    return false;
}
