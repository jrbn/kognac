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

#ifndef FILEREADER_H_
#define FILEREADER_H_

#include <fstream>
#include <iostream>
#include <string>
#include <exception>
#include <boost/iostreams/filtering_stream.hpp>
#include <boost/chrono.hpp>

using namespace std;

class ParseException: public exception {
public:
    virtual const char* what() const throw () {
        return "Line is not parsed correctly";
    }
};

typedef struct FileInfo {
    long size;
    long start;
    bool splittable;
    string path;
} FileInfo;

class FileReader {
private:
    const bool compressed;
    ifstream rawFile;
    boost::iostreams::filtering_istream compressedFile;
    string currentLine;
    bool tripleValid;
    long end;
    long countBytes;

    const char *startS;
    int lengthS;
    const char* startP;
    int lengthP;
    const char *startO;
    int lengthO;

    ParseException ex;
    bool parseLine(const char *input, const int sizeInput);

    void checkRange(const char *pointer, const char* start, const char *end);

public:
    FileReader(FileInfo file);

    bool parseTriple();

    bool isTripleValid();

    const char *getCurrentS(int &length);

    const char *getCurrentP(int &length);

    const char *getCurrentO(int &length);
};

#endif /* FILEREADER_H_ */
