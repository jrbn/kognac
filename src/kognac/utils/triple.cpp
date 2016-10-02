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

#include <kognac/triple.h>
#include <kognac/lz4io.h>
#include <kognac/multidisklz4writer.h>
#include <kognac/multidisklz4reader.h>

void Triple::readFrom(LZ4Reader *reader) {
    s = reader->parseVLong();
    p = reader->parseVLong();
    o = reader->parseVLong();
    count = reader->parseVLong();
}

void Triple::readFrom(int part, MultiDiskLZ4Reader *reader) {
    s = reader->readVLong(part);
    p = reader->readVLong(part);
    o = reader->readVLong(part);
    count = reader->readVLong(part);
}

void Triple::writeTo(LZ4Writer *writer) {
    writer->writeVLong(s);
    writer->writeVLong(p);
    writer->writeVLong(o);
    writer->writeVLong(count);
}

void Triple::writeTo(int part, MultiDiskLZ4Writer *writer) {
    writer->writeVLong(part, s);
    writer->writeVLong(part, p);
    writer->writeVLong(part, o);
    writer->writeVLong(part, count);
}
