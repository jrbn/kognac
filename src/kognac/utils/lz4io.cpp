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

#include <kognac/lz4io.h>
#include <kognac/utils.h>

#include <boost/log/trivial.hpp>

#include <cstring>

void LZ4Writer::compressAndWriteBuffer() {
    //First 8 bytes is LZOBlock. Skip them

    //Then there is a token which has encoded in the 0xF0 bits
    //the type of compression.
    compressedBuffer[8] = 32;

    //Then there is the compressed size but I will write it later...
    //... and finally the uncompressed size
    Utils::encode_intLE(compressedBuffer, 13, uncompressedBufferLen);
    int compressedSize = LZ4_compress(uncompressedBuffer, compressedBuffer + 21,
                                      uncompressedBufferLen);
    Utils::encode_intLE(compressedBuffer, 9, compressedSize);

    os.write(compressedBuffer, compressedSize + 21);

    if (!os.good()) {
        BOOST_LOG_TRIVIAL(error) << "Problems with writing the file " << path <<
                                 "good=" << os.good() << " eof=" << os.eof() << " fail=" << os.fail() << " bad=" << os.bad();
    }

    uncompressedBufferLen = 0;
}

void LZ4Writer::writeByte(char i) {
    if (uncompressedBufferLen == SIZE_SEG) {
        compressAndWriteBuffer();
    }
    uncompressedBuffer[uncompressedBufferLen++] = i;
}

//void LZ4Writer::writeString(const char *rawStr) {
//  if (uncompressedBufferLen == SIZE_SEG) {
//      compressAndWriteBuffer();
//  }
//
//  for (int i = 0; rawStr[i] != '\0'; ++i) {
//      uncompressedBuffer[uncompressedBufferLen++] = rawStr[i];
//      if (uncompressedBufferLen == SIZE_SEG) {
//          compressAndWriteBuffer();
//      }
//  }
//
//  if (uncompressedBufferLen == SIZE_SEG) {
//      compressAndWriteBuffer();
//  }
//
//  uncompressedBuffer[uncompressedBufferLen++] = '\0';
//}

void LZ4Writer::writeString(const char *rawStr, int length) {
    writeVLong(length);
    writeRawArray(rawStr, length);
}

void LZ4Writer::writeShort(short n) {
    if (uncompressedBufferLen == SIZE_SEG) {
        compressAndWriteBuffer();
    } else if (uncompressedBufferLen == SIZE_SEG - 1) {
        char supportBuffer[2];
        Utils::encode_short(supportBuffer, n);
        writeByte(supportBuffer[0]);
        writeByte(supportBuffer[1]);
        return;
    }

    Utils::encode_short(uncompressedBuffer + uncompressedBufferLen, n);
    uncompressedBufferLen += 2;
}

void LZ4Writer::writeRawArray(const char *bytes, int length) {
    if (uncompressedBufferLen + length <= SIZE_SEG) {
        memcpy(uncompressedBuffer + uncompressedBufferLen, bytes, length);
        uncompressedBufferLen += length;
    } else {
        int remSize = SIZE_SEG - uncompressedBufferLen;
        memcpy(uncompressedBuffer + uncompressedBufferLen, bytes, remSize);
        length -= remSize;
        uncompressedBufferLen += remSize;
        compressAndWriteBuffer();
        memcpy(uncompressedBuffer, bytes + remSize, length);
        uncompressedBufferLen = length;
    }
}

void LZ4Writer::writeLong(long n) {
    if (uncompressedBufferLen + 8 <= SIZE_SEG) {
        Utils::encode_long(uncompressedBuffer, uncompressedBufferLen, n);
        uncompressedBufferLen += 8;
    } else {
        char supportBuffer[8];
        Utils::encode_long(supportBuffer, 0, n);
        int i = 0;
        for (; i < 8 && uncompressedBufferLen < SIZE_SEG; ++i) {
            uncompressedBuffer[uncompressedBufferLen++] = supportBuffer[i];
        }
        compressAndWriteBuffer();
        for (; i < 8 && uncompressedBufferLen < SIZE_SEG; ++i) {
            uncompressedBuffer[uncompressedBufferLen++] = supportBuffer[i];
        }
    }
}

void LZ4Writer::writeVLong(long n) {
    int i = 1;
    if (n < 128) { // One byte is enough
        writeByte(n);
        return;
    } else {
        int bytesToStore = 64 - Utils::numberOfLeadingZeros((unsigned long) n);
        while (bytesToStore > 7) {
            i++;
            writeByte((n & 127) + 128);
            n >>= 7;
            bytesToStore -= 7;
        }
        writeByte(n & 127);
    }
}

LZ4Writer::~LZ4Writer() {
    if (uncompressedBufferLen > 0) {
        compressAndWriteBuffer();
    }
    os.flush();

    if (!os.good()) {
        BOOST_LOG_TRIVIAL(error) << "Problems in closing the file " << path;
    }

    os.close();
}

long LZ4Reader::parseLong() {
    if (currentOffset >= uncompressedBufferLen) {
        uncompressedBufferLen = uncompressBuffer();
        currentOffset = 0;
    }

    long n = 0;
    if (currentOffset + 7 < uncompressedBufferLen) {
        //Parse it normally
        n = Utils::decode_long(uncompressedBuffer, currentOffset);
        currentOffset += 8;
    } else {
        //Need to parse the next buffer
        int numBytes = 7;
        for (; currentOffset < uncompressedBufferLen; numBytes--) {
            n += (long) (uncompressedBuffer[currentOffset++] & 0xFF)
                 << (numBytes * 8);
        }

        //Get the new buffer
        uncompressedBufferLen = uncompressBuffer();

        //Read the remaining bytes
        for (; numBytes >= 0; numBytes--) {
            n += (long) (uncompressedBuffer[currentOffset++] & 0xFF)
                 << (numBytes * 8);
        }

    }
    return n;
}

long LZ4Reader::parseVLong() {
    int shift = 7;
    char b = parseByte();
    long n = b & 127;
    while (b < 0) {
        b = parseByte();
        n += (long) (b & 127) << shift;
        shift += 7;
    }
    return n;
}

int LZ4Reader::parseInt() {
    if (currentOffset >= uncompressedBufferLen) {
        uncompressedBufferLen = uncompressBuffer();
        currentOffset = 0;
    }

    int n = 0;
    if (currentOffset + 3 < uncompressedBufferLen) {
        //Parse it normally
        n = Utils::decode_int(uncompressedBuffer, currentOffset);
        currentOffset += 4;
    } else {
        //Need to parse the next buffer
        int numBytes = 3;
        for (; currentOffset < uncompressedBufferLen; numBytes--) {
            n += (uncompressedBuffer[currentOffset++] & 0xFF) << (numBytes * 8);
        }

        //Get the new buffer
        uncompressedBufferLen = uncompressBuffer();
        currentOffset = 0;

        //Read the remaining bytes
        for (; numBytes >= 0; numBytes--) {
            n += (uncompressedBuffer[currentOffset++] & 0xFF) << (numBytes * 8);
        }
    }
    return n;
}

char LZ4Reader::parseByte() {
    if (currentOffset >= uncompressedBufferLen) {
        uncompressedBufferLen = uncompressBuffer();
        currentOffset = 0;
    }
    return uncompressedBuffer[currentOffset++];
}
