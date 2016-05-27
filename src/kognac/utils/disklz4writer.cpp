#include <kognac/lz4io.h>
#include <kognac/disklz4writer.h>

DiskLZ4Writer::DiskLZ4Writer(std::vector<string> &files, int nbuffersPerFile) : inputfiles(files) {
    //Create a number of compressed buffers
    for (int i = 0; i < files.size(); i++) {
        //Create 10 buffers for each file
        for (int j = 0; j < nbuffersPerFile; ++j) {
            buffers.push_back(new char[SIZE_COMPRESSED_SEG]);
        }
    }

    //One uncompressed buffer per file
    for (int i = 0; i < files.size(); ++i) {
        uncompressedbuffers.push_back(new char[SIZE_SEG]);
        sizeuncompressedbuffers.push_back(0);
    }

    streams = new ofstream[files.size()];
    for (int i = 0; i < files.size(); ++i) {
        //Open a file
        streams[i].open(files[i]);
    }
    nterminated = 0;
    currentthread = thread(std::bind(&DiskLZ4Writer::run, this));
    //A thread is now running
}

void DiskLZ4Writer::writeByte(const int id, const int value) {
    assert(id < inputfiles.size());
    char *buffer = uncompressedbuffers[id];
    int sizebuffer = sizeuncompressedbuffers[id];
    assert(sizebuffer <= SIZE_SEG);

    if (sizebuffer == SIZE_SEG) {
        compressAndQueue(id, buffer, sizebuffer);
        sizeuncompressedbuffers[id] = sizebuffer = 0;
    }
    buffer[sizebuffer] = value;
    sizeuncompressedbuffers[id]++;
}

void DiskLZ4Writer::writeVLong(const int id, const long value) {
    int i = 1;
    long n = value;
    if (value < 128) { // One byte is enough
        writeByte(id, n);
        return;
    } else {
        int bytesToStore = 64 - Utils::numberOfLeadingZeros((unsigned long) n);
        while (bytesToStore > 7) {
            i++;
            writeByte(id, (n & 127) + 128);
            n >>= 7;
            bytesToStore -= 7;
        }
        writeByte(id, n & 127);
    }
}

void DiskLZ4Writer::writeLong(const int id, const long value) {
    assert(id < inputfiles.size());
    char *buffer = uncompressedbuffers[id];
    int sizebuffer = sizeuncompressedbuffers[id];

    if (sizebuffer + 8 <= SIZE_SEG) {
        Utils::encode_long(buffer, sizebuffer, value);
        sizeuncompressedbuffers[id] += 8;
    } else {
        char supportBuffer[8];
        Utils::encode_long(supportBuffer, 0, value);
        int i = 0;
        for (; i < 8 && sizebuffer < SIZE_SEG; ++i) {
            buffer[sizebuffer++] = supportBuffer[i];
        }
        compressAndQueue(id, buffer, sizebuffer);
        sizeuncompressedbuffers[id] = sizebuffer = 0;
        for (; i < 8 && sizebuffer < SIZE_SEG; ++i) {
            buffer[sizebuffer++] = supportBuffer[i];
        }
        sizeuncompressedbuffers[id] = sizebuffer;
    }

}

void DiskLZ4Writer::writeRawArray(const int id, const char *bytes,
                                  const size_t length) {
    int len = length;
    assert(id < inputfiles.size());
    char *buffer = uncompressedbuffers[id];
    int sizebuffer = sizeuncompressedbuffers[id];

    if (sizebuffer + len <= SIZE_SEG) {
        memcpy(buffer + sizebuffer, bytes, len);
    } else {
        int remSize = SIZE_SEG - sizebuffer;
        memcpy(buffer + sizebuffer, bytes, remSize);
        len -= remSize;
        sizebuffer += remSize;
        compressAndQueue(id, buffer, sizebuffer);
        sizeuncompressedbuffers[id] = sizebuffer = 0;
        memcpy(buffer, bytes + remSize, len);
    }
    sizeuncompressedbuffers[id] += len;

}

void DiskLZ4Writer::writeShort(const int id, const int value) {
    assert(id < inputfiles.size());
    char *buffer = uncompressedbuffers[id];
    int sizebuffer = sizeuncompressedbuffers[id];

    if (sizebuffer == SIZE_SEG) {
        compressAndQueue(id, buffer, sizebuffer);
        sizebuffer = sizeuncompressedbuffers[id] = 0;
    } else if (sizebuffer == SIZE_SEG - 1) {
        char supportBuffer[2];
        Utils::encode_short(supportBuffer, value);
        writeByte(id, supportBuffer[0]);
        writeByte(id, supportBuffer[1]);
        return;
    }

    Utils::encode_short(buffer + sizebuffer, value);
    sizeuncompressedbuffers[id] += 2;
}

void DiskLZ4Writer::setTerminated(const int id) {
    //Write down the last buffer
    char *buffer = uncompressedbuffers[id];
    int sizebuffer = sizeuncompressedbuffers[id];
    if (sizebuffer > 0)
        compressAndQueue(id, buffer, sizebuffer);
    sizeuncompressedbuffers[id] = 0;

    nterminated++;
    cvBlockToWrite.notify_one();
}

void DiskLZ4Writer::compressAndQueue(const int id, char *input, const size_t sizeinput) {
    //Get a compressed buffer
    std::unique_lock<std::mutex> lk(mutexAvailableBuffer);
    cvAvailableBuffer.wait(lk, std::bind(&DiskLZ4Writer::areAvailableBuffers, this));
    assert(buffers.size() > 0);
    char *buffer = buffers.back();
    buffers.pop_back();
    lk.unlock();

    //Compress the buffer
    //First 8 bytes is LZOBlock.
    //Then there is a token which has encoded in the 0xF0 bits
    //the type of compression.
    strcpy(buffer, "LZOBLOCK");
    buffer[8] = 32;

    //Then there is the compressed size but I will write it later...
    //... and finally the uncompressed size
    Utils::encode_intLE(buffer, 13, sizeinput);
    const int compressedSize = LZ4_compress(input, buffer + 21, sizeinput);
    Utils::encode_intLE(buffer, 9, compressedSize);

    BlockToWrite b;
    b.buffer = buffer;
    b.sizebuffer = compressedSize + 21;
    b.idfile = id;

    //Copy in the writing queue
    std::unique_lock<std::mutex> lk2(mutexBlockToWrite);
    blocksToWrite.push_back(b);
    lk2.unlock();
    cvBlockToWrite.notify_one();
}

bool DiskLZ4Writer::areBlocksToWrite() {
    return !blocksToWrite.empty() || nterminated == inputfiles.size();
}

bool DiskLZ4Writer::areAvailableBuffers() {
    return !buffers.empty();
}

void DiskLZ4Writer::run() {
    while (true) {
        BlockToWrite block;
        std::unique_lock<std::mutex> lk(mutexBlockToWrite);
        cvBlockToWrite.wait(lk, std::bind(&DiskLZ4Writer::areBlocksToWrite, this));
        if (!blocksToWrite.empty()) {
            block = blocksToWrite.front();
            blocksToWrite.pop_front();
            lk.unlock();
        } else { //Exit...
            lk.unlock();
            return;
        }

        streams[block.idfile].write(block.buffer, block.sizebuffer);

        //Return the buffer so that it can be reused
        unique_lock<std::mutex> lk2(mutexAvailableBuffer);
        buffers.push_back(block.buffer);
        lk2.unlock();
        cvAvailableBuffer.notify_one();
    }
}

DiskLZ4Writer::~DiskLZ4Writer() {
    currentthread.join();

    for (int i = 0; i < inputfiles.size(); ++i) {
        assert(sizeuncompressedbuffers[i] == 0);
        streams[i].close();
    }
    delete[] streams;

    for (int i = 0; i < buffers.size(); ++i)
        delete[] buffers[i];

    for (int i = 0; i < uncompressedbuffers.size(); ++i)
        delete[] uncompressedbuffers[i];
}
