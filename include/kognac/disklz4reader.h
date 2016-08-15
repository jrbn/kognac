#ifndef _DISK_LZ4_READER_H
#define _DISK_LZ4_READER_H

#include <kognac/lz4io.h>

#include <boost/chrono.hpp>

#include <fstream>
#include <list>
#include <vector>
#include <thread>
#include <string>
#include <mutex>
#include <condition_variable>

using namespace std;

class DiskLZ4Reader {
private:
    struct FileInfo {
        char *buffer;
        size_t sizebuffer;
        size_t pivot;
    };

    //Info about the files
    string inputfile;
    std::vector<FileInfo> files;
    ifstream reader;
    std::vector<std::vector<long>> beginningBlocks;
    std::vector<long> readBlocks;

    //support buffers for strings
    std::vector<std::unique_ptr<char[]> > supportstringbuffers;

    bool uncompressBuffer(const int id);

    bool getNewCompressedBuffer(std::unique_lock<std::mutex> &lk,
                                const int id);

    virtual void run();

protected:

    struct BlockToRead {
        char *buffer;
        int sizebuffer;
        int pivot;
    };

    std::mutex m_diskbufferpool;
    std::thread currentthread;

    //Pool of compressed buffers
    std::list<BlockToRead> *compressedbuffers;
    //Larger buffers to read from disk. They are proportional to SIZE_SEG
    std::vector<char*> diskbufferpool;
    std::condition_variable cond_diskbufferpool;
    boost::chrono::duration<double> time_diskbufferpool;
    boost::chrono::duration<double> time_rawreading;

    std::mutex *m_files;
    std::condition_variable *cond_files;
    boost::chrono::duration<double> *time_files;

public:
    DiskLZ4Reader(string inputfile, int npartitions, int nbuffersPerFile);

    DiskLZ4Reader(int npartitions, int nbuffersPerFile);

    bool isEOF(const int id);

    int readByte(const int id);

    long readVLong(const int id);

    long readLong(const int id);

    const char* readString(const int id, int &sizeTerm);

    bool availableDiskBuffer();

    virtual bool areNewBuffers(const int id);

    virtual ~DiskLZ4Reader();
};

#endif
