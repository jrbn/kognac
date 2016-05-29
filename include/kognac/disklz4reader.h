#ifndef _DISK_LZ4_READER_H
#define _DISK_LZ4_READER_H

#include <kognac/lz4io.h>

#include <boost/chrono.hpp>

#include <list>
#include <vector>
#include <thread>
#include <string>
#include <mutex>
#include <condition_variable>

using namespace std;

#define SIZE_DISK_BUFFER SIZE_COMPRESSED_SEG *1000

class DiskLZ4Reader {
private:
    struct BlockToRead {
        char *buffer;
        int sizebuffer;
        int pivot;
    };

    struct FileInfo {
        string path;
        bool eof;
        char *buffer;
        size_t sizebuffer;
        size_t pivot;
    };

    //Pool of compressed buffers
    std::list<BlockToRead> *compressedbuffers;
    //Larger buffers to reade from disk. They are proportional to SIZE_SEG
    std::vector<char*> diskbufferpool;
    std::mutex m_diskbufferpool;
    std::condition_variable cond_diskbufferpool;
    boost::chrono::duration<double> time_diskbufferpool;

    //Used to track status of reading
    int neofs;
    int currentFileIdx;

    //Info about the files
    std::vector<FileInfo> files;
    ifstream *readers;
    std::mutex *m_files;
    std::condition_variable *cond_files;
    boost::chrono::duration<double> *time_files;

    //support buffers for strings
    std::vector<std::unique_ptr<char[]>> supportstringbuffers;

    std::thread currentthread;

    bool availableDiskBuffer();

    bool areNewBuffers(const int id);

    bool uncompressBuffer(const int id);

    void getNewCompressedBuffer(std::unique_lock<std::mutex> &lk,
                                const int id);

    void run();

public:
    DiskLZ4Reader(std::vector<string> &files, int nbuffersPerFile);

    bool isEOF(const int id);

    int readByte(const int id);

    long readVLong(const int id);

    long readLong(const int id);

    const char* readString(const int id, int &sizeTerm);

    ~DiskLZ4Reader();
};

#endif
