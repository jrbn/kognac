#ifndef _DISK_LZ4_WRITER
#define _DISK_LZ4_WRITER

#include <kognac/lz4io.h>

#include <string>
#include <vector>
#include <fstream>
#include <thread>
#include <assert.h>
#include <list>
#include <mutex>
#include <condition_variable>

#include <boost/chrono.hpp>

using namespace std;

#define SIZE_COMPRESSED_BUFFER SIZE_COMPRESSED_SEG * 1000

class DiskLZ4Writer {
protected:
    struct BlockToWrite {
        int idfile;
        char *buffer;
        size_t sizebuffer;
    };

    struct FileInfo {
        char *buffer;
        size_t sizebuffer;
        char *compressedbuffer;
        size_t pivotCompressedBuffer;

        FileInfo() {
            buffer = new char[SIZE_SEG];
            sizebuffer = 0;
            compressedbuffer = NULL;
            pivotCompressedBuffer = 0;
        }

        ~FileInfo() {
            delete[] buffer;
        }
    };

    std::mutex mutexBlockToWrite;
    std::condition_variable cvBlockToWrite;
    boost::chrono::duration<double> time_waitingwriting;
    boost::chrono::duration<double> time_rawwriting;
    boost::chrono::duration<double> time_waitingbuffer;
    size_t addedBlocksToWrite;
    int currentWriteFileID;
    std::list<BlockToWrite> *blocksToWrite;

    const int npartitions;

    std::vector<char*> buffers;
    std::mutex mutexAvailableBuffer;
    std::condition_variable cvAvailableBuffer;

    std::thread currentthread;
    bool processStarted;

    DiskLZ4Writer(int npartitions, int nbuffersPerFile);

private:

    string inputfile;
    std::ofstream stream;


    int nterminated;

    //Store the raw buffers to be written
    std::vector<char*> parentbuffers;

    std::vector<FileInfo> fileinfo;

    std::mutex mutexTerminated;



    bool areAvailableBuffers();

    void compressAndQueue(const int id);

public:
    DiskLZ4Writer(string file, int npartitions, int nbuffersPerFile);

    void writeByte(const int id, const int value);

    void writeVLong(const int id, const long value);

    void writeLong(const int id, const long value);

    void writeRawArray(const int id, const char *buffer, const size_t sizebuffer);

    void writeString(const int id, const char *buffer, const size_t sizebuffer);

    void writeShort(const int id, const int value);

    void setTerminated(const int id);

    virtual void run();

    bool areBlocksToWrite();

    virtual ~DiskLZ4Writer();
};

#endif
