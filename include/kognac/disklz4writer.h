#ifndef _DISK_LZ4_WRITER
#define _DISK_LZ4_WRITER

#include <string>
#include <vector>
#include <fstream>
#include <thread>
#include <assert.h>
#include <mutex>
#include <condition_variable>

using namespace std;

#define SIZE_COMPRESSED_BUFFER SIZE_COMPRESSED_SEG * 1000

class DiskLZ4Writer {
private:
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

    std::vector<string> inputfiles;
    std::ofstream *streams;

    std::mutex mutexBlockToWrite;
    std::condition_variable cvBlockToWrite;
    std::list<BlockToWrite> *blocksToWrite;
    size_t addedBlocksToWrite;
    int currentWriteFileID;

    std::thread currentthread;
    int nterminated;

    //Store the raw buffers to be written
    std::vector<char*> parentbuffers;
    std::vector<char*> buffers;

    //std::vector<char*> uncompressedbuffers;
    //std::vector<int> sizeuncompressedbuffers;
    std::vector<FileInfo> fileinfo;

    std::mutex mutexTerminated;

    std::mutex mutexAvailableBuffer;
    std::condition_variable cvAvailableBuffer;

    bool areBlocksToWrite();

    bool areAvailableBuffers();

    void run();

    void compressAndQueue(const int id);

public:
    DiskLZ4Writer(std::vector<string> &files, int nbuffersPerFile);

    void writeByte(const int id, const int value);

    void writeVLong(const int id, const long value);

    void writeLong(const int id, const long value);

    void writeRawArray(const int id, const char *buffer, const size_t sizebuffer);

    void writeShort(const int id, const int value);

    void setTerminated(const int id);

    ~DiskLZ4Writer();
};

#endif
