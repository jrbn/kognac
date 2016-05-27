#ifndef _DISK_LZ4_WRITER
#define _DISK_LZ4_WRITER

#include <string>
#include <vector>
#include <fstream>
#include <thread>
#include <assert.h>

using namespace std;

class DiskLZ4Writer {
private:
    struct BlockToWrite {
        int idfile;
        char *buffer;
        size_t sizebuffer;
    };

    std::vector<string> inputfiles;
    std::ofstream *streams;
    std::vector<BlockToWrite> blocksToWrite;
    std::thread currentthread;
    int nterminated;

    std::vector<char*> buffers;
    std::vector<char*> uncompressedbuffers;
    std::vector<int> sizeuncompressedbuffers;

    std::mutex mutexBlockToWrite;
    std::condition_variable cvBlockToWrite;

    std::mutex mutexAvailableBuffer;
    std::condition_variable cvAvailableBuffer;

    bool areBlocksToWrite();

    bool areAvailableBuffers();

    void run();

    void compressAndQueue(const int id, char *input, const size_t sizeinput);

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
