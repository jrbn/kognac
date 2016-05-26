#ifndef _DISK_READER_H
#define _DISK_READER_H

#include <kognac/filereader.h>

#include <boost/log/trivial.hpp>
#include <boost/filesystem.hpp>

#include <mutex>
#include <condition_variable>
#include <vector>

namespace fs = boost::filesystem;

class DiskReader {
private:
    struct Buffer {
        size_t size;
        char *b;
        bool gzipped;
    };

    std::mutex mutex1;
    std::condition_variable cv1;
    std::mutex mutex2;
    std::condition_variable cv2;

    std::vector<FileInfo> *files;
    std::vector<FileInfo>::iterator itr;

    std::vector<char *> availablebuffers;
    std::vector<Buffer> readybuffers;

    bool finished;

public:
    DiskReader(int nbuffers, std::vector<FileInfo> *files);

    char *getfile(size_t &size, bool &gzipped);

    void releasefile(char *file);

    bool isReady();

    bool isAvailable();

    void run();

    ~DiskReader();
};

#endif
