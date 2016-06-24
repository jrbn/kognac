#ifndef _MULTI_DISK_LZ4_READER_H
#define _MULTI_DISK_LZ4_READER_H

#include <kognac/disklz4reader.h>

#include <vector>

class MultiDiskLZ4Reader : public DiskLZ4Reader {
private:

    struct PartInfo {
        ifstream stream;
        bool opened;
        std::vector<string> files;
        int idxfile;
        bool eof;

        size_t positionfile;
        size_t sizecurrentfile;

        PartInfo() {
            opened = false;
            idxfile = -1;
            positionfile = 0;
            sizecurrentfile = 0;
            eof = false;
        }
    };

    std::vector<PartInfo> partitions;
    int nopenedstreams;
    int nsets;

    const int maxopenedstreams;
    std::list<int> historyopenedfiles;

    std::mutex m_sets;
    std::condition_variable cond_sets;

    bool readAll(int id);

public:
    MultiDiskLZ4Reader(int maxNPartitions,
                       int nbuffersPerPartition,
                       int maxopenedstreams);

    void run();

    void addInput(int id, std::vector<string> &files);

    //    bool isEof(int id);

    bool areNewBuffers(const int id);

    virtual ~MultiDiskLZ4Reader();
};

#endif
