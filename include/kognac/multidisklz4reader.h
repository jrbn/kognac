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
        bool isset;

        size_t positionfile;
        size_t sizecurrentfile;

        PartInfo() {
            opened = false;
            idxfile = -1;
            positionfile = 0;
            sizecurrentfile = 0;
            eof = false;
            isset = false;
        }
    };

    int nopenedstreams;

    const int maxopenedstreams;
    const int nbuffersPerPartition;
    std::list<int> historyopenedfiles;

protected:
    std::mutex m_sets;
    std::condition_variable cond_sets;
    std::vector<PartInfo> partitions;
    int nsets;

    void readbuffer(int partitionToRead, char *buffer);

    bool readAll(int id);

public:
    MultiDiskLZ4Reader(int maxNPartitions,
                       int nbuffersPerPartition,
                       int maxopenedstreams);

    virtual void run();

    virtual void start();

    void addInput(int id, std::vector<string> &files);

    bool areNewBuffers(const int id);

    virtual ~MultiDiskLZ4Reader();
};

#endif
