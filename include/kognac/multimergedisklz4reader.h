#ifndef _MULTI_MDISK_LZ4_READER_H
#define _MULTI_MDISK_LZ4_READER_H

#include <kognac/multidisklz4reader.h>

#include <vector>

class MultiMergeDiskLZ4Reader : public MultiDiskLZ4Reader {
private:
    int nbuffersPerPartition;
    bool shouldExit;

    bool notsetORenoughread(int partition);

public:
    MultiMergeDiskLZ4Reader(int maxNPartitions,
                            int nbuffersPerPartition,
                            int maxopenedstreams);

    void run();

    virtual void start();

    virtual void stop();

    void unsetPartition(int partition);

    virtual ~MultiMergeDiskLZ4Reader();
};

#endif
