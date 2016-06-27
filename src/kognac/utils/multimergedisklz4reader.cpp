#include <kognac/multimergedisklz4reader.h>

MultiMergeDiskLZ4Reader::MultiMergeDiskLZ4Reader(int maxNPartitions,
        int nbuffersPerPartition,
        int maxopenedstreams) : MultiDiskLZ4Reader(maxNPartitions,
                    nbuffersPerPartition,
                    maxopenedstreams) {
    this->nbuffersPerPartition = nbuffersPerPartition;
    shouldExit = false;
}

void MultiMergeDiskLZ4Reader::start() {
    currentthread = thread(std::bind(&MultiMergeDiskLZ4Reader::run, this));
    shouldExit = false;
}

bool MultiMergeDiskLZ4Reader::notsetORenoughread(int partition) {
    bool res = false;
    std::unique_lock<std::mutex> l(m_sets);
    res = !partitions[partition].isset;
    l.unlock();
    if (!res) {
        //Check if there is still a block available for read
        std::unique_lock<std::mutex> lk2(m_files[partition]);
        if (!compressedbuffers[partition].empty()) {
            res = true;
        }
        lk2.unlock();
    }
    return res;
}

void MultiMergeDiskLZ4Reader::stop() {
    shouldExit = true;
    cond_sets.notify_one();
    currentthread.join();
}

void MultiMergeDiskLZ4Reader::unsetPartition(int partition) {
    std::lock_guard<std::mutex> l(m_sets);
    assert(nsets > 0);
    partitions[partition].isset = false;
    partitions[partition].eof = false;
    assert(!partitions[partition].opened);
    partitions[partition].opened = false;
    partitions[partition].idxfile = -1;
    partitions[partition].positionfile = 0;
    partitions[partition].sizecurrentfile = 0;
    nsets--;
    cond_sets.notify_one();
    cond_diskbufferpool.notify_one();
}

void MultiMergeDiskLZ4Reader::run() {
    int partitionToRead = 0;

    while (true) {
        //Are there some files to read?
        std::unique_lock<std::mutex> l2(m_sets);
        while (nsets == 0 && !shouldExit) {
            //No partition to read. Must wait...
            cond_sets.wait(l2);
        }
        l2.unlock();
        if (shouldExit) {
            break;
        }

        std::unique_lock<std::mutex> l(m_diskbufferpool, std::defer_lock);

        //There are some partitions I can read. Which one should I choose?
        //Keep looking until the block is finished, or the block is not set
        int skipped = 0;
        while (nsets > 0 && (readAll(partitionToRead)
                             || notsetORenoughread(partitionToRead))) {
            partitionToRead = (partitionToRead + 1) % partitions.size();
            skipped++;
            if (skipped == partitions.size()) {
                //Wait...
                l.lock();
                cond_diskbufferpool.wait(l);
                skipped = 0;
                l.unlock();
            }
        }
        if (nsets == 0)
            continue;

        //Get a disk buffer
        l.lock();
        boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
        cond_diskbufferpool.wait(l, std::bind(&DiskLZ4Reader::availableDiskBuffer, this));
        time_diskbufferpool += boost::chrono::system_clock::now() - start;
        char *buffer = diskbufferpool.back();
        diskbufferpool.pop_back();
        l.unlock();

        //Read input for one partition
        readbuffer(partitionToRead, buffer);
    }

    //Notify all attached files that might be waiting that there is nothing else to read
    for (int i = 0; i < partitions.size(); ++i)
        cond_files[i].notify_one();
}

MultiMergeDiskLZ4Reader::~MultiMergeDiskLZ4Reader() {
}
