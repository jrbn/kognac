#include <kognac/multidisklz4reader.h>

#include <boost/filesystem.hpp>

namespace fs = boost::filesystem;

MultiDiskLZ4Reader::MultiDiskLZ4Reader(int maxNPartitions,
                                       int nbuffersPerPartition,
                                       int maxopenedstreams) :
    DiskLZ4Reader(maxNPartitions, nbuffersPerPartition),
    maxopenedstreams(maxopenedstreams) {

    partitions.resize(maxNPartitions);
    nopenedstreams = 0;
    nsets = 0;

    currentthread = thread(std::bind(&MultiDiskLZ4Reader::run, this));
}

void MultiDiskLZ4Reader::addInput(int id, std::vector<string> &files) {
    std::unique_lock<std::mutex> l(m_sets);
    partitions[id].files = files;
    nsets++;
    cond_sets.notify_one();
    l.unlock();
}

bool MultiDiskLZ4Reader::readAll(int id) {
    return partitions[id].eof;
}

bool MultiDiskLZ4Reader::areNewBuffers(const int id) {
    return !compressedbuffers[id].empty() || readAll(id);
}

void MultiDiskLZ4Reader::run() {
    int partitionToRead = 0;

    std::unique_lock<std::mutex> l(m_sets);
    while (nsets < partitions.size()) {
        cond_sets.wait(l);
    }
    l.unlock();

    while (true) {
        //Get a disk buffer
        boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
        std::unique_lock<std::mutex> l(m_diskbufferpool);
        cond_diskbufferpool.wait(l, std::bind(&DiskLZ4Reader::availableDiskBuffer, this));
        time_diskbufferpool += boost::chrono::system_clock::now() - start;

        char *buffer = diskbufferpool.back();
        diskbufferpool.pop_back();
        l.unlock();

        //Read the file and put the content in the disk buffer
        start = boost::chrono::system_clock::now();

        //Check whether I can get a buffer from the current file. Otherwise
        //keep looking
        int skipped = 0;
        while (skipped < partitions.size() && readAll(partitionToRead)) {
            partitionToRead = (partitionToRead + 1) % partitions.size();
            skipped++;
        }
        if (skipped == partitions.size())
            break; //It means I read all possible blocks

        PartInfo &part = partitions[partitionToRead];
        if (!part.opened && nopenedstreams == maxopenedstreams) {
            //I must close one stream
            int lastfile = historyopenedfiles.front();
            while (partitions[lastfile].eof) {
                historyopenedfiles.pop_front();
                lastfile = historyopenedfiles.front();
            }
            historyopenedfiles.pop_front();
            assert(partitions[lastfile].opened);
            part.stream.close();
            part.opened = false;
            nopenedstreams--;
        }
        if (part.positionfile == part.sizecurrentfile) {
            if (part.opened) {
                part.stream.close();
                part.opened = false;
                nopenedstreams--;
            }
            part.idxfile++;
            part.positionfile = 0;
            part.sizecurrentfile = fs::file_size(part.files[part.idxfile]);
        }
        if (!part.opened) {
            //Open the file
            part.stream.open(part.files[part.idxfile]);
            part.stream.seekg(part.positionfile);
            nopenedstreams++;
            historyopenedfiles.push_back(partitionToRead);
            part.opened = true;
        }

        //Read the content of the file
        size_t sizeToBeRead = part.sizecurrentfile - part.positionfile;
        sizeToBeRead = min(sizeToBeRead, (size_t) SIZE_DISK_BUFFER);
        part.stream.read(buffer, sizeToBeRead);
        part.positionfile += sizeToBeRead;
        time_rawreading += boost::chrono::system_clock::now() - start;

        //Put the content of the disk buffer in the blockToRead container
        start = boost::chrono::system_clock::now();
        std::unique_lock<std::mutex> lk2(m_files[partitionToRead]);
        time_files[partitionToRead] += boost::chrono::system_clock::now() - start;

        BlockToRead b;
        b.buffer = buffer;
        b.sizebuffer = sizeToBeRead;
        b.pivot = 0;
        compressedbuffers[partitionToRead].push_back(b);
        if (part.idxfile + 1 == part.files.size() &&
                              part.positionfile == part.sizecurrentfile) {
            part.eof = true;
            part.opened = false;
            part.stream.close();
            nopenedstreams--;
        }
        lk2.unlock();
        cond_files[partitionToRead].notify_one();

        //Move to the next file/block
        partitionToRead = (partitionToRead + 1) % partitions.size();
    }

    //Notify all attached files that might be waiting that there is nothing else to read
    for (int i = 0; i < partitions.size(); ++i)
        cond_files[i].notify_one();
}

//bool MultiDiskLZ4Reader::isEof(int id) {
//    return partitions[id].positionfile == partitions[id].sizecurrentfile &&
//           partitions[id].idxfile == partitions[id].files.size();
//}

MultiDiskLZ4Reader::~MultiDiskLZ4Reader() {
    //currentthread.join();
}
