#include <string>
#include <mutex>
#include <condition_variable>

#include <kognac/lz4io.h>
#include <kognac/multidisklz4writer.h>
#include <iostream>

#include <boost/thread.hpp>

using namespace std;

void process(MultiDiskLZ4Writer *writer, int offset, int idxWriter) {
    for (int i = 0; i < 6; ++i) {
        writer->writeByte(idxWriter + i, 0);
    }

    long ntriples = 1000000000;
    long count = 0;
    for(long i = 0; i < ntriples; ++i) {
        if (i % 20000000 == 0 && i != 0)
            cout << "Processed " << i << endl;
        for(int j = 0; j < 6; ++j) {
            writer->writeLong(idxWriter + j, count++);
            writer->writeLong(idxWriter + j, count++);
            writer->writeLong(idxWriter + j, count++);
        }
    }

    for(int i = 0; i < 6; ++i) {
        writer->setTerminated(idxWriter + i);
    }
}

int main(int argc, const char** argv) {

    int nthreads = 72;
    int maxReadingThreads = 4;
    int nperms = 6;
    string output = string(argv[1]);
 
    std::vector<std::vector<string>> chunks;
    chunks.resize(maxReadingThreads);
    for (int i = 0; i < nthreads; ++i) {
        for (int j = 0; j < nperms; ++j) {
            string file = output + string("/") + to_string(j) + string("/chunk") + to_string(i);
            chunks[i % maxReadingThreads].push_back(file);
        }
    }
    MultiDiskLZ4Writer **writers = new MultiDiskLZ4Writer*[maxReadingThreads];
    for (int i = 0; i < maxReadingThreads; ++i) {
        writers[i] = new MultiDiskLZ4Writer(chunks[i], 3, 3);
    }
                
    boost::thread *threads = new boost::thread[nthreads];
    for(int i = 0; i < nthreads; ++i) {
        MultiDiskLZ4Writer *writer = writers[i % maxReadingThreads];
        int idxWriter = (i / maxReadingThreads) * nperms;
        threads[i] = boost::thread(boost::bind(&process, writer, i % maxReadingThreads, idxWriter));
    }

    for (int i = 0; i < nthreads; ++i) {
        threads[i].join();
    }
    delete[] threads;

    for (int i = 0; i < maxReadingThreads; ++i) {
        delete writers[i];
    }
    delete[] writers;
}
