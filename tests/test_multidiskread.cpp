#include <string>
#include <mutex>
#include <condition_variable>

#include <kognac/lz4io.h>
#include <kognac/multidisklz4reader.h>
#include <iostream>

#include <boost/thread.hpp>
#include <boost/filesystem.hpp>
#include <boost/log/trivial.hpp>

using namespace std;

namespace fs = boost::filesystem;

void readElements(int idReader, MultiDiskLZ4Reader *reader) {
	long ntriples = 350000000;
	long count = 0;
	for(long i = 0; i < ntriples; ++i) {
		long s = reader->readLong(idReader);
		long p = reader->readLong(idReader);
		long o = reader->readLong(idReader);
		count++;
		if (count % 100000000 == 0)
			BOOST_LOG_TRIVIAL(debug) << "processed " << count;
	}
	BOOST_LOG_TRIVIAL(debug) << "Finished " << idReader << " " << reader;
}

int main(int argc, const char** argv) {
    	int parallelProcesses = 72;
    	int maxReadingThreads = 8;
	string inputDir = string(argv[1]);

	vector<string> unsortedFiles = Utils::getFiles(inputDir);
        MultiDiskLZ4Reader **readers = new MultiDiskLZ4Reader*[maxReadingThreads];
        std::vector<std::vector<string>> inputsReaders(parallelProcesses);
        int currentPart = 0;
        for(int i = 0; i < unsortedFiles.size(); ++i) {
            if (fs::exists(fs::path(unsortedFiles[i]))) {
                inputsReaders[currentPart].push_back(unsortedFiles[i]);
                currentPart = (currentPart + 1) % parallelProcesses;
            }
        }
        auto itr = inputsReaders.begin();
        int filesPerReader = parallelProcesses / maxReadingThreads;
        for(int i = 0; i < maxReadingThreads; ++i) {
            readers[i] = new MultiDiskLZ4Reader(filesPerReader,
                    3, 4);
            readers[i]->start();
            for(int j = 0; j < filesPerReader; ++j) {
                if (itr->empty()) {
                    BOOST_LOG_TRIVIAL(debug) << "Part " << j << " is empty";
                } else {
                    BOOST_LOG_TRIVIAL(debug) << "Part " << i << " " << j << " " << itr->at(0);
                }
                readers[i]->addInput(j, *itr);
                itr++;
            }
        }

    	boost::thread *threads = new boost::thread[parallelProcesses];
	for (int i = 0; i < parallelProcesses; ++i) {
		MultiDiskLZ4Reader *reader = readers[i % maxReadingThreads];
                int idReader = i / maxReadingThreads;
                threads[i] = boost::thread(boost::bind(&readElements, idReader, reader));
	}
	for (int i = 0; i < parallelProcesses; ++i) {
                threads[i].join();
	}
    	delete[] threads;
}
