#include <kognac/multidisklz4writer.h>

MultiDiskLZ4Writer::MultiDiskLZ4Writer(std::vector<string> files,
                                       int nbuffersPerFile,
                                       int maxopenedstreams) :
    DiskLZ4Writer(files.size(), nbuffersPerFile),
    maxopenedstreams(maxopenedstreams) {
    //assert(files.size() > 0);

    for (auto f : files) {
        PartFiles partfiles;
        partfiles.filestowrite.push_back(f);
        this->files.push_back(partfiles);
    }

    streams = new ofstream[files.size()];
    openedstreams = new bool[files.size()];
    memset(openedstreams, 0, sizeof(bool) * files.size());
    nopenedstreams = 0;

    currentthread = thread(std::bind(&MultiDiskLZ4Writer::run, this));
    processStarted = true;
}

void MultiDiskLZ4Writer::addFileToWrite(int idpart, string file) {
    //Flush all buffers
    flush(idpart);

    //Add a new file in the registered list
    files[idpart].filestowrite.push_back(file);

    //Increase the counter in FileInfo
    fileinfo[idpart].idxfiletowrite++;
}

void MultiDiskLZ4Writer::run() {
    while (true) {
        std::list<BlockToWrite> blocks;

        auto start = boost::chrono::system_clock::now();
        std::unique_lock<std::mutex> lk(mutexBlockToWrite);
        cvBlockToWrite.wait(lk, std::bind(&DiskLZ4Writer::areBlocksToWrite, this));
        time_waitingwriting += boost::chrono::system_clock::now() - start;

        if (addedBlocksToWrite > 0) {
            //Search the first non-empty file to write
            int nextid = (currentWriteFileID + 1) % npartitions;
            while (blocksToWrite[nextid].empty()) {
                nextid = (nextid + 1) % npartitions;
            }
            currentWriteFileID = nextid;
            blocksToWrite[currentWriteFileID].swap(blocks);
            addedBlocksToWrite -= blocks.size();
            lk.unlock();
        } else { //Exit...
            lk.unlock();
            break;
        }

        start = boost::chrono::system_clock::now();

        //First get the right file to open
        auto it = blocks.begin();
        const int idFile = it->idpart;
        int currentfileidx = files[idFile].currentopenedfile;
        if (!openedstreams[idFile]) {
            if (nopenedstreams == maxopenedstreams) {
                //Must close one file. I pick the oldest one
                const int filetoremove = historyopenedfiles.front();
                historyopenedfiles.pop_front();
                streams[filetoremove].close();
                openedstreams[filetoremove] = false;
                nopenedstreams--;
            }
            //BOOST_LOG_TRIVIAL(debug) << "Open file " << files[idFile];
            string path = files[idFile].filestowrite[currentfileidx];
            streams[idFile].open(path, ios_base::ate | ios_base::app);
            if (!streams[idFile].good()) {
                BOOST_LOG_TRIVIAL(error) << "Problems opening file " << idFile;
            }
            openedstreams[idFile] = true;
            historyopenedfiles.push_back(idFile);
            nopenedstreams++;
        }
        while (it != blocks.end()) {
            assert(it->idpart == idFile);
            if (currentfileidx != it->idxfile) {
                //Close the file and open the new one
                currentfileidx = files[idFile].currentopenedfile = it->idxfile;
                string path = files[idFile].filestowrite[currentfileidx];
                streams[idFile].close();
                streams[idFile].open(path, ios_base::ate | ios_base::app);
                if (!streams[idFile].good()) {
                    BOOST_LOG_TRIVIAL(error) << "Problems opening file " << idFile;
                }
            }
            //Write and check the writing was successful
            streams[idFile].write(it->buffer, it->sizebuffer);
            if (!streams[idFile].good()) {
                BOOST_LOG_TRIVIAL(error) << "Problems writing the file " << idFile;
            }

            it++;
        }
        time_rawwriting += boost::chrono::system_clock::now() - start;

        //BOOST_LOG_TRIVIAL(debug) << "WRITING TIME " << time_rawwriting.count()
        //                         << "ec. Waitingwriting " << time_waitingwriting.count()
        //                         << "sec." << " Waiting buffer "
        //                         << time_waitingbuffer.count() << "sec.";

        //Return the buffer so that it can be reused
        unique_lock<std::mutex> lk2(mutexAvailableBuffer);
        it = blocks.begin();
        while (it != blocks.end()) {
            buffers.push_back(it->buffer);
            it++;
        }
        lk2.unlock();
        cvAvailableBuffer.notify_one();
    }

    //Close all files
    for (int i = 0; i < files.size(); ++i) {
        if (openedstreams[i]) {
            streams[i].close();
        }
    }
}

MultiDiskLZ4Writer::~MultiDiskLZ4Writer() {
    currentthread.join();
    for(int i = 0; i < files.size(); ++i) {
        assert(!streams[i].is_open());
    }
    processStarted = false;
    delete[] streams;
    delete[] openedstreams;
}
