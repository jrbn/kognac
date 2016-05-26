#include <kognac/diskreader.h>

DiskReader::DiskReader(int nbuffers, std::vector<FileInfo> *files) {
    this->files = files;
    itr = files->begin();
    finished = false;
    size_t maxsize = 0;
    for (int i = 0; i < files->size(); ++i) {
        if (files->at(i).size > maxsize)
            maxsize = files->at(i).size;
    }

    for (int i = 0; i < nbuffers; ++i) {
        availablebuffers.push_back(new char[maxsize]);
    }
}

bool DiskReader::isReady() {
    return !this->readybuffers.empty() || finished;
}

bool DiskReader::isAvailable() {
    return !availablebuffers.empty();
}

char *DiskReader::getfile(size_t &size, bool &gzipped) {
    std::unique_lock<std::mutex> lk(mutex1);
    cv1.wait(lk, std::bind(&DiskReader::isReady, this));

    Buffer info;
    bool gotit = false;
    if (!finished) {
        gotit = true;
        assert(!readybuffers.empty());
        info = readybuffers.back();
        readybuffers.pop_back();
    }

    lk.unlock();
    //Tell another waiting thread that there might be jobs for them
    cv1.notify_one();

    if (gotit) {
        size = info.size;
        gzipped = info.gzipped;
        return info.b;
    } else {
        size = 0;
        gzipped = false;
        return NULL;
    }
}

void DiskReader::releasefile(char *file) {
    std::unique_lock<std::mutex> lk(mutex2);
    availablebuffers.push_back(file);
    lk.unlock();
    cv2.notify_one();
}

void DiskReader::run() {
    BOOST_LOG_TRIVIAL(debug) << "DiskReader: start reading the files";
    ifstream ifs;
    while (itr != files->end()) {

        //Is there an available buffer that I can use?
        char *buffer = NULL;
        {
            std::unique_lock<std::mutex> lk(mutex2);
            cv2.wait(lk, std::bind(&DiskReader::isAvailable, this));
            buffer = availablebuffers.back();
            availablebuffers.pop_back();
            lk.unlock();
        }

        //Read a file and copy it in buffer
        ifs.open(itr->path);
        assert(itr->start == 0);
        ifs.read(buffer, itr->size);
        ifs.close();

        {
            std::lock_guard<std::mutex> lk(mutex1);
            Buffer newbuffer;
            newbuffer.b = buffer;
            newbuffer.size = itr->size;
            fs::path p(itr->path);
            if (p.has_extension() && p.extension() == string(".gz")) {
                newbuffer.gzipped = true;
            } else {
                newbuffer.gzipped = false;
            }
            readybuffers.push_back(newbuffer);
        }

        //Alert one waiting thread that there is one new buffer
        cv1.notify_one();
        itr++;
    }

    {
        std::lock_guard<std::mutex> lk(mutex);
        finished = true;
    }
    cv1.notify_all();

    BOOST_LOG_TRIVIAL(debug) << "DiskReader: finished reading the files";
}

DiskReader::~DiskReader() {
    for(int i = 0; i < availablebuffers.size(); ++i)
        delete[] availablebuffers[i];
    availablebuffers.clear();
}
