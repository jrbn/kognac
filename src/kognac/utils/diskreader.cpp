#include <kognac/diskreader.h>

DiskReader::DiskReader(int nbuffers, std::vector<FileInfo> *files) {
    this->files = files;
    itr = files->begin();
    finished = false;
    maxsize = 0;
    for (int i = 0; i < files->size(); ++i) {
        if (files->at(i).size > maxsize)
            maxsize = files->at(i).size;
    }
    maxsize += 32 * 1024 + maxsize * 0.1; //min size + add a 10%

    BOOST_LOG_TRIVIAL(debug) << "Max size=" << maxsize;
    for (int i = 0; i < nbuffers; ++i) {
        availablebuffers.push_back(new char[maxsize]);
        memset(availablebuffers.back(), 0, sizeof(char) * maxsize);
    }
    waitingTime = boost::chrono::duration<double>::zero();
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
    if (!readybuffers.empty()) {
        gotit = true;
        info = readybuffers.back();
        readybuffers.pop_back();
    }

    lk.unlock();
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
    ifstream ifs;
    size_t count = 0;
    while (itr != files->end()) {

        //Is there an available buffer that I can use?
        char *buffer = NULL;
        {
            boost::chrono::system_clock::time_point start = boost::chrono::system_clock::now();
            std::unique_lock<std::mutex> lk(mutex2);
            cv2.wait(lk, std::bind(&DiskReader::isAvailable, this));
            waitingTime += boost::chrono::system_clock::now() - start;
            buffer = availablebuffers.back();
            availablebuffers.pop_back();
            lk.unlock();
        }

        //Read a file and copy it in buffer
        ifs.open(itr->path);
        long readSize = itr->size;
        if (itr->start > 0) {
            ifs.seekg(itr->start);
            while (!ifs.eof() && ifs.get() != '\n') {
                readSize--;
            };
            readSize--;
        }
        ifs.read(buffer, readSize);
        assert(ifs);
        //Keep reading until the final '\n'
        while (!ifs.eof()) {
            char b = ifs.get();
            if (b == -1) {
                break; //magic value
            }
            if (readSize > maxsize) {
                BOOST_LOG_TRIVIAL(error) << "Buffers are too small. Must fix this";
                throw 10;
            }
            buffer[readSize++] = b;
            if (b == '\n')
                break;
        };
        ifs.close();
        count++;
        {
            std::lock_guard<std::mutex> lk(mutex1);
            Buffer newbuffer;
            newbuffer.b = buffer;
            newbuffer.size = readSize;
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

}

DiskReader::~DiskReader() {
    for (int i = 0; i < availablebuffers.size(); ++i)
        delete[] availablebuffers[i];
    availablebuffers.clear();
}
