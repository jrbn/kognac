#ifndef _MULTI_DISK_LZ4_WRITER_H
#define _MULTI_DISK_LZ4_WRITER_H

#include <kognac/disklz4writer.h>

#include <vector>

class MultiDiskLZ4Writer : public DiskLZ4Writer {
private:
    struct PartFiles {
        std::vector<string> filestowrite;
        int currentopenedfile;
        PartFiles() {
            currentopenedfile = 0;
        }
    };
    std::vector<PartFiles> files;

    ofstream *streams;
    bool *openedstreams;
    int nopenedstreams;
    const int maxopenedstreams;
    std::list<int> historyopenedfiles;

public:
    MultiDiskLZ4Writer(std::vector<string> files,
                       int nbuffersPerFile,
                       int maxopenedstreams);

    void addFileToWrite(int idpart, string file);

    virtual void run();

    virtual ~MultiDiskLZ4Writer();
};

#endif
