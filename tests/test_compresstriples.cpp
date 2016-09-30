#include <string>
#include <mutex>
#include <condition_variable>

#include <kognac/stringscol.h>
#include <kognac/compressor.h>
#include <kognac/lz4io.h>
#include <iostream>

using namespace std;

int main(int argc, const char** argv) {
    string kbPath = string(argv[1]);

    //Get finalMap and poolForMap
    ByteArrayToNumberMap finalMap;
    finalMap.set_empty_key(EMPTY_KEY);
    finalMap.set_deleted_key(DELETED_KEY);
    StringCollection poolForMap(64*1024*1024);
    LZ4Reader reader(kbPath + "/dict-0"); 
    int count = 0;
    while (!reader.isEof()) {
        long key = reader.parseLong();
        int size = 0;
        const char *value = reader.parseString(size);
        const char *newvalue= poolForMap.addNew(value, size);
        finalMap.insert(std::make_pair(newvalue, key));
        count ++;
    }
    cout << "Finished loading the hashmap" << endl;

    string permDirs[6];
    string tmpFiles[8];
    vector<string> notSoUncommonFiles;
    vector<string> sortedFiles;
    notSoUncommonFiles.push_back(kbPath + "dict-0-np1");
    for(int i = 0; i < 6; ++i) {
        permDirs[i] = kbPath + "/permtmp-" + to_string(i);
    }
    for(int i = 0; i < 8; ++i) {
        sortedFiles.push_back(kbPath + "/listUncommonTerms" + to_string(i));
        tmpFiles[i] = kbPath + "/tmp-" + to_string(i);
    }

    Compressor::compressTriples(8, 72, 1, permDirs, 6, 63, notSoUncommonFiles, sortedFiles, tmpFiles, &poolForMap, &finalMap);
}
