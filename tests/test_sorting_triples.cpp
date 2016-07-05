#include <string>
#include <boost/log/trivial.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <kognac/compressor.h>

using namespace std;

int main(int argc, const char** argv) {
    string input = argv[1];
    int nthreads = atoi(argv[2]);
    int readthreads = atoi(argv[3]);
    string output = argv[4];

    std::vector<string> inputFiles;
    auto pdir = fs::path(input).parent_path();
    fs::directory_iterator ei;
    for (fs::directory_iterator diter(pdir); diter != ei; ++diter) {
        if (fs::is_regular_file(diter->status())) {
            auto pfile = diter->path();
            if (boost::algorithm::contains(pfile.filename().string(), "-np2")) {
                if (pfile.has_extension()) {
                    string ext = pfile.extension().string();
                    int idx;
                    stringstream(ext.substr(1, ext.length() - 1)) >> idx;
                    inputFiles.push_back(pfile.string());
                }
            }
        }
    }

    std::vector<string> sortedFiles;
    Compressor::sortFilesByTripleSource(input,
                                        readthreads,
                                        nthreads,
                                        1,
                                        inputFiles,
                                        sortedFiles);
}
