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
    inputFiles.push_back(input);
    std::vector<string> sortedFiles;
    auto pdir = fs::path(input).parent_path();
    Compressor::sortFilesByTripleSource(pdir.string(),
                                        readthreads,
                                        nthreads,
                                        1,
                                        inputFiles,
                                        sortedFiles);
}
