#include <string>
#include <mutex>
#include <condition_variable>

#include <kognac/compressor.h>
#include <kognac/lz4io.h>
#include <iostream>

using namespace std;

int main(int argc, const char** argv) {
    std::vector<string> input;
    input.push_back(argv[1]);
    std::vector<string> output;
    string kbPath = string(argv[2]);
    Compressor::sortFilesByTripleSource(kbPath, 8, 72, 1, input, output);
}
