#include <string>
#include <boost/log/trivial.hpp>

#include <kognac/compressor.h>

using namespace std;

int main(int argc, const char** argv) {
    string input = argv[1];
    int nthreads = atoi(argv[2]);
    int readthreads = atoi(argv[3]);
    string output = argv[4];

    //prepare inputs
    string **inputs;
    inputs = new string*[readthreads];
    for (int i = 0; i < readthreads; ++i) {
        inputs[i] = new string[1];
        inputs[i][0] = input + to_string(i) + string("-0");
    }
    string *outputs;
    outputs = new string[1];
    outputs[0] = output;

    Compressor::sortDictionaryEntriesByText(inputs,
                                            1,
                                            readthreads,
                                            nthreads,
                                            outputs,
                                            NULL,
                                            false,
                                            true);
}
