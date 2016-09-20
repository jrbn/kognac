#include <string>
#include <mutex>
#include <condition_variable>

#include <kognac/lz4io.h>
#include <iostream>

using namespace std;

int main(int argc, const char** argv) {
    LZ4Reader reader(argv[1]);

    const bool quad = reader.parseByte() != 0;
    long count = 0;
    while (!reader.isEof()) {
        long t1 = reader.parseLong();
        long t2 = reader.parseLong();
        long t3 = reader.parseLong();
        count += 1;
        if (count % 100000000 == 0) {
            cout << "Processed " << count << endl;
        }
    }
}
