KOGNAC.

==Installation==

The project requires the Boost libraries, which must be compiled with
multi-threading support and made available in accessable locations. KOGNAC uses
also the LZ4 library, but in case this library is not available, it will
automatically download it.

We used CMake to ease the installation process. To build KOGNAC, the following
commands should suffice:

mkdir build
cd build
cmake ..
make

