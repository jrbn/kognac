KOGNAC.

==Installation==

The project requires the Boost libraries, which must be compiled with
multi-threading support and made available in accessable locations. KOGNAC uses
also the LZ4 library, but if this library is not available then it will
automatically download it.

We used CMake to ease the installation process. To build KOGNAC, the following
commands should suffice:

mkdir build

cd build

cmake ..

(If you want to build the DEBUG version of the library, add the parameter: -D CMAKE_BUILD_TYPE=Debug. e.g. cmake -D CMAKE_BUILD_TYPE=Debug ..)

make

==Potential problems==

- The system already has a old version of Boost installed.

- In this case, you can override the Boost settings by setting the variables:
BOOST_NO_BOOST_CMAKE=TRUE
BOOST_NO_SYSTEM_PATHS=TRUE
BOOST_ROOT=<path to the BOOST directory>

