cmake_minimum_required (VERSION 2.8)

set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_C_COMPILER "gcc")

project (kognac)
include(ExternalProject)

#Set to release mode if nothing is specified
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    message(STATUS "Setting build type to 'Release' as none was specified.")
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)
    # Set the possible values of build type for cmake-gui
      set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "Release" "MinSizeRel" "RelWithDebInfo")
endif()


#Add macPorts
# Detect if the "port" command is valid on this system; if so, return full path
EXECUTE_PROCESS(COMMAND which port RESULT_VARIABLE DETECT_MACPORTS OUTPUT_VARIABLE MACPORTS_PREFIX ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)

IF (${DETECT_MACPORTS} EQUAL 0)
    # "/opt/local/bin/port" doesn't have libs, so we get the parent directory
    GET_FILENAME_COMPONENT(MACPORTS_PREFIX ${MACPORTS_PREFIX} DIRECTORY)

    # "/opt/local/bin" doesn't have libs, so we get the parent directory
    GET_FILENAME_COMPONENT(MACPORTS_PREFIX ${MACPORTS_PREFIX} DIRECTORY)

    # "/opt/local" is where MacPorts lives, add `/lib` suffix and link
    LINK_DIRECTORIES(${MACPORTS_PREFIX}/lib)
    INCLUDE_DIRECTORIES(${MACPORTS_PREFIX}/include)
ENDIF()

#Add homebrew
EXECUTE_PROCESS(COMMAND brew --prefix RESULT_VARIABLE exitstatus OUTPUT_VARIABLE BREW_PREFIX ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE)

IF (${exitstatus} EQUAL 0)
    LINK_DIRECTORIES(${BREW_PREFIX}/lib)
    INCLUDE_DIRECTORIES(${BREW_PREFIX}/include)
ENDIF()

#add files to compile
file(GLOB kognac_SRC
    "src/kognac/**/*.cpp"
)

#Create both a library and the executable program
add_library(kognac STATIC ${kognac_SRC})
add_executable(kognac_exec src/launcher/main.cpp)

#Set compiler options
set(CMAKE_CXX_FLAGS_RELEASE "-DNDEBUG -O3")
set(CMAKE_CXX_FLAGS_DEBUG "-O0 -g")
set(COMPILE_FLAGS "-c -DBOOST_LOG_DYN_LINK -fPIC -std=c++11 -Wall -Wno-sign-compare")
set_target_properties(kognac PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
set_target_properties(kognac_exec PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")

#PTHREADS
find_package(Threads REQUIRED)
if (THREADS_HAVE_PTHREAD_ARG)
    target_compile_options(PUBLIC kognac_exec "-pthread")
endif()
if (CMAKE_THREAD_LIBS_INIT)
    target_link_libraries(kognac_exec "${CMAKE_THREAD_LIBS_INIT}")
endif()
    
#SPARSEHASH
find_path (sparsehash sparsehash/sparse_hash_map)
IF (${sparsehash} STREQUAL "sparsehash-NOTFOUND")
    message("Could not find sparsehash. I'm going to download it from the GIT repository ...")
    ExternalProject_Add(git-sparsehash
        DOWNLOAD_COMMAND git clone https://github.com/sparsehash/sparsehash.git
        DOWNLOAD_DIR external
        SOURCE_DIR external/sparsehash
        CONFIGURE_COMMAND ./configure
        BUILD_IN_SOURCE 1
        BUILD_COMMAND make src/sparsehash/internal/sparseconfig.h
        INSTALL_COMMAND ""
    )
    ExternalProject_Get_Property(git-sparsehash SOURCE_DIR)
    include_directories(${SOURCE_DIR}/src/)
    add_dependencies(kognac git-sparsehash)
ENDIF()

#LZ4
find_library(lz4 lz4)
find_path (lz4h lz4.h)
IF ((${lz4} STREQUAL "lz4-NOTFOUND") OR (${lz4h} STREQUAL "lz4h-NOTFOUND"))
    message("Could not find LZ4. I'm going to download it from the GIT repository ...")
    ExternalProject_Add(git-lz4
        DOWNLOAD_COMMAND git clone https://github.com/Cyan4973/lz4.git
        DOWNLOAD_DIR external
        SOURCE_DIR external/lz4/
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND cd lib; make lib MOREFLAGS=-fPIC
        INSTALL_COMMAND ""
    )
    ExternalProject_Get_Property(git-lz4 SOURCE_DIR)
    include_directories(${SOURCE_DIR}/lib/)
    add_library(lz4 STATIC IMPORTED)
    set_property(TARGET lz4 PROPERTY IMPORTED_LOCATION external/lz4/lib/liblz4.a)
    add_dependencies(lz4 git-lz4)
    add_dependencies(kognac lz4)
ELSE()
    include_directories(lz4h)
    message("Found LZ4")
    
ENDIF()

#Boost
set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF)
FIND_PACKAGE(Boost COMPONENTS chrono thread system filesystem program_options log log_setup iostreams REQUIRED)
IF (Boost_FOUND)
    INCLUDE_DIRECTORIES(${Boost_INCLUDE_DIR})
ENDIF()

#standard include
include_directories(include/)
TARGET_LINK_LIBRARIES(kognac lz4 ${Boost_LIBRARIES})
TARGET_LINK_LIBRARIES(kognac_exec kognac)
