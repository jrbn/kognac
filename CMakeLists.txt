cmake_minimum_required (VERSION 2.8)
project (kognac)
include(ExternalProject)

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
set(COMPILE_FLAGS "-c -DBOOST_LOG_DYN_LINK -fPIC -O3 -g -std=c++11 -Wall -Wno-sign-compare")
set_target_properties(kognac PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")
set_target_properties(kognac_exec PROPERTIES COMPILE_FLAGS "${COMPILE_FLAGS}")

#LZ4
find_library(lz4 lz4)
find_path (lz4h lz4.h)
IF ((${lz4} STREQUAL "lz4-NOTFOUND") OR (${lz4h} STREQUAL "lz4h-NOTFOUND"))
    message("Could not find LZ4. I'm going to download it from the GIT repository ...")
    ExternalProject_Add(git-lz4
        DOWNLOAD_COMMAND git clone https://github.com/Cyan4973/lz4.git
        DOWNLOAD_DIR external
        SOURCE_DIR external/lz4
        CONFIGURE_COMMAND ""
        BUILD_IN_SOURCE 1
        BUILD_COMMAND make lib 
        INSTALL_COMMAND ""
    )
    ExternalProject_Get_Property(git-lz4 SOURCE_DIR)
    include_directories(${SOURCE_DIR}/lib/)
    add_library(lz4 STATIC IMPORTED)
    set_property(TARGET lz4 PROPERTY IMPORTED_LOCATION external/lz4/lib/liblz4.a)
    add_dependencies(lz4 git-lz4)
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
