#! /usr/bin/env bash

# CMake build dir path
CMAKE_BUILD=${1}

if [ -d "$CMAKE_BUILD" ]; then
    # install shared libraries
    cp $CMAKE_BUILD/extstore/libextstore.so /usr/lib/
    cp $CMAKE_BUILD/kvsns/libkvsns.so /usr/lib/
    cp $CMAKE_BUILD/kvsal/libkvsal.so /usr/lib/

    # install headers
    cp -r ./include/kvsns /usr/include/
    exit 0
fi

echo usage: ./install.sh /path/to/cmake/build/dir
exit 1
