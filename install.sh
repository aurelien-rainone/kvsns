#! /usr/bin/env bash

# CMake build dir path
CMAKE_BUILD=${1}

if [ -d "$CMAKE_BUILD" ]; then
    # install shared libraries
    echo "installing /usr/lib/libextstore.so"
    cp $CMAKE_BUILD/extstore/libextstore.so /usr/lib/
    echo "installing /usr/lib/libkvsns.so"
    cp $CMAKE_BUILD/kvsns/libkvsns.so /usr/lib/
    echo "installing /usr/lib/libkvsal.so"
    cp $CMAKE_BUILD/kvsal/libkvsal.so /usr/lib/

    # install headers
    echo "installing headers in /usr/include"
    cp -r ./include/kvsns /usr/include/
    exit 0
fi

echo usage: ./install.sh /path/to/cmake/build/dir
exit 1
