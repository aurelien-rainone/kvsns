SET(kvsns_LIB_SRCS
    kvsns_init.c
    kvsns_handle.c
    kvsns_file.c
    kvsns_internal.c
    kvsns_xattr.c
    kvsns_copy.c
    kvsns_log.c
    kvsns_utils.c
    inode_cache.c
    extstore_s3/extstore.c
    extstore_s3/s3_common.c
    extstore_s3/s3_stats_object.c
    extstore_s3/s3_test_bucket.c
    extstore_s3/s3_put_object.c
    extstore_s3/s3_get_object.c
    extstore_s3/s3_del_object.c
    extstore_s3/s3_list_object.c
    extstore_s3/fd_cache.c
    extstore_s3/mru.c
    extstore_s3/internal.c
)

include(CheckIncludeFiles)
include(CheckLibraryExists)

## Check ini_config
check_include_files("ini_config.h" HAVE_INI_CONFIG_H)
find_library(HAVE_INI_CONFIG ini_config)
check_library_exists(
    ini_config
    config_from_file
    ""
    HAVE_INI_CONFIG
    )


if((NOT HAVE_INI_CONFIG) OR (NOT HAVE_INI_CONFIG_H))
      message(FATAL_ERROR "Cannot find ini_config")
endif((NOT HAVE_INI_CONFIG) OR (NOT HAVE_INI_CONFIG_H))

# require LIBS3 w/API version 1.x
find_package(LibS3 REQUIRED)
if(NOT LIBS3_FOUND)
    message(FATAL_ERROR "Cannot find libs3.")
endif(NOT LIBS3_FOUND)

# require GLib 2
find_package(GLib REQUIRED)
if(NOT GLib_FOUND)
    message(FATAL_ERROR "Cannot find GLib.")
endif(NOT GLib_FOUND)

include_directories(${GLib_INCLUDE_DIRS})

add_library(kvsns SHARED ${kvsns_LIB_SRCS})
target_link_libraries(kvsns s3 ini_config ${GLib_LIBRARY} ${KVSAL_LIBRARY})
add_custom_command(TARGET kvsns
                   COMMAND ${CMAKE_COMMAND} -E copy libkvsns.so ..)
