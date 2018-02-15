
SET(extstore_LIB_SRCS
   extstore.c
   s3_common.c
   s3_stats_object.c
   s3_test_bucket.c
   s3_put_object.c
   s3_get_object.c
   s3_del_object.c
   mru.c
   internal.c
)

# require LIBS3 w/API version 1.x
find_package(LibS3 REQUIRED)
if(NOT LIBS3_FOUND)
    message(FATAL_ERROR "Cannot find libs3.")
endif(NOT LIBS3_FOUND)

find_package(GLib REQUIRED)
if(NOT GLib_FOUND)
    message(FATAL_ERROR "Cannot find GLib.")
endif(NOT GLib_FOUND)

include_directories(${GLib_INCLUDE_DIRS})
add_library(extstore SHARED ${extstore_LIB_SRCS})
target_link_libraries(extstore s3 ini_config ${GLib_LIBRARY})

add_custom_command(TARGET extstore
                   COMMAND ${CMAKE_COMMAND} -E copy libextstore.so ..)
