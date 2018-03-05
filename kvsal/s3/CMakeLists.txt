
SET(kvsal_LIB_SRCS
   kvsal_redis.c
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

add_library(kvsal SHARED ${kvsal_LIB_SRCS})
target_link_libraries(kvsal hiredis s3 ini_config ${GLib_LIBRARY})

add_custom_command(TARGET kvsal
                   COMMAND ${CMAKE_COMMAND} -E copy libkvsal.so ..)
