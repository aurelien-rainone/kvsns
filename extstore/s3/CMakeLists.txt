
SET(extstore_LIB_SRCS
   extstore.c
)

# require LIBS3 w/API version 1.x
find_package(LibS3)
if(NOT LIBS3_FOUND)
    message(FATAL_ERROR "Cannot find libs3.")
endif(NOT LIBS3_FOUND)

add_library(extstore SHARED ${extstore_LIB_SRCS})
target_link_libraries(extstore s3 ini_config)

add_custom_command(TARGET extstore
                   COMMAND ${CMAKE_COMMAND} -E copy libextstore.so ..)
