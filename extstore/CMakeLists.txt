if(USE_POSIX_STORE)
	add_subdirectory(posix_store)
endif(USE_POSIX_STORE)

if(USE_POSIX_OBJ)
	add_subdirectory(posix_obj)
endif(USE_POSIX_OBJ)

if(USE_RADOS)
	add_subdirectory(rados)
endif(USE_RADOS)

if(USE_S3)
	add_subdirectory(s3)
endif(USE_S3)

