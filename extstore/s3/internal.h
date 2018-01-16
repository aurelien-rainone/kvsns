/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (C) Cynny Space, 2018
 * Author: Aurélien Rainone  aurelien.rainone@gmail.com
 *
 * contributeur : Aurélien Rainone  aurelien.rainone@gmail.com
 *
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 *
 * -------------
 */

/* internal.h
 * KVSNS: S3 extstore internal declarations.
 */

#ifndef _S3_EXTSTORE_INTERNAL_H
#define _S3_EXTSTORE_INTERNAL_H

#include <errno.h>
#include <assert.h>
#include <kvsns/kvsns.h>
#include <libs3.h>


#define RC_WRAP(__function, ...) ({\
	int __rc = __function(__VA_ARGS__);\
	if (__rc != 0)	\
		return __rc; })

#define RC_WRAP_LABEL(__rc, __label, __function, ...) ({\
	__rc = __function(__VA_ARGS__);\
	if (__rc != 0)        \
		goto __label; })

#ifdef DEBUG
	#define ASSERT_FAIL(ex, fi, l, fu) \
	do { \
		fprintf(stderr, "%s:%i: %s: Assertion `%s` failed.\n", fi, li, fu, ex); \
		assert(0); \
	} while(0)
#else
	#define ASSERT_FAIL(ex, fi, l, fu) ((void)0)
#endif

#if DEBUG
	#define ASSERT(expr) do { if(!(expr)) ASSERT_FAIL(#expr, __FILE__, __LINE__, __func__); } while(0)
#else
	#define ASSERT(expr)  ((void)0)
#endif

/* S3 constants/limits nor provided by libs3.h */
#define S3_MAX_ACCESS_KEY_ID_SIZE 256		/* not sure about this */
#define S3_MAX_SECRET_ACCESS_KEY_ID_SIZE 256	/* not sure about this */

/* Default values for S3 requests configuration */
#define S3_REQ_DEFAULT_RETRIES 3
#define S3_REQ_DEFAULT_SLEEP_INTERVAL 1
#define S3_REQ_DEFAULT_TIMEOUT 10000

/* S3 request configuration */
typedef struct extstore_s3_req_cfg_ {
	int retries;	    /* max retries for failed S3 requests */
	int sleep_interval; /* sleep interval between successive retries (s) */
	int timeout;	    /* request timeout (ms) */
	int log_props;	    /* [DBG] log response properties */
} extstore_s3_req_cfg_t;

/**
 * @brief posix error code from libS3 status error
 *
 * This function returns a posix errno equivalent from an libs3 S3Status.
 *
 * @param[in] s3_errorcode libs3 error
 *
 * @return negative posix error numbers.
 */
int s3status2posix_error(const S3Status s3_errorcode);

typedef struct growbuffer_ {
	int size;			/* total number of bytes */
	int start;			/* start byte */
	char data[64 * 1024];		/* blocks */
	struct growbuffer_ *prev;
	struct growbuffer_ *next;
} growbuffer_t;

/**
 * @brief returns nonzero on success, zero on out of memory
 *
 * @param[in] s3_errorcode libs3 error
 *
 * @return 0 on success
 */
int growbuffer_append(growbuffer_t **gb, const char *data, int data_len);
void growbuffer_read(growbuffer_t **gb, int amt, int *amt_ret, char *buffer);
void growbuffer_destroy(growbuffer_t *gb);

/**
 * Prepends t into s. Assumes s has enough space allocated
 * for the combined string.
 */
void prepend(char* s, const char* t);

/**
 * Build path of S3 Object and return object directory and filename.
 *
 * @param object - object inode.
 * @param obj_dir - [OUT] full S3 directory path.
 * @param obj_fname - [OUT] S3 object filename, empty for a directory.
 *
 * @note Returned directory path doesn't start with a '/' as libs3 requires 
 * object keys to be formatted in this way. The bucket root is an empty string.
 * However directory paths are returned with a trailing '/', this is a S3 
 * requirement.
 * 
 * @return 0 if successful, a negative "-errno" value in case of failure
 */
int build_objpath(kvsns_ino_t object, char *obj_dir, char *obj_fname);

/**
 * Build full path of S3 Object.
 *
 * @param object - object inode.
 * @param obj_path - [OUT] full S3 path
 * 
 * @note Returned path doesn't start with a '/' as libs3 requires object keys
 * to be formatted in this way. The bucket root is an empty string.
 * However directory paths are returned with a trailing '/', this is a S3 
 * requirement.
 *
 * @return 0 if successful, a negative "-errno" value in case of failure
 */
int build_fullpath(kvsns_ino_t object, char *obj_path);

#endif
