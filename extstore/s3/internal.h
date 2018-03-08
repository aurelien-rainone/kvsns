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

#ifndef _S3_EXTSTORE_INTERNAL_H
#define _S3_EXTSTORE_INTERNAL_H

#include <errno.h>
#include <assert.h>
#include <kvsns/kvsns.h>
#include <gmodule.h>
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
#define S3_REQ_DEFAULT_RETRIES 3			/* maximum number of retries */
#define S3_REQ_DEFAULT_SLEEP_INTERVAL 1		/*< 1s between 2 successive retries */
#define S3_REQ_DEFAULT_TIMEOUT 10000		/*< 10s before considering failure */

/* Default number of threads for multipart upload */
#define S3_NUM_THREADS_UPLOAD 4


/* S3 request configuration */
typedef struct extstore_s3_req_cfg_ {
	int retries;	    /* max retries for failed S3 requests */
	int sleep_interval; /* sleep interval between successive retries (s) */
	int timeout;	    /* request timeout (ms) */
	int upload_nthreads;/* number of threads for upload */
} extstore_s3_req_cfg_t;

int s3status2posix_error(const S3Status s3_errorcode);

typedef struct growbuffer_ {
	int size;			/* total number of bytes */
	int start;			/* start byte */
	char data[64 * 1024];		/* blocks */
	struct growbuffer_ *prev;
	struct growbuffer_ *next;
} growbuffer_t;

int growbuffer_append(growbuffer_t **gb, const char *data, int data_len);
void growbuffer_read(growbuffer_t **gb, int amt, int *amt_ret, char *buffer);
void growbuffer_destroy(growbuffer_t *gb);

void prepend(char* s, const char* t);

int build_s3_path(kvsns_ino_t object, char *obj_path, size_t pathlen);

typedef enum cache_ { read_cache_t, write_cache_t } cache_t;

int build_cache_path(kvsns_ino_t object,
		     char *data_cache_path,
		     cache_t cache_type,
		     size_t pathlen);

void remove_files_in(const char *dirname);

char* printf_open_flags(char *dst, int flags, const size_t len);

gint g_key_cmp_func (gconstpointer a, gconstpointer b);

int mru_key_cmp_func (void *a, void *b);
int rino_close(kvsns_ino_t ino);
int wino_close(kvsns_ino_t ino);
void rino_mru_remove (void *item, void *data);

/*
 * globals declarations
 */

/* s3/libs3 configuration */
extern S3BucketContext bucket_ctx;
extern char host[S3_MAX_HOSTNAME_SIZE];
extern char bucket[S3_MAX_BUCKET_NAME_SIZE];
extern char access_key[S3_MAX_ACCESS_KEY_ID_SIZE];
extern char secret_key[S3_MAX_SECRET_ACCESS_KEY_ID_SIZE];
extern extstore_s3_req_cfg_t def_s3_req_cfg;

/* inode cache data structures */
extern char ino_cache_dir[MAXPATHLEN];
extern GTree *wino_cache;
extern GTree *rino_cache;
extern struct mru rino_mru;
extern const size_t rino_mru_maxlen;

#endif
