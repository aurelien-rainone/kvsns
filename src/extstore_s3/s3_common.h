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

/* s3_methods.h
 * KVSNS: S3 specific declarations.
 */

#ifndef _S3_EXTSTORE_S3_METHODS_H
#define _S3_EXTSTORE_S3_METHODS_H

#include <libs3.h>
#include "internal.h"


/* S3 constants/limits nor provided by libs3.h */
#define S3_MAX_ACCESS_KEY_ID_SIZE 256		/* not sure about this */
#define S3_MAX_SECRET_ACCESS_KEY_ID_SIZE 256	/* not sure about this */

/* Default values for S3 requests configuration */
#define S3_REQ_DEFAULT_RETRIES 3			/* maximum number of retries */
#define S3_REQ_DEFAULT_SLEEP_INTERVAL 1		/*< 1s between 2 successive retries */
#define S3_REQ_DEFAULT_TIMEOUT 10000		/*< 10s before considering failure */

/* s3/libs3 configuration */
extern S3BucketContext bucket_ctx;
extern char host[S3_MAX_HOSTNAME_SIZE];
extern char bucket[S3_MAX_BUCKET_NAME_SIZE];
extern char access_key[S3_MAX_ACCESS_KEY_ID_SIZE];
extern char secret_key[S3_MAX_SECRET_ACCESS_KEY_ID_SIZE];

/* S3 request configuration */
typedef struct extstore_s3_req_cfg_ {
	int retries;	    /* max retries for failed S3 requests */
	int sleep_interval; /* sleep interval between successive retries (s) */
	int timeout;	    /* request timeout (ms) */
} extstore_s3_req_cfg_t;
/* default s3 request configuration */
extern extstore_s3_req_cfg_t def_s3_req_cfg;

/**
 * Test bucket existence/access.
 *
 * @param ctx - [IN] libs3 bucket context.
 * @param req_cfg - [IN] config for this request.
 *
 * @return 0 on success, a negative posix error code in case of error.
 */
int test_bucket(const S3BucketContext *ctx,
		extstore_s3_req_cfg_t *req_cfg);

/**
 * Request S3 object stat.
 *
 * @param ctx - [IN] libs3 bucket context.
 * @param key - [IN] object key.
 * @param req_cfg - [IN] config for this request.
 * @param mtime - [OUT] object mtime
 * @param size - [OUT] object size
 * @param posix_stat - [OUT] object posix attrs, stored in s3 md
 * @param has_posix_stat - [OUT] reports if object md has all posix attributes.
 *
 * @return 0 on success, a negative posix error code in case of error.
 */
int get_stats_object(const S3BucketContext *ctx, const char *key,
		     extstore_s3_req_cfg_t *req_cfg,
		     time_t *mtime, uint64_t *size,
		     struct stat *posix_stat,
		     bool *has_posix_stat);

/*TODO: write documentation */
int set_stats_object(const S3BucketContext *ctx, const char *key,
		     extstore_s3_req_cfg_t *req_cfg,
		     struct stat *posix_stat);

/**
 * Upload a file to S3.
 *
 * Depending on the file length, the file will be sent as a single POST request
 * or a serie of chunks (multipart).
 *
 * @param ctx - [IN] libs3 bucket context.
 * @param key - [IN] object key.
 * @param req_cfg - [IN] config for this request.
 * @param src_file - [IN] path of the file to read and send.
 *
 * @return 0 on success, a negative posix error code in case of error.
 */
int put_object(const S3BucketContext *ctx,
	       const char *key,
	       extstore_s3_req_cfg_t *req_cfg,
	       const char *src_file);

/**
 * Download a file from S3.
 *
 * @param ctx - [IN] libs3 bucket context.
 * @param key - [IN] object key.
 * @param req_cfg - [IN] config for this request.
 * @param dst_file - [IN] path of the file to create/truncate and write.
 * @param mtime - [OUT] object mtime
 * @param size - [OUT] object size
 *
 * @return 0 on success, a negative posix error code in case of error.
 */
int get_object(const S3BucketContext *ctx,
	       const char *key,
	       extstore_s3_req_cfg_t *req_cfg,
	       const char *dst_file,
	       time_t * mtime,
	       size_t *size);

/**
 * Delete an S3 object.
 *
 * @param ctx - [IN] libs3 bucket context.
 * @param key - [IN] object key.
 * @param req_cfg - [IN] config for this request.
 *
 * @return 0 on success, a negative posix error code in case of error.
 */
int del_object(const S3BucketContext *ctx,
	       const char *key,
	       extstore_s3_req_cfg_t *req_cfg);

/* S3 internal functions */
int should_retry(S3Status st, int retries, int interval);
S3Status log_response_properties(const S3ResponseProperties *props, void *data);
void log_response_status_error(S3Status status, const S3ErrorDetails *error);

#endif
