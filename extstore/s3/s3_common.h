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

#define USE_DATACACHE 1

/**
 * Test bucket existence/access.
 *
 * @param ctx - libs3 bucket context.
 * @param req_cfg - config for this request.
 *
 * @return 0 if successful, a negative posix error code in case of error.
 */
int test_bucket(const S3BucketContext *ctx,
		extstore_s3_req_cfg_t *req_cfg);

/**
 * Request S3 object stat.
 *
 * @param ctx - libs3 bucket context.
 * @param key - object key.
 * @param req_cfg - config for this request.
 * @param mtime - [OUT] object mtime
 * @param size - [OUT] object size
 *
 * @return a negative posix error code in case of error, a positive value
 *   represents the number of bytes actually received from S3, 0 meaning all bytes
 *   requested have been received.
 */
int stats_object(const S3BucketContext *ctx, const char *key,
		 extstore_s3_req_cfg_t *req_cfg,
		 time_t *mtime, uint64_t *size);


#define MULTIPART_CHUNK_SIZE 1024*1024

/* forward declarations */
typedef struct extstore_s3_req_cfg_ extstore_s3_req_cfg_t;

void multipart_manager_init(const S3BucketContext *bucket_ctx);
void multipart_manager_free();

/* initiate multipart */
int multipart_inode_init(kvsns_ino_t ino,
			 char *fpath,
			 extstore_s3_req_cfg_t *req_cfg);

/* upload multipart chunk */
int multipart_inode_upload_chunk(kvsns_ino_t ino,
				 size_t chunkidx,
				 void *buffer,
				 size_t buffer_size);

int multipart_inode_complete(kvsns_ino_t ino);

int multipart_inode_free(kvsns_ino_t ino);

/* S3 internal functions */
int should_retry(S3Status st, int retries, int interval);
S3Status log_response_properties(const S3ResponseProperties *props, void *data);
void log_response_status_error(S3Status status, const S3ErrorDetails *error);

#endif
