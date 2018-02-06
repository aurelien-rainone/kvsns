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

#ifndef _S3_EXTSTORE_S3_MULTIPART_H
#define _S3_EXTSTORE_S3_MULTIPART_H

#include "internal.h"


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

#endif
