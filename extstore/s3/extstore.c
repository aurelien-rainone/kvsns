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

#include <kvsns/extstore.h>
#include <kvsns/kvsal.h>
#include <hiredis/hiredis.h>
#include "internal.h"
#include "s3_common.h"


static S3BucketContext bucket_ctx;
static char host[S3_MAX_HOSTNAME_SIZE];
static char bucket[S3_MAX_BUCKET_NAME_SIZE];
static char access_key[S3_MAX_ACCESS_KEY_ID_SIZE];
static char secret_key[S3_MAX_SECRET_ACCESS_KEY_ID_SIZE];

/* 
 * S3 request configuration, may be overriden for specific requests
 */
extstore_s3_req_cfg_t s3_req_cfg;

int extstore_create(kvsns_ino_t object)
{
	char fullpath[256];

	build_fullpath(object, fullpath);

	LogDebug(COMPONENT_EXTSTORE, "ino=%llu path=%s", object, fullpath);

	/* perform 0 bytes PUT to create the objet if it doesn't exist or set
	 * its length to 0 bytes in case it does */
	//rc = put_object(&bucket_ctx, fullpath, &s3_req_cfg, NULL, 0);
	return 0;
}

int extstore_attach(kvsns_ino_t *ino, char *objid, int objid_len)
{
	/* XXX Look into another way of adding S3 specific keys in the KVS, the
	 * inode.name entry. Couldn't this call to extstore_attach be the
	 * perfect occastion to add those keys. If that is the case, this would
	 * be way cleaner than modifying the libkvsns code and guarding the code
	 * with KVSNS_S3 defines.
	 */

	LogDebug(COMPONENT_EXTSTORE, "%s ino=%llu objid=%s objid_len=%d",
	       *ino, objid, objid_len);
	return 0;
}

int extstore_init(struct collection_item *cfg_items)
{
	int rc;
	S3Status s3rc;
	struct collection_item *item;

	LogDebug(COMPONENT_EXTSTORE, "initialising s3 store");

	if (cfg_items == NULL)
		return -EINVAL;

	RC_WRAP(kvsal_init, cfg_items);

	/* Allocate the S3 bucket context */
	memset(&bucket_ctx, 0, sizeof(S3BucketContext));

	/* Get config from ini file */
	item = NULL;
	rc = get_config_item("s3", "host",
			      cfg_items, &item);
	if (rc != 0)
		return -rc;
	if (item == NULL)
		return -EINVAL;

	strncpy(host, get_string_config_value(item, NULL),
		S3_MAX_HOSTNAME_SIZE);

	item = NULL;
	rc = get_config_item("s3", "bucket",
			      cfg_items, &item);
	if (rc != 0)
		return -rc;
	if (item == NULL)
		return -EINVAL;
	strncpy(bucket, get_string_config_value(item, NULL),
		S3_MAX_BUCKET_NAME_SIZE);

	item = NULL;
	rc = get_config_item("s3", "access_key",
			      cfg_items, &item);
	if (rc != 0)
		return -rc;
	if (item == NULL)
		return -EINVAL;
	strncpy(access_key, get_string_config_value(item, NULL),
		S3_MAX_ACCESS_KEY_ID_SIZE);

	item = NULL;
	rc = get_config_item("s3", "secret_key",
			      cfg_items, &item);
	if (rc != 0)
		return -rc;
	if (item == NULL)
		return -EINVAL;
	strncpy(secret_key, get_string_config_value(item, NULL),
		S3_MAX_SECRET_ACCESS_KEY_ID_SIZE);

	/* Fill the bucket context */
	bucket_ctx.hostName = host;
	bucket_ctx.bucketName = bucket;
	bucket_ctx.accessKeyId = access_key;
	bucket_ctx.secretAccessKey = secret_key;
	bucket_ctx.authRegion = NULL;
	bucket_ctx.protocol = S3ProtocolHTTP;
	bucket_ctx.uriStyle = S3UriStylePath;

	LogDebug(COMPONENT_EXTSTORE, "bucket=%s host=%s",
			 bucket_ctx.bucketName, bucket_ctx.hostName);

	/* Initialize libs3 */
	s3rc = S3_initialize("kvsns", S3_INIT_ALL, host);
	if (s3rc != S3StatusOK)
		return s3status2posix_error(s3rc);

	/* Initialize S3 request config */
	memset(&s3_req_cfg, 0, sizeof(extstore_s3_req_cfg_t));
	s3_req_cfg.retries = S3_REQ_DEFAULT_RETRIES;
	s3_req_cfg.sleep_interval = S3_REQ_DEFAULT_SLEEP_INTERVAL;
	s3_req_cfg.timeout = S3_REQ_DEFAULT_TIMEOUT;
	s3_req_cfg.log_props = 1;

	rc = test_bucket(&bucket_ctx, &s3_req_cfg);

	multipart_manager_init(&bucket_ctx);
	return rc;
}

int extstore_fini()
{
	LogDebug(COMPONENT_EXTSTORE, "releasing s3 store");

	/* release resources */
	S3_deinitialize();
	multipart_manager_free();

	return 0;
}

int extstore_del(kvsns_ino_t *ino)
{
	LogDebug(COMPONENT_EXTSTORE, "ino=%llu not implemented", *ino)
	return 0;
}

int extstore_read(kvsns_ino_t *ino,
		  off_t offset,
		  size_t buffer_size,
		  void *buffer,
		  bool *end_of_file,
		  struct stat *stat)
{
	LogDebug(COMPONENT_EXTSTORE, "ino=%llu off=%ld bufsize=%lu",
	         *ino, offset, buffer_size);
	return 0;
}

int extstore_write(kvsns_ino_t *ino,
		   off_t offset,
		   size_t buffer_size,
		   void *buffer,
		   bool *fsal_stable,
		   struct stat *stat)
{
	int rc;
	ssize_t bytes_written;
	char fullpath[256];	

	build_fullpath(*ino, fullpath);
	ASSERT(fullpath[0] == '/');

	LogDebug(COMPONENT_EXTSTORE, "ino=%llu off=%ld bufsize=%lu path=%s s3sz=%lu",
		 *ino, offset, buffer_size, fullpath, stat->st_size);

	/* get attr from the store (HEAD object), if size is 0, we can start a multipart */
	rc = extstore_getattr(ino, stat);

	/* TODO: error handling (rc < 0) */
	(void) rc;

	if (stat->st_size == 0) {
		/* we are uploading an object, it may exist already, but anyway
		 * its size being 0 we can overwrite it without problems :-) */

		if (offset == 0) {
			LogDebug(COMPONENT_EXTSTORE, "initiating multipart ino=%llu", *ino);
			/* initiate multipart */
			multipart_inode_init(*ino, fullpath, &s3_req_cfg);
		}
		size_t chunkidx = offset / MULTIPART_CHUNK_SIZE;
		bytes_written = multipart_inode_upload_chunk(*ino, chunkidx, buffer, buffer_size);
	} else {
		/* we are modifying an existing object */
	}

	/* TODO: error handling */

//	if (bytes_written < 0)
//		return bytes_written;

//	rc = extstore_getattr(ino, stat);
//	if (rc != 0)
//		return rc;

	return bytes_written;
}

int extstore_truncate(kvsns_ino_t *ino,
		      off_t filesize,
		      bool on_obj_store,
		      struct stat *stat)
{
	LogDebug(COMPONENT_EXTSTORE, "ino=%llu filesize=%lu not implemented",
	         *ino, filesize);
	return 0;
}

int extstore_getattr(kvsns_ino_t *ino,
		     struct stat *stat)
{
	int rc;
	time_t mtime;
	uint64_t size;
	char fullpath[256];

	LogDebug(COMPONENT_EXTSTORE, "ino=%llu", *ino);

	build_fullpath(*ino, fullpath);
	ASSERT(fullpath[0] == '/');

	/* perform HEAD on S3 object*/
	/* TODO: what happens if the object doesn't exist?
	 * if that's the case, the return value of stats_objects
	 * should be ENOENT, we should treat that case specifically
	 * and zero st_size, st_mtime and st_atime.
	 * (@see `extstore_getattr` in rados).
	 */
	rc = stats_object(&bucket_ctx, fullpath, &s3_req_cfg,
			    &mtime, &size);

	if (rc != 0)
		return rc;

	stat->st_size = size;
	stat->st_mtime = mtime;
	stat->st_atime = mtime;

	return 0;
}

int extstore_open(kvsns_ino_t ino,
		  int flags)
{
	char strflags[1024] = "\0";
	LogDebug(COMPONENT_EXTSTORE, "ino=%d flags=o%o", ino, flags);
	LogDebug(COMPONENT_EXTSTORE, "flags=%s", printf_open_flags(strflags, flags, 1024));
	return 0;
}

int extstore_close(kvsns_ino_t ino)
{
	LogDebug(COMPONENT_EXTSTORE, "ino=%d", ino);
	return 0;
}
