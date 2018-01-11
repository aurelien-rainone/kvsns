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

/* extstore.c
 * KVSNS: expose an S3 object store. Use Redis to retrieve object full paths.
 */


#include <kvsns/extstore.h>
#include <kvsns/kvsal.h>
#include <hiredis/hiredis.h>
#include "internal.h"
#include "s3_methods.h"

#define RC_WRAP(__function, ...) ({\
	int __rc = __function(__VA_ARGS__);\
	if (__rc != 0)	\
		return __rc; })

#define RC_WRAP_LABEL(__rc, __label, __function, ...) ({\
	__rc = __function(__VA_ARGS__);\
	if (__rc != 0)        \
		goto __label; })


#define S3_MAX_ACCESS_KEY_ID_SIZE 256		/* not sure about this */
#define S3_MAX_SECRET_ACCESS_KEY_ID_SIZE 256	/* not sure about this */

/* Default values for S3 requests configuration */
#define S3_REQ_DEFAULT_RETRIES 3
#define S3_REQ_DEFAULT_SLEEP_INTERVAL 1
#define S3_REQ_DEFAULT_TIMEOUT 10000


static S3BucketContext bucket_ctx;
static char host[S3_MAX_HOSTNAME_SIZE];
static char bucket[S3_MAX_BUCKET_NAME_SIZE];
static char access_key[S3_MAX_ACCESS_KEY_ID_SIZE];
static char secret_key[S3_MAX_SECRET_ACCESS_KEY_ID_SIZE];

/* 
 * S3 request configuration, may be overriden for specific requests
 */
extstore_s3_req_cfg_t s3_req_cfg;

/**
 * Prepends t into s. Assumes s has enough space allocated
 * for the combined string.
 */
void prepend(char* s, const char* t)
{
	size_t i, len = strlen(t);
	memmove(s + len, s, strlen(s) + 1);
	for (i = 0; i < len; ++i)
		s[i] = t[i];
}

/**
 * Build path of S3 Object and return object directory and filename.
 *
 * @param object - object inode.
 * @param obj_dir - [OUT] full S3 directory path, with trailing slash.
 * @param obj_fname - [OUT] S3 object filename, empty for a directory.
 *
 * @return 0 if successful, a negative "-errno" value in case of failure
 */
static int build_objpath(kvsns_ino_t object, char *obj_dir, char *obj_fname)
{
	char k[KLEN];
	char v[VLEN];
	kvsns_ino_t ino = object;
	kvsns_ino_t root_ino = 0LL;
	struct stat stat;

	/* get root inode number */
	RC_WRAP(kvsal_get_char, "KVSNS_PARENT_INODE", v);
	sscanf(v, "%llu|", &root_ino);

	/* init return values */
	strcpy(obj_dir, "/");
	obj_fname[0] = '\0';

	while (ino != root_ino) {
	
		/* current inode name */
		snprintf(k, KLEN, "%llu.name", ino);
		RC_WRAP(kvsal_get_char, k, v);

		snprintf(k, KLEN, "%llu.stat", ino);
		RC_WRAP(kvsal_get_stat, k, &stat);
		if (stat.st_mode & S_IFDIR) {
			prepend(obj_dir, v);
			prepend(obj_dir, "/");
		} else {
			strcpy(obj_fname, v);
		}

		/* get parent inode */
		snprintf(k, KLEN, "%llu.parentdir", ino);
		RC_WRAP(kvsal_get_char, k, v);
		sscanf(v, "%llu|", &ino);
	};

	printf("%s obj=%llu dir=%s fname=%s\n",
	       __func__,
	       object, obj_dir, obj_fname);

	return 0;	
}

/**
 * Build full path of S3 Object.
 *
 * @param object - object inode.
 * @param obj_path - [OUT] full S3 path, ends with slash for directories.
 *
 * @return 0 if successful, a negative "-errno" value in case of failure
 */
int build_fullpath(kvsns_ino_t object, char *obj_path)
{
	char fname[VLEN];
	RC_WRAP(build_objpath, object, obj_path, fname);
	strcat(obj_path, fname);
	return 0;
}

int extstore_create(kvsns_ino_t object)
{
	S3Status s3rc;

	printf("%s obj=%llu\n", __func__, object);
	char fullpath[256];

	build_fullpath(object, fullpath);

	/* perform PUT */
	if ((s3rc = put_object(&bucket_ctx, fullpath, &s3_req_cfg, NULL, 0)
				  != S3StatusOK)) {
		return s3status2posix_error(s3rc);
	}

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

	printf("%s ino=%llu objid=%s objid_len=%d\n",
	       __func__,
	       *ino, objid, objid_len);

	return 0;
}

int extstore_init(struct collection_item *cfg_items)
{
	struct collection_item *item;
	S3Status s3rc;
	int rc;

	printf("%s\n", __func__);


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

	/* Initialize libs3 */
	if ((s3rc = S3_initialize("kvsns", S3_INIT_ALL, host)
				  != S3StatusOK)) {
		return s3status2posix_error(s3rc);
	}

	/* Initialize S3 request config */
	memset(&s3_req_cfg, 0, sizeof(extstore_s3_req_cfg_t));
	s3_req_cfg.retries = S3_REQ_DEFAULT_RETRIES;
	s3_req_cfg.sleep_interval = S3_REQ_DEFAULT_SLEEP_INTERVAL;
	s3_req_cfg.timeout = S3_REQ_DEFAULT_TIMEOUT;
	s3_req_cfg.log_props = 1;

	/* TODO: Add S3_WRAP macro that wraps S3Status check and converts
	 * error code to posix if necessary */
	if ((s3rc = test_bucket(&bucket_ctx, &s3_req_cfg)) != S3StatusOK) {
		return s3status2posix_error(s3rc);
	}

	return 0;
}

int extstore_fini()
{
	printf("%s\n", __func__);

	/* release libs3 */
	S3_deinitialize();
	return 0;
}

int extstore_del(kvsns_ino_t *ino)
{
	printf("%s ino=%llu\n", __func__, *ino);
	return 0;
}

int extstore_read(kvsns_ino_t *ino,
		  off_t offset,
		  size_t buffer_size,
		  void *buffer,
		  bool *end_of_file,
		  struct stat *stat)
{
	printf("%s ino=%llu off=%ld bufsize=%lu\n",
	       __func__,
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
	printf("%s ino=%llu off=%ld bufsize=%lu\n",
	       __func__,
	       *ino, offset, buffer_size);
	return  0;
}


int extstore_truncate(kvsns_ino_t *ino,
		      off_t filesize,
		      bool on_obj_store,
		      struct stat *stat)
{
	printf("%s ino=%llu filesize=%ld on_obj_store=%d\n",
	       __func__,
	       *ino, filesize, on_obj_store);
	return 0;
}

int extstore_getattr(kvsns_ino_t *ino,
		     struct stat *stat)
{
	printf("%s ino=%llu\n", __func__, *ino);
	return 0;
}
