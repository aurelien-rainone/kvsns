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
#include <libs3.h>

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


static S3BucketContext bucket_ctx;
static char host[S3_MAX_HOSTNAME_SIZE];
static char bucket[S3_MAX_BUCKET_NAME_SIZE];
static char access_key[S3_MAX_ACCESS_KEY_ID_SIZE];
static char secret_key[S3_MAX_SECRET_ACCESS_KEY_ID_SIZE];

int extstore_create(kvsns_ino_t object)
{
	printf("%s obj=%llu\n", __func__, object);
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

	/* Initialize libs3 */
	if ((s3rc = S3_initialize("kvsns", S3_INIT_ALL, host)
				  != S3StatusOK)) {
		/* TODO: add libs32fsal_error function */
		rc = 1;
	}
#if 0
	/* Verify credentials */
	rc = rados_create2(&cluster, clustername, user, 0LL);
	if (rc < 0)
		return rc;

	rc = rados_conf_read_file(cluster, ceph_conf);
	if (rc < 0)
		return rc;

	/* Connect to the cluster */
	rc = rados_connect(cluster);
	if (rc < 0)
		return rc;
#endif
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
