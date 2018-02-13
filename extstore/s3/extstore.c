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
#include <gmodule.h>
#include "internal.h"
#include "s3_common.h"


/* s3 bucket configuration */
static S3BucketContext bucket_ctx;
static char host[S3_MAX_HOSTNAME_SIZE];
static char bucket[S3_MAX_BUCKET_NAME_SIZE];
static char access_key[S3_MAX_ACCESS_KEY_ID_SIZE];
static char secret_key[S3_MAX_SECRET_ACCESS_KEY_ID_SIZE];

/* Inode cache management is split in 2 non intersecting caches, one for the
 * downloaded inodes and the other for the inodes to upload.
 */

/* Inode cache directory (read and write). Cached files are named after the
 * inode number they represent, preceded by the letter 'w' or 'r', depending on
 * the type of inode cache they are in */
char ino_cache_dir[MAXPATHLEN];

/* `wino_cache`, for 'written inode cache', is only used to upload
 * new files to s3. After the creation of a new inode and until the reception of
 * close, all successive write operations on this inode are performed on the
 * local filesystem and thus do not involve networking. It's only when NFS
 * requests the file closure that we start transfering the file to stable
 * storage. When the transfer is complete the locally cached inode becomes
 * useless and can safely be deleted.
 *
 * wino_cache is a tree in which keys are inodes and values are file descriptors
 * of the cached files, on which `pwrite` can be called */
GTree *wino_cache;

/*
 * s3 request configuration, may be overriden for specific requests
 */
extstore_s3_req_cfg_t s3_req_cfg;

int build_s3_object_path(kvsns_ino_t object, char *obj_dir, char *obj_fname);

int extstore_create(kvsns_ino_t object)
{
	int rc;
	int fd;
	char s3_path[S3_MAX_KEY_SIZE];
	char cache_path[MAXPATHLEN];

	build_s3_path(object, s3_path, S3_MAX_KEY_SIZE);
	build_cache_path(object, cache_path, write_cache_t, MAXPATHLEN);

	LogDebug(COMPONENT_EXTSTORE, "ino=%llu path=%s cache=%s", object, s3_path, cache_path);

	/* for safety, remove the cache file and start anew */
	rc = remove(cache_path);
	if (rc < 0) {
		rc = -errno;
		if (rc != -ENOENT) {
			/* we have a real error */
			return rc;
		}
		/* reset return code */
		rc = 0;
	}

	/* create the cache entry */
	fd = creat(cache_path, O_RDONLY);
	if (fd < 0) {
		rc = -errno;
		return rc;
	}

	/* keep track of the created file */
	g_tree_insert(wino_cache, (gpointer) object, (gpointer)((gintptr) fd));

	return 0;
}

int extstore_attach(kvsns_ino_t *ino, char *objid, int objid_len)
{
	/* XXX Look into another way of adding s3 specific keys in the KVS, the
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
	S3Status s3_status;
	struct collection_item *item;

	LogDebug(COMPONENT_EXTSTORE, "initialising s3 store");

	if (cfg_items == NULL)
		return -EINVAL;

	RC_WRAP(kvsal_init, cfg_items);

	/* read location of data cache directory */
	rc = get_config_item("s3", "data_ino_cache_dir",
			      cfg_items, &item);
	if (rc != 0)
		return -rc;
	if (item == NULL)
		return -EINVAL;
	strncpy(ino_cache_dir, get_string_config_value(item, NULL),
		MAXPATHLEN);

	/* Allocate the s3 bucket context */
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
	s3_status = S3_initialize("kvsns", S3_INIT_ALL, host);
	if (s3_status != S3StatusOK)
		return s3status2posix_error(s3_status);

	/* Initialize s3 request config */
	memset(&s3_req_cfg, 0, sizeof(extstore_s3_req_cfg_t));
	s3_req_cfg.retries = S3_REQ_DEFAULT_RETRIES;
	s3_req_cfg.sleep_interval = S3_REQ_DEFAULT_SLEEP_INTERVAL;
	s3_req_cfg.timeout = S3_REQ_DEFAULT_TIMEOUT;
	s3_req_cfg.log_props = 1;

	/* check we can actually access the bucket */
	rc = test_bucket(&bucket_ctx, &s3_req_cfg);
	if (rc < 0) {
		LogWarn(COMPONENT_EXTSTORE,
			"Can't access bucket bucket=%s host=%s rc=%d",
			bucket_ctx.bucketName, bucket_ctx.hostName, rc);
		return -rc;
	}

	/* init data caches structures and create cache directory if needed */
	wino_cache = g_tree_new(key_cmp_func);

	struct stat st = {0};
	if (stat(ino_cache_dir, &st) == -1) {

		if (mkdir(ino_cache_dir, 0700) < 0) {
			rc = -errno;
			LogCrit(COMPONENT_EXTSTORE,
				"Can't create data cache directory dir=%s rc=%d",
				ino_cache_dir, rc);
			return rc;
		}
	}

	return rc;
}

int extstore_fini()
{
	LogDebug(COMPONENT_EXTSTORE, "releasing s3 store");

	/* release resources */
	S3_deinitialize();

	/* destroy cache data structures */
	g_tree_destroy(wino_cache);
	wino_cache = NULL;

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

#if USE_DATACACHE

int extstore_write(kvsns_ino_t *ino,
		   off_t offset,
		   size_t buffer_size,
		   void *buffer,
		   bool *fsal_stable,
		   struct stat *stat)
{
	int rc;
	ssize_t bytes_written;
	char s3_path[S3_MAX_KEY_SIZE];
	char cache_path[MAXPATHLEN];

	build_s3_path(*ino, s3_path, S3_MAX_KEY_SIZE);
	build_cache_path(*ino, cache_path, write_cache_t, MAXPATHLEN);

	LogDebug(COMPONENT_EXTSTORE, "ino=%llu off=%ld bufsize=%lu s3key=%s s3sz=%lu cache=%s",
		 *ino, offset, buffer_size, s3_path, stat->st_size, cache_path);


	/* retrieve file descriptor */
	gpointer key = g_tree_lookup(wino_cache, (gpointer) *ino);
	if (key == NULL) {
		return -ENOENT;
	}
	bytes_written = pwrite((int) ((intptr_t) key), buffer, buffer_size, offset);
	if (bytes_written == -1) {
		rc = -errno;
		LogDebug(COMPONENT_EXTSTORE, "error pwrite, errno=%d", rc);
		return rc;
	}

	return bytes_written;
}

#else

int extstore_write(kvsns_ino_t *ino,
		   off_t offset,
		   size_t buffer_size,
		   void *buffer,
		   bool *fsal_stable,
		   struct stat *stat)
{
	int rc;
	ssize_t bytes_written;
	char s3_path[S3_MAX_KEY_SIZE];	

	build_s3_path(*ino, s3_path, S3_MAX_KEY_SIZE);
	ASSERT(s3_path[0] == '/');

	LogDebug(COMPONENT_EXTSTORE, "ino=%llu off=%ld bufsize=%lu path=%s s3sz=%lu",
		 *ino, offset, buffer_size, s3_path, stat->st_size);

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
			multipart_inode_init(*ino, s3_path, &s3_req_cfg);
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

#endif


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
	char s3_path[S3_MAX_KEY_SIZE];

	LogDebug(COMPONENT_EXTSTORE, "ino=%llu", *ino);

	build_s3_path(*ino, s3_path, S3_MAX_KEY_SIZE);
	ASSERT(s3_path[0] == '/');

	/* check if a cached file descriptor exists for this inode, that would
	 * mean we currently have the file content cached and opened, so its
	 * content (and attrs) are changing, we can spare a remote call in that
	 * case. Plus in case we are uploading a file that didn't exist, s3 will
	 * report it as 'not found' (-ENOENT) until the multipart completes */
	gpointer key = g_tree_lookup(wino_cache, (gpointer) *ino);
	if (key != NULL) {
		/* In that case temporarily report the attrs of the cached file.
		 * As soon as the upload will be completed, s3 will report it
		 * accurrately.
		 */
		struct stat fdsta;
		fstat((int) ((intptr_t) key), &fdsta);
		stat->st_size = fdsta.st_size;
		stat->st_mtime = fdsta.st_mtime;
		stat->st_atime = fdsta.st_atime;
		return 0;
	}

	/* perform HEAD on s3 object*/
	rc = stats_object(&bucket_ctx, s3_path, &s3_req_cfg,
			    &mtime, &size);

	if (rc != 0) {
		return rc;
	}

	stat->st_size = size;
	stat->st_mtime = mtime;
	stat->st_atime = mtime;

	return 0;
}

int extstore_open(kvsns_ino_t ino,
		  int flags)
{
	char strflags[1024] = "";

	/* check if an entry has been added in the write cache */
	gpointer key = g_tree_lookup(wino_cache, (gpointer) ino);
	if (key == NULL) {
		/* didn't pass by create before open, we probably want to read
		 * that file */
		LogDebug(COMPONENT_EXTSTORE,
			"opening in order to read? ino=%d flags=o%o flagsstr=%s", ino, flags,
			 printf_open_flags(strflags, flags, 1024));
		return -ENOENT;
	} else {

		/* found a file descriptor */
		LogDebug(COMPONENT_EXTSTORE,
			"opening in order to write/upload? ino=%d flags=o%o flagsstr=%s", ino, flags,
			 printf_open_flags(strflags, flags, 1024));
	}

	return 0;
}

int extstore_close(kvsns_ino_t ino)
{
	int rc;
	int fd;
	char s3_path[S3_MAX_KEY_SIZE];
	char write_cache_path[MAXPATHLEN];

	build_s3_path(ino, s3_path, S3_MAX_KEY_SIZE);
	build_cache_path(ino, write_cache_path, write_cache_t, MAXPATHLEN);

	LogDebug(COMPONENT_EXTSTORE,
		 "ino=%d s3key=%s",
		 ino, s3_path);

	/* was this file in the write cache? */
	gpointer key = g_tree_lookup(wino_cache, (gpointer) ino);
	if (key == NULL) {
		return -ENOENT;
	} else {
		/* this file is in the write cache, close it */
		fd = (int) ((intptr_t) key);
		rc = close(fd);
		if (rc == -1) {
			rc = -errno;
			LogDebug(COMPONENT_EXTSTORE,
				 "error closing file descriptor, fd=%d errno=%d",
				 fd, rc);
			goto cleanup_write_cache;
		}

		/* upload file */
		rc = put_object(&bucket_ctx, s3_path, &s3_req_cfg, write_cache_path);
		if (rc != 0) {
			LogWarn(COMPONENT_EXTSTORE,
				 "couldn't upload file ino=%d s3key=%s fd=%d",
				 ino, s3_path, fd);
			return rc;
		}

cleanup_write_cache:
		/* remove file descriptor from tree */
		g_tree_remove(wino_cache, (gpointer) ino);
	}

	return rc;
}

/**
 * Build path of s3 object and return object directory and filename.
 *
 * @param object - object inode.
 * @param obj_dir - [OUT] full s3 directory path.
 * @param obj_fname - [OUT] s3 object filename, empty for a directory.
 *
 * @note Returned directory path doesn't start with a '/' as libs3 requires
 * object keys to be formatted in this way. The bucket root is an empty string.
 * However directory paths are returned with a trailing '/', this is a s3
 * requirement.
 *
 * @return 0 if successful, a negative "-errno" value in case of failure
 */
int build_s3_object_path(kvsns_ino_t object, char *obj_dir, char *obj_fname)
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
	obj_dir[0] = '\0';
	obj_fname[0] = '\0';

	while (ino != root_ino) {

		/* current inode name */
		snprintf(k, KLEN, "%llu.name", ino);
		RC_WRAP(kvsal_get_char, k, v);

		snprintf(k, KLEN, "%llu.stat", ino);
		RC_WRAP(kvsal_get_stat, k, &stat);
		if (stat.st_mode & S_IFDIR) {
			prepend(obj_dir, "/");
			prepend(obj_dir, v);
		} else {
			strcpy(obj_fname, v);
		}

		/* get parent inode */
		snprintf(k, KLEN, "%llu.parentdir", ino);
		RC_WRAP(kvsal_get_char, k, v);
		sscanf(v, "%llu|", &ino);
	};

	return 0;
}

int build_s3_path(kvsns_ino_t object, char *obj_path, size_t pathlen)
{
	char fname[VLEN];
	RC_WRAP(build_s3_object_path, object, obj_path, fname);
	strcat(obj_path, fname);
	return 0;
}

int build_cache_path(kvsns_ino_t object,
		     char *data_cache_path,
		     cache_t cache_type,
		     size_t pathlen)
{
	return snprintf(data_cache_path, pathlen,
			"%s/%c%llu",
			ino_cache_dir,
			(cache_type == read_cache_t)? 'r':'w',
			(unsigned long long)object);
}
