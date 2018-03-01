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
#include "../kvsns_utils.h"
#include "../kvsns_internal.h"
#include <kvsns/kvsal.h>
#include <hiredis/hiredis.h>
#include <gmodule.h>
#include "internal.h"
#include "inode_cache.h"
#include "s3_common.h"
#include "mru.h"


void extstore_fill_stats(struct stat *stat, kvsns_ino_t ino, time_t mtime,
			 int isdir, size_t size);

int extstore_create(kvsns_ino_t object)
{
	int rc;
	int fd;
	int isdir;
	char keypath[S3_MAX_KEY_SIZE];
	char cache_path[MAXPATHLEN];

	RC_WRAP(kvsns_get_s3_path, object, S3_MAX_KEY_SIZE, keypath, &isdir);

	build_cache_path(object, cache_path, write_cache_t, MAXPATHLEN);

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%llu key=%s cache=%s",
		 object, keypath, cache_path);

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
		rc = errno;
		LogCrit(KVSNS_COMPONENT_EXTSTORE,
			"Failed creating a write cache inode ino=%lu errno=%d",
			object, rc);
		return -rc;
	}

	/* keep track of the created file */
	g_tree_insert(wino_cache, (gpointer) object, (gpointer)((gintptr) fd));

	return 0;
}

int extstore_attach(kvsns_ino_t *ino, char *objid, int objid_len)
{
	LogDebug(KVSNS_COMPONENT_EXTSTORE, "%s ino=%llu objid=%s objid_len=%d",
	       *ino, objid, objid_len);
	return 0;
}

int extstore_init(struct collection_item *cfg_items)
{
	int rc;
	S3Status s3_status;
	struct collection_item *item;

	LogInfo(KVSNS_COMPONENT_EXTSTORE, "initialising s3 store");

	if (cfg_items == NULL)
		return -EINVAL;

	/* set log level from inifile */
	item = NULL;
	rc = get_config_item("s3", "log_level",
			      cfg_items, &item);
	if (rc != 0)
		return -rc;
	if (item != NULL) {
		char strlvl[64];
		kvsns_log_levels_t lvl;
		strncpy(strlvl, get_string_config_value(item, NULL), 64);
		rc = kvsns_parse_log_level(strlvl, &lvl);
		if (!rc) {
			LogInfo(KVSNS_COMPONENT_EXTSTORE, "setting log level to %s", strlvl);
			kvsns_set_log_level(KVSNS_COMPONENT_EXTSTORE, lvl);
		} else
			LogWarn(KVSNS_COMPONENT_EXTSTORE,
				"Can't parse log level, default unchanged");
	}

	/* read location of data cache directory */
	item = NULL;
	rc = get_config_item("s3", "ino_cache_dir",
			      cfg_items, &item);
	if (rc != 0)
		return -rc;
	if (item == NULL)
		return -EINVAL;
	strncpy(ino_cache_dir, get_string_config_value(item, NULL),
		MAXPATHLEN);

	/* Allocate the s3 bucket context */
	memset(&bucket_ctx, 0, sizeof(S3BucketContext));

	/* Get s3 config from ini file */
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

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "bucket=%s host=%s",
			 bucket_ctx.bucketName, bucket_ctx.hostName);

	/* Initialize libs3 */
	s3_status = S3_initialize("kvsns", S3_INIT_ALL, host);
	if (s3_status != S3StatusOK)
		return s3status2posix_error(s3_status);

	/* Initialize default s3 request config */
	def_s3_req_cfg.retries = S3_REQ_DEFAULT_RETRIES;
	def_s3_req_cfg.timeout = S3_REQ_DEFAULT_TIMEOUT;
	def_s3_req_cfg.sleep_interval = S3_REQ_DEFAULT_SLEEP_INTERVAL;

	item = NULL;
	rc = get_config_item("s3", "req_timeout", cfg_items, &item);
	if (rc != 0)
		return -rc;
	if (item != NULL)
	    def_s3_req_cfg.timeout =
		    get_int_config_value(item, 0, S3_REQ_DEFAULT_TIMEOUT, NULL);

	item = NULL;
	rc = get_config_item("s3", "req_max_retries", cfg_items, &item);
	if (rc != 0)
		return -rc;
	if (item != NULL)
	    def_s3_req_cfg.retries =
		    get_int_config_value(item, 1, S3_REQ_DEFAULT_RETRIES, NULL);

	item = NULL;
	rc = get_config_item("s3", "req_sleep_interval", cfg_items, &item);
	if (rc != 0)
		return -rc;
	if (item != NULL)
	    def_s3_req_cfg.sleep_interval =
		    get_int_config_value(item, 1, S3_REQ_DEFAULT_SLEEP_INTERVAL, NULL);

	/* check we can actually access the bucket */
	rc = test_bucket(&bucket_ctx, &def_s3_req_cfg);
	if (rc < 0) {
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			"Can't access bucket bucket=%s host=%s rc=%d",
			bucket_ctx.bucketName, bucket_ctx.hostName, rc);
		return -rc;
	}

	/* init data caches structures and create cache directory if needed */
	wino_cache = g_tree_new(g_key_cmp_func);
	rino_cache = g_tree_new(g_key_cmp_func);
	memset(&rino_mru, 0, sizeof(struct mru));

	struct stat st = {0};
	if (stat(ino_cache_dir, &st) == -1) {
		LogInfo(KVSNS_COMPONENT_EXTSTORE,
				"Inode cache directory not found, create it dir=%s",
				ino_cache_dir);
		if (mkdir(ino_cache_dir, 0700) < 0) {
			rc = -errno;
			LogCrit(KVSNS_COMPONENT_EXTSTORE,
				"Couldn't create inode cache directory dir=%s rc=%d",
				ino_cache_dir, rc);
			return rc;
		}
	} else {
		LogInfo(KVSNS_COMPONENT_EXTSTORE,
				"Found inode cache directory, empty it dir=%s",
				ino_cache_dir);
		/* remove cached inodes */
		remove_files_in(ino_cache_dir);
	}

	kvsns_init_root(false);
	return rc;
}

int extstore_fini()
{
	LogDebug(KVSNS_COMPONENT_EXTSTORE, "releasing s3 store");

	/* release resources */
	S3_deinitialize();

	/* destroy cache data structures */
	g_tree_destroy(wino_cache);
	wino_cache = NULL;
	g_tree_destroy(rino_cache);
	rino_cache = NULL;
	mru_clear(&rino_mru);

	/* remove cached inodes */
	remove_files_in(ino_cache_dir);
	/* remove cache directory */
	remove(ino_cache_dir);

	return 0;
}

int extstore_del(kvsns_ino_t *ino)
{
	int rc;
	int isdir;
	char keypath[S3_MAX_KEY_SIZE];

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%llu", *ino);

	RC_WRAP(kvsns_get_s3_path, *ino, S3_MAX_KEY_SIZE, keypath, &isdir);

	rc = del_object(&bucket_ctx, keypath, &def_s3_req_cfg);
	if (rc != 0) {
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			"Couldn't delete object ino=%lu key=%s",
			*ino, keypath);
	}
	return rc;
}

int extstore_read(kvsns_ino_t *ino,
		  off_t offset,
		  size_t buffer_size,
		  void *buffer,
		  bool *end_of_file,
		  struct stat *stat)
{
	int rc;
	int fd;
	int isdir;
	time_t mtime;
	uint64_t size;
	ssize_t bytes_read;
	char cache_path[MAXPATHLEN];
	char keypath[S3_MAX_KEY_SIZE];
	build_cache_path(*ino, cache_path, read_cache_t, MAXPATHLEN);

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%llu off=%ld bufsize=%lu",
	         *ino, offset, buffer_size);

	/* Try to obtain a file descriptor to read from */
	gpointer key = g_tree_lookup(rino_cache, (gpointer) *ino);
	if (key == NULL) {

		/* So we don't its file descriptor, but the inode may be cached
		 * locally. Check in the MRU. */
		if (!mru_mark_item(&rino_mru, mru_key_cmp_func, (void*) *ino)) {

			/* File is not cached locally, we must download it.*/
			RC_WRAP(kvsns_get_s3_path, *ino, S3_MAX_KEY_SIZE, keypath, &isdir);

			rc = get_object(&bucket_ctx, keypath, &def_s3_req_cfg,
					cache_path, &mtime, &size);
			if (rc != 0) {
				LogWarn(KVSNS_COMPONENT_EXTSTORE,
					"Can't download s3 object ino=%lu key=%s rc=%d",
					*ino, keypath, rc);
				return rc;
			}

			/* Before appending the inode to the MRU, ensure there's
			 * at least one free entry (i.e maxlen - 1) */
			mru_remove_unused(&rino_mru, rino_mru_maxlen - 1,
					  rino_mru_remove, NULL);

			/* Add the inode to the MRU and immediately mark it */
			mru_append(&rino_mru, (void*) *ino);
			mru_mark_item(&rino_mru, mru_key_cmp_func, (void*) *ino);
		}

		fd = open(cache_path, O_RDONLY);
		if (fd < 0) {
			rc = errno;
			LogCrit(KVSNS_COMPONENT_EXTSTORE,
				"Cached inode should have been found ino=%lu errno=%d path=%s",
				*ino, rc, cache_path);

			return -rc;
		}

		/* keep track of the file descriptor */
		g_tree_insert(rino_cache, (gpointer) *ino, (gpointer)((gintptr) fd));

	} else {
		/* The file descriptor can't be 0 as it's reserved for stdin. */
		fd = (int) ((intptr_t) key);
	}

	bytes_read = pread(fd, buffer, buffer_size, offset);
	*end_of_file = bytes_read == 0;
	if (bytes_read == -1) {
		rc = -errno;
		LogDebug(KVSNS_COMPONENT_EXTSTORE, "error pread, errno=%d", rc);
		return rc;
	}

	return bytes_read;
}

int extstore_write(kvsns_ino_t *ino,
		   off_t offset,
		   size_t buffer_size,
		   void *buffer,
		   bool *fsal_stable,
		   struct stat *stat)
{
	int rc;
	int fd;
	int isdir;
	ssize_t bytes_written;
	char keypath[S3_MAX_KEY_SIZE];
	char cache_path[MAXPATHLEN];

	RC_WRAP(kvsns_get_s3_path, *ino, S3_MAX_KEY_SIZE, keypath, &isdir);
	build_cache_path(*ino, cache_path, write_cache_t, MAXPATHLEN);

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%llu off=%ld bufsize=%lu key=%s s3sz=%lu cache=%s",
		 *ino, offset, buffer_size, keypath, stat->st_size, cache_path);


	/* retrieve file descriptor */
	gpointer key = g_tree_lookup(wino_cache, (gpointer) *ino);
	if (key == NULL) {
		return -ENOENT;
	}
	fd = (int) ((intptr_t) key);
	bytes_written = pwrite(fd, buffer, buffer_size, offset);
	if (bytes_written == -1) {
		rc = -errno;
		LogDebug(KVSNS_COMPONENT_EXTSTORE, "error pwrite, errno=%d", rc);
		return rc;
	}

	return bytes_written;
}

int extstore_truncate(kvsns_ino_t *ino,
		      off_t filesize,
		      bool on_obj_store,
		      struct stat *stat)
{
	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%llu filesize=%lu not implemented",
	         *ino, filesize);
	return 0;
}

int extstore_getattr(kvsns_ino_t *ino,
		     struct stat *stat)
{
	int rc = 0;
	int isdir;
	time_t mtime;
	uint64_t size;
	bool posixified;
	char keypath[S3_MAX_KEY_SIZE];

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%llu", *ino);

	RC_WRAP(kvsns_get_s3_path, *ino, S3_MAX_KEY_SIZE, keypath, &isdir);

	/* check if a cached file descriptor exists for this inode, that would
	 * mean we currently have the file content cached and opened, so its
	 * content (and attrs) are changing, we can spare a remote call in that
	 * case. Plus in case we are uploading a file that didn't exist, s3 will
	 * report it as 'not found' (-ENOENT) until the multipart completes */
	gpointer key = g_tree_lookup(wino_cache, (gpointer) *ino);
	if (key != NULL) {
		/* In that case temporarily report the attrs of the cached file.
		 * As soon as the upload will be completed, s3 will report it
		 * accurately.  */
		struct stat fdsta;
		fstat((int) ((intptr_t) key), &fdsta);
		extstore_fill_stats(&fdsta, *ino, 0, 0, 0);
		stat->st_size = fdsta.st_size;
		stat->st_mtime = fdsta.st_mtime;
		stat->st_atime = fdsta.st_atime;
		return 0;
	}

	/* perform HEAD on s3 object*/
	if (isdir)
		strncat(keypath, "/", S3_MAX_KEY_SIZE);
	rc = get_stats_object(&bucket_ctx, keypath, &def_s3_req_cfg,
			      &mtime, &size, stat, &posixified);
	if (rc != 0)
		return rc;

	if (!posixified) {
		/* "posixify" this s3 object */
		struct stat bufstat;
		extstore_fill_stats(&bufstat, *ino, mtime, isdir, size);
		rc = set_stats_object(&bucket_ctx, keypath, &def_s3_req_cfg,
				      &bufstat);
		if (rc != 0) {
			LogCrit(KVSNS_COMPONENT_EXTSTORE,
				"couldn't set posix attrs key=%s rc=%d ino=%lu",
				keypath, rc, *ino);
			return rc;
		}

		memcpy(stat, &bufstat, sizeof(struct stat));
	}

	return 0;
}

int extstore_setattr(kvsns_ino_t *ino,
		     const struct stat *stat)
{
	int rc = 0;
	int isdir;
	char keypath[S3_MAX_KEY_SIZE];

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%llu", *ino);

	RC_WRAP(kvsns_get_s3_path, *ino, S3_MAX_KEY_SIZE, keypath, &isdir);

	/* do not set attributes of a currently 'write-opened' inode */
	gpointer key = g_tree_lookup(wino_cache, (gpointer) *ino);
	if (key != NULL) {
		return 0;
	}

	rc = set_stats_object(&bucket_ctx, keypath, &def_s3_req_cfg,
			      stat);
	if (rc != 0)
		return rc;

	return 0;
}

void extstore_fill_stats(struct stat *stat, kvsns_ino_t ino, time_t mtime,
			 int isdir, size_t size)
{
	const bool openbar = true;
	const int def_dirmode = 0755;
	const int def_filemode = 0644;
	const int def_dirmode_open = 0777;
	const int def_filemode_open = 0666;
	int dirmode, filemode;
	if (openbar) {
		dirmode = def_dirmode_open;	
		filemode = def_filemode_open;
	} else {
		dirmode = def_dirmode;
		filemode = def_filemode;
	}

	memset(stat, 0, sizeof(struct stat));
	stat->st_uid = 0;
	stat->st_gid = 0;
	stat->st_ino = ino;

	stat->st_atim.tv_sec = mtime;
	stat->st_atim.tv_nsec = 0; /* time_t only hold seconds */

	stat->st_mtim.tv_sec = stat->st_atim.tv_sec;
	stat->st_mtim.tv_nsec = stat->st_atim.tv_nsec;

	stat->st_ctim.tv_sec = stat->st_atim.tv_sec;
	stat->st_ctim.tv_nsec = stat->st_atim.tv_nsec;

	if (isdir) {
		stat->st_mode = S_IFDIR|dirmode;
		stat->st_nlink = 2;
		stat->st_size = 0;
	} else {
		stat->st_mode = S_IFREG|filemode;
		stat->st_nlink = 1;
		stat->st_size = size;
	}
}

int extstore_lookup(kvsns_ino_t *parent, char *name,
		    struct stat *stat, kvsns_ino_t *ino)
{
	int rc;
	int isdir;
	time_t mtime;
	uint64_t size;
	bool posixified;
	char dirpath[S3_MAX_KEY_SIZE] = "";
	char keypath[S3_MAX_KEY_SIZE] = "";

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "parent=%llu name=%s", *parent, name);

	RC_WRAP(kvsns_get_s3_path, *parent, S3_MAX_KEY_SIZE, dirpath, &isdir);

	snprintf(keypath, S3_MAX_KEY_SIZE, "%s%s", dirpath, name);
	if (!kvsns_get_s3_inode(keypath, false, ino, &isdir)) {
		/* this file already has an inode */
		return extstore_getattr(ino, stat);
	}

	/* Either this file doesn't exist or it's the first time we see it.
	 * It may also be a directory, in which case we'll need to query the
	 * same s3 key with a trailing slash */

	/* file lookup */
	isdir = 0;
	rc = get_stats_object(&bucket_ctx, keypath, &def_s3_req_cfg,
			      &mtime, &size, stat, &posixified);

	if (rc == -ENOENT) {
		/* directory lookup (append a slash to s3 key)*/
		isdir = 1;
		strncat(keypath, "/", S3_MAX_KEY_SIZE);
		rc = get_stats_object(&bucket_ctx, keypath, &def_s3_req_cfg,
				      &mtime, &size, stat, &posixified);
		if (rc == -ENOENT)
			return rc;
		if (rc)
			LogWarn(KVSNS_COMPONENT_EXTSTORE,
				"directory lookup failed rc=%d parent=%llu key=%s",
				rc, *parent, keypath);
			return rc;

		/* remove the trailing slash we just added */
		keypath[strlen(keypath) - 1] = '\0';

	} else if (rc)
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			"file lookup failed rc=%d parent=%llu key=%s",
			rc, *parent, keypath);

	/* let's get this file an inode number */
	RC_WRAP(kvsns_add_s3_path, keypath, isdir, ino);
	return rc;
}

int extstore_open(kvsns_ino_t ino, int flags)
{
	char strflags[1024] = "";

	/* check if an entry has been added in the write cache */
	gpointer key = g_tree_lookup(wino_cache, (gpointer) ino);
	if (key == NULL) {
		/* didn't pass by create before open, we probably want to read
		 * that file */
		LogDebug(KVSNS_COMPONENT_EXTSTORE,
			"opening in order to read? ino=%d flags=o%o flagsstr=%s",
			ino, flags, printf_open_flags(strflags, flags, 1024));
		return -ENOENT;
	} 

	/* found a file descriptor */
	LogDebug(KVSNS_COMPONENT_EXTSTORE,
		"opening in order to write/upload? ino=%d flags=o%o flagsstr=%s",
		ino, flags, printf_open_flags(strflags, flags, 1024));

	return 0;
}

int extstore_close(kvsns_ino_t ino)
{
	int rc;
	int isdir;
	char keypath[S3_MAX_KEY_SIZE];

	RC_WRAP(kvsns_get_s3_path, ino, S3_MAX_KEY_SIZE, keypath, &isdir);

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%d key=%s", ino, keypath);

	/* an inode should not be located in both read and write caches */
	rc = wino_close(ino);
	if (rc)
		return rc;
	rc = rino_close(ino);
	if (rc)
		return rc;

	return 0;
}

