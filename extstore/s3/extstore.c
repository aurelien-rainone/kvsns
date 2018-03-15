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
#include "mru.h"


int extstore_create(kvsns_ino_t object)
{
	int rc;
	int fd;
	char s3_path[S3_MAX_KEY_SIZE];
	char cache_path[MAXPATHLEN];

	fullpath_from_inode(object, S3_MAX_KEY_SIZE, s3_path);
	build_cache_path(object, cache_path, write_cache_t, MAXPATHLEN);

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%llu s3_path=%s cache=%s", object, s3_path, cache_path);

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
	/* XXX Look into another way of adding s3 specific keys in the KVS, the
	 * inode.name entry. Couldn't this call to extstore_attach be the
	 * perfect occastion to add those keys. If that is the case, this would
	 * be way cleaner than modifying the libkvsns code and guarding the code
	 * with KVSNS_S3 defines.
	 */

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

	RC_WRAP(kvsal_init, cfg_items);

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
	def_s3_req_cfg.upload_nthreads = S3_NUM_THREADS_UPLOAD;

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

	item = NULL;
	rc = get_config_item("s3", "upload_numthreads", cfg_items, &item);
	if (rc != 0)
		return -rc;
	if (item != NULL)
	    def_s3_req_cfg.upload_nthreads =
		    get_int_config_value(item, 1, S3_NUM_THREADS_UPLOAD, NULL);

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
	char s3_path[S3_MAX_KEY_SIZE];

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%llu", *ino);

	fullpath_from_inode(*ino, S3_MAX_KEY_SIZE, s3_path);

	rc = del_object(&bucket_ctx, s3_path, &def_s3_req_cfg);
	if (rc != 0) {
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			"Couldn't delete object ino=%lu s3key=%s",
			*ino, s3_path);
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
	time_t mtime;
	uint64_t size;
	ssize_t bytes_read;
	char cache_path[MAXPATHLEN];
	char s3_path[S3_MAX_KEY_SIZE];
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
			fullpath_from_inode(*ino, S3_MAX_KEY_SIZE, s3_path);
			rc = get_object(&bucket_ctx, s3_path, &def_s3_req_cfg,
					cache_path, &mtime, &size);
			if (rc != 0) {
				LogWarn(KVSNS_COMPONENT_EXTSTORE,
					"Can't download s3 object ino=%lu s3_path=%s rc=%d",
					*ino, s3_path, rc);
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
	ssize_t bytes_written;
	char s3_path[S3_MAX_KEY_SIZE];
	char cache_path[MAXPATHLEN];

	fullpath_from_inode(*ino, S3_MAX_KEY_SIZE, s3_path);
	build_cache_path(*ino, cache_path, write_cache_t, MAXPATHLEN);

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%llu off=%ld bufsize=%lu s3key=%s s3sz=%lu cache=%s",
		 *ino, offset, buffer_size, s3_path, stat->st_size, cache_path);


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
	int rc;
	time_t mtime;
	uint64_t size;
	char s3_path[S3_MAX_KEY_SIZE];

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%llu", *ino);

	fullpath_from_inode(*ino, S3_MAX_KEY_SIZE, s3_path);
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
	rc = get_stats_object(&bucket_ctx, s3_path, &def_s3_req_cfg,
			      &mtime, &size, NULL, NULL);

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
		LogDebug(KVSNS_COMPONENT_EXTSTORE,
			"opening in order to read? ino=%d flags=o%o flagsstr=%s", ino, flags,
			 printf_open_flags(strflags, flags, 1024));
		return -ENOENT;
	} else {

		/* found a file descriptor */
		LogDebug(KVSNS_COMPONENT_EXTSTORE,
			"opening in order to write/upload? ino=%d flags=o%o flagsstr=%s", ino, flags,
			 printf_open_flags(strflags, flags, 1024));
	}

	return 0;
}

int extstore_close(kvsns_ino_t ino)
{
	int rc;
	char s3_path[S3_MAX_KEY_SIZE];

	fullpath_from_inode(ino, S3_MAX_KEY_SIZE, s3_path);

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%d s3key=%s", ino, s3_path);

	/* an inode should not be located in both read and write caches */
	rc = wino_close(ino);
	if (rc)
		return rc;
	rc = rino_close(ino);
	if (rc)
		return rc;

	return 0;
}

int fill_stats(struct stat *stat, kvsns_ino_t ino,
	       kvsns_cred_t *cred, time_t mtime,
	       int isdir, size_t size)
{
#define DEFAULT_DIRMODE 0755
#define DEFAULT_FILEMODE 0644
#define OPENBAR_DIRMODE 0777
#define OPENBAR_FILEMODE 0666

	int mode;
	struct timeval t;
	const bool openbar = true;

	if (!stat || !cred)
		return -EINVAL;

	memset(stat, 0, sizeof(struct stat));

	stat->st_uid = cred->uid;
	stat->st_gid = cred->gid;
	stat->st_ino = ino;

	if (mtime) {
		/* time_t only hold seconds */
		stat->st_mtim.tv_sec = mtime;
		stat->st_mtim.tv_nsec = 0;
	} else {
		if (gettimeofday(&t, NULL) != 0)
			return -errno;
		stat->st_mtim.tv_sec = t.tv_sec;
		stat->st_mtim.tv_nsec = 1000 * t.tv_usec;
	}

	stat->st_atim.tv_sec = stat->st_mtim.tv_sec;
	stat->st_atim.tv_nsec = stat->st_mtim.tv_nsec;

	stat->st_ctim.tv_sec = stat->st_mtim.tv_sec;
	stat->st_ctim.tv_nsec = stat->st_mtim.tv_nsec;

	if (isdir) {
		mode = openbar? OPENBAR_DIRMODE: DEFAULT_DIRMODE;
		stat->st_mode = S_IFDIR|mode;
		stat->st_nlink = 2;
		stat->st_size = 0;
	} else {
		mode = openbar? OPENBAR_FILEMODE: DEFAULT_FILEMODE;
		stat->st_mode = S_IFREG|mode;
		stat->st_nlink = 1;
		stat->st_size = size;
	}
	return 0;
}

int extstore_readdir(kvsns_cred_t *cred, kvsns_ino_t ino,
		     off_t offset, kvsns_dentry_t *dirent,
		     int *size)
{
	int index;
	time_t mtime;
	uint64_t filelen;
	kvsns_ino_t new_entry;
	struct stat stat;
	bool isdir;
	int rc;
	const int maxentries = *size;
	char s3_path[S3_MAX_KEY_SIZE];

	if (!cred || !dirent || !size)
		return -EINVAL;

	fullpath_from_inode(ino, S3_MAX_KEY_SIZE, s3_path);
	rc = list_bucket(&bucket_ctx, s3_path, &def_s3_req_cfg, dirent, size);
	if (rc) {
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			"couldn't list bucket content rc=%d key=%s",
			rc, s3_path);
		return rc;
	}

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%d key=%s", ino, s3_path);

	for (index = 0; index < maxentries; index++) {

		/* If name is NULL, that's the end of the list */
		if (dirent[index].name[0] == '\0')
			break;

		isdir = dirent[index].stats.st_mode == KVSNS_DIR;

		if (isdir) {
			/* we do not persist nothing for directories, directory
			 * attributes are created and managed at runtime only*/
			fill_stats(&stat, 0, cred, 0, isdir, 0);
		} else {
			char s3key[S3_MAX_KEY_SIZE];
			bool has_posix_stats;

			format_s3_key(s3_path, dirent[index].name,
				      isdir, S3_MAX_KEY_SIZE, s3key);
			rc = get_stats_object(&bucket_ctx, s3key, &def_s3_req_cfg,
					      &mtime, &filelen, &stat, &has_posix_stats);
			if (rc) {
				LogCrit(KVSNS_COMPONENT_EXTSTORE,
					"couldn't get stats on s3 key=%s rc=%d",
					s3key, rc
					);
				return rc;
			}

			if (has_posix_stats) {
				/* complete posix stats */
				stat.st_nlink = 2;
				stat.st_size = filelen;
			} else {
				fill_stats(&stat, 0, cred, mtime, isdir, filelen);
				rc = set_stats_object(&bucket_ctx, s3key,
						      &def_s3_req_cfg, &stat);
				if (rc) {
					LogCrit(KVSNS_COMPONENT_EXTSTORE,
						"couldn't set posix stats on s3 key=%s rc=%d",
						s3key, rc
						);
					return rc;
				}
			}
		}

		rc = kvsns_create_entry2(&ino, dirent[index].name,
					 &new_entry, &stat);
		if (rc == -EEXIST) {
			LogFatal(KVSNS_COMPONENT_KVSNS,
				 "entry already exists dirino=%llu key=%s name=%s ino=%llu",
				 ino, dirent[index].name, new_entry);
		}
	}

	return 0;
}

int extstore_mkdir(kvsns_ino_t *parent, const char *name, const struct stat *stat)
{
	int rc;
	char parentkey[S3_MAX_KEY_SIZE];
	char key[S3_MAX_KEY_SIZE];

	fullpath_from_inode(*parent, S3_MAX_KEY_SIZE, parentkey);

	format_s3_key(parentkey, name, true, S3_MAX_KEY_SIZE, key);
	rc = put_object(&bucket_ctx, key, &def_s3_req_cfg, NULL);
	if (rc) {
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			"error creating empty `dir` file key=%s rc=%d",
			key, rc);
	}
	return rc;
}
