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

#include <gmodule.h>
#include "../kvsns_utils.h"
#include "../kvsns_internal.h"
#include "internal.h"
#include "s3_common.h"
#include "fd_cache.h"


/* global variables initialization */
char ino_cache_dir[MAXPATHLEN] = "";
GTree *wfd_cache = NULL;
GTree *rfd_cache = NULL;
struct mru rfd_mru = {};
const size_t rfd_mru_maxlen = 10;

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

int wfd_close(kvsns_ino_t ino)
{
	int rc, fd;
	int isdir;
	gpointer wkey;
	char s3_path[S3_MAX_KEY_SIZE];
	char write_cache_path[MAXPATHLEN];

	wkey = g_tree_lookup(wfd_cache, (gpointer) ino);
	if (!wkey)
		return 0;

	fd = (int) ((intptr_t) wkey);
	rc = close(fd);
	if (rc == -1) {
		rc = -errno;
		LogCrit(KVSNS_COMPONENT_EXTSTORE,
			 "error closing fd=%d errno=%d",
			 fd, rc);
		goto remove_fd;
	}

	RC_WRAP(inocache_get_path, ino, S3_MAX_KEY_SIZE, s3_path, &isdir, NULL);
	build_cache_path(ino, write_cache_path, write_cache_t, MAXPATHLEN);

	/* override default s3 request config */
	extstore_s3_req_cfg_t put_req_cfg;
	memcpy(&put_req_cfg, &def_s3_req_cfg, sizeof(put_req_cfg));
	put_req_cfg.retries = 3;
	put_req_cfg.sleep_interval = 1;
	/* for multipart, it's the timeout for the transfer of one part, it's
	 * reset after each part */
	put_req_cfg.timeout = 5 * 60 * 1000; /* 5Min*/

	/* transfer file to stable storage */
	rc = put_object(&bucket_ctx, s3_path, &put_req_cfg, write_cache_path);
	if (rc != 0) {		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			 "Couldn't upload file ino=%d s3key=%s fd=%d",
			 ino, s3_path, fd);
	}

remove_fd:

	g_tree_remove(wfd_cache, (gpointer) ino);
	if (remove(write_cache_path)) {
		LogWarn(KVSNS_COMPONENT_EXTSTORE, "Couldn't remove cached inode path=%s",
				write_cache_path);
	}
	return rc;
}

int rfd_close(kvsns_ino_t ino)
{
	int rc, fd;
	gpointer rkey;

	rkey = g_tree_lookup(rfd_cache, (gpointer) ino);
	if (!rkey)
		return 0;

	fd = (int) ((intptr_t) rkey);
	rc = close(fd);
	if (rc == -1) {
		rc = -errno;
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			 "error closing fd=%d errno=%d",
			 fd, rc);
	}

	g_tree_remove(rfd_cache, (gpointer) ino);
	return rc;
}

void rfd_mru_remove (void *item, void *data)
{
	kvsns_ino_t ino = (kvsns_ino_t) item;
	char cache_path[MAXPATHLEN];

	/* cleanup the file descriptor and its references */
	rfd_close(ino);

	/* delete the cached file from local filesystem */
	build_cache_path(ino, cache_path, read_cache_t, MAXPATHLEN);
	if (remove(cache_path) < 0) {
		int rc = errno;
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			"Couldn't remove file ino=%lu path=%s errno=%d",
			ino, cache_path, rc);
	}
}
