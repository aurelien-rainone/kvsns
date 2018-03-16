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

#include <pthread.h>
#include <kvsns/kvsns.h>
#include "s3_common.h"
#include "pthreadpool.h"


typedef struct thread_context_ {
	kvsns_cred_t *cred;	/* [IN] credentials */
	char *dirpath;		/* [IN] parent path */
	kvsns_dentry_t *dirent;	/* [IN] dir entries */
	size_t *indices;	/* [IN] index map, (threadidx => stat index)*/
} thread_context_t;

void persist_posix(void *cb_data_, size_t idx)
{
	int rc;
	time_t mtime;
	uint64_t filelen;
	bool has_posix_stats;
	char s3key[S3_MAX_KEY_SIZE];
	thread_context_t *tctx;
	tctx = (thread_context_t *) cb_data_;

	size_t sidx = tctx->indices[idx];
	struct stat *stat = &tctx->dirent[sidx].stats;

	format_s3_key(tctx->dirpath, tctx->dirent[sidx].name,
		      0, S3_MAX_KEY_SIZE, s3key);
	LogDebug(KVSNS_COMPONENT_EXTSTORE,
		 "idx=%lu sidx=%lu s3key=%s",
		 idx, sidx, s3key);

	rc = get_stats_object(&bucket_ctx, s3key, &def_s3_req_cfg,
			      &mtime, &filelen, stat, &has_posix_stats);
	if (rc) {
		LogCrit(KVSNS_COMPONENT_EXTSTORE,
			"couldn't get stats on s3 key=%s rc=%d",
			s3key, rc);
		/* TODO: handle error */
		/*return rc;*/
	}

	if (has_posix_stats) {
		/* complete posix stats */
		stat->st_nlink = 1;
		stat->st_size = filelen;
	} else {
		fill_stats(stat, 0, tctx->cred, mtime, 0, filelen);
		rc = set_stats_object(&bucket_ctx, s3key,
				      &def_s3_req_cfg, stat);
		if (rc) {
			LogCrit(KVSNS_COMPONENT_EXTSTORE,
				"couldn't set posix stats on s3 key=%s rc=%d",
				s3key, rc);
			/* TODO: handle error */
			/*return rc;*/
		}
	}
}

int extstore_readdir(kvsns_cred_t *cred, kvsns_ino_t ino,
		     off_t offset, kvsns_dentry_t *dirent,
		     int *size)
{
	int idx;
	int rc;
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
	if (*size == 0) {
		/* empty directory */
		return 0;
	}

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "ino=%d key=%s", ino, s3_path);

	/* first step, get stats for each object */
	/* TODO: have another value (or rename it because upload_nthreads
	 * doesn't represent this )*/
	pthreadpool_t thpool = pthreadpool_create(def_s3_req_cfg.upload_nthreads);
	thread_context_t tctx;
	tctx.dirent = &dirent[0];
	tctx.dirpath = &s3_path[0];
	tctx.indices = malloc(*size * sizeof(size_t));
	tctx.cred = cred;

	size_t fidx;
	for (idx = 0, fidx = 0; idx < *size; idx++) {

		if (dirent[idx].stats.st_mode == KVSNS_DIR) {
			/* we do not persist nothing for directories, directory
			 * attributes are created and managed at runtime only*/
			fill_stats(&dirent[idx].stats, 0, cred, 0, 1, 0);
			continue;
		}

		/* store stat index */
		tctx.indices[fidx++] = idx;
	}

	pthreadpool_compute_1d(thpool, persist_posix, &tctx, fidx);
	free(tctx.indices);

	/* second step, create kvsal entries */
	for (idx = 0; idx < *size; idx++) {
		kvsns_ino_t new_entry;

		rc = kvsns_create_entry2(&ino, dirent[idx].name,
					 &new_entry, &dirent[idx].stats);
		if (rc == -EEXIST) {
			LogFatal(KVSNS_COMPONENT_KVSNS,
				 "entry already exists dirino=%llu key=%s name=%s ino=%llu",
				 ino, dirent[idx].name, new_entry);
		}
	}
	return 0;
}
