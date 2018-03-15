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

#include "extstore_internal.h"
#include "s3_common.h"
#include <kvsns/kvsns.h>


typedef struct list_bucket_cb_data_t
{
	time_t mtime;		/*< [OUT] mtime to be written by callback */
	uint64_t size;		/*< [OUT] size to be writte by callback */
	int status;		/*< [OUT] request status */
	bool truncated;		/*< [OUT] report request truncation */
	const char *prefix;	/*< [IN] prefix use for list request */
	char next_marker[1024];	/*< [IN] next marker */
	kvsns_dentry_t *dirent; /*< [INOUT] array of dir entries to be filled */
	size_t ndirent;		/*< [IN] size of dir entries array */
} list_bucket_cb_data_t;

static S3Status _resp_props_cb(const S3ResponseProperties *props,
			       void *cb_data_)
{
	struct list_bucket_cb_data_t *cb_data;
	cb_data = (struct list_bucket_cb_data_t*) (cb_data_);

	log_response_properties(props, NULL);

	cb_data->mtime = (time_t) props->lastModified;
	cb_data->size = (uint64_t) props->contentLength;

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "mtime=%lu size=%lu",
		 cb_data->mtime,
		 cb_data->size);

	return S3StatusOK;
}

static void _resp_complete_cb(S3Status status,
			      const S3ErrorDetails *error,
			      void *cb_data_)
{
	struct list_bucket_cb_data_t *cb_data;
	cb_data = (struct list_bucket_cb_data_t*) (cb_data_);

	/* set status and log error */
	cb_data->status = status;
	log_response_status_error(status, error);
}

static char *extract_s3_filename(const char *prefix, const char *fullkey)
{
	if (strstr(fullkey, prefix) != fullkey)
		return NULL;
	return (char *) fullkey + strlen(prefix);
}

static S3Status list_bucket_cb(int truncated,
			       const char *next_marker,
			       int ncontents,
			       const S3ListBucketContent *contents,
			       int ncommonprefix,
			       const char **commonprefix,
			       void *cb_data_)
{
	int i;
	char *filename = NULL;
	struct list_bucket_cb_data_t *cb_data;
	cb_data = (struct list_bucket_cb_data_t*) (cb_data_);

	cb_data->truncated = truncated? true:false;

	// This is tricky. S3 doesn't return the next_marker if there is no
	// delimiter. Why, I don't know, since it's still useful for paging
	// through results. We want next_marker to be the last content in the
	// list, so set it to that if necessary.
	if ((!next_marker || !next_marker[0]) && ncontents)
		next_marker = contents[ncontents - 1].key;
	if (next_marker)
		snprintf(cb_data->next_marker, sizeof(cb_data->next_marker),
			 "%s", next_marker);
	else
		cb_data->next_marker[0] = 0;

	for (i = 0; i < ncontents; ++i, ++cb_data->ndirent) {
		filename = extract_s3_filename(cb_data->prefix, contents[i].key);
		if (filename) {
			strncpy(&(cb_data->dirent[cb_data->ndirent].name[0]), filename, NAME_MAX);
			/* let's temporarily abuse st_mode */
			cb_data->dirent[cb_data->ndirent].stats.st_mode = KVSNS_FILE;
		}
		else
			LogWarn(KVSNS_COMPONENT_KVSNS,
				"Couldn't extract s3 filename prefix=%s key=%s",
				cb_data->prefix, contents[i].key);
	}

	for (i = 0; i < ncommonprefix; ++i, ++cb_data->ndirent) {
		filename = extract_s3_filename(cb_data->prefix, commonprefix[i]);
		if (filename) {
			/* let's temporarily abuse st_mode */
			cb_data->dirent[cb_data->ndirent].stats.st_mode = KVSNS_DIR;
			/* remove trailing directory slash */
			filename[strlen(filename) - 1] = '\0';
			strncpy(&(cb_data->dirent[cb_data->ndirent].name[0]), filename, NAME_MAX);
		}
		else
			LogWarn(KVSNS_COMPONENT_KVSNS,
				"Couldn't extract s3 filename prefix=%s commonprefix=%s",
				cb_data->prefix, commonprefix[i]);
	}

	return S3StatusOK;
}

int list_bucket(const S3BucketContext *ctx,
		const char *key,
		extstore_s3_req_cfg_t *req_cfg,
		kvsns_dentry_t *dirent,
		int *ndirent)
{
	int rc = 0;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;
	char delimiter[] = "/";
	char *marker = NULL;

	if (!ctx || !req_cfg || !dirent || !ndirent)
		return -EINVAL;

	ASSERT(!strlen(key) || key[0] != '/');

	/* don't ask more entries that we can stuff in dirent array */
	const int maxkeys = *ndirent;

	/* define callback data */
	list_bucket_cb_data_t cb_data;
	memset(&cb_data, 0, sizeof(list_bucket_cb_data_t));

	cb_data.status = S3StatusOK;

	/* define callbacks */
	S3ListBucketHandler list_bucket_handler = {
		{
			&_resp_props_cb,
			&_resp_complete_cb
		},
		&list_bucket_cb
	};

	if (marker)
		snprintf(cb_data.next_marker, sizeof(cb_data.next_marker),
			 "%s", marker);
	else
		cb_data.next_marker[0] = 0;

	cb_data.ndirent = 0;
	cb_data.dirent = dirent;
	cb_data.prefix = key;

	LogDebug(KVSNS_COMPONENT_EXTSTORE,
		"Listing bucket prefix=%s next_marker=%s delim=%s maxkeys=%d",
		 key, cb_data.next_marker, delimiter, maxkeys);

	/* TODO: current implementation is totally is wrong and will break as
	 * soon as more than *keys will be present in the a directory-like.
	 * We should NOT page more than maxkeys keys.
	 * As soon as the ListBucket paging API will be implemented on the S3
	 * backend we should use it.
	 */

	do {
		cb_data.truncated = 0;
		do {
			S3_list_bucket(ctx, key, cb_data.next_marker, delimiter,
				       maxkeys, 0, req_cfg->timeout,
				       &list_bucket_handler, &cb_data);

			/* Decrement retries and wait 1 second longer */
			--retries;
			++interval;
		} while (should_retry(cb_data.status, retries, interval));

		if (cb_data.status != S3StatusOK)
			break;
	} while (cb_data.truncated && (!maxkeys || (cb_data.ndirent < maxkeys)));

	*ndirent = cb_data.ndirent;

	if (cb_data.status != S3StatusOK) {
		rc = s3status2posix_error(cb_data.status);
		LogWarn(KVSNS_COMPONENT_EXTSTORE, "error %s s3sta=%d rc=%d",
			S3_get_status_name(cb_data.status),
			cb_data.status, rc);
	}
	return rc;
}
