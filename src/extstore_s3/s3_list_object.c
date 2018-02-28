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

#include "internal.h"
#include "s3_common.h"

typedef struct list_bucket_cb_data_t
{
	time_t mtime;		/*< [OUT] mtime to be written by callback */
	uint64_t size;		/*< [OUT] size to be writte by callback */
	int status;		/*< [OUT] request status */
	bool truncated;		/*< [OUT] report request truncation */
	char *prefix;		/*< [IN] prefix use for list request */
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

static S3Status list_bucket_cb(int truncated, const char *next_marker,
				   int ncontents,
				   const S3ListBucketContent *contents,
				   int ncommonprefix,
				   const char **commonprefix,
				   void *cb_data_)
{
	int i, curdirent;
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

	for (i = 0, curdirent = cb_data->ndirent; i < ncontents; i++, curdirent++) {
		const S3ListBucketContent *content = &(contents[i]);
		char timebuf[256];

		time_t t = (time_t) content->lastModified;
		strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ",
		gmtime(&t));

		printf("%-50s  %s  %lu", content->key, timebuf, content->size);

		filename = extract_s3_filename(cb_data->prefix, content->key);
		if (filename) {
			strncpy(cb_data->dirent[curdirent].name, filename, S3_MAX_KEY_SIZE);
			cb_data->dirent[curdirent].stats.st_mode = S_IFREG|0777;
		}
		else
			LogCrit(KVSNS_COMPONENT_KVSNS,
				 "Couldn't extract s3 filename prefix=%s key=%s",
				 cb_data->prefix, content->key);
		printf("\n");
	}

	for (i = 0; i < ncommonprefix; i++, curdirent++) {
		printf("\nCommon Prefix: %s\n", commonprefix[i]);
		filename = extract_s3_filename(cb_data->prefix, commonprefix[i]);
		if (filename) {
			/* remove trailing directory slash */
			cb_data->dirent[curdirent].stats.st_mode = S_IFDIR|0777;
			filename[strlen(filename) - 1] = '\0';
			strncpy(cb_data->dirent[curdirent].name, filename, S3_MAX_KEY_SIZE);
		}
		else
			LogCrit(KVSNS_COMPONENT_KVSNS,
				 "Couldn't extract s3 filename prefix=%s commonprefix=%s",
				 cb_data->prefix, commonprefix[i]);
	}

	cb_data->ndirent += curdirent;

	return S3StatusOK;
}

int list_object(const S3BucketContext *ctx,
		const char *key,
		extstore_s3_req_cfg_t *req_cfg,
		kvsns_dentry_t *dirent, int *ndirent)
{
	int rc = 0;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;
	char delimiter[] = "/";
	char *marker = NULL;
	char *prefix;

	if (!ctx || !req_cfg || !dirent || !ndirent)
		return -EINVAL;

	/* don't ask more entries that we can stuff in dirent array */
	const int maxkeys = *ndirent;

	/* we don't want the first slash */
	prefix = (char*) key + 1;

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
	cb_data.prefix = prefix;

	LogDebug(KVSNS_COMPONENT_EXTSTORE,
		"calling S3_list_bucket with prefix=%s next_marker=%s delim=%s",
		 prefix, cb_data.next_marker, delimiter);

	do {
		cb_data.truncated = 0;
		do {
			S3_list_bucket(ctx, prefix, cb_data.next_marker, delimiter,
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
