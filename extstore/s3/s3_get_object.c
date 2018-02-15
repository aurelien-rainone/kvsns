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


struct _resp_cb_data_t {
	FILE *outfile;		/*< [IN] opened file where to write received data */
	time_t mtime;		/*< [OUT] mtime to be by callback */
	uint64_t size;		/*< [OUT] size to be by callback */
	int status;		/*< [OUT] request status */
};

static S3Status _resp_props_cb(const S3ResponseProperties *props,
			void *cb_data_)
{
	struct _resp_cb_data_t *cb_data;
	cb_data = (struct _resp_cb_data_t*) (cb_data_);

	log_response_properties(props, NULL);

	cb_data->mtime = (time_t) props->lastModified;
	cb_data->size = (uint64_t) props->contentLength;

	LogDebug(COMPONENT_EXTSTORE, "mtime=%lu size=%lu",
		 cb_data->mtime,
		 cb_data->size);

	return S3StatusOK;
}

static void _resp_complete_cb(S3Status status,
		       const S3ErrorDetails *error,
		       void *cb_data_)
{
	struct _resp_cb_data_t *cb_data;
	cb_data = (struct _resp_cb_data_t*) (cb_data_);

	/* set status and log error */
	cb_data->status = status;
	log_response_status_error(status, error);
}

static S3Status _get_object_data_cb(int bufsize, const char *buffer,
				    void *cb_data_)
{
	struct _resp_cb_data_t *cb_data;
	cb_data = (struct _resp_cb_data_t*) (cb_data_);

	size_t written = fwrite(buffer, 1, bufsize, cb_data->outfile);
	return ((written < (size_t) bufsize) ?
		S3StatusAbortedByCallback : S3StatusOK);
}

int get_object(const S3BucketContext *ctx,
	       const char *key,
	       extstore_s3_req_cfg_t *req_cfg,
	       const char *dst_file,
	       time_t * mtime,
	       size_t *size)
{
	int rc;
	int64_t if_modified_since = -1;
	int64_t if_not_modified_since = -1;
	const char *if_match = NULL;
	const char *if_not_match = NULL;
	uint64_t offset = 0, count = 0;
	FILE *outfile = 0;

	if (dst_file) {
		/* stat the file. If it doesn't exist, open it in write mode */
		struct stat buf;
		if (stat(dst_file, &buf) == -1)
			outfile = fopen(dst_file, "w");
		else
			/*
			 * Open in r+ so that we don't truncate the file, just
			 * in case there is an error and we write no bytes, we
			 * leave the file unmodified.
			 */
			outfile = fopen(dst_file, "r+");
	}

	if (!outfile) {
		rc = errno;
		LogCrit(COMPONENT_EXTSTORE,
		"failed to open destination file dst_file=%s errno=%d",
		dst_file, rc);
		return -rc;
	}

	S3GetConditions get_conditions = {
		if_modified_since,
		if_not_modified_since,
		if_match,
		if_not_match
	};

	S3GetObjectHandler get_obj_handler = {
		{
		&_resp_props_cb,
		&_resp_complete_cb
		},
		&_get_object_data_cb
	};

	struct _resp_cb_data_t cb_data;
	cb_data.outfile = outfile;
	cb_data.mtime = 0;
	cb_data.size = 0;
	cb_data.status = S3StatusOK;

	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;

	do {
		S3_get_object(ctx, key, &get_conditions, offset,
		count, 0, 0, &get_obj_handler, &cb_data);

		/* Decrement retries and wait 1 second longer */
		--retries;
		++interval;
	} while (should_retry(cb_data.status, retries, interval));

	if (cb_data.status == S3StatusOK) {
		*mtime = cb_data.mtime;
		*size = cb_data.size;
		rc = 0;
		LogDebug(COMPONENT_EXTSTORE,
			 "successfully retrieved s3 object key=%s dst_file=%s mtime=%lu size=%lu",
			 key, dst_file, *mtime, *size);
	} else {
		rc = s3status2posix_error(cb_data.status);
		LogWarn(COMPONENT_EXTSTORE, "error %s s3sta=%d rc=%d",
			S3_get_status_name(cb_data.status),
			cb_data.status, rc);
	}

	fclose(cb_data.outfile);
	return rc;
}


