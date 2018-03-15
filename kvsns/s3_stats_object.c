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

#include <stdbool.h>
#include <strings.h>
#include "extstore_internal.h"
#include "s3_common.h"


struct _resp_cb_data_t {
	time_t mtime;		/*< [OUT] mtime to be by callback */
	uint64_t size;		/*< [OUT] size to be by callback */
	struct stat bufstat;	/*< [OUT] object stat buffer */
	bool has_posix_stat;	/*< [OUT] true if stats are stored in metadata */
	int statver;		/*< [OUT] version of posix attributes metadata set*/
	int status;		/*< [OUT] request status */
};

static S3Status _resp_props_cb(const S3ResponseProperties *props,
			       void *cb_data_)
{
	struct _resp_cb_data_t *cb_data;
	cb_data = (struct _resp_cb_data_t*) (cb_data_);

	log_response_properties(props, NULL);

	/* copy non-posix stat */
	cb_data->mtime = (time_t) props->lastModified;
	cb_data->size = (uint64_t) props->contentLength;

	/* fill stat with posix attr stored in s3 mds (if present) */
	cb_data->has_posix_stat = s3mds2posix(&cb_data->bufstat,
					      &cb_data->statver,
					      props->metaData,
					      props->metaDataCount);

	/* complete stat with standard s3 attributes (not coming from extra mds) */
	cb_data->bufstat.st_size = (uint64_t) props->contentLength;
	cb_data->bufstat.st_mtime = (time_t) props->lastModified;

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "mtime=%lu size=%lu posix_stat=%d statver=%d",
		 cb_data->mtime, cb_data->size, cb_data->has_posix_stat, cb_data->statver);

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

int get_stats_object(const S3BucketContext *ctx, const char *key,
		     extstore_s3_req_cfg_t *req_cfg,
		     time_t *mtime, uint64_t *size,
		     struct stat *posix_stat, bool *has_posix_stat)
{
	int rc = 0;
	struct _resp_cb_data_t cb_data;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;

	if (!ctx || !key || !req_cfg || !mtime || !size)
		return -EINVAL;

	/* define callback data */
	memset(&cb_data, 0, sizeof(struct _resp_cb_data_t));
	cb_data.status = S3StatusOK;
	cb_data.has_posix_stat = false;
	memset(&cb_data.bufstat, 0, sizeof(struct stat));

	/* define callbacks */
	S3ResponseHandler resp_handler = {
		&_resp_props_cb,
		&_resp_complete_cb
	};

	do {
		S3_head_object(ctx, key, 0, 0, &resp_handler, &cb_data);
		/* Decrement retries and wait 1 second longer */
		--retries;
		++interval;
	} while (should_retry(cb_data.status, retries, interval));

	if (cb_data.status != S3StatusOK) {
		rc = s3status2posix_error(cb_data.status);
		LogWarn(KVSNS_COMPONENT_EXTSTORE, "libs3 error errstr=%s s3sta=%d rc=%d key=%s",
			S3_get_status_name(cb_data.status), cb_data.status, rc, key);
		return rc;
	}

	if (has_posix_stat) {
		*has_posix_stat = cb_data.has_posix_stat;
		if (*has_posix_stat && posix_stat)
			memcpy(posix_stat, &cb_data.bufstat, sizeof(struct stat));
	}
	*mtime = cb_data.mtime;
	*size = cb_data.size;
	return rc;
}

int set_stats_object(const S3BucketContext *ctx, const char *key,
		     extstore_s3_req_cfg_t *req_cfg,
		     const struct stat *posix_stat)
{
	int rc = 0;
	int i;
	int64_t lastModified;
	char eTag[256];
	struct _resp_cb_data_t cb_data;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;

	if (!ctx || !key || !req_cfg || !posix_stat)
		return -EINVAL;

	/* define callback data */
	memset(&cb_data, 0, sizeof(struct _resp_cb_data_t));

	/* define request properties data */
	S3NameValue mds[S3_POSIX_MD_COUNT];
	for (i = 0; i < S3_POSIX_MD_COUNT; ++i) {
		mds[i].name = malloc(sizeof(char) * S3_POSIX_MAXNAME_LEN);
		mds[i].value = malloc(sizeof(char) * S3_POSIX_MAXVALUE_LEN);
	}
	posix2s3mds(&mds[0], posix_stat);

	S3PutProperties put_props = {
		PUT_CONTENT_TYPE,
		PUT_MD5,
		PUT_CACHE_CONTROL,
		PUT_CONTENT_DISP_FNAME,
		PUT_CONTENT_ENCODING,
		PUT_EXPIRES,
		PUT_CANNED_ACL,
		S3_POSIX_MD_COUNT,
		mds,
		PUT_SERVERSIDE_ENCRYPT,
	};

	S3ResponseHandler resp_handler = {
		&_resp_props_cb,
		&_resp_complete_cb,
	};

	do {
		S3_copy_object(ctx, key, ctx->bucketName,
			       key, &put_props,
			       &lastModified, sizeof(eTag), eTag, NULL,
			       req_cfg->timeout,
			       &resp_handler, &cb_data);
		/* Decrement retries and wait 1 second longer */
		--retries;
		++interval;
	} while (should_retry(cb_data.status, retries, interval));

	if (cb_data.status != S3StatusOK) {
		rc = s3status2posix_error(cb_data.status);
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			"libs3 error errstr=%s s3sta=%d rc=%d key=%s",
			S3_get_status_name(cb_data.status),
			cb_data.status, rc, key);
	}

	/* free md strings */
	for (i = 0; i < S3_POSIX_MD_COUNT; ++i) {
		free((char *) mds[i].name);
		free((char *) mds[i].value);
	}
	return rc;
}
