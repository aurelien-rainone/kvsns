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
	time_t mtime;		/*< [OUT mtime to be by callback */
	uint64_t size;		/*< [OUT] size to be by callback */
	int status;		/*< [OUT] request status */
};

S3Status _resp_props_cb(const S3ResponseProperties *props,
		       void *cb_data_)
{
	struct _resp_cb_data_t *cb_data;
	cb_data = (struct _resp_cb_data_t*) (cb_data_);

	log_response_properties(props, NULL);

	cb_data->mtime = (time_t) props->lastModified;
	cb_data->size = (uint64_t) props->contentLength;

	LogDebug(COMPONENT_EXTSTORE, "set_stats=1 mtime=%lu size=%lu",
		 cb_data->mtime,
		 cb_data->size);

	return S3StatusOK;
}

void _resp_complete_cb(S3Status status,
		      const S3ErrorDetails *error,
		      void *cb_data_)
{
	struct _resp_cb_data_t *cb_data;
	cb_data = (struct _resp_cb_data_t*) (cb_data_);

	/* set status and log error */
	cb_data->status = status;
	log_response_status_error(status, error);
}

int stats_object(const S3BucketContext *ctx,
		      const char *key,
		      extstore_s3_req_cfg_t *req_cfg,
		      time_t *mtime, uint64_t *size)
{
	struct _resp_cb_data_t cb_data;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;

	/* define callback data */
	cb_data.status = S3StatusOK;

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

	if (cb_data.status == S3StatusOK) {
		*mtime = cb_data.mtime;
		*size = cb_data.size;
	} else {
		int rc = s3status2posix_error(cb_data.status);
		LogCrit(COMPONENT_EXTSTORE, "error s3rc=%d rc=%d",
			cb_data.status, rc);
		return rc;
	}
	return 0;
}
