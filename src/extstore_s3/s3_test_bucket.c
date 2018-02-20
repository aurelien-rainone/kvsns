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
	int status;		/*< [OUT] request status */
};

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

int test_bucket(const S3BucketContext *ctx,
		extstore_s3_req_cfg_t *req_cfg)
{
	char location[64];
	struct _resp_cb_data_t cb_data;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;

	/* define callback data */
	cb_data.status = S3StatusOK;

	/* define callbacks */
	S3ResponseHandler resp_handler = {
		&log_response_properties,
		&_resp_complete_cb
	};

	LogInfo(KVSNS_COMPONENT_EXTSTORE, "bkt=%s", ctx->bucketName);

	do {
		S3_test_bucket(ctx->protocol,
			       ctx->uriStyle,
			       ctx->accessKeyId,
			       ctx->secretAccessKey,
			       0, 0,
			       ctx->bucketName,
			       ctx->authRegion,
			       sizeof(location),
			       location,
			       0, req_cfg->timeout,
			       &resp_handler,
			       &cb_data);

		/* Decrement retries and wait 1 second longer */
		--retries;
		++interval;
	} while (should_retry(cb_data.status, retries, interval));

	if (cb_data.status != S3StatusOK) {
		int rc = s3status2posix_error(cb_data.status);

		LogCrit(KVSNS_COMPONENT_EXTSTORE, "error %s s3sta=%d rc=%d",
			S3_get_status_name(cb_data.status),
			cb_data.status, rc);
		return rc;
	}
	return 0;
}
