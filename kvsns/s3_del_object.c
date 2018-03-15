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

static void _resp_complete_cb(S3Status status,
		      const S3ErrorDetails *error,
		      void *cb_data_)
{
	int *cb_data;
	cb_data = (int *) (cb_data_);

	/* set status and log error */
	*cb_data = status;
	log_response_status_error(*cb_data, error);
}

int del_object(const S3BucketContext *ctx,
	       const char *key,
	       extstore_s3_req_cfg_t *req_cfg)
{
	int rc = 0;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;
	int status;

	/* define callback data */
	status = S3StatusOK;

	/* define callback */
	S3ResponseHandler resp_handler = {
		NULL,
		&_resp_complete_cb
	};

	do {
		S3_delete_object(ctx, key, 0, req_cfg->timeout, &resp_handler, &status);
		/* Decrement retries and wait 1 second longer */
		--retries;
		++interval;
	} while (should_retry(status, retries, interval));

	if (status != S3StatusOK) {
		rc = s3status2posix_error(status);
		LogWarn(KVSNS_COMPONENT_EXTSTORE, "error %s s3sta=%d rc=%d",
			S3_get_status_name(status),
			status, rc);
	}
	return rc;
}
