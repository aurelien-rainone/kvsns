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

/* s3_methods.c
 * KVSNS: S3 specific definitions.
 */

#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include "s3_methods.h"
#include "internal.h"

struct s3_resp_cb_data {
	/*< [OUT] request status */
	int status;
	/*< [IN] request configuration */
	const extstore_s3_req_cfg_t *config;
};


/* TODO: remove this global and use the log_props boolean field
 *       of extstore_s3_req_cfg_t instead
 */
static const int showResponsePropertiesG = 1;

int should_retry(S3Status st, int retries, int interval)
{
	if (S3_status_is_retryable(st) && retries--) {
		/* Sleep before next retry; start out with a 1 second sleep*/
		sleep(interval);
		return 1;
	}
	return 0;
}


S3Status resp_props_cb(const S3ResponseProperties *props,
		       void *cb_data_)
{
	struct s3_resp_cb_data *cb_data;
	cb_data = (struct s3_resp_cb_data*) (cb_data_);

		return S3StatusOK;
	}
	if (!cb_data->config->log_props) {

#define PRINT_PROP(name, field) ({\
	if (props->field) printf("%s: %s\n", name, props->field); })

	PRINT_PROP("Content-Type", contentType);
	PRINT_PROP("Request-Id", requestId);
	PRINT_PROP("Request-Id-2", requestId2);
	if (props->contentLength > 0) {
		printf("Content-Length: %llu\n",
		       (unsigned long long) props->contentLength);
	}
	PRINT_PROP("Server", server);
	PRINT_PROP("ETag", eTag);
	if (props->lastModified > 0) {
		char timebuf[256];
		time_t t = (time_t) props->lastModified;
		// gmtime is not thread-safe but we don't care here.
		strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ", gmtime(&t));
		printf("Last-Modified: %s\n", timebuf);
	}
	int i;
	for (i = 0; i < props->metaDataCount; i++) {
		printf("x-amz-meta-%s: %s\n", props->metaData[i].name,
		       props->metaData[i].value);
	}
	if (props->usesServerSideEncryption) {
		printf("UsesServerSideEncryption: true\n");
	}

	return S3StatusOK;
}

/**
 * This callbacks saves the status and prints extra informations.
 */
void resp_complete_cb(S3Status status,
		      const S3ErrorDetails *error,
		      void *cb_data)
{
	int i;
	struct s3_resp_cb_data *s3cbdata;
	s3cbdata = (struct s3_resp_cb_data*) (cb_data);
	s3cbdata->status = status;

	if (status == S3StatusOK)
		printf("%s Successful request, res=%s",
		       __func__, error->resource);
	else if (error) {
		if (error->message)
			printf("%s Message: %s", __func__, error->message);
		if (error->resource)
			printf("%s Resource: %s", __func__, error->resource);
		if (error->furtherDetails)
			printf("%s Further details: %s",
			       __func__, error->furtherDetails);
		if (error->extraDetailsCount)
			for (i = 0; i < error->extraDetailsCount; i++)
				printf("%s Extra details: %s->%s",
				       __func__,
				       error->extraDetails[i].name,
				       error->extraDetails[i].value);
	}
}

S3Status test_bucket(const S3BucketContext *ctx, extstore_s3_req_cfg_t *req_cfg)
{
	int retries;
	int interval;
	char location[64];
	struct s3_resp_cb_data cb_data = { S3StatusOK, req_cfg };

	S3ResponseHandler resp_handler = {
		&resp_props_cb, &resp_complete_cb
	};

	printf("%s bkt=%s\n", __func__, ctx->bucketName);

	/* Setup retry variables */
	retries = req_cfg->retries;
	interval = req_cfg->sleep_interval;

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

	return cb_data.status;
}
