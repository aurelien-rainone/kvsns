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

#include "s3_common.h"


int should_retry(S3Status st, int retries, int interval)
{
	if (S3_status_is_retryable(st) && retries--) {
		/* Sleep before next retry; start out with a 1 second sleep*/
		sleep(interval);
		return 1;
	}
	return 0;
}

S3Status log_response_properties(const S3ResponseProperties *props, void *data)
{
#define PRINT_PROP(name, field) ({\
	if (props->field) LogDebug(COMPONENT_EXTSTORE, "%s=%s", name, props->field); })

	PRINT_PROP("Content-Type", contentType);
	PRINT_PROP("Request-Id", requestId);
	PRINT_PROP("Request-Id-2", requestId2);
	if (props->contentLength > 0)
		LogDebug(COMPONENT_EXTSTORE, "Content-Length=%llu",
			 (unsigned long long) props->contentLength);
	PRINT_PROP("Server", server);
	PRINT_PROP("ETag", eTag);
	if (props->lastModified > 0) {
		char timebuf[256];
		time_t t = (time_t) props->lastModified;
		/* gmtime is not thread-safe but we don't care here. */
		strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ",
			 gmtime(&t));
		LogDebug(COMPONENT_EXTSTORE, "Last-Modified=%s", timebuf);
	}
	int i;
	for (i = 0; i < props->metaDataCount; i++) {
		LogDebug(COMPONENT_EXTSTORE, "x-amz-meta-%s=%s",
			 props->metaData[i].name,
			 props->metaData[i].value);
	}
	if (props->usesServerSideEncryption)
		LogDebug(COMPONENT_EXTSTORE, "UsesServerSideEncryption=true");
	return S3StatusOK;
}

void log_response_status_error(S3Status status, const S3ErrorDetails *error)
{
	if (status != S3StatusOK && error) {
		int i;
		if (error->message)
			LogWarn(COMPONENT_EXTSTORE,
				"msg=%s",
				error->message);
		if (error->resource)
			LogWarn(COMPONENT_EXTSTORE,
				"resource=%s",
				error->resource);
		if (error->furtherDetails)
			LogWarn(COMPONENT_EXTSTORE,
				"details=%s",
				error->furtherDetails);
		if (error->extraDetailsCount)
			for (i = 0; i < error->extraDetailsCount; i++)
				LogWarn(COMPONENT_EXTSTORE,
					"extra-details %s=%s",
					error->extraDetails[i].name,
					error->extraDetails[i].value);
	}
}
