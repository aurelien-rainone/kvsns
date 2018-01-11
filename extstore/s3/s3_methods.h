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

/* s3_methods.h
 * KVSNS: S3 specific declarations.
 */

#ifndef _S3_EXTSTORE_S3_METHODS_H
#define _S3_EXTSTORE_S3_METHODS_H

#include <libs3.h>
#include "internal.h"

/**
 * Test bucket existence/access.
 *
 * @param ctx - libs3 bucket context.
 * @param req_cfg - config for this request.
 *
 * @return S3StatusOK if successful.
 */
S3Status test_bucket(const S3BucketContext *ctx,
		     extstore_s3_req_cfg_t *req_cfg);

/**
 * Send object to S3.
 *
 * @param ctx - libs3 bucket context.
 * @param key - key to create/replace/modify.
 * @param req_cfg - config for this request.
 * @param buf - bytes to be sent to S3.
 * @param buflen - length of buf (0 means an empty S3 object).
 *
 * @return S3StatusOK if successful.
 */
S3Status put_object(const S3BucketContext *ctx, const char *key,
		    extstore_s3_req_cfg_t *req_cfg,
		    const char *buf, size_t buflen);
#endif
