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

/* internal.h
 * KVSNS: S3 extstore internal declarations.
 */

#ifndef _S3_EXTSTORE_INTERNAL_H
#define _S3_EXTSTORE_INTERNAL_H

#include <errno.h>
#include <libs3.h>


/* S3 request configuration */
typedef struct extstore_s3_req_cfg_ {
	int retries;	    /* max retries for failed S3 requests */
	int sleep_interval; /* sleep interval between successive retries (s) */
	int timeout;	    /* request timeout (ms) */
	int log_props;	    /* [DBG] log response properties */
} extstore_s3_req_cfg_t;


/**
 * @brief posix error code from libS3 status error
 *
 * This function returns a posix errno equivalent from an libs3 S3Status.
 *
 * @param[in] s3_errorcode libs3 error
 *
 * @return posix errno.
 */
int s3status2posix_error(const S3Status s3_errorcode);


#endif
