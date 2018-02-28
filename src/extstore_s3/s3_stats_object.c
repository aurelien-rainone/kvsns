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
#include "internal.h"
#include "s3_common.h"


#define S3_POSIX_MODE S3_METADATA_HEADER_NAME_PREFIX "posix-mode"
#define S3_POSIX_UID S3_METADATA_HEADER_NAME_PREFIX "posix-uid"
#define S3_POSIX_GID S3_METADATA_HEADER_NAME_PREFIX "posix-gid"
#define S3_POSIX_ATIM S3_METADATA_HEADER_NAME_PREFIX "posix-atim"
#define S3_POSIX_MTIM S3_METADATA_HEADER_NAME_PREFIX "posix-mtim"
#define S3_POSIX_CTIM S3_METADATA_HEADER_NAME_PREFIX "posix-ctim"
#define S3_POSIX_VER S3_METADATA_HEADER_NAME_PREFIX "posix-ver"

#define S3_POSIX_MINNAME_LEN 32		/*< min length for a posix name s3 metadata */
#define S3_POSIX_MINVALUE_LEN 32	/*< min length for a posix value s3 metadata */

/* In case the posix attributes stored on s3 are changed */
#define S3_POSIX_STAT_VERSION 1
#define S3_POSIX_MD_COUNT 7

struct _resp_cb_data_t {
	time_t mtime;		/*< [OUT] mtime to be by callback */
	uint64_t size;		/*< [OUT] size to be by callback */
	struct stat bufstat;	/*< [OUT] object stat buffer */
	bool has_posix_stat;	/*< [OUT] true if stats are stored in metadata */
	int statver;		/*< [OUT] version of posix attributes metadata set*/
	int status;		/*< [OUT] request status */
};

/**
 * @brief posix2s3mds convert posix attributes to s3 metadata
 * @param dstmds [INOUT] array of preallocated s3 object metadata "key-value
 *        pairs", containing at least S3_POSIX_MD_COUNT elements. Name and
 * value minimum sizes are defined in S3_POSIX_MINNAME_LEN and
 * S3_POSIX_MINVALUE_LEN.
 * @param srcstat posix stat structure to convert from
 */
void posix2s3mds(const S3NameValue *dstmds, const struct stat *srcstat)
{
	int i = 0;
	strncpy((char*) dstmds[i].name, S3_POSIX_MODE, S3_POSIX_MINNAME_LEN);
	snprintf((char*) dstmds[i].value, S3_POSIX_MINVALUE_LEN, "%u", srcstat->st_mode);
	i++;
	strncpy((char*) dstmds[i].name, S3_POSIX_UID, S3_POSIX_MINNAME_LEN);
	snprintf((char*) dstmds[i].value, S3_POSIX_MINVALUE_LEN, "%u", srcstat->st_uid);
	i++;
	strncpy((char*) dstmds[i].name, S3_POSIX_GID, S3_POSIX_MINNAME_LEN);
	snprintf((char*) dstmds[i].value, S3_POSIX_MINVALUE_LEN, "%u", srcstat->st_gid);
	i++;
	strncpy((char*) dstmds[i].name, S3_POSIX_ATIM, S3_POSIX_MINNAME_LEN);
	snprintf((char*) dstmds[i].value, S3_POSIX_MINVALUE_LEN, "%ld", srcstat->st_atim.tv_sec);
	i++;
	strncpy((char*) dstmds[i].name, S3_POSIX_MTIM, S3_POSIX_MINNAME_LEN);
	snprintf((char*) dstmds[i].value, S3_POSIX_MINVALUE_LEN, "%ld", srcstat->st_mtim.tv_sec);
	i++;
	strncpy((char*) dstmds[i].name, S3_POSIX_CTIM, S3_POSIX_MINNAME_LEN);
	snprintf((char*) dstmds[i].value, S3_POSIX_MINVALUE_LEN, "%ld", srcstat->st_ctim.tv_sec);
	i++;
	strncpy((char*) dstmds[i].name, S3_POSIX_VER, S3_POSIX_MINNAME_LEN);
	snprintf((char*) dstmds[i].value, S3_POSIX_MINVALUE_LEN, "%d", S3_POSIX_STAT_VERSION);
}

/**
 * @brief s3mds2posix convert s3 metadata to posix attributes
 * @param dststat [OUT] the posix stat structure to fill
 * @param ver [OUT] version of the metadata attributes
 * @param srcmds [IN] array of s3 object metadata "key-value pairs"
 * @param nmds [IN] number of elements in srcmds
 * @return true if every required attributes has been found and parsed, false
 * otherwise.
 */
bool s3mds2posix(struct stat *dststat, int *ver, const S3NameValue *srcmds, int nmds)
{
	int i, nvals = 0;
	for (i = 0; i < nmds; i ++) {
		const char *name = srcmds[i].name;
		const char *value = srcmds[i].name;

		/* st_ino and st_nlink are not stored on s3, as they depend on
		 * the client context */

		if (!strcmp(name, S3_POSIX_MODE)) {
			dststat->st_mode = atoi(value);
			nvals++;
			continue;
		}
		if (!strcmp(name, S3_POSIX_UID)) {
			dststat->st_uid = atoi(value);
			nvals++;
			continue;
		}

		if (!strcmp(name, S3_POSIX_GID)) {
			dststat->st_gid = atoi(value);
			nvals++;
			continue;
		}
		if (!strcmp(name, S3_POSIX_ATIM)) {
			dststat->st_atim.tv_sec = atoi(value);
			dststat->st_atim.tv_nsec = 0;
			nvals++;
			continue;
		}
		if (!strcmp(name, S3_POSIX_MTIM)) {
			dststat->st_mtim.tv_sec = atoi(value);
			dststat->st_mtim.tv_nsec = 0;
			nvals++;
			continue;
		}
		if (!strcmp(name, S3_POSIX_CTIM)) {
			dststat->st_ctim.tv_sec = atoi(value);
			dststat->st_ctim.tv_nsec = 0;
			nvals++;
			continue;
		}
		if (!strcmp(name, S3_POSIX_CTIM)) {
			dststat->st_ctim.tv_sec = atoi(value);
			dststat->st_ctim.tv_nsec = 0;
			nvals++;
			continue;
		}
	}
	return nvals == S3_POSIX_MD_COUNT;
}

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
		     struct stat *posix_stat,
		     bool *has_posix_stat)

{
	int rc;
	struct _resp_cb_data_t cb_data;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;

	/* define callback data */
	cb_data.status = S3StatusOK;
	cb_data.has_posix_stat = false;
	memset(&cb_data.bufstat, 0, sizeof(struct stat));

	LogDebug(KVSNS_COMPONENT_EXTSTORE, "retrieving object stats key=%s", key);

	if (!has_posix_stat)
		return -EINVAL;

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

	*has_posix_stat = cb_data.has_posix_stat;
	if (!cb_data.has_posix_stat)
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			"posix stats inexistent in object metadata key=%s", key);
	else if (posix_stat)
		memcpy(posix_stat, &cb_data.bufstat, sizeof(struct stat));

	*mtime = cb_data.mtime;
	*size = cb_data.size;
	rc = 0;
	return rc;
}

int set_stats_object(const S3BucketContext *ctx, const char *key,
		     extstore_s3_req_cfg_t *req_cfg,
		     const struct stat *posix_stat)
{

	return 0;
}
