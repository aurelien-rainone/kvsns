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

#include <pthread.h>
#include "internal.h"
#include "s3_common.h"
#include "thpool.h"


#define MULTIPART_CHUNK_SIZE S3_MULTIPART_CHUNK_SIZE
#define MAX_ETAG_SIZE 256
/* Overrides timeout defined in extstore_s3_req_cfg_t for PUT requests (ms) */
#define PUT_REQUEST_TIMEOUT 10000000

/* Default PUT properties */
#define PUT_CONTENT_TYPE NULL
#define PUT_MD5 NULL
#define PUT_CACHE_CONTROL NULL
#define PUT_CONTENT_DISP_FNAME NULL
#define PUT_CONTENT_ENCODING NULL
#define PUT_EXPIRES -1
#define PUT_CANNED_ACL S3CannedAclPrivate
#define PUT_META_PROPS_COUNT 0
#define PUT_SERVERSIDE_ENCRYPT 0

typedef struct upload_mgr_{
	char * upload_id;	/* filled during multipart prologue */
	char **etags;		/* filled upon successfull part upload */
	GString *commitstr;	/* commit upload xml string */
	int ntowrite;		/* number of (xml) bytes to write */
	S3Status status;	/* request final libs3 status */
} upload_mgr_t;

typedef struct put_object_callback_data_ {
	int fd;			/* file descriptor to read from */
	uint64_t nremain;	/* remaining number of bytes to read */
	off_t off;		/* offset where to start read at from fd */
	S3Status status;	/* request libs3 status */
} put_object_callback_data_t;

static int put_object_data_cb(int bufsize, char *buffer, void *cb_data_)
{
	put_object_callback_data_t *cb_data =
	(put_object_callback_data_t *) cb_data_;

	ssize_t nread  = 0;
	if (cb_data->nremain) {
		nread = ((cb_data->nremain > (unsigned) bufsize) ?
			(unsigned) bufsize : cb_data->nremain);

		nread = pread(cb_data->fd, buffer, nread, cb_data->off);
		if (nread < 0) {
			int rc = -errno;
			LogCrit(KVSNS_COMPONENT_EXTSTORE,
				"can't read from cache file errno=%d", rc);
			return rc;
		}

		/* advance read offset */
		cb_data->off += nread;
	}

	cb_data->nremain -= nread;
	return nread;
}

S3Status initial_multipart_callback(const char * upload_id, void * cb_data_)
{
	upload_mgr_t *cb_data;
	cb_data = (upload_mgr_t *) cb_data_;
	cb_data->upload_id = strdup(upload_id);
	return S3StatusOK;
}

typedef struct multipart_part_data_ {
	put_object_callback_data_t put_object_data;
	char **etag;	/* points to manager.etags[curpart] */
	int *set_etag;	/* points to manager.set_etags[curpart] */
} multipart_part_data_t;

S3Status multiparts_resp_props_cb(const S3ResponseProperties *props,
				  void *cb_data_)
{
	multipart_part_data_t *cb_data;
	cb_data = (multipart_part_data_t *) cb_data_;
	strncpy(*(cb_data->etag), props->eTag, MAX_ETAG_SIZE);
	return S3StatusOK;
}

void _put_object_resp_complete_cb(S3Status status,
		      const S3ErrorDetails *error,
		      void *cb_data_)
{
	struct put_object_callback_data_ *cb_data;
	cb_data = (struct put_object_callback_data_*) (cb_data_);

	/* set status and log error */
	cb_data->status = status;
	log_response_status_error(status, error);
}

void _manager_resp_complete_cb(S3Status status,
		      const S3ErrorDetails *error,
		      void *cb_data_)
{
	upload_mgr_t *cb_data;
	cb_data = (upload_mgr_t*) (cb_data_);

	/* set status and log error */
	cb_data->status = status;
	log_response_status_error(status, error);
}

int multipart_put_xml_cb(int bufsize, char *buffer, void *cb_data_)
{
	upload_mgr_t *cb_data;
	cb_data = (upload_mgr_t*) (cb_data_);
	int nwritten = 0;
	if (cb_data->ntowrite) {
		nwritten = ((cb_data->ntowrite > bufsize) ?
				bufsize : cb_data->ntowrite);
		off_t off = cb_data->commitstr->len - cb_data->ntowrite;
		memcpy(buffer, cb_data->commitstr->str + off, nwritten);
		cb_data->ntowrite -= nwritten;
	}
	return nwritten;
}

typedef struct thread_part_data_ {
	const char *key;		/* s3 key  */
	S3BucketContext *ctx;		/* s3 bucket context */
	char *upload_id;		/* previously requested upload id */
	int fd;				/* descriptor of the file to upload */
	off_t part_off;			/* offset of the 1st byte for the part */
	int curpart;			/* 0-based part sequence number */
	S3PutProperties *put_props;	/* http PUT properties */
	int retries;			/* number of retries */
	int interval;			/* interval between failed replies */
	int part_len;			/* size of current part */
	char **etag;			/* [OUT] etag to fill upon completion */
	int *set_etag;			/* [OUT] has etag been allocated */
	/*int *cancelled;			[> [INOUT] <]*/
} thread_part_data_t;

void send_part(void* cb_data_)
{
	int retries, interval, status;
	thread_part_data_t *cb_data;
	multipart_part_data_t part_data;

	S3PutObjectHandler put_obj_handler = {
		{
			&multiparts_resp_props_cb,
			&_put_object_resp_complete_cb
		},
		&put_object_data_cb
	};

	cb_data = (thread_part_data_t*) cb_data_;
	memset(&part_data, 0, sizeof(multipart_part_data_t));
	part_data.etag = cb_data->etag;
	part_data.put_object_data.nremain = cb_data->part_len;
	part_data.put_object_data.status = 0;
	part_data.put_object_data.off = cb_data->part_off;
	part_data.put_object_data.fd = cb_data->fd;

	retries = cb_data->retries;
	interval = cb_data->interval;
	status = S3StatusOK;

	LogDebug(KVSNS_COMPONENT_EXTSTORE,
	         "(multipart) uploading part partnum=%d partsz=%d",
	         cb_data->curpart, cb_data->part_len);

	do {
		S3_upload_part(cb_data->ctx,
			       cb_data->key,
			       cb_data->put_props,
			       &put_obj_handler,
			       cb_data->curpart + 1,
			       cb_data->upload_id,
			       cb_data->part_len,
			       NULL,
			       PUT_REQUEST_TIMEOUT,
			       &part_data);

		/* Decrement retries and wait 1 second longer */
		--retries;
		++interval;
		status = part_data.put_object_data.status;

	} while (should_retry(status, retries, interval));

	/* TODO: implement an event for premature cancellation */
	int final_status = status;
	if (final_status != S3StatusOK) {
		LogCrit(KVSNS_COMPONENT_EXTSTORE,
			"(multipart) error uploading part: %s s3sta=%d partnum=%d partlen=%d",
			S3_get_status_name(final_status),
			final_status,
			cb_data->curpart, cb_data->part_len);
		/*goto clean;*/
	} else  {
		LogDebug(KVSNS_COMPONENT_EXTSTORE,
			 "(multipart) part uploaded partnum=%d partsz=%d",
			 cb_data->curpart, cb_data->part_len);
	}
}

int put_object(const S3BucketContext *ctx,
	       const char *key,
	       extstore_s3_req_cfg_t *req_cfg,
	       const char *src_file)
{
	int rc;
	int i;
	int fd;
	S3NameValue meta_properties[S3_MAX_METADATA_COUNT];
	S3Status final_status;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;

	fd = open(src_file, O_RDONLY, "rb");
	if (fd == -1) {
		rc = errno;
		LogCrit(KVSNS_COMPONENT_EXTSTORE,
			"can't open cached file src=%s rc=%d",
			src_file, rc);
		return -rc;
	}

	/* stat the file to get its length */
	struct stat statbuf;
	if (stat(src_file, &statbuf) == -1) {
		rc = errno;
		LogCrit(KVSNS_COMPONENT_EXTSTORE,
			"can't stat cached file, src=%d rc=%d",
			src_file, rc);
		return -rc;
	}

	const uint64_t total_len = statbuf.st_size;

	S3PutProperties put_props = {
		PUT_CONTENT_TYPE,
		PUT_MD5,
		PUT_CACHE_CONTROL,
		PUT_CONTENT_DISP_FNAME,
		PUT_CONTENT_ENCODING,
		PUT_EXPIRES,
		PUT_CANNED_ACL,
		PUT_META_PROPS_COUNT,
		meta_properties,
		PUT_SERVERSIDE_ENCRYPT,
	};

	if (total_len <= MULTIPART_CHUNK_SIZE) {

		/* single part upload */

		S3PutObjectHandler put_obj_handler = {
			{
				&log_response_properties,
				&_put_object_resp_complete_cb
			},
			&put_object_data_cb
		};

		put_object_callback_data_t cb_data;
		cb_data.nremain = total_len;
		cb_data.status = S3StatusOK;
		cb_data.fd = fd;
		cb_data.off = 0;

		LogInfo(KVSNS_COMPONENT_EXTSTORE,
			 "(singlepart) uploading object totsz=%lu",
			 total_len);

		do {
			S3_put_object(ctx,
				      key,
				      total_len,
				      &put_props,
				      NULL,
				      PUT_REQUEST_TIMEOUT,
				      &put_obj_handler,
				      &cb_data);

			/* Decrement retries and wait 1 second longer */
			--retries;
			++interval;
			final_status = cb_data.status;
		} while (should_retry(final_status, retries, interval));

		if (fd != -1)
			close(fd);

		if (final_status != S3StatusOK)
			LogWarn(KVSNS_COMPONENT_EXTSTORE,
				"(singlepart) error upload %s s3sta=%d",
				S3_get_status_name(final_status), cb_data.status);
		else if (cb_data.nremain)
			LogWarn(KVSNS_COMPONENT_EXTSTORE,
				"(singlepart) error upload, %llu bytes were not uploaded",
				(unsigned long long) cb_data.nremain);

	} else {

		/* multipart upload
		 *
		 * Because the number of parts can be gigantic, we want a clear
		 * limit on the memory allocated during a multipart upload.
		 * The number of parts is divided into groups of parts, called
		 * blocks, blocks being processed sequentially.
		 */

		/* number of threads in the pool  */
		const int nmaxthreads = 2;
		/* process the parts by blocks of 'parts_per_block' parts */
		const int parts_per_block = 4;	/* TODO: find a `good` value ...*/

		/* the number of parts */
		const int nparts = (total_len + MULTIPART_CHUNK_SIZE - 1)
				       / MULTIPART_CHUNK_SIZE;
		const int nblocks = (nparts + parts_per_block - 1) / parts_per_block;

		int remaining_len = total_len;
		int remaining_parts = nparts;

		upload_mgr_t manager;
		manager.upload_id = NULL;
		manager.commitstr = NULL;
		manager.etags = malloc(sizeof(char *) * nparts);
		for (i = 0; i < nparts; ++i) {
			manager.etags[i] = malloc(sizeof(char) * MAX_ETAG_SIZE);
			memset(manager.etags[i], 0, MAX_ETAG_SIZE);
		}
		manager.status = S3StatusOK;

		S3MultipartInitialHandler handler = {
			{
				&log_response_properties,
				&_manager_resp_complete_cb
			},
			&initial_multipart_callback
		};

		S3MultipartCommitHandler commit_handler = {
			{
				&log_response_properties,
				&_manager_resp_complete_cb
			},
			&multipart_put_xml_cb,
			NULL
		};

		do {
			S3_initiate_multipart((S3BucketContext*)ctx,
					      key,
					      NULL,
					      &handler,
					      NULL,
					      req_cfg->timeout,
					      &manager);

			/* Decrement retries and wait 1 second longer */
			--retries;
			++interval;
			final_status = manager.status;
		} while (should_retry(final_status, retries, interval));

		if (manager.upload_id == NULL) {
			final_status = S3StatusInterrupted;
			LogWarn(KVSNS_COMPONENT_EXTSTORE,
				"(multipart) error initiating multipart (nul upload_id), forcing error %s s3sta=%d",
				S3_get_status_name(final_status), final_status);
			goto clean;
		}

		if (final_status != S3StatusOK) {
			LogWarn(KVSNS_COMPONENT_EXTSTORE,
				"(multipart) error initiating multipart %s s3sta=%d",
				S3_get_status_name(final_status), final_status);
			goto clean;
		}


		LogInfo(KVSNS_COMPONENT_EXTSTORE, 
			"(multipart) initiating upload upload nparts=%d totsz=%d",
			nparts, total_len);

		/* allocate thread part data */
		thread_part_data_t *thread_data;

		/* TODO: cleanup in case of thread cancellation */
		thread_data = malloc(sizeof(thread_part_data_t) * (
				     nparts < parts_per_block ?
				     nparts : parts_per_block));

		/* setup a thread pool with 4 threads */
		threadpool thpool = thpool_init(nmaxthreads);

		int curpart = 0, curblock = 0;
		for (curblock = 0; curblock < nblocks; ++curblock) {
			int nparts_curblock = (remaining_parts > parts_per_block ? parts_per_block : remaining_parts);

			LogDebug(KVSNS_COMPONENT_EXTSTORE, 
				"starting upload block %d/%d (%d parts)",
				curblock, nblocks, nparts_curblock);

			for (i = 0; i < nparts_curblock; ++i, ++curpart) {

				/* copy thread data */
				thread_data[i].ctx = (S3BucketContext*) ctx;
				thread_data[i].key = key;
				thread_data[i].curpart = curpart;
				thread_data[i].put_props = &put_props;
				thread_data[i].retries = retries;
				thread_data[i].interval = interval;
				thread_data[i].part_len = ((remaining_len > MULTIPART_CHUNK_SIZE)
							   ? MULTIPART_CHUNK_SIZE : remaining_len);
				thread_data[i].etag = &(manager.etags[curpart]);
				thread_data[i].upload_id = manager.upload_id;
				thread_data[i].part_off = curpart * MULTIPART_CHUNK_SIZE;
				thread_data[i].fd = fd;

				final_status = S3StatusOK;

				remaining_len -= MULTIPART_CHUNK_SIZE;

				thpool_add_work(thpool, send_part, &thread_data[i]);
			}

			thpool_wait(thpool);

			LogDebug(KVSNS_COMPONENT_EXTSTORE, 
				"processed upload block %d/%d (%d parts)",
				curblock, nblocks, nparts_curblock);

			remaining_parts -= nparts_curblock;
		}

		LogDebug(KVSNS_COMPONENT_EXTSTORE, "destroying thread pool");

		thpool_destroy(thpool);
		free(thread_data);

		manager.commitstr = g_string_new("<CompleteMultipartUpload>");
		for (curpart = 0; curpart < nparts; ++curpart) {
			g_string_append_printf(manager.commitstr,
					       "<Part><PartNumber>%d</PartNumber>"
					       "<ETag>%s</ETag></Part>",
					       curpart + 1, manager.etags[curpart]);
		}
		g_string_append(manager.commitstr, "</CompleteMultipartUpload>");
		manager.ntowrite = manager.commitstr->len;
		manager.status = S3StatusOK;

		do {
			S3_complete_multipart_upload((S3BucketContext*)ctx,
						     key,
						     &commit_handler,
						     manager.upload_id,
						     manager.commitstr->len,
						     0,
						     req_cfg->timeout,
						     &manager);

			/* Decrement retries and wait 1 second longer */
			--retries;
			++interval;

			final_status = manager.status;

		} while (should_retry(final_status, retries, interval));

		if (final_status != S3StatusOK) {
			LogWarn(KVSNS_COMPONENT_EXTSTORE,
				"error completing multipart upload %s s3sta=%d",
				S3_get_status_name(final_status),
				final_status);
			goto clean;
		}

clean:
		if (manager.upload_id)
			free(manager.upload_id);
		for (curpart = 0; curpart < nparts; curpart++)
			free(manager.etags[curpart]);
		if (manager.commitstr)
			g_string_free(manager.commitstr, TRUE);
		free(manager.etags);
	}

	if (final_status != S3StatusOK) {
		int rc = s3status2posix_error(final_status);
		LogCrit(KVSNS_COMPONENT_EXTSTORE, "error %s s3sta=%d rc=%d",
			S3_get_status_name(final_status),
			final_status,
			rc);
		return rc;
	}
	return 0;
}
