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
#include "extstore_internal.h"
#include "s3_common.h"
#include "pthreadpool.h"


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
	off64_t off;		/* offset where to start read at from fd */
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

		nread = pread64(cb_data->fd, buffer, nread, cb_data->off);
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
	strncpy(*(cb_data->etag), props->eTag, S3_MAX_ETAG_SIZE);
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
	off64_t part_off;		/* offset of the 1st byte for the part */
	size_t curpart;			/* 0-based part sequence number */
	size_t part_len;		/* size of current part */
	char **etag;			/* [OUT] etag to fill upon completion */
	int *set_etag;			/* [OUT] has etag been allocated */
} thread_part_data_t;

typedef struct thread_context_ {
	const char *key;		/* s3 key  */
	S3BucketContext *ctx;		/* s3 bucket context */
	S3PutProperties *put_props;	/* http PUT properties */
	int retries;			/* number of retries */
	int interval;			/* interval between failed replies */
	char *upload_id;		/* previously requested upload id */
	int fd;				/* descriptor of the file to upload */
	volatile int *cancel;		/* [IN/OUT] indicates upload cancellation */
	pthread_rwlock_t *cancel_lock;	/* locks `cancel` access */

	thread_part_data_t *data;
} thread_context_t;

void send_part(void* cb_data_, size_t idx)
{
	int retries, interval, status;
	thread_context_t *tctx;
	multipart_part_data_t part_data;

	S3PutObjectHandler put_obj_handler = {
		{
			&multiparts_resp_props_cb,
			&_put_object_resp_complete_cb
		},
		&put_object_data_cb
	};

	tctx = (thread_context_t *) cb_data_;

	memset(&part_data, 0, sizeof(multipart_part_data_t));
	part_data.etag = tctx->data[idx].etag;
	part_data.put_object_data.nremain = tctx->data[idx].part_len;
	part_data.put_object_data.status = 0;
	part_data.put_object_data.off = tctx->data[idx].part_off;
	part_data.put_object_data.fd = tctx->fd;

	retries = tctx->retries;
	interval = tctx->interval;
	status = S3StatusOK;

	LogDebug(KVSNS_COMPONENT_EXTSTORE,
	         "(multipart) uploading part idx=%lu partnum=%lu partsz=%lu",
	         idx, idx + 1, tctx->data[idx].part_len);

	int should_cancel = 0;

	do {
		/* check for cancellation before starting part upload */
		pthread_rwlock_rdlock(tctx->cancel_lock);
		should_cancel = *(tctx->cancel);
		pthread_rwlock_unlock(tctx->cancel_lock);
		if (should_cancel)
			goto cancelled;

		S3_upload_part(tctx->ctx,
			       tctx->key,
			       tctx->put_props,
			       &put_obj_handler,
			       idx + 1,
			       tctx->upload_id,
			       tctx->data[idx].part_len,
			       NULL,
			       S3_PUT_REQ_TIMEOUT,
			       &part_data);

		/* decrement retries and wait 1 second longer */
		--retries;
		++interval;
		status = part_data.put_object_data.status;

		/* check for cancellation before retrying part upload */
		pthread_rwlock_rdlock(tctx->cancel_lock);
		should_cancel = *(tctx->cancel);
		pthread_rwlock_unlock(tctx->cancel_lock);
		if (should_cancel)
			goto cancelled;

	} while (should_retry(status, retries, interval));

	if (status != S3StatusOK) {

		/* part failure promotes itself to upload failure */
		pthread_rwlock_wrlock(tctx->cancel_lock);
		*(tctx->cancel) = 1;
		pthread_rwlock_unlock(tctx->cancel_lock);

		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			"(multipart) error uploading part: %s s3sta=%d idx=%lu partnum=%d partlen=%d",
			S3_get_status_name(status), status,
			idx, idx + 1, tctx->data[idx].part_len);
	} else  {
		LogDebug(KVSNS_COMPONENT_EXTSTORE,
			 "(multipart) part uploaded idx=%lu partnum=%d partsz=%d",
			 idx, idx + 1, tctx->data[idx].part_len);
	}

	return;

cancelled:
	LogDebug(KVSNS_COMPONENT_EXTSTORE,
	         "(multipart) cancelled part idx=%lu partnum=%d partsz=%d",
	         idx, idx + 1, tctx->data[idx].part_len);
}

int put_object(const S3BucketContext *ctx,
	       const char *key,
	       extstore_s3_req_cfg_t *req_cfg,
	       const char *src_file,
	       const struct stat *posix_stat)
{
	int rc;
	size_t i;
	int fd;
	S3Status final_status;
	uint64_t total_len;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;

	if (!ctx || !key | !req_cfg | !posix_stat)
		return -EINVAL;

	if (src_file) {
		fd = open(src_file, O_RDONLY, "rb");
		if (fd == -1) {
			rc = errno;
			LogCrit(KVSNS_COMPONENT_EXTSTORE,
				"can't open cached file src=%s rc=%d",
				src_file, rc);
			return -rc;
		}

		/* stat the file to get its length */
		struct stat64 statbuf;
		if (stat64(src_file, &statbuf) == -1) {
			rc = errno;
			LogCrit(KVSNS_COMPONENT_EXTSTORE,
				"can't stat cached file, src=%d rc=%d",
				src_file, rc);
			return -rc;
		}
		total_len = statbuf.st_size;

	} else {
		/* no source file, we will 'send' an empty file. Let's abuse the
		 * file descriptor 0, normally reserved for stdin, as we should
		 * never get it from `open`. */
		fd = 0;
		total_len = 0;
	}

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
				      S3_PUT_REQ_TIMEOUT,
				      &put_obj_handler,
				      &cb_data);

			/* Decrement retries and wait 1 second longer */
			--retries;
			++interval;
			final_status = cb_data.status;
		} while (should_retry(final_status, retries, interval));

		if (fd > 0)
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

		/* multipart upload */

		/* number of threads in the pool  */
		const size_t nmaxthreads = req_cfg->upload_nthreads;

		/* the number of parts */
		const size_t nparts = (total_len + MULTIPART_CHUNK_SIZE - 1)
				       / MULTIPART_CHUNK_SIZE;

		size_t remaining_len = total_len;

		upload_mgr_t manager;
		manager.upload_id = NULL;
		manager.commitstr = NULL;
		manager.etags = malloc(sizeof(char *) * nparts);
		for (i = 0; i < nparts; ++i) {
			manager.etags[i] = malloc(sizeof(char) * S3_MAX_ETAG_SIZE);
			memset(manager.etags[i], 0, S3_MAX_ETAG_SIZE);
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
					      &put_props,
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
			"(multipart) initiating upload nthreads=%d nparts=%lu totsz=%lu key=%s",
			req_cfg->upload_nthreads, nparts, total_len, key);

		/* setup a thread pool with `nmaxthreads` threads */
		pthreadpool_t thpool = pthreadpool_create(nmaxthreads);

		/* use a rwlock for cancellation */
		pthread_rwlock_t cancel_lock;
		pthread_rwlock_init(&cancel_lock, NULL);
		volatile int cancel_upload = 0;

		/* allocate thread data array (one per thread) */
		thread_context_t tctx;
		tctx.ctx = (S3BucketContext*) ctx;
		tctx.key = key;
		tctx.put_props = &put_props;
		tctx.retries = retries;
		tctx.interval = interval;
		tctx.upload_id = manager.upload_id;
		tctx.fd = fd;
		tctx.cancel = &cancel_upload;
		tctx.cancel_lock = &cancel_lock;
		tctx.data = malloc(sizeof(thread_part_data_t) * nparts);
		memset(tctx.data, 0, sizeof(thread_part_data_t) * nparts);
		for (i = 0; i < nparts; ++i) {
			tctx.data[i].curpart = i;
			tctx.data[i].part_len = ((remaining_len > MULTIPART_CHUNK_SIZE)
						? MULTIPART_CHUNK_SIZE : remaining_len);
			tctx.data[i].etag = &(manager.etags[i]);
			tctx.data[i].part_off = (off64_t)(i * ((size_t)MULTIPART_CHUNK_SIZE));
			remaining_len -= MULTIPART_CHUNK_SIZE;
		}
		pthreadpool_compute_1d(thpool, send_part, &tctx, nparts);

		final_status = S3StatusOK;

		LogDebug(KVSNS_COMPONENT_EXTSTORE, "destroying thread pool");

		manager.commitstr = g_string_new("<CompleteMultipartUpload>");
		for (i = 0; i < nparts; ++i) {
			g_string_append_printf(manager.commitstr,
					       "<Part><PartNumber>%lu</PartNumber>"
					       "<ETag>%s</ETag></Part>",
					       i + 1, manager.etags[i]);
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
		for (i = 0; i < nparts; i++)
			free(manager.etags[i]);
		if (manager.commitstr)
			g_string_free(manager.commitstr, TRUE);
		free(manager.etags);
		pthreadpool_destroy(thpool);
		if (tctx.data);
			free(tctx.data);
	}

	rc = 0;
	if (final_status != S3StatusOK) {
		rc = s3status2posix_error(final_status);
		LogCrit(KVSNS_COMPONENT_EXTSTORE, "error %s s3sta=%d rc=%d",
			S3_get_status_name(final_status),
			final_status,
			rc);
	}

	/* free md strings */
	for (i = 0; i < S3_POSIX_MD_COUNT; ++i) {
		free((char *) mds[i].name);
		free((char *) mds[i].value);
	}

	return rc;
}

