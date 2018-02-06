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

#include <string.h>
#include <errno.h>
#include <gmodule.h>
#include <libs3.h>
#include "s3_common.h"


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

typedef struct multipart_mgr_ {
	GTree *inodes;
	const S3BucketContext *bucket_ctx;
} multipart_mgr_t;


typedef struct inode_context_ {
	/* for convenience */
	kvsns_ino_t ino;

	/* S3 object key */
	char key[S3_MAX_KEY_SIZE];

	/* request config */
	extstore_s3_req_cfg_t *req_cfg;

	S3PutProperties put_props;

	/* used for initial multipart */
	char * upload_id;

	GPtrArray *etags;
	int next_etags_pos;

	/* used for commit upload */
	growbuffer_t *gb;
	int remaining;
	int status;
} inode_context_t;

typedef struct put_object_callback_data_
{
	/* [OUT] request status */
	int status;
	/* [IN] request configuration */
	const extstore_s3_req_cfg_t *config;
	/* [IN] buffer */
	const void *buffer;
	/* [IN] buffer size */
	size_t buffer_size;

	int no_status;
} put_object_callback_data_t;

typedef struct multipart_part_data_ {
	put_object_callback_data_t put_object_data;
	int seq;
	inode_context_t *inode_ctx;
} multipart_part_data_t;

static multipart_mgr_t _mgr;

gint _inodes_cmp_func (gconstpointer a, gconstpointer b)
{
	if ((kvsns_ino_t) a < (kvsns_ino_t) b)
		return -1;
	if ((kvsns_ino_t) a > (kvsns_ino_t) b)
		return 1;
	return 0;
}

void _standard_free_func(gpointer data)
{
	free(data);
}

void multipart_manager_init(const S3BucketContext *bucket_ctx)
{
	memset(&_mgr, 0, sizeof(multipart_mgr_t));
	_mgr.inodes = g_tree_new (_inodes_cmp_func);
	_mgr.bucket_ctx = bucket_ctx;
}

void multipart_manager_free()
{
	g_tree_destroy(_mgr.inodes);
	memset(&_mgr, 0, sizeof(multipart_mgr_t));
}

/* callbacks */

static void _resp_complete_cb(S3Status status,
		      const S3ErrorDetails *error,
		      void *cb_data_)
{
	inode_context_t *cb_data;
	cb_data = (inode_context_t*) (cb_data_);

	/* set status and log error */
	cb_data->status = status;
	log_response_status_error(status, error);
}

S3Status multiparts_resp_props_cb
(const S3ResponseProperties *props, void *cb_data_)
{
	log_response_properties(props, NULL);

	multipart_part_data_t *cb_data = (multipart_part_data_t *) cb_data;
	int seq = cb_data->seq;

	/* copy the etag */
	const size_t etag_len = strlen(props->eTag);
	char *etag = malloc((etag_len + 1) * sizeof(char));
	memcpy(etag, props->eTag, etag_len);
	etag[etag_len] = '\0';
	g_ptr_array_add(cb_data->inode_ctx->etags, etag);
	cb_data->inode_ctx->next_etags_pos = seq;
	return S3StatusOK;
}

S3Status initial_multipart_callback(const char *upload_id,
				    void *cb_data_)
{
	inode_context_t *inode_ctx = (inode_context_t *) cb_data_;

	/* copy the upload id */
	const size_t upload_id_len = strlen(upload_id);
	inode_ctx->upload_id = malloc((upload_id_len + 1) * sizeof(char));
	memcpy(inode_ctx->upload_id, upload_id, upload_id_len);
	inode_ctx->upload_id[upload_id_len] = '\0';
	return S3StatusOK;
}

/* initiate multipart */

int multipart_inode_init(kvsns_ino_t ino,
			 char *fpath,
			 extstore_s3_req_cfg_t *req_cfg)
{
	int retries;
	int interval;
	int rc;

	inode_context_t *inode_ctx = (inode_context_t*)	g_tree_lookup (_mgr.inodes, (gconstpointer) ino);
	if (inode_ctx) {
		/* if an entry already exists for this inode, we have a problem */
		return -EBUSY;
	}

	/* allocate an inode context and insert it in the tree */
	inode_ctx = (inode_context_t*) malloc(sizeof(inode_context_t));
	memset(inode_ctx, 0, sizeof(inode_context_t));
	g_tree_insert(_mgr.inodes, (gpointer) ino, (gpointer) inode_ctx);
	inode_ctx->ino = ino;
	inode_ctx->upload_id = NULL;
	inode_ctx->gb = 0;
	inode_ctx->etags = g_ptr_array_new_full(16, _standard_free_func);
	inode_ctx->next_etags_pos = 0;
	inode_ctx->req_cfg = req_cfg;
	strncpy(inode_ctx->key, fpath, S3_MAX_KEY_SIZE);

	// div round up
//	int total_seq = ((content_len + S3_MULTIPART_CHUNK_SIZE - 1) /
//		S3_MULTIPART_CHUNK_SIZE);


	/* TODO: we have a problem because both resp_props_cb and resp_complete_cb
	 * expect s3_resp_cb_data to be passed as void*, but that's not the case here...
	*/

	S3MultipartInitialHandler handler = {
		{
			&log_response_properties,
			&_resp_complete_cb
		},
		&initial_multipart_callback
	};

	S3PutProperties *put_props = &inode_ctx->put_props;
	put_props->contentType = PUT_CONTENT_TYPE;
	put_props->md5 = PUT_MD5;
	put_props->cacheControl = PUT_CACHE_CONTROL;
	put_props->contentDispositionFilename = PUT_CONTENT_DISP_FNAME;
	put_props->contentEncoding = PUT_CONTENT_ENCODING;
	put_props->expires = PUT_EXPIRES;
	put_props->cannedAcl = PUT_CANNED_ACL;
	put_props->metaDataCount = PUT_META_PROPS_COUNT;
	put_props->metaData = malloc(S3_MAX_METADATA_COUNT * sizeof(S3NameValue));
	put_props->useServerSideEncryption = PUT_SERVERSIDE_ENCRYPT;

#if 0
	if (upload_id) {
		manager.upload_id = strdup(upload_id);
		manager.remaining = content_len;
		if (!try_get_parts_info(ctx, key, &manager, req_cfg)) {
			fseek(cb_data.infile, -(manager.remaining), 2);
			content_len = manager.remaining;
			goto upload;
		} else
			goto clean;
	}
#endif
	retries = inode_ctx->req_cfg->retries;
	interval = inode_ctx->req_cfg->sleep_interval;

	/* define callback data */
//	struct polymorph_resp_cb_data pm_cb_data;
//	pm_cb_data.status = S3StatusOK;
//	pm_cb_data.req_cfg = req_cfg;
//	pm_cb_data.type = inode_context_cb;
//	pm_cb_data.m_inode = inode_ctx;

	do {
		S3_initiate_multipart((S3BucketContext*)_mgr.bucket_ctx, fpath, 0,
				      &handler, 0,
				      inode_ctx->req_cfg->timeout,
				      inode_ctx);
		/* Decrement retries and wait 1 second longer */
		--retries;
		++interval;
	} while (should_retry(inode_ctx->status, retries, interval));

	S3Status final_status = inode_ctx->status;
	if (inode_ctx->upload_id == NULL || final_status != S3StatusOK) {
		/* can't initiate multipart, clean the inode entry*/
		LogWarn(COMPONENT_EXTSTORE, "s3rc=%d s3sta=%s",
			final_status,
			S3_get_status_name(final_status));

		/* upload id was not set, force unknown error */
		if (final_status == S3StatusOK)
			final_status = S3StatusErrorUnknown;
		multipart_inode_free(ino);
	}

	if (final_status != S3StatusOK) {
		rc = s3status2posix_error(final_status);
		LogCrit(COMPONENT_EXTSTORE, "error s3rc=%d rc=%d",
			final_status, rc);
	}
	return rc;
}

/* upload multipart chunk */

static int put_object_data_cb(int bufsize,
			      char *buffer,
			      void *cb_data_)
{
	put_object_callback_data_t *cb_data =
	(put_object_callback_data_t *) cb_data_;

	int ret = 0;

	if (cb_data->buffer_size) {
		int to_read = ((cb_data->buffer_size > (size_t) bufsize) ?
				(size_t) bufsize : cb_data->buffer_size);
		memcpy(buffer, cb_data->buffer, to_read);
		cb_data->buffer_size -= to_read;
	}

	if (cb_data->buffer_size && !cb_data->no_status) {
		LogDebug(COMPONENT_EXTSTORE,
			 "uploading chunks ...\n");
	}

	return ret;
}

//continuer ici l'upload chunk

int multipart_inode_upload_chunk(kvsns_ino_t ino,
				 size_t chunkidx,
				 void *buffer,
				 size_t buffer_size)
{
	inode_context_t *inode_ctx = (inode_context_t*)	g_tree_lookup (_mgr.inodes, (gconstpointer) ino);
	if (inode_ctx)
		return -ENOENT;

	put_object_callback_data_t cb_data;

	cb_data.no_status = 0;
	cb_data.status = S3StatusOK;
	cb_data.config = inode_ctx->req_cfg;
	cb_data.buffer = buffer;
	cb_data.buffer_size = buffer_size;
//	cb_data.total_org_content_len = content_len;
//	cb_data.content_len = content_len;
//	cb_data.org_content_len = content_len;

	multipart_part_data_t part_data;
	int part_content_len = 0;

	memset(&part_data, 0, sizeof(multipart_part_data_t));
	part_data.inode_ctx = inode_ctx;
	part_data.seq = chunkidx + 1; /* upload id starts from 1 */
	part_data.put_object_data = cb_data;
	part_content_len = ((buffer_size > MULTIPART_CHUNK_SIZE) ?
		MULTIPART_CHUNK_SIZE : buffer_size);

	LogDebug(COMPONENT_EXTSTORE,
		"sending part=%d partlen=%d", chunkidx + 1, part_content_len);
	part_data.put_object_data.buffer = buffer;
	part_data.put_object_data.buffer_size = buffer_size;
	inode_ctx->put_props.md5 = 0;


	S3PutObjectHandler put_obj_handler = {
		{
			&multiparts_resp_props_cb,
			&_resp_complete_cb
		},
		&put_object_data_cb
	};

	int retries = inode_ctx->req_cfg->retries;
	int interval = inode_ctx->req_cfg->sleep_interval;

	do {
		S3_upload_part((S3BucketContext*)_mgr.bucket_ctx,
			       inode_ctx->key,
			       &inode_ctx->put_props,
			       &put_obj_handler,
			       part_data.seq,
			       inode_ctx->upload_id,
			       part_content_len,
			       0,
			       inode_ctx->req_cfg->timeout,
			       &part_data);

	} while (should_retry(cb_data.status, retries, interval));

	if (cb_data.status != S3StatusOK) {
		LogWarn(COMPONENT_EXTSTORE, "s3rc=%d s3sta=%s",
			cb_data.status,
			S3_get_status_name(cb_data.status));
		goto clean;
	}
//	content_len -= S3_MULTIPART_CHUNK_SIZE;
//	todo_content_len -= S3_MULTIPART_CHUNK_SIZE;
clean:
	/* TODO: implement cleaning */

	return 0;
}

/* complete multipart */

int multipart_put_xml_cb(int bufsize, char *buffer,
			 void *cb_data_)
{
	inode_context_t *inode_ctx = (inode_context_t*)	cb_data_;

	int ret = 0;
	if (inode_ctx->remaining) {
		int to_read = ((inode_ctx->remaining > bufsize) ?
		bufsize : inode_ctx->remaining);
		printf("what should we do with to_read=%d\n", to_read);

		/* TODO: continuer */

//		growbuffer_read(&(mgr->gb), to_read, &ret, buffer);
	}
	inode_ctx->remaining -= ret;
	return ret;
}

int multipart_inode_complete(kvsns_ino_t ino)
{
	inode_context_t *inode_ctx = (inode_context_t*)	g_tree_lookup (_mgr.inodes, (gconstpointer) ino);
	if (inode_ctx)
		return -ENOENT;

	S3MultipartCommitHandler commit_handler = {
		{
			&log_response_properties,
			&_resp_complete_cb
		},
		&multipart_put_xml_cb,
		0
	};

	int i;
	int size = 0;
	GString *gstr = g_string_new("<CompleteMultipartUpload>");

//	for (i = 0; i < total_seq; i++) {
	for (i = 0; i < inode_ctx->next_etags_pos; i++) {
		g_string_append_printf(gstr,
			"<Part><PartNumber>%d</PartNumber>"
			"<ETag>%s</ETag></Part>",
			i + 1, (char*) g_ptr_array_index(inode_ctx->etags, i));
	}
	g_string_append(gstr, "</CompleteMultipartUpload>");
	inode_ctx->remaining = size;

	/*XXX: here the callback should have set the status, but which
	* callback and in which callback_data?
	* */
	int fake_statusG = 1;
	int retries = inode_ctx->req_cfg->retries;
	int interval = inode_ctx->req_cfg->sleep_interval;
	do {
		S3_complete_multipart_upload(
				(S3BucketContext*)_mgr.bucket_ctx,
				inode_ctx->key,
				&commit_handler,
				inode_ctx->upload_id,
				inode_ctx->remaining,
				0,
				inode_ctx->req_cfg->timeout,
				&inode_ctx);
		/* Decrement retries and wait 1 second longer */
		--retries;
		++interval;
	} while (should_retry(fake_statusG, retries, interval));

	if (fake_statusG != S3StatusOK) {
		LogWarn(COMPONENT_EXTSTORE, "s3rc=%d s3sta=%s",
			fake_statusG,
			S3_get_status_name(fake_statusG));

		goto clean;
	}

clean: /* TODO: implement cleaning */

	return 0;
}

int multipart_inode_free(kvsns_ino_t ino)
{
	inode_context_t *inode_ctx = (inode_context_t*)	g_tree_lookup (_mgr.inodes, (gconstpointer) ino);
	if (inode_ctx)
		return -ENOENT;
	/* free the etags array */
	if (inode_ctx->etags) {
		g_ptr_array_free(inode_ctx->etags, TRUE);
		inode_ctx->etags = NULL;
	}

	/* free the upload id copy */
	if (inode_ctx->upload_id) {
		free(inode_ctx->upload_id);
		inode_ctx->upload_id = NULL;
	}

	/* free array of metadata properties */
	if (inode_ctx->put_props.metaData) {
		free((void*) inode_ctx->put_props.metaData);
	}
	return 0;
}
