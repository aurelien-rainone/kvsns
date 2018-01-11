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
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include "s3_methods.h"
#include "internal.h"


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

struct s3_resp_cb_data {
	/*< [OUT] request status */
	int status;
	/*< [IN] request configuration */
	const extstore_s3_req_cfg_t *config;

	/*< [IN] should callback set stats? */
	int should_set_stats;
	time_t mtime;	/*< [OUT mtime to be by callback */
	uint64_t size;	/*< [OUT] size to be by callback */
};

static char errorDetailsG[4096] = { 0 };
void printError(S3Status status)
{
	fprintf(stderr, "\nERROR: %s\n", S3_get_status_name(status));
	if (status >= S3StatusErrorAccessDenied)
		fprintf(stderr, "%s\n", errorDetailsG);
}

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

	if (cb_data->config->log_props) {

#define PRINT_PROP(name, field) ({\
		if (props->field) printf("%s: %s\n", name, props->field); })

		PRINT_PROP("Content-Type", contentType);
		PRINT_PROP("Request-Id", requestId);
		PRINT_PROP("Request-Id-2", requestId2);
		if (props->contentLength > 0)
			printf("Content-Length: %llu\n",
			       (unsigned long long) props->contentLength);
		PRINT_PROP("Server", server);
		PRINT_PROP("ETag", eTag);
		if (props->lastModified > 0) {
			char timebuf[256];
			time_t t = (time_t) props->lastModified;
			/* gmtime is not thread-safe but we don't care here. */
			strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ",
				 gmtime(&t));
			printf("Last-Modified: %s\n", timebuf);
		}
		int i;
		for (i = 0; i < props->metaDataCount; i++) {
			printf("x-amz-meta-%s: %s\n",
			       props->metaData[i].name,
			       props->metaData[i].value);
		}
		if (props->usesServerSideEncryption)
			printf("UsesServerSideEncryption: true\n");
	}

	if (cb_data->should_set_stats) {
                cb_data->mtime = (time_t) props->lastModified;
                cb_data->size = (uint64_t) props->contentLength;

		printf("%s set_stats=1 mtime=%lu size=%lu\n",
		       __func__,
		       cb_data->mtime,
		       cb_data->size);
	}

	return S3StatusOK;
}

/**
 * This callbacks saves the status and prints extra informations.
 */
void resp_complete_cb(S3Status status,
		      const S3ErrorDetails *error,
		      void *cb_data_)
{
	int i;
	struct s3_resp_cb_data *cb_data;
	cb_data = (struct s3_resp_cb_data*) (cb_data_);

	/* set status */
	cb_data->status = status;

	if (status == S3StatusOK)
		printf("%s Successful request, res=%s\n",
		       __func__, error->resource);
	else if (error) {
		if (error->message)
			printf("%s Message: %s\n", __func__, error->message);
		if (error->resource)
			printf("%s Resource: %s\n", __func__, error->resource);
		if (error->furtherDetails)
			printf("%s Further details: %s\n",
			       __func__, error->furtherDetails);
		if (error->extraDetailsCount)
			for (i = 0; i < error->extraDetailsCount; i++)
				printf("%s Extra details: %s->%s\n",
				       __func__,
				       error->extraDetails[i].name,
				       error->extraDetails[i].value);
	}
}

S3Status test_bucket(const S3BucketContext *ctx,
		     extstore_s3_req_cfg_t *req_cfg)
{
	char location[64];
	struct s3_resp_cb_data cb_data;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;

	/* define callback data */
	cb_data.status = S3StatusOK;
	cb_data.config = req_cfg;
	cb_data.should_set_stats = 0;

	/* define callbacks */
	S3ResponseHandler resp_handler = {
		&resp_props_cb,
		&resp_complete_cb
	};

	printf("%s bkt=%s\n", __func__, ctx->bucketName);

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

	/* TODO: log errors */
	return cb_data.status;
}

typedef struct upload_mgr_{
	/* used for initial multipart */
	char * upload_id;

	/* used for upload part object */
	char **etags;
	int next_etags_pos;

	/* used for commit Upload */
	growbuffer_t *gb;
	int remaining;
} upload_mgr_t;

typedef struct list_parts_callback_data_
{
	int status;
	int is_truncated;
	char next_partnum_marker[24];
	char initiator_id[256];
	char initiator_name[256];
	char owner_id[256];
	char owner_name[256];
	char storage_cls[256];
	int num_parts;
	int handle_parts_start;
	int all_details;
	int no_print;
	upload_mgr_t *manager;
} list_parts_callback_data_t;

void print_list_multipart_hdr(int all_details)
{
	(void)all_details;
}

void print_list_parts_hdr()
{
	printf("%-25s  %-30s  %-30s   %-15s",
	       "LastModified",
	       "PartNumber", "ETag", "SIZE");

	printf("\n");
	printf("---------------------  "
	       "    -------------    "
	       "-------------------------------  "
	       "               -----");
	printf("\n");
}

S3Status list_parts_cb(int is_truncated,
		      const char *next_partnum_marker,
		      const char *initiator_id,
		      const char *initiator_name,
		      const char *owner_id,
		      const char *owner_name,
		      const char *storage_cls,
		      int num_parts,
		      int handle_parts_start,
		      const S3ListPart *parts,
		      void *cb_data_)
{
	list_parts_callback_data_t *cb_data =
		(list_parts_callback_data_t *) cb_data_;

	cb_data->is_truncated = is_truncated;
	cb_data->handle_parts_start = handle_parts_start;
	upload_mgr_t *manager = cb_data->manager;

	if (next_partnum_marker) {
		snprintf(cb_data->next_partnum_marker,
		sizeof(cb_data->next_partnum_marker), "%s",
		next_partnum_marker);
	} else {
		cb_data->next_partnum_marker[0] = 0;
	}

	if (initiator_id) {
		snprintf(cb_data->initiator_id,
			 sizeof(cb_data->initiator_id),
			 "%s", initiator_id);
	} else {
		cb_data->initiator_id[0] = 0;
	}

	if (initiator_name) {
		snprintf(cb_data->initiator_name,
			 sizeof(cb_data->initiator_name),
			 "%s", initiator_name);
	} else {
		cb_data->initiator_name[0] = 0;
	}

	if (owner_id) {
		snprintf(cb_data->owner_id,
			 sizeof(cb_data->owner_id),
			 "%s", owner_id);
	} else {
		cb_data->owner_id[0] = 0;
	}

	if (owner_name) {
		snprintf(cb_data->owner_name,
			 sizeof(cb_data->owner_name),
			 "%s", owner_name);
	} else {
		cb_data->owner_name[0] = 0;
	}

	if (storage_cls) {
		snprintf(cb_data->storage_cls,
			 sizeof(cb_data->storage_cls),
			 "%s", storage_cls);
	} else {
		cb_data->storage_cls[0] = 0;
	}

	if (num_parts && !cb_data->num_parts && !cb_data->no_print)
		print_list_parts_hdr();

	int i;
	for (i = 0; i < num_parts; i++) {
		const S3ListPart *part = &(parts[i]);
		char timebuf[256];
		if (cb_data->no_print) {
			manager->etags[handle_parts_start+i] = strdup(part->eTag);
			manager->next_etags_pos++;
			manager->remaining = manager->remaining - part->size;
		} else {
			time_t t = (time_t) part->lastModified;
			strftime(timebuf, sizeof(timebuf), "%Y-%m-%dT%H:%M:%SZ",
				 gmtime(&t));
			printf("%-30s", timebuf);
			printf("%-15llu", (unsigned long long) part->partNumber);
			printf("%-45s", part->eTag);
			printf("%-15llu\n", (unsigned long long) part->size);
		}
	}

	cb_data->num_parts += num_parts;

	return S3StatusOK;
}

typedef struct put_object_callback_data_
{
	/*< [OUT] request status */
	int status;
	/*< [IN] request configuration */
	const extstore_s3_req_cfg_t *config;

	FILE *infile;
	growbuffer_t *gb;
	uint64_t content_len, org_content_len;
	uint64_t total_content_len, total_org_content_len;
	int no_status;
} put_object_callback_data_t;

static int putObjectDataCallback(int bufsize, char *buffer,
				 void *cb_data_)
{
	put_object_callback_data_t *cb_data =
	(put_object_callback_data_t *) cb_data_;

	int ret = 0;

	if (cb_data->content_len) {
		int to_read = ((cb_data->content_len > (unsigned) bufsize) ?
				(unsigned) bufsize : cb_data->content_len);
		if (cb_data->gb)
			growbuffer_read(&(cb_data->gb), to_read, &ret, buffer);
		else if (cb_data->infile)
			ret = fread(buffer, 1, to_read, cb_data->infile);
	}

	cb_data->content_len -= ret;
	cb_data->total_content_len -= ret;

	if (cb_data->content_len && !cb_data->no_status) {
		printf("%llu bytes remaining (%d%% complete) ...\n",
		      (unsigned long long) cb_data->total_content_len,
		      (int) (((cb_data->total_org_content_len -
			cb_data->total_content_len) * 100) /
			cb_data->total_org_content_len));
	}

	return ret;
}

#define MULTIPART_CHUNK_SIZE (15 << 20) // multipart is 15M

typedef struct multipart_part_data_ {
	put_object_callback_data_t put_object_data;
	int seq;
	upload_mgr_t *manager;
} multipart_part_data_t;

S3Status initial_multipart_callback(const char * upload_id,
				    void * cb_data_)
{
	upload_mgr_t *mgr = (upload_mgr_t *) cb_data_;
	mgr->upload_id = strdup(upload_id);
	return S3StatusOK;
}

S3Status multiparts_resp_props_cb
(const S3ResponseProperties *props, void *cb_data_)
{
	resp_props_cb(props, cb_data_);
	multipart_part_data_t *cb_data = (multipart_part_data_t *) cb_data;
	int seq = cb_data->seq;
	const char *etag = props->eTag;
	cb_data->manager->etags[seq - 1] = strdup(etag);
	cb_data->manager->next_etags_pos = seq;
	return S3StatusOK;
}

int multipart_put_xml_cb(int bufsize, char *buffer,
			    void *cb_data_)
{
	upload_mgr_t *mgr = (upload_mgr_t*)cb_data_;
	int ret = 0;
	if (mgr->remaining) {
		int to_read = ((mgr->remaining > bufsize) ?
		bufsize : mgr->remaining);
		growbuffer_read(&(mgr->gb), to_read, &ret, buffer);
	}
	mgr->remaining -= ret;
	return ret;
}

int try_get_parts_info(const S3BucketContext *ctx, const char *key,
		       upload_mgr_t *manager, extstore_s3_req_cfg_t *req_cfg)
{
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;
	S3ListPartsHandler list_parts_handler = {
		{
			&resp_props_cb,
			&resp_complete_cb
		},
		&list_parts_cb
	};

	list_parts_callback_data_t cb_data;

	memset(&cb_data, 0, sizeof(list_parts_callback_data_t));

	cb_data.num_parts = 0;
	cb_data.all_details = 0;
	cb_data.manager = manager;
	cb_data.no_print = 1;
	cb_data.status = S3StatusOK;
	do {
		cb_data.is_truncated = 0;
		do {
			S3_list_parts((S3BucketContext*)ctx, key,
				      cb_data.next_partnum_marker,
				      manager->upload_id, 0, 0, 0,
				      req_cfg->timeout, &list_parts_handler,
				      &cb_data);

			/* Decrement retries and wait 1 second longer */
			--retries;
			++interval;
		} while (should_retry(cb_data.status, retries, interval));
			if (cb_data.status != S3StatusOK)
				break;
	} while (cb_data.is_truncated);

	if (cb_data.status == S3StatusOK) {
		if (!cb_data.num_parts)
			print_list_multipart_hdr(cb_data.all_details);
	} else {
		printError(cb_data.status);
		return -1;
	}

	return 0;
}

S3Status put_object(const S3BucketContext *ctx, const char *key,
		    extstore_s3_req_cfg_t *req_cfg,
		    const char *buf, size_t buflen)
{
	const char *upload_id = 0;
	/*const char *filename = 0;*/
	uint64_t content_len = buflen;
	S3NameValue meta_properties[S3_MAX_METADATA_COUNT];
	int no_status = 0;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;

	put_object_callback_data_t cb_data;

	cb_data.infile = 0;
	cb_data.gb = 0;
	cb_data.no_status = no_status;
	cb_data.status = S3StatusOK;
	cb_data.config = req_cfg;

	/* Read from stdin. If contentLength is not provided, we have to read it
	 * all in to get contentLength.
	 */
	if (content_len) {

		/* copy the buffer data to the grow buffer */
		if (!growbuffer_append(&(cb_data.gb), buf, buflen)) {
			printf("ERROR: Out of memory creating PUT buffer\n");
			       return S3StatusOutOfMemory;
		}
		content_len = buflen;
	}

	cb_data.total_content_len = content_len;
	cb_data.total_org_content_len = content_len;
	cb_data.content_len = content_len;
	cb_data.org_content_len = content_len;

	S3PutProperties put_props =
	{
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

	if (content_len <= MULTIPART_CHUNK_SIZE) {
		S3PutObjectHandler put_obj_handler = {
			{
				&resp_props_cb,
				&resp_complete_cb
			},
			&putObjectDataCallback
		};

		do {
			S3_put_object(ctx, key, content_len, &put_props,
				      0, 0, &put_obj_handler, &cb_data);

			/* Decrement retries and wait 1 second longer */
			--retries;
			++interval;
		} while (should_retry(cb_data.status, retries, interval));

		if (cb_data.infile)
			fclose(cb_data.infile);
		else if (cb_data.gb)
			growbuffer_destroy(cb_data.gb);

		if (cb_data.status != S3StatusOK) {
			printf("%s status=%d err=%s\n", __func__,
			       cb_data.status, S3_get_status_name(cb_data.status));
		} else if (cb_data.content_len) {
			fprintf(stderr,
				"\nERROR: Failed to read remaining %llu bytes from input\n",
				(unsigned long long) cb_data.content_len);
		}

	} else {

		uint64_t total_content_len = content_len;
		uint64_t todo_content_len = content_len;
		upload_mgr_t manager;
		manager.upload_id = 0;
		manager.gb = 0;

		//div round up
		int seq;
		int total_seq = ((content_len + MULTIPART_CHUNK_SIZE- 1) /
			MULTIPART_CHUNK_SIZE);

		multipart_part_data_t part_data;
		int part_content_len = 0;

		S3MultipartInitialHandler handler = {
			{
				&resp_props_cb,
				&resp_complete_cb
			},
			&initial_multipart_callback
		};

		S3PutObjectHandler put_obj_handler = {
			{
				&multiparts_resp_props_cb,
				&resp_complete_cb
			},
			&putObjectDataCallback
		};

		S3MultipartCommitHandler commit_handler = {
			{
				&resp_props_cb,
				&resp_complete_cb
			},
			&multipart_put_xml_cb,
			0
		};

		manager.etags = (char **) malloc(sizeof(char *) * total_seq);
		manager.next_etags_pos = 0;

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

		/*XXX: AR here the callback should have set the status, but which
		* callback and in which callback_data?
		* */
		int fake_statusG = 1;
		do {
			S3_initiate_multipart((S3BucketContext*)ctx, key, 0,
					      &handler, 0,
					      req_cfg->timeout,
					      &manager);
			/* Decrement retries and wait 1 second longer */
			--retries;
			++interval;
		} while (should_retry(fake_statusG, retries, interval));

		if (manager.upload_id == 0 || fake_statusG != S3StatusOK) {
			printError(fake_statusG);
			goto clean;
		}

upload:
		todo_content_len -= MULTIPART_CHUNK_SIZE * manager.next_etags_pos;
		for (seq = manager.next_etags_pos + 1; seq <= total_seq; seq++) {
			memset(&part_data, 0, sizeof(multipart_part_data_t));
			part_data.manager = &manager;
			part_data.seq = seq;
			part_data.put_object_data = cb_data;
			part_content_len = ((content_len > MULTIPART_CHUNK_SIZE) ?
				MULTIPART_CHUNK_SIZE : content_len);
			printf("%s Part Seq %d, length=%d\n", "Sending", seq, part_content_len);
			part_data.put_object_data.content_len = part_content_len;
			part_data.put_object_data.org_content_len = part_content_len;
			part_data.put_object_data.total_content_len = todo_content_len;
			part_data.put_object_data.total_org_content_len = total_content_len;
			put_props.md5 = 0;
			do {
				S3_upload_part((S3BucketContext*)ctx, key, &put_props,
				&put_obj_handler, seq, manager.upload_id,
				part_content_len,
				0, req_cfg->timeout,
				&part_data);
				/*}*/
			} while (should_retry(cb_data.status, retries, interval));
			if (cb_data.status != S3StatusOK) {
				printError(cb_data.status);
				goto clean;
			}
			content_len -= MULTIPART_CHUNK_SIZE;
			todo_content_len -= MULTIPART_CHUNK_SIZE;
		}

		int i;
		int size = 0;
		size += growbuffer_append(&(manager.gb), "<CompleteMultipartUpload>",
		strlen("<CompleteMultipartUpload>"));
		char buf[256];
		int n;
		for (i = 0; i < total_seq; i++) {
			n = snprintf(buf, sizeof(buf),
				     "<Part><PartNumber>%d</PartNumber>"
				     "<ETag>%s</ETag></Part>",
				     i + 1, manager.etags[i]);
			size += growbuffer_append(&(manager.gb), buf, n);
		}
		size += growbuffer_append(&(manager.gb), "</CompleteMultipartUpload>",
					  strlen("</CompleteMultipartUpload>"));
		manager.remaining = size;

		/*XXX: here the callback should have set the status, but which
		* callback and in which callback_data?
		* */
		fake_statusG = 1;
		do {
			S3_complete_multipart_upload((S3BucketContext*)ctx, key, &commit_handler,
			manager.upload_id, manager.remaining,
			0, req_cfg->timeout, &manager);
			/* Decrement retries and wait 1 second longer */
			--retries;
			++interval;
		} while (should_retry(fake_statusG, retries, interval));

		if (fake_statusG != S3StatusOK) {
			printError(fake_statusG);
			goto clean;
		}

clean:
		if (manager.upload_id)
			free(manager.upload_id);
		for (i = 0; i < manager.next_etags_pos; i++)
			free(manager.etags[i]);
		growbuffer_destroy(manager.gb);
		free(manager.etags);
	}

	/* TODO: AR check if this is always the correct status*/
	return cb_data.status;
}

S3Status stats_object(const S3BucketContext *ctx,
		      const char *key,
		      extstore_s3_req_cfg_t *req_cfg,
		      time_t *mtime, uint64_t *size)
{
	struct s3_resp_cb_data cb_data;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;

	/* define callback data */
	cb_data.status = S3StatusOK;
	cb_data.config = req_cfg;
	cb_data.should_set_stats = 1;

	/* define callbacks */
	S3ResponseHandler resp_handler = {
		&resp_props_cb,
		&resp_complete_cb
	};

	do {
		S3_head_object(ctx, key, 0, 0, &resp_handler, &cb_data);
		/* Decrement retries and wait 1 second longer */
		--retries;
		++interval;
	} while (should_retry(cb_data.status, retries, interval));

	/* TODO: log errors */
	return cb_data.status;
}