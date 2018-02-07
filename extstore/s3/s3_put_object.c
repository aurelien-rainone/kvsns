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

#include "internal.h"
#include "s3_common.h"


#define MULTIPART_CHUNK_SIZE S3_MULTIPART_CHUNK_SIZE

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
	/* used for initial multipart */
	char * upload_id;

	/* used for upload part object */
	char **etags;
	int next_etags_pos;

	/* used for commit Upload */
	GString *commitstr;
	int remaining;
	S3Status status;
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
			LogDebug(COMPONENT_EXTSTORE, "%-30s", timebuf);
			LogDebug(COMPONENT_EXTSTORE, "%-15llu", (unsigned long long) part->partNumber);
			LogDebug(COMPONENT_EXTSTORE, "%-45s", part->eTag);
			LogDebug(COMPONENT_EXTSTORE, "%-15llu\n", (unsigned long long) part->size);
		}
	}

	cb_data->num_parts += num_parts;

	return S3StatusOK;
}

typedef struct put_object_callback_data_
{
	FILE *infile;
	growbuffer_t *gb;
	uint64_t content_len, org_content_len;
	uint64_t total_content_len, total_org_content_len;
	/*< [OUT] request status */
	S3Status status;
	/*< [IN] request configuration */
	const extstore_s3_req_cfg_t *config;

} put_object_callback_data_t;

static int put_object_data_cb(int bufsize, char *buffer, void *cb_data_)
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

	if (cb_data->content_len) {
		LogDebug(COMPONENT_EXTSTORE,
			 "%llu bytes remaining (%d%% complete)",
			 (unsigned long long) cb_data->total_content_len,
			 (int) (((cb_data->total_org_content_len -
				  cb_data->total_content_len) * 100) /
				  cb_data->total_org_content_len));
	} else {
		LogInfo(COMPONENT_EXTSTORE,
			"0 bytes remaining (100%% complete) ...");
	}

	return ret;
}

typedef struct multipart_part_data_ {
	put_object_callback_data_t put_object_data;
	int seq;
	upload_mgr_t *manager;
} multipart_part_data_t;

S3Status initial_multipart_callback(const char * upload_id,
				    void * cb_data_)
{
	upload_mgr_t *cb_data;
	cb_data = (upload_mgr_t *) cb_data_;
	cb_data->upload_id = strdup(upload_id);
	return S3StatusOK;
}

S3Status multiparts_resp_props_cb(const S3ResponseProperties *props,
				  void *cb_data_)
{
	log_response_properties(props, cb_data_);
	multipart_part_data_t *cb_data;
	cb_data = (multipart_part_data_t *) cb_data_;
	int seq = cb_data->seq;
	const char *etag = props->eTag;
	cb_data->manager->etags[seq - 1] = strdup(etag);
	cb_data->manager->next_etags_pos = seq;
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

void _list_part_resp_complete_cb(S3Status status,
		      const S3ErrorDetails *error,
		      void *cb_data_)
{
	struct list_parts_callback_data_ *cb_data;
	cb_data = (struct list_parts_callback_data_*) (cb_data_);

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
	int ret;
	if (cb_data->remaining) {
		int to_read = ((cb_data->remaining > bufsize) ?
				bufsize : cb_data->remaining);
		int stridx = cb_data->commitstr->len - to_read;
		memcpy(buffer, cb_data->commitstr->str + stridx, to_read);
		ret = to_read;
	}
	cb_data->remaining -= ret;
	return ret;
}

int try_get_parts_info(const S3BucketContext *ctx,
		       const char *key,
		       upload_mgr_t *manager,
		       extstore_s3_req_cfg_t *req_cfg)
{
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;

	S3ListPartsHandler list_parts_handler = {
		{
			&log_response_properties,
			&_list_part_resp_complete_cb
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
		LogWarn(COMPONENT_EXTSTORE, "s3rc=%d s3sta=%s", cb_data.status,
			S3_get_status_name(cb_data.status));
		return -1;
	}

	return 0;
}

int put_object(const S3BucketContext *ctx,
	       const char *key,
	       extstore_s3_req_cfg_t *req_cfg,
	       const char *src_file)
{
	int rc;
	uint64_t content_len;
	S3NameValue meta_properties[S3_MAX_METADATA_COUNT];
	S3Status final_status;
	int retries = req_cfg->retries;
	int interval = req_cfg->sleep_interval;

	put_object_callback_data_t cb_data;

	cb_data.infile = fopen(src_file, "rb");
	if (!cb_data.infile) {
		rc = errno;
		LogCrit(COMPONENT_EXTSTORE,
			"can't open stream from source file src=%s rc=%d",
			src_file, rc);
		return -rc;
	}

	/* stat the file to get its length */
	struct stat statbuf;
	if (stat(src_file, &statbuf) == -1) {
		rc = errno;
		LogCrit(COMPONENT_EXTSTORE,
			"can't stat source file, src=%d rc=%d",
			src_file, rc);
		return -rc;
	}

	content_len = statbuf.st_size;
	cb_data.total_content_len = content_len;
	cb_data.total_org_content_len = content_len;
	cb_data.content_len = content_len;
	cb_data.org_content_len = content_len;
	cb_data.gb = 0;
	cb_data.status = S3StatusOK;
	cb_data.config = req_cfg;

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

	if (content_len <= MULTIPART_CHUNK_SIZE) {
		S3PutObjectHandler put_obj_handler = {
			{
				&log_response_properties,
				&_put_object_resp_complete_cb
			},
			&put_object_data_cb
		};

		do {
			S3_put_object(ctx, key, content_len, &put_props,
				      0, 0, &put_obj_handler, &cb_data);

			/* Decrement retries and wait 1 second longer */
			--retries;
			++interval;
			final_status = cb_data.status;
		} while (should_retry(final_status, retries, interval));

		if (cb_data.infile)
			fclose(cb_data.infile);
		else if (cb_data.gb)
			growbuffer_destroy(cb_data.gb);

		if (final_status != S3StatusOK) {
			LogCrit(COMPONENT_EXTSTORE, "s3rc=%d s3sta=%s",
			       cb_data.status, S3_get_status_name(final_status));
		} else if (cb_data.content_len) {
			LogCrit(COMPONENT_EXTSTORE,
				"Failed to read remaining %llu bytes from input",
				(unsigned long long) cb_data.content_len);
		}

	} else {

		upload_mgr_t manager;
		uint64_t total_content_len = content_len;
		uint64_t todo_content_len = content_len;
		multipart_part_data_t part_data;
		int part_content_len = 0;
		int seq;
		const int total_seq = (content_len
			+ MULTIPART_CHUNK_SIZE - 1)
				/ MULTIPART_CHUNK_SIZE;

		manager.upload_id = 0;
		manager.commitstr = NULL;
		manager.etags = (char **) malloc(sizeof(char *) * total_seq);
		manager.next_etags_pos = 0;
		manager.status = S3StatusOK;

		S3MultipartInitialHandler handler = {
			{
				&log_response_properties,
				&_manager_resp_complete_cb
			},
			&initial_multipart_callback
		};

		S3PutObjectHandler put_obj_handler = {
			{
				&multiparts_resp_props_cb,
				&_put_object_resp_complete_cb
			},
			&put_object_data_cb
		};

		S3MultipartCommitHandler commit_handler = {
			{
				&log_response_properties,
				&_manager_resp_complete_cb
			},
			&multipart_put_xml_cb,
			0
		};


		do {
			S3_initiate_multipart((S3BucketContext*)ctx, key, 0,
					      &handler, 0,
					      req_cfg->timeout,
					      &manager);
			/* Decrement retries and wait 1 second longer */
			--retries;
			++interval;
			final_status = manager.status;
		} while (should_retry(final_status, retries, interval));

		if (manager.upload_id == 0 || final_status != S3StatusOK) {
			LogWarn(COMPONENT_EXTSTORE, "s3rc=%d s3sta=%s",
				final_status,
				S3_get_status_name(final_status));
			goto clean;
		}

		todo_content_len -= MULTIPART_CHUNK_SIZE * manager.next_etags_pos;
		for (seq = manager.next_etags_pos + 1; seq <= total_seq; seq++) {
			memset(&part_data, 0, sizeof(multipart_part_data_t));
			part_data.manager = &manager;
			part_data.seq = seq;
			part_data.put_object_data = cb_data;
			part_content_len = ((content_len > MULTIPART_CHUNK_SIZE) ?
				MULTIPART_CHUNK_SIZE : content_len);

			LogDebug(COMPONENT_EXTSTORE,
				"sending part=%d partlen=%d", seq, part_content_len);
			part_data.put_object_data.content_len = part_content_len;
			part_data.put_object_data.org_content_len = part_content_len;
			part_data.put_object_data.total_content_len = todo_content_len;
			part_data.put_object_data.total_org_content_len = total_content_len;
			put_props.md5 = 0;
			retries = req_cfg->retries;
			interval = req_cfg->sleep_interval;


			/* TODO: Aurélien this is just a test for developement, we'll need to find
			 * a value that suits all needs */
			req_cfg->timeout = 10000000;

			do {
				S3_upload_part((S3BucketContext*)ctx,
					       key,
					       &put_props,
					       &put_obj_handler,
					       seq,
					       manager.upload_id,
					       part_content_len,
					       0,
					       req_cfg->timeout,
					       &part_data);

				/* Decrement retries and wait 1 second longer */
				--retries;
				++interval;
				final_status = part_data.put_object_data.status;

			} while (should_retry(final_status, retries, interval));

			if (final_status != S3StatusOK) {
				LogWarn(COMPONENT_EXTSTORE, "s3rc=%d s3sta=%s",
					final_status,
					S3_get_status_name(final_status));
				goto clean;
			}
			content_len -= MULTIPART_CHUNK_SIZE;
			todo_content_len -= MULTIPART_CHUNK_SIZE;
		}

		int i;
		manager.commitstr = g_string_new("<CompleteMultipartUpload>");
		for (i = 0; i < total_seq; i++) {
			g_string_append_printf(manager.commitstr,
					       "<Part><PartNumber>%d</PartNumber>"
					       "<ETag>%s</ETag></Part>",
					       i + 1, manager.etags[i]);
		}
		g_string_append(manager.commitstr, "</CompleteMultipartUpload>");
		manager.remaining = manager.commitstr->len;
		manager.status = S3StatusOK;

		do {
			S3_complete_multipart_upload((S3BucketContext*)ctx,
						     key,
						     &commit_handler,
						     manager.upload_id,
						     manager.remaining,
						     0,
						     req_cfg->timeout,
						     &manager);

			/* Decrement retries and wait 1 second longer */
			--retries;
			++interval;

			final_status = manager.status;

		} while (should_retry(final_status, retries, interval));

		if (final_status != S3StatusOK) {
			LogWarn(COMPONENT_EXTSTORE, "s3rc=%d s3sta=%s",
				final_status,
				S3_get_status_name(final_status));

			goto clean;
		}

clean:
		if (manager.upload_id)
			free(manager.upload_id);
		for (i = 0; i < manager.next_etags_pos; i++)
			free(manager.etags[i]);
		if (manager.commitstr)
			g_string_free(manager.commitstr, TRUE);
		free(manager.etags);
	}

	if (final_status != S3StatusOK) {
		int rc = s3status2posix_error(final_status);
		LogCrit(COMPONENT_EXTSTORE, "error s3rc=%d rc=%d",
			final_status, rc);
		return rc;
	}
	return 0;
}
