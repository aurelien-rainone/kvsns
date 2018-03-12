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

/* internal.c
 * KVSNS: S3 extstore internal definitions.
 */

#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <dirent.h>
#include "internal.h"
#include "s3_common.h"
#include "mru.h"


/* Inode cache management is split in 2 non intersecting caches, one for the
 * downloaded inodes and the other for the inodes to upload. */

/* Inode cache directory (read and write). Cached files are named after the
 * inode number they represent, preceded by the letter 'w' or 'r', depending on
 * the type of inode cache they are in */
char ino_cache_dir[MAXPATHLEN] = "";

/* `wino_cache`, for 'written inode cache', is only used to upload
 * new files to s3. After the creation of a new inode and until the reception of
 * close, all successive write operations on this inode are performed on the
 * local filesystem and thus do not involve networking. It's only when NFS
 * requests the file closure that we start transfering the file to stable
 * storage. When the transfer is complete the locally cached inode becomes
 * useless and can safely be deleted.
 *
 * wino_cache is a tree in which keys are inodes and values are file descriptors
 * of the cached files, on which `pwrite` can be called */
GTree *wino_cache = NULL;

/* The read inode cache involves 2 data structures.
 *  - `rino_mru` keeps track of the most recently used inodes. An inode is
 *  present in `rino_mru` when the cache contains a local copy of an s3
 *  object. Disk space being somewhat limited, when an inode has not been used
 *  recently, its local copy gets deleted and frees up disk space.
 *  - `rino_cache` is a tree which keys are inodes and values are file
 *  descriptors of the cached files, on which pread can be called
 *  - `rino_mru_maxlen` is the maximum number of inodes to keep in the read
 *  cache. There will never be more than this number as the list is shrunk
 *  before appending.
 */
GTree *rino_cache = NULL;
struct mru rino_mru = {};
const size_t rino_mru_maxlen = 10;

/**
 * @brief posix error code from libS3 status error
 *
 * This function returns a posix errno equivalent from an libs3 S3Status.
 *
 * @param[in] s3_errorcode libs3 error
 *
 * @return negative posix error numbers.
 */
int s3status2posix_error(const S3Status s3_errorcode)
{
	int rc = 0;

	switch (s3_errorcode) {

		case S3StatusOK:
			rc = 0;
			break;

		/*
		 * Errors that prevent the S3 request from being issued or
		 * response from being read
		 */
		case S3StatusInternalError:
			rc = ECONNABORTED;
			break;

		case S3StatusOutOfMemory:
			rc = ENOMEM;
			break;

		case S3StatusInterrupted:
			rc = ECONNABORTED;
			break;

		case S3StatusInvalidBucketNameTooLong:
		case S3StatusInvalidBucketNameFirstCharacter:
		case S3StatusInvalidBucketNameCharacter:
		case S3StatusInvalidBucketNameCharacterSequence:
		case S3StatusInvalidBucketNameTooShort:
		case S3StatusInvalidBucketNameDotQuadNotation:
		case S3StatusQueryParamsTooLong:
		case S3StatusFailedToInitializeRequest:
			rc = EINVAL;
			break;

		case S3StatusMetaDataHeadersTooLong:
		case S3StatusBadMetaData:
		case S3StatusBadContentType:
		case S3StatusContentTypeTooLong:
		case S3StatusBadMD5:
		case S3StatusMD5TooLong:
		case S3StatusBadCacheControl:
		case S3StatusCacheControlTooLong:
		case S3StatusBadContentDispositionFilename:
		case S3StatusContentDispositionFilenameTooLong:
		case S3StatusBadContentEncoding:
		case S3StatusContentEncodingTooLong:
		case S3StatusBadIfMatchETag:
		case S3StatusIfMatchETagTooLong:
		case S3StatusBadIfNotMatchETag:
		case S3StatusIfNotMatchETagTooLong:
		case S3StatusHeadersTooLong:
		case S3StatusKeyTooLong:
		case S3StatusUriTooLong:
		case S3StatusXmlParseFailure:
		case S3StatusEmailAddressTooLong:
		case S3StatusUserIdTooLong:
		case S3StatusUserDisplayNameTooLong:
		case S3StatusGroupUriTooLong:
		case S3StatusPermissionTooLong:
		case S3StatusTargetBucketTooLong:
		case S3StatusTargetPrefixTooLong:
			rc = EINVAL;
			break;

		case S3StatusTooManyGrants:
			rc = E2BIG;
			break;

		case S3StatusBadGrantee:
		case S3StatusBadPermission:
			rc = EINVAL;
			break;

		case S3StatusXmlDocumentTooLarge:
			rc = E2BIG;
			break;

		case S3StatusNameLookupError:
			rc = ENOENT;
			break;

		case S3StatusFailedToConnect:
			rc = EHOSTUNREACH;
			break;

		case S3StatusServerFailedVerification:
			rc = EACCES;
			break;

		case S3StatusConnectionFailed:
			rc = ENOTCONN;
			break;

		case S3StatusAbortedByCallback:
			rc = EINTR;
			break;

		case S3StatusNotSupported:
			rc = EPERM;
			break;

		/*
		 * Errors from the S3 service
		 */
		case S3StatusErrorAccessDenied:
		case S3StatusErrorAccountProblem:
		case S3StatusErrorAmbiguousGrantByEmailAddress:
		case S3StatusErrorBadDigest:
			rc = EACCES;
			break;

		case S3StatusErrorBucketAlreadyExists:
			rc = EEXIST;
			break;

		case S3StatusErrorBucketAlreadyOwnedByYou:
		case S3StatusErrorBucketNotEmpty:
			rc = ENOTEMPTY;
			break;

		case S3StatusErrorCredentialsNotSupported:
		case S3StatusErrorCrossLocationLoggingProhibited:
			rc = EACCES;
			break;

		case S3StatusErrorEntityTooSmall:
		case S3StatusErrorEntityTooLarge:
			rc = EINVAL;
			break;

		case S3StatusErrorExpiredToken:
			rc = EKEYEXPIRED;
			break;

		case S3StatusErrorIllegalVersioningConfigurationException:
		case S3StatusErrorIncompleteBody:
		case S3StatusErrorIncorrectNumberOfFilesInPostRequest:
			rc = EINVAL;
			break;

		case S3StatusErrorInlineDataTooLarge:
		case S3StatusErrorInternalError:
		case S3StatusErrorInvalidAccessKeyId:
			rc = EACCES;
			break;

		case S3StatusErrorInvalidAddressingHeader:
		case S3StatusErrorInvalidArgument:
		case S3StatusErrorInvalidBucketName:
		case S3StatusErrorInvalidBucketState:
		case S3StatusErrorInvalidDigest:
		case S3StatusErrorInvalidEncryptionAlgorithmError:
		case S3StatusErrorInvalidLocationConstraint:
		case S3StatusErrorInvalidObjectState:
		case S3StatusErrorInvalidPart:
		case S3StatusErrorInvalidPartOrder:
		case S3StatusErrorInvalidPayer:
		case S3StatusErrorInvalidPolicyDocument:
		case S3StatusErrorInvalidRange:
		case S3StatusErrorInvalidRequest:
		case S3StatusErrorInvalidSecurity:
		case S3StatusErrorInvalidSOAPRequest:
		case S3StatusErrorInvalidStorageClass:
		case S3StatusErrorInvalidTargetBucketForLogging:
		case S3StatusErrorInvalidToken:
		case S3StatusErrorInvalidURI:
			rc = EINVAL;
			break;

		case S3StatusErrorKeyTooLong:
			rc = ENAMETOOLONG;
			break;

		case S3StatusErrorMalformedACLError:
		case S3StatusErrorMalformedPOSTRequest:
		case S3StatusErrorMalformedXML:
		case S3StatusErrorMaxMessageLengthExceeded:
		case S3StatusErrorMaxPostPreDataLengthExceededError:
		case S3StatusErrorMetadataTooLarge:
		case S3StatusErrorMethodNotAllowed:
		case S3StatusErrorMissingAttachment:
		case S3StatusErrorMissingContentLength:
		case S3StatusErrorMissingRequestBodyError:
		case S3StatusErrorMissingSecurityElement:
		case S3StatusErrorMissingSecurityHeader:
		case S3StatusErrorNoLoggingStatusForKey:
			rc = EINVAL;
			break;

		case S3StatusErrorNoSuchBucket:
		case S3StatusErrorNoSuchKey:
			rc = ENOKEY;
			break;

		case S3StatusErrorNoSuchLifecycleConfiguration:
		case S3StatusErrorNoSuchUpload:
		case S3StatusErrorNoSuchVersion:
			rc = EEXIST;
			break;

		case S3StatusErrorNotImplemented:
			rc = EPERM;
			break;

		case S3StatusErrorNotSignedUp:
			rc = EACCES;
			break;

		case S3StatusErrorNoSuchBucketPolicy:
			rc = ENOKEY;
			break;

		case S3StatusErrorOperationAborted:
		case S3StatusErrorPermanentRedirect:
		case S3StatusErrorPreconditionFailed:
		case S3StatusErrorRedirect:
		case S3StatusErrorRestoreAlreadyInProgress:
		case S3StatusErrorRequestIsNotMultiPartContent:
			rc = ECONNABORTED;
			break;

		case S3StatusErrorRequestTimeout:
			rc = ETIMEDOUT;
			break;

		case S3StatusErrorRequestTimeTooSkewed:
		case S3StatusErrorRequestTorrentOfBucketError:
		case S3StatusErrorSignatureDoesNotMatch:
		case S3StatusErrorServiceUnavailable:
		case S3StatusErrorSlowDown:
		case S3StatusErrorTemporaryRedirect:
		case S3StatusErrorTokenRefreshRequired:
		case S3StatusErrorTooManyBuckets:
		case S3StatusErrorUnexpectedContent:
		case S3StatusErrorUnresolvableGrantByEmailAddress:
		case S3StatusErrorUserKeyMustBeSpecified:
		case S3StatusErrorQuotaExceeded:
		case S3StatusErrorUnknown:
			rc = ECONNABORTED;
			break;

		case S3StatusHttpErrorNotFound:
			rc = ENOENT;
			break;
		case S3StatusHttpErrorForbidden:
			rc = EACCES;

		/*
		* The following are HTTP errors returned by S3 without enough
		* detail to distinguish any of the above S3StatusError
		* conditions
		*/
		case S3StatusHttpErrorMovedTemporarily:
		case S3StatusHttpErrorBadRequest:
		case S3StatusHttpErrorConflict:
		case S3StatusHttpErrorUnknown:
			rc = ECONNABORTED;
			break;

	}
	if (s3_errorcode != S3StatusOK) {
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			"Mapping %d(%s) to errno %d",
			s3_errorcode, S3_get_status_name(s3_errorcode), rc);
	}

	return -rc;
}

/**
 * @brief returns nonzero on success, zero on out of memory
 *
 * @param[in] s3_errorcode libs3 error
 *
 * @return 0 on success
 */
int growbuffer_append(growbuffer_t **gb, const char *data, int data_len)
{
	int toCopy = 0 ;
	while (data_len) {
		growbuffer_t *buf = *gb ? (*gb)->prev : 0;
		if (!buf || (buf->size == sizeof(buf->data))) {
			buf = (growbuffer_t *) malloc(sizeof(growbuffer_t));
			if (!buf) {
				// TODO: should return NOMEM here and 0 on success!!!
				return 0;
			}
			buf->size = 0;
			buf->start = 0;
			if (*gb && (*gb)->prev) {
				buf->prev = (*gb)->prev;
				buf->next = *gb;
				(*gb)->prev->next = buf;
				(*gb)->prev = buf;
			} else {
				buf->prev = buf->next = buf;
				*gb = buf;
			}
		}

		toCopy = (sizeof(buf->data) - buf->size);
		if (toCopy > data_len)
			toCopy = data_len;

		memcpy(&(buf->data[buf->size]), data, toCopy);

		buf->size += toCopy, data += toCopy, data_len -= toCopy;
	}

	return toCopy;
}

void growbuffer_read(growbuffer_t **gb, int amt, int *amt_ret,
		     char *buffer)
{
	*amt_ret = 0;

	growbuffer_t *buf = *gb;

	if (!buf)
		return;

	*amt_ret = (buf->size > amt) ? amt : buf->size;

	memcpy(buffer, &(buf->data[buf->start]), *amt_ret);

	buf->start += *amt_ret, buf->size -= *amt_ret;

	if (buf->size == 0) {
		if (buf->next == buf)
			*gb = 0;
		else {
			*gb = buf->next;
			buf->prev->next = buf->next;
			buf->next->prev = buf->prev;
		}
		free(buf);
	}
}

void growbuffer_destroy(growbuffer_t *gb)
{
	growbuffer_t *start = gb;

	while (gb) {
		growbuffer_t *next = gb->next;
		free(gb);
		gb = (next == start) ? 0 : next;
	}
}

/**
 * Prepends t into s. Assumes s has enough space allocated
 * for the combined string.
 */
void prepend(char* s, const char* t)
{
	size_t i, len = strlen(t);
	memmove(s + len, s, strlen(s) + 1);
	for (i = 0; i < len; ++i)
		s[i] = t[i];
}

char* printf_open_flags(char *dst, int flags, const size_t len)
{
	if (flags & O_ACCMODE) strncat(dst, "O_ACCMODE ", len);
	if (flags & O_RDONLY) strncat(dst, "O_RDONLY ", len);
	if (flags & O_WRONLY) strncat(dst, "O_WRONLY ", len);
	if (flags & O_RDWR) strncat(dst, "O_RDWR ", len);
	if (flags & O_CREAT) strncat(dst, "O_CREAT ", len);
	if (flags & O_EXCL) strncat(dst, "O_EXCL ", len);
	if (flags & O_NOCTTY) strncat(dst, "O_NOCTTY ", len);
	if (flags & O_TRUNC) strncat(dst, "O_TRUNC ", len);
	if (flags & O_APPEND) strncat(dst, "O_APPEND ", len);
	if (flags & O_NONBLOCK) strncat(dst, "O_NONBLOCK ", len);
	if (flags & O_DSYNC) strncat(dst, "O_DSYNC ", len);
	if (flags & FASYNC) strncat(dst, "FASYNC ", len);
#ifdef O_DIRECT
	if (flags & O_DIRECT) strncat(dst, "O_DIRECT ", len);
#endif
#ifdef O_LARGEFILE
	if (flags & O_LARGEFILE) strncat(dst, "O_LARGEFILE ", len);
#endif
	if (flags & O_DIRECTORY) strncat(dst, "O_DIRECTORY ", len);
	if (flags & O_NOFOLLOW) strncat(dst, "O_NOFOLLOW ", len);
#ifdef O_NOATIME
	if (flags & O_NOATIME) strncat(dst, "O_NOATIME ", len);
#endif
	if (flags & O_CLOEXEC) strncat(dst, "O_CLOEXEC ", len);
	return dst;
}

gint g_key_cmp_func (gconstpointer a, gconstpointer b)
{
	if (a < b)
		return -1;
	if (a > b)
		return 1;
	return 0;
}

int mru_key_cmp_func (void *a, void *b)
{
	if (a < b)
		return -1;
	if (a > b)
		return 1;
	return 0;
}

/*
 * Remove everything found under `dirname` directory by calling `remove`. Not
 * recursive.
 */
void remove_files_in(const char *dirname)
{
	/* quick and dirty safety check */
	if (!dirname || !strcmp(dirname, "/") || !strcmp(dirname, ".")) {
		LogFatal(KVSNS_COMPONENT_EXTSTORE, "invalid argument dirname=%s", dirname);
		return;
	}

	DIR *dir;
	struct dirent *dp;
	char path[MAXPATHLEN];
	dir = opendir(dirname);
	while ((dp = readdir(dir)) != NULL) {
		if (strcmp(dp->d_name, ".") && strcmp(dp->d_name, "..")) {
			snprintf(path, MAXPATHLEN, "%s/%s", dirname, dp->d_name);
			if (remove(path))
				LogWarn(KVSNS_COMPONENT_EXTSTORE,
					"Couldn't remove file dir=%s file=%s",
					dirname, dp->d_name);
		}
	}
	closedir(dir);
}

/* Let the 'write inode cache' handle closure of an inode.
 * Once the cached inode has been closed, we upload the file to stable storage.
 */
int wino_close(kvsns_ino_t ino)
{
	int rc, fd;
	gpointer wkey;
	char s3_path[S3_MAX_KEY_SIZE];
	char write_cache_path[MAXPATHLEN];

	wkey = g_tree_lookup(wino_cache, (gpointer) ino);
	if (!wkey)
		return 0;

	fd = (int) ((intptr_t) wkey);
	rc = close(fd);
	if (rc == -1) {
		rc = -errno;
		LogCrit(KVSNS_COMPONENT_EXTSTORE,
			 "error closing fd=%d errno=%d",
			 fd, rc);
		goto remove_fd;
	}

	fullpath_from_inode(ino, S3_MAX_KEY_SIZE, s3_path);
	build_cache_path(ino, write_cache_path, write_cache_t, MAXPATHLEN);

	/* override default s3 request config */
	extstore_s3_req_cfg_t put_req_cfg;
	memcpy(&put_req_cfg, &def_s3_req_cfg, sizeof(put_req_cfg));
	put_req_cfg.retries = 3;
	put_req_cfg.sleep_interval = 1;
	/* for multipart, it's the timeout for the transfer of one part, it's
	 * reset after each part */
	put_req_cfg.timeout = 5 * 60 * 1000; /* 5Min*/

	/* transfer file to stable storage */
	rc = put_object(&bucket_ctx, s3_path, &put_req_cfg, write_cache_path);
	if (rc != 0) {
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			 "Couldn't upload file ino=%d s3key=%s fd=%d",
			 ino, s3_path, fd);
	}

remove_fd:

	g_tree_remove(wino_cache, (gpointer) ino);
	if (remove(write_cache_path)) {
		LogWarn(KVSNS_COMPONENT_EXTSTORE, "Couldn't remove cached inode path=%s",
				write_cache_path);
	}
	return rc;
}

/* Let the 'read inode cache' handle closure of an inode.
 * Nothing else to than closing the cached inode and removing the entry to its
 * file descriptor from the tree.
 */
int rino_close(kvsns_ino_t ino)
{
	int rc, fd;
	gpointer rkey;

	rkey = g_tree_lookup(rino_cache, (gpointer) ino);
	if (!rkey)
		return 0;

	fd = (int) ((intptr_t) rkey);
	rc = close(fd);
	if (rc == -1) {
		rc = -errno;
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			 "error closing fd=%d errno=%d",
			 fd, rc);
	}

	g_tree_remove(rino_cache, (gpointer) ino);
	return rc;
}

/* callback triggered when an cached inode is evicted from the read inode
 * cache */
void rino_mru_remove (void *item, void *data)
{
	kvsns_ino_t ino = (kvsns_ino_t) item;
	char cache_path[MAXPATHLEN];

	/* cleanup the file descriptor and its references */
	rino_close(ino);

	/* delete the cached file from local filesystem */
	build_cache_path(ino, cache_path, read_cache_t, MAXPATHLEN);
	if (remove(cache_path) < 0) {
		int rc = errno;
		LogWarn(KVSNS_COMPONENT_EXTSTORE,
			"Couldn't remove file ino=%lu path=%s errno=%d",
			ino, cache_path, rc);
	}
}

/**
 * Build path of s3 object and return object directory and filename.
 *
 * @param object - [IN] object inode.
 * @param dirlen - [IN] max dir length
 * @param namelen - [IN] max name length
 * @param obj_dir - [OUT] full s3 directory path
 * @param obj_name - [OUT] s3 object filename, empty for a directory
 *
 * @note returned directory path doesn't start with a '/' as libs3 requires
 * object keys to be formatted in this way. The bucket root is an empty string.
 * However directory paths are returned with a trailing '/', this is a s3
 * requirement.
 *
 * @return 0 if successful, a negative "-errno" value in case of failure
 */
int splitpath_from_inode(kvsns_ino_t object, size_t dirlen, size_t namelen,
			 char *obj_dir, char *obj_name)
{
	char k[KLEN];
	char v[VLEN];
	kvsns_ino_t ino = object;
	/*kvsns_ino_t root_ino = 0LL;*/
	struct stat stat;

	/* get root inode number */
	/*RC_WRAP(kvsal_get_char, "KVSNS_PARENT_INODE", v);*/
	/*sscanf(v, "%llu|", &root_ino);*/

	/* init return values */
	obj_dir[0] = '\0';
	obj_name[0] = '\0';

	while (ino != KVSNS_ROOT_INODE) {

		/* current inode name */
		snprintf(k, KLEN, "%llu.name", ino);
		RC_WRAP(kvsal_get_char, k, v);

		snprintf(k, KLEN, "%llu.stat", ino);
		RC_WRAP(kvsal_get_stat, k, &stat);
		if (stat.st_mode & S_IFDIR) {
			prepend(obj_dir, "/");
			prepend(obj_dir, v);
		} else {
			strncpy(obj_name, v, namelen);
		}

		/* get parent inode */
		snprintf(k, KLEN, "%llu.parentdir", ino);
		RC_WRAP(kvsal_get_char, k, v);
		sscanf(v, "%llu|", &ino);
	};

	return 0;
}

/**
 * Fetch the full path of an s3 path from its inode
 *
 * @param object - [IN] object inode
 * @param pathlen - [IN] max path length
 * @param obj_path - [OUT] full s3 path
 * 
 * @note returned path doesn't start with a '/' as libs3 requires object keys
 * to be formatted in this way. The bucket root is an empty string.
 * However directory paths are returned with a trailing '/', this is a s3 
 * requirement.
 *
 * @return 0 if successful, a negative "-errno" value in case of failure
 */
int fullpath_from_inode(kvsns_ino_t object, size_t pathlen, char *obj_path)
{
	char fname[VLEN];
	RC_WRAP(splitpath_from_inode, object, pathlen, VLEN, obj_path, fname);
	strcat(obj_path, fname);
	return 0;
}

/**
 * Build path of data cache file for a given inode.
 *
 * @param object - object inode.
 * @param datacache_path - [OUT] data cache file path
 * @param read - [IN] read or write cache
 * @param pathlen - [IN] max path length
 *
 * @return 0 if successful, a negative "-errno" value in case of failure
 */
int build_cache_path(kvsns_ino_t object,
		     char *data_cache_path,
		     cache_t cache_type,
		     size_t pathlen)
{
	return snprintf(data_cache_path, pathlen,
			"%s/%c%llu",
			ino_cache_dir,
			(cache_type == read_cache_t)? 'r':'w',
			(unsigned long long)object);
}
