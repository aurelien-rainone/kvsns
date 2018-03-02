/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (C) CEA, 2016
 * Author: Philippe Deniel  philippe.deniel@cea.fr
 *
 * contributeur : Philippe DENIEL   philippe.deniel@cea.fr
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

/* kvsns_internal.c
 * KVSNS: set of internal functions
 */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <kvsns/kvsal.h>
#include <kvsns/kvsns.h>
#include "kvsns_internal.h"
#include <gmodule.h>

typedef struct s3_path_ {
	kvsns_ino_t ino;
	char *path;
	int isdir;
} s3_path_t;

static GTree *s3_paths;		/*< s3_path_t indexed by inode */
static GTree *s3_inodes;	/*< s3_path_t indexed by path (char*) */
static kvsns_ino_t next_ino = KVSNS_ROOT_INODE;
struct stat root_stat = {};

void kvsns_next_inode(kvsns_ino_t *ino)
{
	*ino = next_ino++;
}

int kvsns_str2parentlist(kvsns_ino_t *inolist, int *size, char *str)
{
	char *token;
	char *rest = str;
	int maxsize = 0;
	int pos = 0;

	if (!inolist || !str || !size)
		return -EINVAL;

	maxsize = *size;

	while ((token = strtok_r(rest, "|", &rest))) {
		sscanf(token, "%llu", &inolist[pos++]);

		if (pos == maxsize)
			break;
	}

	*size = pos;

	return 0;
}

int kvsns_parentlist2str(kvsns_ino_t *inolist, int size, char *str)
{
	int i;
	char tmp[VLEN];

	if (!inolist || !str)
		return -EINVAL;

	strcpy(str, "");

	for (i = 0; i < size ; i++)
		if (inolist[i] != 0LL) {
			snprintf(tmp, VLEN, "%llu|", inolist[i]);
			strcat(str, tmp);
		}

	return 0;
}

int kvsns_update_stat(kvsns_ino_t *ino, int flags)
{
	char k[KLEN];
	struct stat stat;

	if (!ino)
		return -EINVAL;

	snprintf(k, KLEN, "%llu.stat", *ino);
	RC_WRAP(kvsal_get_stat, k, &stat);
	RC_WRAP(kvsns_amend_stat, &stat, flags);
	RC_WRAP(kvsal_set_stat, k, &stat);

	return 0;
}

int kvsns_amend_stat(struct stat *stat, int flags)
{
	struct timeval t;

	if (!stat)
		return -EINVAL;

	if (gettimeofday(&t, NULL) != 0)
		return -errno;

	if (flags & STAT_ATIME_SET) {
		stat->st_atim.tv_sec = t.tv_sec;
		stat->st_atim.tv_nsec = 1000 * t.tv_usec;
	}

	if (flags & STAT_MTIME_SET) {
		stat->st_mtim.tv_sec = t.tv_sec;
		stat->st_mtim.tv_nsec = 1000 * t.tv_usec;
	}

	if (flags & STAT_CTIME_SET) {
		stat->st_ctim.tv_sec = t.tv_sec;
		stat->st_ctim.tv_nsec = 1000 * t.tv_usec;
	}

	if (flags & STAT_INCR_LINK)
		stat->st_nlink += 1;

	if (flags & STAT_DECR_LINK) {
		if (stat->st_nlink == 1)
			return -EINVAL;

		stat->st_nlink -= 1;
	}
	return 0;
}

void kvsns_merge_s3_stats(struct stat *stat, kvsns_ino_t ino, size_t size)
{
	stat->st_ino = ino;
	if (S_ISDIR(stat->st_mode)) {
		stat->st_nlink = 2;
		stat->st_size = 0;
	} else if (S_ISREG(stat->st_mode)) {
		stat->st_nlink = 1;
		stat->st_size = size;
	}
}

void kvsns_fill_stats(struct stat *stat, kvsns_ino_t ino, time_t mtime,
		      int isdir, size_t size)
{
	int mode;
	const bool openbar = true;

	memset(stat, 0, sizeof(struct stat));

	stat->st_uid = 0;
	stat->st_gid = 0;
	stat->st_ino = ino;

	stat->st_atim.tv_sec = mtime;
	stat->st_atim.tv_nsec = 0; /* time_t only hold seconds */

	stat->st_mtim.tv_sec = stat->st_atim.tv_sec;
	stat->st_mtim.tv_nsec = stat->st_atim.tv_nsec;

	stat->st_ctim.tv_sec = stat->st_atim.tv_sec;
	stat->st_ctim.tv_nsec = stat->st_atim.tv_nsec;

	if (isdir) {
		mode = openbar? OPENBAR_DIRMODE: DEFAULT_DIRMODE;
		stat->st_mode = S_IFDIR|mode;
		stat->st_nlink = 2;
		stat->st_size = 0;
	} else {
		mode = openbar? OPENBAR_FILEMODE: DEFAULT_FILEMODE;
		stat->st_mode = S_IFREG|mode;
		stat->st_nlink = 1;
		stat->st_size = size;
	}
}


int kvsns_create_entry(kvsns_cred_t *cred, kvsns_ino_t *parent,
		       char *name, char *lnk, mode_t mode,
		       kvsns_ino_t *new_entry, enum kvsns_type type)
{
	int rc;
	char k[KLEN];
	char v[KLEN];
	struct stat bufstat;
	struct stat parent_stat;
	struct timeval t;

	if (!cred || !parent || !name || !new_entry)
		return -EINVAL;

	if ((type == KVSNS_SYMLINK) && (lnk == NULL))
		return -EINVAL;

	rc = kvsns_lookup(cred, parent, name, new_entry, &parent_stat);
	if (rc == 0)
		return -EEXIST;

	kvsns_next_inode(new_entry);

	RC_WRAP(kvsal_begin_transaction);

#ifdef KVSNS_S3
	snprintf(k, KLEN, "%llu.name", *new_entry);
	snprintf(v, VLEN, "%s", name);

	RC_WRAP_LABEL(rc, aborted, kvsal_set_char, k, v);
#endif

	snprintf(k, KLEN, "%llu.dentries.%s",
		 *parent, name);
	snprintf(v, VLEN, "%llu", *new_entry);

	RC_WRAP_LABEL(rc, aborted, kvsal_set_char, k, v);

	snprintf(k, KLEN, "%llu.parentdir", *new_entry);
	snprintf(v, VLEN, "%llu|", *parent);

	RC_WRAP_LABEL(rc, aborted, kvsal_set_char, k, v);

	/* Set stat */
	memset(&bufstat, 0, sizeof(struct stat));
	bufstat.st_uid = cred->uid; 
	bufstat.st_gid = cred->gid;
	bufstat.st_ino = *new_entry;

	if (gettimeofday(&t, NULL) != 0)
		return -1;

	bufstat.st_atim.tv_sec = t.tv_sec;
	bufstat.st_atim.tv_nsec = 1000 * t.tv_usec;

	bufstat.st_mtim.tv_sec = bufstat.st_atim.tv_sec;
	bufstat.st_mtim.tv_nsec = bufstat.st_atim.tv_nsec;

	bufstat.st_ctim.tv_sec = bufstat.st_atim.tv_sec;
	bufstat.st_ctim.tv_nsec = bufstat.st_atim.tv_nsec;

	switch (type) {
	case KVSNS_DIR:
		bufstat.st_mode = S_IFDIR|mode;
		bufstat.st_nlink = 2;
		break;

	case KVSNS_FILE:
		bufstat.st_mode = S_IFREG|mode;
		bufstat.st_nlink = 1;
		break;

	case KVSNS_SYMLINK:
		bufstat.st_mode = S_IFLNK|mode;
		bufstat.st_nlink = 1;
		break;

	default:
		return -EINVAL;
	}
	snprintf(k, KLEN, "%llu.stat", *new_entry);
	RC_WRAP_LABEL(rc, aborted, kvsal_set_stat, k, &bufstat);

	if (type == KVSNS_SYMLINK) {
		snprintf(k, KLEN, "%llu.link", *new_entry);
		RC_WRAP_LABEL(rc, aborted, kvsal_set_char, k, lnk);
	}

	RC_WRAP_LABEL(rc, aborted, kvsns_amend_stat, &parent_stat,
		      STAT_CTIME_SET|STAT_MTIME_SET);
	RC_WRAP_LABEL(rc, aborted, kvsns_set_stat, parent, &parent_stat);

	RC_WRAP(kvsal_end_transaction);
	return 0;

aborted:
	kvsal_discard_transaction();
	return rc;
}


/* Access routines */
static int kvsns_access_check(kvsns_cred_t *cred, struct stat *stat, int flags)
{
	int check = 0;

	if (!stat || !cred)
		return -EINVAL;

	/* Root's superpowers */
	if (cred->uid == KVSNS_ROOT_UID)
		return 0;

	if (cred->uid == stat->st_uid) {
		if (flags & KVSNS_ACCESS_READ)
			check |= STAT_OWNER_READ;

		if (flags & KVSNS_ACCESS_WRITE)
			check |= STAT_OWNER_WRITE;

		if (flags & KVSNS_ACCESS_EXEC)
			check |= STAT_OWNER_EXEC;
	} else if (cred->gid == stat->st_gid) {
		if (flags & KVSNS_ACCESS_READ)
			check |= STAT_GROUP_READ;

		if (flags & KVSNS_ACCESS_WRITE)
			check |= STAT_GROUP_WRITE;

		if (flags & KVSNS_ACCESS_EXEC)
			check |= STAT_GROUP_EXEC;
	} else {
		if (flags & KVSNS_ACCESS_READ)
			check |= STAT_OTHER_READ;

		if (flags & KVSNS_ACCESS_WRITE)
			check |= STAT_OTHER_WRITE;

		if (flags & KVSNS_ACCESS_EXEC)
			check |= STAT_OTHER_EXEC;
	}

	if ((check & stat->st_mode) != check)
		return -EPERM;
	else
		return 0;

	/* Should not be reached */
	return -EPERM;
}

int kvsns_access(kvsns_cred_t *cred, kvsns_ino_t *ino, int flags)
{
	struct stat stat;

	if (!cred || !ino)
		return -EINVAL;

	RC_WRAP(kvsns_getattr, cred, ino, &stat);

	return kvsns_access_check(cred, &stat, flags);
}

int kvsns_get_stat(kvsns_ino_t *ino, struct stat *bufstat)
{
	char k[KLEN];

	if (!ino || !bufstat)
		return -EINVAL;

	snprintf(k, KLEN, "%llu.stat", *ino);
	return kvsal_get_stat(k, bufstat);
}

int kvsns_set_stat(kvsns_ino_t *ino, struct stat *bufstat)
{
	char k[KLEN];

	if (!ino || !bufstat)
		return -EINVAL;

	snprintf(k, KLEN, "%llu.stat", *ino);
	return kvsal_set_stat(k, bufstat);
}

gint _ino_cmp_func (gconstpointer a, gconstpointer b, gpointer user_data)
{
	if (a < b)
		return -1;
	if (a > b)
		return 1;
	return 0;
}

gint _path_cmp_func (gconstpointer a, gconstpointer b)
{
	return strcmp((const char*) a, (const char*) b);
}

void _free_s3_path(gpointer data)
{
	s3_path_t *p = data;
	free(p->path);
	p->path = NULL;
	p->ino = 0;
	free(p);
}

void noop_func(gpointer data) {}


/*
 * S3 path storage
 * - paths are stored without the leading '/'.
 * - dir paths are stored with the trailing slash, but it's kvsns_add_s3_path
 *   function that takes care of storing the path with the trailing slash (by
 *   setting`is_dir` to true)
 */

void kvsns_init_s3_paths()
{
	s3_paths = g_tree_new_full (_ino_cmp_func, NULL, noop_func, _free_s3_path);
	s3_inodes = g_tree_new (_path_cmp_func);
}

void kvsns_free_s3_paths()
{
	g_tree_destroy(s3_paths);
	g_tree_destroy(s3_inodes);
	s3_paths = NULL;
	s3_inodes = NULL;
}

/**
 * @brief kvsns_get_s3_inode get the inode associated with an s3 path
 * @param str[IN]	s3 path for which to retrieve the inode
 * @param create[IN]	if create is true and the inode is not found, a new
 *			inode number is created and associated to the s3 path.
 *			If create is false and the inode is not found, -ENOENT
 *			is returned.
 * @param ino[OUT]	inode to retrieve
 * @param isdir[INOUT]	IN used only when create is true. OUT indicates a directory
 * @return 0 for success or a negative errno value.
 * @note values are not modified in case of error.
 */
int kvsns_get_s3_inode(const char *str, const int create,
		       kvsns_ino_t *ino, int *isdir)
{
	s3_path_t *s3_path;

	if (!str || !ino || !isdir)
		return -EINVAL;

	ASSERT(!strlen(str) || (str[0] != '/'));
	ASSERT(!strlen(str) || (str[strlen(str)-1] != '/'));

	s3_path = (s3_path_t*) g_tree_lookup(s3_inodes, (gconstpointer) str);
	if (!s3_path) {
		if (!create)
			return -ENOENT;
		/* add this path with the provided `isdir` value */
		return kvsns_add_s3_path(str, *isdir, ino);
	}

	*ino = s3_path->ino;
	*isdir = s3_path->isdir;
	return 0;
}

/**
 * @brief kvsns_get_s3_path get the s3 path associated with an inode
 * @param ino[IN]	inode for which to retrieve the s3 path
 * @param size[IN]	maximum writable size of the str buffer
 * @param str[OUT]	s3 object path
 * @param isdir[OUT]	indicates if the inode is a directory
 * @return 0 for success or a negative errno value
 */
int kvsns_get_s3_path(kvsns_ino_t ino, const int size, char *str, int *isdir)
{
	s3_path_t *s3_path;

	if (!str || !ino || !size || !isdir)
		return -EINVAL;

	s3_path = (s3_path_t*) g_tree_lookup(s3_paths, (gconstpointer) ino);
	if (!s3_path)
		return -ENOENT;

	strncpy(str, s3_path->path, size);
	*isdir = s3_path->isdir;
	return 0;
}

/**
 * @brief kvsns_add_s3_path associates the s3 path with an unique inode number
 * @param str[IN]	s3 path to store
 * @param isdir[IN]	indicates if the inode is a directory
 * @param ino[OUT]	associated inode number
 *
 * @note paths should neither have a leading nor trailing slashes
 * @return 0 for success or a negative errno value
 */
int kvsns_add_s3_path(const char *str, const int isdir, kvsns_ino_t *ino)
{
	s3_path_t *s3_path;
	size_t len;
	/* added paths should be */
	if (!ino || !str)
		return -EINVAL;

	/* increment inode counter */
	kvsns_next_inode(ino);

	/* no leading nor trailing slashes */
	ASSERT(!strlen(str) || (str[0] != '/'));
	ASSERT(!strlen(str) || (str[strlen(str) - 1] != '/'));

	/* allocate a s3_path_t entry, containing its own copy of the char
	 * array representing the s3 key */
	s3_path = malloc(sizeof(s3_path_t));
	memset(s3_path, 0, sizeof(s3_path_t));
	len = strlen(str) + 1;
	s3_path->path = malloc(sizeof(char) * len);
	memset(s3_path->path, 0, sizeof(char) * len);
	strcpy(s3_path->path, str);
	s3_path->isdir = isdir;
	s3_path->ino = *ino;

	/* s3_paths: fast s3 key lookup by inode */
	g_tree_insert(s3_paths, (gpointer) *ino, (gpointer) s3_path);
	/* s3_inodes: fast inode lookpu by s3 key */
	g_tree_insert(s3_inodes, (gpointer) s3_path->path, (gpointer) s3_path);

	LogInfo(KVSNS_COMPONENT_KVSNS,
		 "associated s3 path and inode path=%s ino=%d isdir=%d",
		 s3_path->path, *ino, s3_path->isdir);

	return 0;
}
