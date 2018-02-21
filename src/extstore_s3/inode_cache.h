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
#ifndef _S3_EXTSTORE_INODE_CACHE_H
#define _S3_EXTSTORE_INODE_CACHE_H

#include <kvsns/kvsns.h>
#include "mru.h"


/* forward declarations */
typedef GTree GTree;

/* Inode cache management is split in 2 non intersecting caches, one for the
 * downloaded inodes and the other for the inodes to upload. */

typedef enum cache_ { read_cache_t, write_cache_t } cache_t;

/* Inode cache directory (read and write). Cached files are named after the
 * inode number they represent, preceded by the letter 'w' or 'r', depending on
 * the type of inode cache they are in */
extern char ino_cache_dir[MAXPATHLEN];

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
extern GTree *wino_cache;

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
extern GTree *rino_cache;
extern struct mru rino_mru;
extern const size_t rino_mru_maxlen;

/* Let the 'read inode cache' handle closure of an inode.
 * Nothing else to than closing the cached inode and removing the entry to its
 * file descriptor from the tree.
 */
int rino_close(kvsns_ino_t ino);

/* Let the 'write inode cache' handle closure of an inode.
 * Once the cached inode has been closed, we upload the file to stable storage.
 */
int wino_close(kvsns_ino_t ino);

/* callback triggered when an cached inode is evicted from the read inode
 * cache */
void rino_mru_remove (void *item, void *data);

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
int build_cache_path(kvsns_ino_t object, char *data_cache_path,
		     cache_t cache_type, size_t pathlen);
#endif
