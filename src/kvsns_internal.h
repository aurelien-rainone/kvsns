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

/* kvsns_internal.h.c
 * KVSNS: headers for internal functions and macros
 */

#ifndef KVSNS_INTERNAL_H
#define KVSNS_INTERNAL_H

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <errno.h>
#include <ini_config.h>
#include <kvsns/kvsns.h>
#include "kvsns_utils.h"
#include <string.h>


void kvsns_next_inode(kvsns_ino_t *ino);
int kvsns_str2parentlist(kvsns_ino_t *inolist, int *size, char *str);
int kvsns_parentlist2str(kvsns_ino_t *inolist, int size, char *str);
int kvsns_create_entry(kvsns_cred_t *cred, kvsns_ino_t *parent,
		       char *name, char *lnk, mode_t mode,
		       kvsns_ino_t *newdir, enum kvsns_type type);
int kvsns_get_stat(kvsns_ino_t *ino, struct stat *bufstat);
int kvsns_set_stat(kvsns_ino_t *ino, struct stat *bufstat);
int kvsns_update_stat(kvsns_ino_t *ino, int flags);

/* merge a stat structure (prefilled with attributes stored on s3)
 * with the provided attributes (some attributes shouldn't be stored on s3)*/
void kvsns_merge_s3_stats(struct stat *stat, kvsns_ino_t ino, size_t size);

/* fill an empty stat structure, using the provided values */
void kvsns_fill_stats(struct stat *stat, kvsns_ino_t ino, time_t mtime,
		      int isdir, size_t size);

int kvsns_amend_stat(struct stat *stat, int flags);
int kvsns_delall_xattr(kvsns_cred_t *cred, kvsns_ino_t *ino);

void kvsns_init_s3_paths();
void kvsns_free_s3_paths();

/**
 * @brief kvsns_get_s3_inode get the inode associated with an s3 path
 * @param str[IN]	s3 path for which to retrieve the inode
 * @param create[IN]	if create is true and the inode is not found, a new
 *			inode number is created and associated to the s3 path.
 *			If create is false and the inode is not found, -ENOENT
 *			is returned.
 * @param ino[OUT]	inode to retrieve
 * @param isdir[INOUT]	IN used only when create is true. OUT indicates a directory
 * @return 0 for success or a negative errno value
 */
int kvsns_get_s3_inode(const char *str, const int create,
		       kvsns_ino_t *ino, int *isdir);

/**
 * @brief kvsns_get_s3_path get the s3 path associated with an inode
 * @param ino[IN]	inode for which to retrieve the s3 path
 * @param size[IN]	maximum writable size of the str buffer
 * @param str[OUT]	s3 object path
 * @param isdir[OUT]	indicates if the inode is a directory
 * @return 0 for success or a negative errno value
 */
int kvsns_get_s3_path(kvsns_ino_t ino, const int size, char *str, int *isdir);

/**
 * @brief kvsns_add_s3_path associates the s3 path with an unique inode number
 * @param str[IN]	s3 path to store
 * @param isdir[IN]	indicates if the inode is a directory
 * @param ino[OUT]	associated inode number
 *
 * @note paths should neither have a leading nor trailing slashes
 * @return 0 for success or a negative errno value
 */
int kvsns_add_s3_path(const char *str, const int isdir, kvsns_ino_t *ino);

extern struct stat root_stat;

#endif
