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
#include <string.h>

#define RC_WRAP(__function, ...) ({\
	int __rc = __function(__VA_ARGS__);\
	if (__rc != 0)        \
		return __rc; })

#define RC_WRAP_LABEL(__rc, __label, __function, ...) ({\
	__rc = __function(__VA_ARGS__);\
	if (__rc != 0)        \
		goto __label; })

int kvsns_next_inode(kvsns_ino_t *ino);
int kvsns_str2parentlist(kvsns_ino_t *inolist, int *size, char *str);
int kvsns_parentlist2str(kvsns_ino_t *inolist, int size, char *str);
int kvsns_create_entry(kvsns_cred_t *cred, kvsns_ino_t *parent,
		       char *name, char *lnk, mode_t mode,
		       kvsns_ino_t *newdir, enum kvsns_type type);
int kvsns_get_stat(kvsns_ino_t *ino, struct stat *bufstat);
int kvsns_set_stat(kvsns_ino_t *ino, struct stat *bufstat);
int kvsns_update_stat(kvsns_ino_t *ino, int flags);
int kvsns_amend_stat(struct stat *stat, int flags);
int kvsns_delall_xattr(kvsns_cred_t *cred, kvsns_ino_t *ino);

void kvsns_init_s3_paths();
void kvsns_free_s3_paths();
/**
 * @brief kvsns_get_s3_path get the s3 object path associated with an inode
 * @param ino[IN] inode for which to retrieve the s3 object path
 * @param size[IN] maximum writable size of the str buffer
 * @param str[OUT] s3 object path
 * @return 0 for success or a negative errno value
 */
int kvsns_get_s3_path(kvsns_ino_t ino, int size, char *str);

/**
 * @brief kvsns_add_s3_path associates the path of an s3 object with the next
 * inode number
 * @param str[IN] s3 object path to store
 * @param ino[OUT] associated inode number
 * @return 0 for success or a negative errno value
 */
int kvsns_add_s3_path(const char *str, kvsns_ino_t *ino);

extern struct stat root_stat;

#endif
