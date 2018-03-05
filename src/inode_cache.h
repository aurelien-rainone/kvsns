/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (C) CEA, 2016
 * Author: Aurélien Rainone a.rainone@cynnyspace.com
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

#ifndef INODE_CACHE_H
#define INODE_CACHE_H

#include <kvsns/kvsns.h>

void inocache_next(kvsns_ino_t *ino);
void inocache_init();
void inocache_deinit();

/**
 * @brief inocache_get_ino get the inode associated with an path
 * @param str[IN]	path for which to retrieve the inode
 * @param create[IN]	if create is true and the inode is not found, a new
 *			inode number is created and associated to the path.
 *			If create is false and the inode is not found, -ENOENT
 *			is returned.
 * @param ino[OUT]	inode to retrieve
 * @param isdir[INOUT]	IN used only when create is true. OUT indicates a directory
 * @return 0 for success or a negative errno value
 */
int inocache_get_ino(const char *str, const int create,
		     kvsns_ino_t *ino, int *isdir);

/**
 * @brief inocache_get_path get the path associated with an inode
 * @param ino[IN]	inode for which to retrieve the path
 * @param size[IN]	maximum writable size of the str buffer
 * @param str[OUT]	path
 * @param isdir[OUT]	indicates if the inode is a directory
 * @param stat[OUT]	set to the stored stat or NULL if no stat stored (may be
 * NULL in input as in output)
 * @return 0 for success or a negative errno value
 */
int inocache_get_path(kvsns_ino_t ino, const int size, char *str, int *isdir, struct stat **stat);

/**
 * @brief inocache_create associates the path with an unique inode number
 * @param str[IN]	path to cache
 * @param isdir[IN]	indicates if the inode is a directory
 * @param stat[IN]	attrs to associate with the new inode (may be NULL)
 * @param ino[OUT]	associated inode number
 *
 * @note paths should neither have a leading nor trailing slashes
 * @return 0 for success or a negative errno value
 */
int inocache_create(const char *str, const int isdir, kvsns_ino_t *ino, struct stat *stat);

extern struct stat root_stat;

#endif
