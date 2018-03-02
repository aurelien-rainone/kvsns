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

#include "kvsns_internal.h"
#include <gmodule.h>
#include "inode_cache.h"

typedef struct ino_ent_ {
	kvsns_ino_t ino;
	char *path;
	int isdir;
} ino_ent_t;

static GTree *ino_ents;		/*< ino_ent_t indexed by inode */
static GTree *ino_inodes;	/*< ino_ent_t indexed by path (char*) */
static kvsns_ino_t next_ino = KVSNS_ROOT_INODE;
struct stat root_stat = {};

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

void _free_ino_ent(gpointer data)
{
	ino_ent_t *p = data;
	free(p->path);
	p->path = NULL;
	p->ino = 0;
	free(p);
}

void noop_func(gpointer data) {}

void inocache_next(kvsns_ino_t *ino)
{
	*ino = next_ino++;
}

void inocache_init()
{
	ino_ents = g_tree_new_full (_ino_cmp_func, NULL, noop_func, _free_ino_ent);
	ino_inodes = g_tree_new (_path_cmp_func);
}

void inocache_deinit()
{
	g_tree_destroy(ino_ents);
	g_tree_destroy(ino_inodes);
	ino_ents = NULL;
	ino_inodes = NULL;
}

int inocache_get_ino(const char *str, const int create,
		       kvsns_ino_t *ino, int *isdir)
{
	ino_ent_t *ino_ent;

	if (!str || !ino || !isdir)
		return -EINVAL;

	ASSERT(!strlen(str) || (str[0] != '/'));
	ASSERT(!strlen(str) || (str[strlen(str)-1] != '/'));

	ino_ent = (ino_ent_t*) g_tree_lookup(ino_inodes, (gconstpointer) str);
	if (!ino_ent) {
		if (!create)
			return -ENOENT;
		/* add this path with the provided `isdir` value */
		return inocache_add(str, *isdir, ino);
	}

	*ino = ino_ent->ino;
	*isdir = ino_ent->isdir;
	return 0;
}

int inocache_get_path(kvsns_ino_t ino, const int size, char *str, int *isdir)
{
	ino_ent_t *ino_ent;

	if (!str || !ino || !size || !isdir)
		return -EINVAL;

	ino_ent = (ino_ent_t*) g_tree_lookup(ino_ents, (gconstpointer) ino);
	if (!ino_ent)
		return -ENOENT;

	strncpy(str, ino_ent->path, size);
	*isdir = ino_ent->isdir;
	return 0;
}

int inocache_add(const char *str, const int isdir, kvsns_ino_t *ino)
{
	ino_ent_t *ino_ent;
	size_t len;
	/* added paths should be */
	if (!ino || !str)
		return -EINVAL;

	/* increment inode counter */
	inocache_next(ino);

	/* no leading nor trailing slashes */
	ASSERT(!strlen(str) || (str[0] != '/'));
	ASSERT(!strlen(str) || (str[strlen(str) - 1] != '/'));

	/* allocate a ino_ent_t entry, containing its own copy of the char
	 * array representing the path */
	ino_ent = malloc(sizeof(ino_ent_t));
	memset(ino_ent, 0, sizeof(ino_ent_t));
	len = strlen(str) + 1;
	ino_ent->path = malloc(sizeof(char) * len);
	memset(ino_ent->path, 0, sizeof(char) * len);
	strcpy(ino_ent->path, str);
	ino_ent->isdir = isdir;
	ino_ent->ino = *ino;

	/* ino_ents: fast path  lookup by inode */
	g_tree_insert(ino_ents, (gpointer) *ino, (gpointer) ino_ent);
	/* ino_inodes: fast inode lookup by path */
	g_tree_insert(ino_inodes, (gpointer) ino_ent->path, (gpointer) ino_ent);
	return 0;
}
