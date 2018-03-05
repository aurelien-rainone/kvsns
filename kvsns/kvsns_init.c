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

/* kvsns_handle.c
 * KVSNS: functions to manage handles
 */


#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <ini_config.h>
#include <kvsns/kvsal.h>
#include <kvsns/kvsns.h>
#include <kvsns/extstore.h>
#include "kvsns_internal.h"

static struct collection_item *cfg_items;

int kvsns_start(const char *configpath)
{
	struct collection_item *errors = NULL;
	int rc;

	LogInfo(KVSNS_COMPONENT_KVSNS, "--- Starting kvsns ---");

	rc = config_from_file("libkvsns", configpath, &cfg_items,
			      INI_STOP_ON_ERROR, &errors);
	if (rc) {
		LogCrit(KVSNS_COMPONENT_KVSNS, "Can't load config rc=%d", rc);
		free_ini_config_errors(errors);
		return -rc;
	}

	rc = kvsal_init(cfg_items);
	if (rc != 0) {
		LogCrit(KVSNS_COMPONENT_KVSNS, "Can't init kvsal");
		return rc;
	}

	rc = extstore_init(cfg_items);
	if (rc != 0) {
		LogCrit(KVSNS_COMPONENT_KVSNS, "Can't init extstore");
		return rc;
	}

	/** @todo : remove all existing opened FD (crash recovery) */
	return 0;
}

int kvsns_stop(void)
{
	RC_WRAP(kvsal_fini);
	RC_WRAP(extstore_fini);
	free_ini_config_errors(cfg_items);
	return 0;
}

int kvsns_init_root(int openbar)
{
	char k[KLEN];
	char v[KLEN];
	struct stat bufstat;
	kvsns_ino_t ino;

	/* start with a clean cache */
	kvsns_mr_proper();

	ino = KVSNS_ROOT_INODE;

	snprintf(k, KLEN, "%llu.parentdir", ino);
	snprintf(v, VLEN, "%llu|", ino);
	RC_WRAP(kvsal_set_char, k, v);

	snprintf(k, KLEN, "ino_counter");
	snprintf(v, VLEN, "3");
	RC_WRAP(kvsal_set_char, k, v);

	/* Set stat */
	memset(&bufstat, 0, sizeof(struct stat));
	if (openbar != 0)
		bufstat.st_mode = S_IFDIR|0777;
	else
		bufstat.st_mode = S_IFDIR|0755;
	bufstat.st_ino = KVSNS_ROOT_INODE;
	bufstat.st_nlink = 2;
	bufstat.st_uid = 0;
	bufstat.st_gid = 0;
	bufstat.st_atim.tv_sec = 0;
	bufstat.st_mtim.tv_sec = 0;
	bufstat.st_ctim.tv_sec = 0;

	snprintf(k, KLEN, "%llu.stat", ino);
	RC_WRAP(kvsal_set_stat, k, &bufstat);

	strncpy(k, "KVSNS_INODE", KLEN);
	snprintf(v, VLEN, "%llu", KVSNS_ROOT_INODE);
	RC_WRAP(kvsal_set_char, k, v);

	strncpy(k, "KVSNS_PARENT_INODE", KLEN);
	snprintf(v, VLEN, "%llu", KVSNS_ROOT_INODE);
	RC_WRAP(kvsal_set_char, k, v);

	strncpy(k, "KVSNS_PATH", KLEN);
	strncpy(v, "/", VLEN);
	RC_WRAP(kvsal_set_char, k, v);

	strncpy(k, "KVSNS_PREV_PATH", KLEN);
	strncpy(v, "/", VLEN);
	RC_WRAP(kvsal_set_char, k, v);

	/* indicates the directory has never been listed */
	snprintf(k, KLEN, "%llu.listed", ino);
	RC_WRAP(kvsal_set_char, k, "0");

	return 0;
}
