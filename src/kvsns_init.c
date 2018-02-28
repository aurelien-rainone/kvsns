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
#include "kvsns_utils.h"

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

	kvsns_init_s3_paths();

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
	kvsns_free_s3_paths();
	return 0;
}

int kvsns_init_root(int openbar)
{
	kvsns_ino_t ino;

	/* create root dir entry */
	RC_WRAP(kvsns_add_s3_path, KVSNS_S3_ROOT_PATH, 1, &ino);

	/* create and set the root dir stat, the stats for the root dir are the
	 *  only to not be stored on stable storage. The file corresponding to
	 * the root doesn't actually exist on s3.
	 */

	memset(&root_stat, 0, sizeof(struct stat));
	if (openbar != 0)
		root_stat.st_mode = S_IFDIR|0777;
	else
		root_stat.st_mode = S_IFDIR|0755;
	root_stat.st_ino = KVSNS_ROOT_INODE;
	root_stat.st_nlink = 2;
	root_stat.st_uid = 0;
	root_stat.st_gid = 0;
	root_stat.st_atim.tv_sec = 0;
	root_stat.st_mtim.tv_sec = 0;
	root_stat.st_ctim.tv_sec = 0;

	return 0;
}
