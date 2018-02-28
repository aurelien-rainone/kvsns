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

#ifndef _S3_EXTSTORE_INTERNAL_H
#define _S3_EXTSTORE_INTERNAL_H

#include <errno.h>
#include <assert.h>
#include <kvsns/kvsns.h>
#include <gmodule.h>
#include <libs3.h>


int s3status2posix_error(const S3Status s3_errorcode);

typedef struct growbuffer_ {
	int size;			/* total number of bytes */
	int start;			/* start byte */
	char data[64 * 1024];		/* blocks */
	struct growbuffer_ *prev;
	struct growbuffer_ *next;
} growbuffer_t;

int growbuffer_append(growbuffer_t **gb, const char *data, int data_len);
void growbuffer_read(growbuffer_t **gb, int amt, int *amt_ret, char *buffer);
void growbuffer_destroy(growbuffer_t *gb);

void remove_files_in(const char *dirname);

char* printf_open_flags(char *dst, int flags, const size_t len);

gint g_key_cmp_func (gconstpointer a, gconstpointer b);

int mru_key_cmp_func (void *a, void *b);

#endif
