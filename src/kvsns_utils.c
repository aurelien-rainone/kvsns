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

/* kvsns_utils.c
 * KVSNS: set of common utility functions
 */
#include <string.h>
#include <fcntl.h>
#include "kvsns_utils.h"


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
