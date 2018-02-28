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

/* kvsns_utils.h
 * KVSNS: set of common utility functions
 */

#ifndef KVSNS_UTILS_H
#define KVSNS_UTILS_H

#include <stdio.h>
#include <assert.h>


#define RC_WRAP(__function, ...) ({\
	int __rc = __function(__VA_ARGS__);\
	if (__rc != 0)        \
		return __rc; })

#define RC_WRAP_LABEL(__rc, __label, __function, ...) ({\
	__rc = __function(__VA_ARGS__);\
	if (__rc != 0)        \
		goto __label; })

#ifdef DEBUG
	#define ASSERT_FAIL(ex, fi, li, fu) \
	do { \
		fprintf(stderr, "%s:%i: %s: Assertion `%s` failed.\n", fi, li, fu, ex); \
		assert(0); \
	} while(0)
#else
	#define ASSERT_FAIL(ex, fi, li, fu) ((void)0)
#endif

#if DEBUG
	#define ASSERT(expr) do { if(!(expr)) ASSERT_FAIL(#expr, __FILE__, __LINE__, __func__); } while(0)
#else
	#define ASSERT(expr)  ((void)0)
#endif

/**
* Prepends t into s. Assumes s has enough space allocated
* for the combined string.
*/
void prepend(char* s, const char* t);
char* printf_open_flags(char *dst, int flags, const size_t len);

#endif
