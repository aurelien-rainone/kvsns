/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (C) CEA, 2016
 * Author: Philippe Deniel philippe.deniel@cea.fr
 *
 * contributeur : Aurélien Rainone aurelien.rainone@gmail.com
 *
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 *
 * -------------
 */

/* kvsns_log.c
 * KVSNS: basic logging facility
 */

#include <stdarg.h>
#include <syslog.h>
#include <kvsns/kvsns.h>


typedef struct loglev {
	char *str;
	char *short_str;
	int syslog_level;
} log_level_t;

struct log_component_info {
	const char *comp_name;	/* component name */
	const char *comp_str;	/* shorter, more useful name */
};

kvsns_log_levels_t default_log_levels[] = {
	[KVSNS_COMPONENT_ALL] = LVL_NULL,
	[KVSNS_COMPONENT_KVSNS] = LVL_INFO,
	[KVSNS_COMPONENT_KVSAL] = LVL_INFO,
	[KVSNS_COMPONENT_EXTSTORE] = LVL_INFO,
};

kvsns_log_levels_t *kvsns_component_levels = default_log_levels;

static log_level_t tabLogLevel[] = {
	[LVL_NULL] = {"LVL_NULL", "NULL", LOG_NOTICE},
	[LVL_FATAL] = {"LVL_FATAL", "FATAL", LOG_CRIT},
	[LVL_CRIT] = {"LVL_CRIT", "CRIT", LOG_ERR},
	[LVL_WARN] = {"LVL_WARN", "WARN", LOG_WARNING},
	[LVL_INFO] = {"LVL_INFO", "INFO", LOG_INFO},
	[LVL_DEBUG] = {"LVL_DEBUG", "DEBUG", LOG_DEBUG},
};

struct log_component_info LogComponents[KVSNS_COMPONENT_COUNT] = {
	[KVSNS_COMPONENT_ALL] = {
		.comp_name = "KVSNS_COMPONENT_ALL",
		.comp_str = "",},
	[KVSNS_COMPONENT_KVSNS] = {
		.comp_name = "KVSNS_COMPONENT_KVSNS",
		.comp_str = "KVSNS",},
	[KVSNS_COMPONENT_KVSAL] = {
		.comp_name = "KVSNS_COMPONENT_KVSAL",
		.comp_str = "KVSAL",},
	[KVSNS_COMPONENT_EXTSTORE] = {
		.comp_name = "KVSNS_COMPONENT_EXTSTORE",
		.comp_str = "EXTSTORE",}
};

/*static int syslog_opened = 0;*/

void LogWithComponentAndLevel(kvsns_log_components_t component, char *file, int line,
			      char *function, kvsns_log_levels_t level, char *format,
			      ...)
{
	va_list arguments;
	va_start(arguments, format);

	/* TODO: openlog should already have been called from the executable loading
	 * this library, but what happens if that's not the case. Ensure that is
	 * always the case or syslog is disabled (and redirected to stderr?)
	 **/
	/*if (!syslog_opened) {
		openlog("kvsns", LOG_PID, LOG_USER);
		syslog_opened = 1;
	}*/
	char fmtbuf[1024];
	snprintf(fmtbuf, sizeof(fmtbuf), "%s :%s :%s %s",
		 function,
		 LogComponents[component].comp_str,
		 tabLogLevel[level].short_str,
		 format);

	vsyslog(tabLogLevel[level].syslog_level, fmtbuf, arguments);
	va_end(arguments);

	if (level == LVL_FATAL)
		exit(2);
}

int kvsns_parse_log_level(const char * strlevel, kvsns_log_levels_t *lvl)
{
	if (strlevel && lvl) {
		if (!strcmp(strlevel, "NULL")) {
			*lvl = LVL_NULL;
			return 0;
		}
		if (!strcmp(strlevel, "FATAL")) {
			*lvl = LVL_FATAL;
			return 0;
		}
		if (!strcmp(strlevel, "CRIT")) {
			*lvl = LVL_CRIT;
			return 0;
		}
		if (!strcmp(strlevel, "WARN")) {
			*lvl = LVL_WARN;
			return 0;
		}
		if (!strcmp(strlevel, "INFO")) {
			*lvl = LVL_INFO;
			return 0;
		}
		if (!strcmp(strlevel, "DEBUG")) {
			*lvl = LVL_DEBUG;
			return 0;
		}
	}
	return -1;
}

void kvsns_set_log_level(kvsns_log_components_t component, kvsns_log_levels_t lvl)
{
	kvsns_component_levels[component] = lvl;
}

