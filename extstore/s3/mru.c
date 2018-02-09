#include <stdlib.h>
#include "mru.h"


void mru_append(struct mru *mru, void *item)
{
	struct mru_entry *cur = malloc(sizeof(*cur));
	cur->item = item;
	cur->prev = mru->tail;
	cur->next = NULL;

	if (mru->tail)
		mru->tail->next = cur;
	else
		mru->head = cur;
	mru->tail = cur;
}

void mru_mark(struct mru *mru, struct mru_entry *entry)
{
	/* If we're already at the front of the list, nothing to do */
	if (mru->head == entry)
		return;

	/* Otherwise, remove us from our current slot... */
	if (entry->prev)
		entry->prev->next = entry->next;
	if (entry->next)
		entry->next->prev = entry->prev;
	else
		mru->tail = entry->prev;

	/* And insert us at the beginning. */
	entry->prev = NULL;
	entry->next = mru->head;
	if (mru->head)
		mru->head->prev = entry;
	mru->head = entry;
}

void mru_clear(struct mru *mru)
{
	struct mru_entry *p = mru->head;

	while (p) {
		struct mru_entry *to_free = p;
		p = p->next;
		free(to_free);
	}
	mru->head = mru->tail = NULL;
}

void mru_remove_unused(struct mru *mru, size_t nkeep,
		       free_item_func ffree, void *data)
{
	struct mru_entry *p;
	size_t nkept = 0;

	/* find the first item to remove */
	for (p = mru->head; p; p = p->next) {
		if (nkept++ == nkeep)
			break;
	}

	/* the previous item becomes the new tail */
	if (p->prev) {
		mru->tail = p->prev;
	} else {
		/* unless there's no previous item, in which case that's all the
		 * MRU that is being cleared */
		mru->tail = mru->head = NULL;
	}

	/* remove old entries, call user callback on removed items */
	while (p) {
		struct mru_entry *to_free = p;
		if (p->prev)
			p->prev->next = NULL;
		ffree(p->item, data);
		p = p->next;
		free(to_free);
	}
}

void mru_mark_item(struct mru *mru, cmp_item_func fcmp, void *item)
{
	struct mru_entry *p = mru->head;

	while (p) {
		struct mru_entry *to_cmp = p;
		p = p->next;
		if (!fcmp(to_cmp->item, item)) {
			mru_mark(mru, to_cmp);
			break;
		}
	}
}
