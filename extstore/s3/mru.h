#ifndef MRU_H
#define MRU_H

#include <stdlib.h>

/**
 * A simple most-recently-used cache, backed by a doubly-linked list.
 *
 * Usage is roughly:
 *
 *   // Create a list.  Zero-initialization is required.
 *   static struct mru cache;
 *   mru_append(&cache, item);
 *   ...
 *
 *   // Iterate in MRU order.
 *   struct mru_entry *p;
 *   for (p = cache.head; p; p = p->next) {
 *	if (matches(p->item))
 *		break;
 *   }
 *
 *   // Mark an item as used, moving it to the front of the list.
 *   mru_mark(&cache, p);
 *
 *   // Reset the list to empty, cleaning up all resources.
 *   mru_clear(&cache);
 *
 * Note that you SHOULD NOT call mru_mark() nor mru_mark_item and then continue
 * traversing the list; it reorders the marked item to the front of the list,
 * and therefore you will begin traversing the whole list again.
 */

struct mru_entry {
	void *item;
	struct mru_entry *prev, *next;
};

struct mru {
	struct mru_entry *head, *tail;
};

void mru_append(struct mru *mru, void *item);
void mru_mark(struct mru *mru, struct mru_entry *entry);
void mru_clear(struct mru *mru);

/* Type of the function used to compare an item to another
 */
typedef int (*cmp_item_func) (void *a, void *b);

/* Mark an entry from its item, the entry to mark is found by calling the
 * user-provided comparison function.
 */
void mru_mark_item(struct mru *mru, cmp_item_func fcmp, void *item);

/* Type of the function used to free an item.
 */
typedef void (*free_item_func) (void *item, void *data);

/* Remove unused entries from an mru. Indicate the number of entries to keep,
 * then free_item_func will be called with every extra item, plus an user
 * defined data pointer.
 */
void mru_remove_unused(struct mru *mru, size_t nkeep, free_item_func ffree, void *data);

#endif /* MRU_H */
