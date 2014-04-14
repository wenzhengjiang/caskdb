#include <stdint.h>
#include <stdio.h>
#include <errno.h>
#include <stdlib.h>
#include "slice.h"

#ifndef __DICT_H
#define __DICT_H

#define DICT_OK 0
#define DICT_ERR 1

#define DICT_HT_INITIAL_SIZE     4

static void *my_malloc(size_t s, const char *file, int line, const char *func) {
    void *p = malloc(s);
    if (p == NULL) {
        fprintf(stderr, "Out of memory: %d, %li bytes in %s (%s:%i)\n", errno, s, func, file, line);
        exit(1);
    }
    //memset(p, 0, s);
    return p;
}

#define malloc(X) my_malloc(X, __FILE__, __LINE__, __FUNCTION__)

typedef struct item {
    int fid;
    int vsz;
    int vpos;
    long long tstamp;
}Item;

typedef struct entry {
    Slice key;
    Item value;
    struct entry *next;
} Entry;

typedef struct ht {
    Entry **table;
    uint32 size;
    uint32 used;
} HT;

typedef struct dict {
    HT ht[2];
    uint32 rehashidx;
} Dict;

/* API */

Dict *dict_new();
int dict_add(Dict *d, Slice key, Item value);
Item dict_get(Dict *d, Slice key);
int dict_delete(Dict *d, Slice key);
int dict_replace(Dict *d, Slice key, Item value);
void dict_destroy(Dict *d);

#endif
