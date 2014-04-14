#include <stdint.h>
#include <stdlib.h>

#ifndef _SLICE_H
#define _SLICE_H

typedef uint32_t uint32;

// 1. O(1)获得大小
// 2. 二进制安全

typedef struct slice {
    uint32 size;
    char *data;
}Slice;


#define slice2bytes(s) s.data
#define slice_size(s) s.size

Slice slice_new(char *str, int size);
int slice_cmp(Slice s1, Slice s2);
void slice_destroy(Slice s);

#endif
