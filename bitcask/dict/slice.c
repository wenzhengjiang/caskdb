/*
 *       Filename:  slice.c
 *    Description:
 *         Author:  Wenzheng Jiang , jwzh.hi@gmail.com
 */
#include "slice.h"
// O(n)
Slice slice_new(char *str, int size)
{
    Slice s;
    int i;
    s.data = (char*)malloc(size);
    for(i = 0; i < size; i++)
        s.data[i] = str[i];
    s.size = size;
    return s;
}
// O(n)
int slice_cmp(Slice s1, Slice s2)
{
    int i;
    if(s1.size != s2.size) {
       return s1.size - s2.size;
    }
    for(i = 0; i < s1.size; i++)
        if(s1.data[i] != s2.data[i])
            return s1.data[i] - s2.data[i];
    return 0;
}

void slice_destroy(Slice s)
{
    free(s.data);
}

