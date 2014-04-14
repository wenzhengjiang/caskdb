/*
 *       Filename:  dict.c
 *    Description:
 *         Author:  Wenzheng Jiang , jwzh.hi@gmail.com
 */

#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include <limits.h>
#include "fnv1a.h"
#include "dict.h"

#define REHASH_CONN  1
#define REHASH_OVER  0

#define dict_is_rehashing(d) (d->rehashidx != -1)

static int dict_force_resize_ratio = 5;

static int dict_key_idx(Dict *d, Slice key);
static uint32 dict_next_power(uint32 size) ;
static int ht_destroy(HT *ht);
static uint32 hash_key(Slice key);
static Entry *dict_find(Dict *d, Slice key);
static int dict_need_expand(Dict *d);

// 初始化哈希表各项属性
void dict_reset(HT *ht)
{
    ht->table = NULL;
    ht->size = 0;
    ht->used = 0;
}

Dict *dict_new()
{
    Dict *d = malloc(sizeof(Dict));
    dict_reset(&d->ht[0]);
    dict_reset(&d->ht[1]);
    d->rehashidx = -1;
}

// 调整哈希表大小，使 used / size <= 1
int dict_resize(Dict *d)
{
    int minimal = d->ht[0].used;

    if(dict_is_rehashing(d)) return DICT_ERR;

    if (minimal < DICT_HT_INITIAL_SIZE )
        minimal = DICT_HT_INITIAL_SIZE;

    return dict_expand(d, minimal);
}

int dict_expand(Dict *d, uint32 size)
{
    HT n;
    // 计算真实值，哈希表大小一定要是2的幂
    uint32 realsize = dict_next_power(size);

    if(dict_is_rehashing(d))
        return DICT_ERR;

    // 创建新的哈希表并且初始化
    n.size = realsize;
    n.table = malloc(realsize * sizeof(Entry*));
    n.used = 0;

    // 是否是第一次创建哈希表
    if(d->ht[0].table == NULL) {
        d->ht[0] = n;
        return DICT_OK;
    }

    // 扩展字典
    // 开启rehash标识

    d->ht[1] = n;
    d->rehashidx = 0;
    return DICT_OK;
}

// 渐进式rehash
// 执行n次key迁移
// 迁移的时候把旧表中的值链接到新表

int dict_rehash(Dict *d, int n)
{
    if(!dict_is_rehashing(d)) return REHASH_CONN;

    while(n--) {
        Entry *p, *np;
        // 如果已经迁移完毕
        if(d->ht[0].used == 0) {
            // 释放Entry*数组
            free(d->ht[0].table);
            d->ht[0] = d->ht[1];
            dict_reset(&d->ht[1]);
            d->rehashidx = -1;
            return REHASH_CONN;
        }

        assert(d->ht[0].size > d->rehashidx) ;

        // 确保key值是有效的
        while(d->ht[0].table[d->rehashidx] == NULL) d->rehashidx++;
        p = d->ht[0].table[d->rehashidx];
        while(p) {
            // 得到key在新哈希表中的索引
            uint32 idx = hash_key(p->key) & (d->ht[1].size-1);
            np = p->next;
            // 添加节点
            p->next = d->ht[1].table[idx];
            d->ht[1].table[idx] = p;

            d->ht[0].used--;
            d->ht[1].used++;

            p = np;
        }
        //该链表已经迁移完毕，和原来的哈希表断掉联系
        d->ht[0].table[d->rehashidx] = NULL;
        d->rehashidx++;
    }
    return REHASH_CONN;
}

int dict_add(Dict *d, Slice key, Item val)
{
    Entry *entry;
    HT *ht;
    int idx;

    if(dict_is_rehashing(d)) dict_rehash(d, 1);

    //  如果key已经存在
    if((idx = dict_key_idx(d, key)) == -1)
        return dict_replace(d, key, val);

    // 选择哈希表
    ht = dict_is_rehashing(d) ? &d->ht[1] : &d->ht[0];
    // 新的entry
    entry = malloc(sizeof(Entry));
    entry->key = key;
    entry->value = val;
    //插入
    entry->next = ht->table[idx];
    ht->table[idx] = entry;
    // 更新
    ht->used++;

    return DICT_OK;
}



int dict_replace(Dict *d, Slice key, Item val)
{
    Entry *entry = dict_find(d, key);
    // key并不存在
    if(entry == NULL)
        return DICT_ERR;
    //释放旧值
    entry->value = val;
    return DICT_OK;
}

Item dict_get(Dict *d, Slice key)
{
    Entry *p = dict_find(d, key);
    if(p == NULL) {
        Item i = {-1,-1,-1,-1};
        return i;
    }
    else return p->value;
}

int dict_delete(Dict *d, Slice key)
{
    uint32 h, idx;
    Entry *p, *pre;
    int tb;
    // 空表
    if(d->ht[0].size == 0) return DICT_ERR;

    if(dict_is_rehashing(d)) dict_rehash(d, 1);

    h = hash_key(key);
    for(tb = 0; tb <= 1; tb++){
        idx = h & (d->ht[tb].size-1);
        p = d->ht[tb].table[idx];
        pre = NULL;

        while(p) {
            // 找到
            if(slice_cmp(key, p->key) == 0) {
                if(pre)
                    pre->next = p->next;
                else
                    d->ht[tb].table[idx] = p->next;

                // 销毁这个entry
                slice_destroy(p->key);
                free(p);
                d->ht[tb].used--;
                return DICT_OK;
            }
            pre = p;
            p = p->next;
        }
        if(!dict_is_rehashing(d)) break;
    }
    //没找到
    return DICT_ERR;
}

void dict_destroy(Dict *d)
{
    ht_destroy(&d->ht[0]);
    ht_destroy(&d->ht[1]);
    free(d);
}

/* private functions */

// 得到key可以插入的idx
// 如果key以存在则返回-1
//
static int dict_key_idx(Dict *d, Slice key)
{
    uint32 tb, h, idx;
    //检查是否需要扩展
    if(dict_need_expand(d) == DICT_ERR)
        return -1;

    h = hash_key(key);
    for(tb = 0; tb <= 1; tb++){
        idx = h & (d->ht[tb].size-1);
        Entry *p = d->ht[tb].table[idx];
        while(p){
            if(slice_cmp(key, p->key) == 0)
                return -1;
            p = p->next;
        }
        if(!dict_is_rehashing(d)) break;
    }
    return idx;
}
static int dict_need_expand(Dict *d)
{
    if(dict_is_rehashing(d)) return DICT_OK;

    // 哈希表为空则扩展程初始大小
    if(d->ht[0].size == 0) return dict_expand(d, DICT_HT_INITIAL_SIZE);

    // 是否需要rehash
    if(d->ht[0].used / d->ht[0].size > dict_force_resize_ratio)
        return dict_expand(d, d->ht[0].used*2);

    return DICT_OK;
}

static uint32 dict_next_power(uint32 size)
{
    uint32 i = DICT_HT_INITIAL_SIZE;
    if(size >= UINT_MAX)
        return UINT_MAX;
    while( i < size) i *= 2;
    return i;
}



static int ht_destroy(HT *ht)
{
    uint32 i;

    for(i = 0; i < ht->size && ht->used > 0; i++){
        Entry *p, *next;

        if((p = ht->table[i]) == NULL) continue;

        // 释放这个链表
        //
        while(p) {
            next = p->next;
            slice_destroy(p->key);
            free(p);
            ht->used--;
            p = p->next;
        }
    }
    free(ht->table);
    dict_reset(ht);
    return DICT_OK;
}



static uint32 hash_key(Slice key)
{
    return fnv1a(key.data, key.size);
}

static Entry *dict_find(Dict *d, Slice key)
{
    Entry *p;
    uint32 h, idx, tb;

    if(d->ht[0].size == 0) return NULL;

    if(dict_is_rehashing(d)) dict_rehash(d, 1);

    h = hash_key(key);

    for(tb = 0; tb <= 1; tb++){
        idx = h & (d->ht[tb].size-1);
        p = d->ht[tb].table[idx];

        while(p){
            if(slice_cmp(key, p->key) == 0)
                return p;
            p = p->next;
        }

        if(!dict_is_rehashing(d)) break;
    }
    return NULL;
}
