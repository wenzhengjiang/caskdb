///*
// *       Filename:  dict_test.c
// *    Description: 
// *         Author:  Wenzheng Jiang , jwzh.hi@gmail.com
// */
//#include <stdio.h>
//#include <time.h>
//#include "dict.h"
//
//int N = 1024 * 1024 * 10;
//
//int main(int argc, const char *argv[])
//{
//    Dict * d = dict_new(); 
//    Item v = {1,1,1,1};
//    int i;
//    clock_t st, et;
//    st = clock();
//    for(i = 0; i < N ; i++){
//        char s[10];
//        sprintf(s,"%10d",i);
//        Slice sl = slice_new(s, 5);
//        dict_add(d, sl, v);
//    }
//    et = clock();
//    printf("%.3lf ops/sec\n", N / ((double)(et-st)/CLOCKS_PER_SEC));
//    st = clock();
//    for(i = 0; i < N ; i++){
//        char s[10];
//        sprintf(s,"%10d",i);
//        Slice sl = slice_new(s, 5);
//        dict_get(d, sl);
//    }
//    et = clock();
//    printf("%.3lf ops/sec\n", N / ((double)(et-st)/CLOCKS_PER_SEC));
//    dict_destroy(d);
//    return 0;
//}
