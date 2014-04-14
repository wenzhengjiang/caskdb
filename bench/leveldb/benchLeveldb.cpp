/*
 *       Filename:  benchLeveldb.cpp
 *    Description: 
 *         Author:  Wenzheng Jiang , jwzh.hi@gmail.com
 */

#include <assert.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <ctime>
#include "db.h"

using namespace std;
const int MAXN = 1000 * 1000 + 5;
int key[MAXN];
std::string dbpath = "/home/jwzh/tmpdata/testleveldb";
bool asyc_flag = true;

std::string genValue(int vsz)
{
    std::string ret = "";
    for (int i = 0; i < vsz; i++) {
       ret += std::to_string(rand()%10);
    }
    return ret;
}

leveldb::DB* init(int N) 
{
	leveldb::DB* db;
	leveldb::Options options;
	options.create_if_missing = true;
	leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
	assert(status.ok());

    for (int i = 0; i < N; i++) {
       key[i] = (int)((double)rand() / RAND_MAX * N);
    }
    return db;
}

clock_t bench_write(leveldb::DB *db, int N, int vsz) 
{
    std::string value = genValue(vsz);
	leveldb::Status s ;
    leveldb::WriteOptions o = leveldb::WriteOptions();
    o.sync = !asyc_flag;
    
    clock_t st = clock();
    for (int i = 0; i < N; i++) {
        s = db->Put(o, std::to_string(key[i]), value);
    }
    clock_t et = clock();

    return et - st; 
}

clock_t bench_read(leveldb::DB *db, int N, int vsz)
{
    leveldb::WriteOptions wo = leveldb::WriteOptions();
    leveldb::ReadOptions ro = leveldb::ReadOptions();
    std::string value = genValue(vsz);
	leveldb::Status s ;

    for (int i = 0; i < N; i++) {
        s = db->Put(wo, std::to_string(i), value);
    }

    clock_t st = clock();
    for (int i = 0; i < N; i++) {
        s = db->Get(ro, std::to_string(key[i]), &value);
    }
    clock_t et = clock();

    return et - st;
}

// ./leveldb N vsz
int main(int argc,char * argv[])
{
    int N, vsz;
    system("/usr/bin/rm -rf /home/jwzh/tmpdata/testleveldb");
    printf("Benchmark %s vsz = %s, ", argv[1], argv[3]);
    {
        sscanf(argv[2], "%d", &N);
        int len = strlen(argv[3]);
        char c = argv[3][len-1];
        argv[3][len-1] = 0;
        sscanf(argv[3], "%d", &vsz);
        if (c == 'K') vsz *= 1024;
        else if (c == 'M') vsz *= 1024 * 1024;
    }
    if (argv[4][0] == 'A') asyc_flag = true;
    else asyc_flag = false;

    leveldb::DB * db = init(N);
    clock_t du;

    switch (argv[1][0]) {
        case 'W' : 
            du = bench_write(db, N, vsz) ; 
            break;
        case 'R' :
            du = bench_read(db, N, vsz);
            break;
    }
    printf("%.3lf ops/sec\n", N / ((double)(du)/CLOCKS_PER_SEC));
	delete db;

	return 0;
}
