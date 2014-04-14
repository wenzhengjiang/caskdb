/*
 *       Filename:  tt.cpp
 *    Description: 
 *         Author:  Wenzheng Jiang , jwzh.hi@gmail.com
 */
#include <assert.h>
#include <iostream>
#include "db.h"

using namespace std;

int main(int argc,char * argv[])
{
	leveldb::DB* db;
	leveldb::Options options;
	options.create_if_missing = true;
	std::string dbpath = "testdb";
	leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
	assert(status.ok());
	std::string key1 = "lyc";
	std::string key2 = "liyc7711@gamil.com";
	cout<<"Open db OK"<<std::endl;
	
	std::string value;
	leveldb::Status s ;
	s = db->Put(leveldb::WriteOptions(), key1, key2);/*key1和key2作为一对key-value对插入*/
	s = db->Get(leveldb::ReadOptions(), key1, &value);/*根据key返回对应的value值*/
 	
	cout<<value<<std::endl;
	delete db;/*删除数据库*/

	return 0;
}

