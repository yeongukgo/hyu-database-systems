#ifndef __API_H__
#define __API_H__

#include "index_manager.h"

int init_db(int buf_num);
int shutdown_db();

int open_table(char* pathname);
int close_table(int table_id);

int db_insert(int table_id, int64_t key, char* value);
int db_find(int table_id, int64_t key, char* ret_val);
int db_delete(int table_id, int64_t key);

#endif // __API_H__