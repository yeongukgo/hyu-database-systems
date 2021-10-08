#ifndef __API_H__
#define __API_H__

#include "relational_set_operator.h"

int init_db(int buf_num);
int shutdown_db(void);

int open_table(char* pathname);
int close_table(int table_id);

int db_insert(int table_id, int64_t key, char* value);
int db_find(int table_id, int64_t key, char* ret_val);
int db_delete(int table_id, int64_t key);

int join_table(int table_id_1, int table_id_2, char* pathname);

#endif // __API_H__