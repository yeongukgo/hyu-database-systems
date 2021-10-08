#ifndef __RSO_H__
#define __RSO_H__

#include "index_manager.h"

// Wrapper functions.
int rso_init_db(int buf_num);
int rso_shutdown_db(void);
int rso_open_table(char* pathname);
int rso_close_table(int table_id);
int rso_insert(int table_id, int64_t key, char* value);
int rso_find(int table_id, int64_t key, char* ret_val, int trx_id);
int rso_update(int table_id, int64_t key, char* values, int trx_id);
int rso_delete(int table_id, int64_t key);

// Utility.
int rso_advance(int *idx, buf_block_t **cur_buf, page_t **cur_page);

// Relational set operator.
int rso_join_table(int table_id_1, int table_id_2, char* pathname);

#endif // __RSO_H__