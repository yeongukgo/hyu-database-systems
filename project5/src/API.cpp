#include "API.h"

// Wrapper functions.

int init_db(int buf_num) {
    return rso_init_db(buf_num);
}

int shutdown_db(void) {
    return rso_shutdown_db();
}

int open_table(char* pathname) {
    return rso_open_table(pathname);
}

int close_table(int table_id) {
    return rso_close_table(table_id);
}

int begin_trx(void){
    return trx_begin_trx();
}

int end_trx(int tid) {
    return trx_end_trx(tid);
}

int db_insert(int table_id, int64_t key, char* value) {
    return rso_insert(table_id, key, value);
}

int db_find(int table_id, int64_t key, char* ret_val, int trx_id) {
    return rso_find(table_id, key, ret_val, trx_id);
}


int db_update(int table_id, int64_t key, char* values, int trx_id) {
    return rso_update(table_id, key, values, trx_id);
}

int db_delete(int table_id, int64_t key) {
    return rso_delete(table_id, key);
}

int join_table(int table_id_1, int table_id_2, char* pathname) {
    return rso_join_table(table_id_1, table_id_2, pathname);
}