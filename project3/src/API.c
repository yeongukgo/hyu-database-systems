#include "API.h"

// Wrapper functions.

int init_db(int buf_num) {
    return index_init_db(buf_num);
}

int shutdown_db() {
    return index_shutdown_db();
}

int open_table(char* pathname) {
    return index_open_table(pathname);
}

int close_table(int table_id) {
    return index_close_table(table_id);
}

int db_insert(int table_id, int64_t key, char* value) {
    return index_insert(table_id, key, value);
}

int db_find(int table_id, int64_t key, char* ret_val) {
    return index_find(table_id, key, ret_val);
}

int db_delete(int table_id, int64_t key) {
    return index_delete(table_id, key);
}
