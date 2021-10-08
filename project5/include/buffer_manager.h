#ifndef __BUFFER_MANAGER_H__
#define __BUFFER_MANAGER_H__

#include <stdlib.h>
#include <pthread.h>
#include "disk_space_manager.h"

#define MAX_NUM_OF_TABLE_ID 10

typedef struct buf_block_t buf_block_t;
typedef struct buf_table_t buf_table_t;
typedef struct hash_bkt_t hash_bkt_t;

// Buffer block structure.
struct buf_block_t {
    // Buffer page frame.
    page_t frame;

    // Buffer control block.
    int table_id;
    pagenum_t page_num;
    bool is_dirty;
    int is_pinned;
    buf_block_t* next_of_LRU;
    buf_block_t* prev_of_LRU;
    buf_block_t* next_of_table;
    buf_block_t* prev_of_table;

    pthread_mutex_t buf_page_latch;
};

// Table structure.
struct buf_table_t {
    int table_id;
    int fd;
    int block_num;
    buf_block_t* head_of_table;
};

// Hash bucket structure.

struct hash_bkt_t {
    int table_id;
    pagenum_t pagenum;
    buf_block_t* buf_block;
    hash_bkt_t* next;
};

// UTILITIES
int buf_find_table_idx(int table_id);
int buf_evict_buf_block(buf_block_t* buf_block);
int buf_set_buf_block(int table_id, pagenum_t pagenum, page_t buf_page, buf_block_t **dest);
void buf_acquire_buf_pool_latch(void);
void buf_release_buf_pool_latch(void);

// HASH
int buf_hash_insert(int table_id, pagenum_t pagenum, buf_block_t *buf_block);
buf_block_t* buf_hash_find(int table_id, pagenum_t pagenum);
int buf_hash_delete(int table_id, pagenum_t pagenum);

// API
int buf_init_db(int buf_num);
int buf_shutdown_db(void);
int buf_open_table(char* pathname);
int buf_close_table(int table_id);
pagenum_t buf_alloc_page(int table_id);
int buf_free_page(int table_id, pagenum_t pagenum);
int buf_request_page(int table_id, pagenum_t pagenum, buf_block_t** dest);
int buf_release_page(buf_block_t* buf_block, bool is_writed);

#endif // __BUFFER_MANAGER_H__