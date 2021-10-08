#ifndef __BUFFER_MANAGER_H__
#define __BUFFER_MANAGER_H__

#include <stdlib.h>
#include "disk_space_manager.h"

#define MAX_NUM_OF_TABLE_ID 10

// Buffer block structure.
typedef struct buffer_block buffer_block_t;
struct buffer_block {
    // Buffer page frame.
    page_t frame;

    // Buffer control block.
    int table_id;
    pagenum_t page_num;
    bool is_dirty;
    bool is_pinned;
    buffer_block_t* next_of_LRU;
    buffer_block_t* prev_of_LRU;
    buffer_block_t* next_of_table;
    buffer_block_t* prev_of_table;
};

// Table structure.
typedef struct buffer_table buffer_table_t;
struct buffer_table {
    int table_id;
    int fd;
    int block_num;
    buffer_block_t* head_of_table;
};

/*
// Buffer header structure.
typedef struct buffer_header buffer_header_t;
struct buffer_header {
    int buf_num; // Size of buffer pool. Maximum number of buffer blocks.
    int table_num; // Number of opened tables.
    int block_num; // Number of allocated buffer blocks.
    buffer_table_t buf_tables[MAX_NUM_OF_TABLE_ID];
    buffer_block_t* head_of_LRU_list;
    buffer_block_t* tail_of_LRU_list;
};
*/

// UTILITIES
int find_table_index(int table_id);
//int find_buffer_block_index(buffer_block_t* buf_block);
int evict_buffer_block(buffer_block_t* buf_block);
int create_buffer_block(int table_id, pagenum_t pagenum, page_t buf_page, buffer_block_t **dest);

// API
int buffer_init_db(int buf_num);

int buffer_shutdown_db(void);

int buffer_open_table(char* pathname);

int buffer_close_table(int table_id);

pagenum_t buffer_alloc_page(int table_id);

int buffer_free_page(int table_id, pagenum_t pagenum);

/*
int buffer_read_page(int table_id, pagenum_t pagenum, page_t* dest);
int buffer_write_page(int table_id, pagenum_t pagenum, const page_t* src);
*/

int buffer_request_page(int table_id, pagenum_t pagenum, buffer_block_t** dest);

int buffer_release_page(buffer_block_t* buf_block, bool is_writed);

#endif // __BUFFER_MANAGER_H__