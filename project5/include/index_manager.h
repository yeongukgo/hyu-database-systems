#ifndef __INDEX_MANAGER_H__
#define __INDEX_MANAGER_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "transaction_manager.h"

// Return values.


#define PAGE_NOT_OPENED -1
#define ERROR -5

// For index_find_left_leaf
#define FIND_LEFT_LEAF_LEFTMOST -1
#define FIND_LEFT_LEAF_CANNOT_FIND -2

// For index_find.
#define FIND_EMPTY_TREE -2
#define FIND_NOT_EXISTS -3

// For index_insert.
#define INSERT_DUPLICATED -2

// For index_get_index.
#define GET_INDEX_LEFTMOST -1
#define GET_INDEX_ERROR -3

// For index_get_neighbor_index.
#define GET_NEIGHBOR_INDEX_LEFTMOST -2
#define GET_NEIGHBOR_INDEX_ERROR -3

// For index_delete.
#define DELETE_NOT_EXISTS -1

// Wrapper functions

int idx_init_db(int buf_num);
int idx_shutdown_db();

int idx_open_table(char * pathname);
int idx_close_table(int table_id);

int idx_request_page(int table_id, pagenum_t pagenum, buf_block_t** dest);
int idx_release_page(buf_block_t* buf_block, bool is_writed);

// UTILITIES

int idx_cut(int length);
int idx_get_idx(buf_block_t *parent_buf, pagenum_t current_pagenum);
buf_block_t* idx_find_leftmost_leaf(buf_block_t *header_buf);

// FIND

buf_block_t* idx_find_leaf(int table_id, int64_t key);
int idx_find(int table_id, int64_t key, char * ret_val);
int idx_find(int table_id, int64_t key, char * ret_val, int trx_id);

// UPDATE

int idx_update(int table_id, int64_t key, char* values, int trx_id);

// INSERTION

record idx_make_record(int64_t key, char * value);
int idx_make_page(page_t* page);
int idx_make_leaf(page_t* page);
int idx_insert_into_leaf(buf_block_t* leaf_buf, record new_record);
int idx_insert_into_leaf_after_splitting(buf_block_t* leaf_buf, record new_record);
int idx_insert_into_internal(buf_block_t* parent_buf, int left_idx, int64_t key, pagenum_t right_pagenum);
int idx_insert_into_internal_after_splitting(buf_block_t* parent_buf, int left_idx, int64_t key, pagenum_t right_pagenum);
int idx_insert_into_parent(buf_block_t* left_buf, int64_t key, buf_block_t* right_buf);
int idx_insert_into_new_root(buf_block_t* left_buf, int64_t key, buf_block_t* right_buf);
int idx_start_new_tree(int table_id, record new_record);
int idx_insert(int table_id, int64_t key, char * value);

// DELETION
int idx_adjust_root(buf_block_t *header_buf, buf_block_t *root_buf);
int idx_remove_entry_from_internal(buf_block_t *current_buf);
int idx_remove_record_from_leaf(page_t* leaf_page, int64_t key);
int idx_merge_pages(buf_block_t *current_buf, buf_block_t *neighbor_buf, buf_block_t *parent_buf,
        int neighbor_idx, int k_prime_idx, int64_t k_prime);
int idx_redistribute_pages(buf_block_t *current_buf, buf_block_t *neighbor_buf, buf_block_t *parent_buf,
        int neighbor_idx, int k_prime_idx, int64_t k_prime);
int idx_delete_entry(buf_block_t *current_buf);
int idx_delete_record(int table_id, buf_block_t *leaf_buf, int64_t key);
int idx_delete(int table_id, int64_t key);

#endif // __INDEX_MANAGER_H__