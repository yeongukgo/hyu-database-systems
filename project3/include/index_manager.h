#ifndef __INDEX_MANAGER_H__
#define __INDEX_MANAGER_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
//#include "disk_space_manager.h"
#include "buffer_manager.h"

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

// UTILITIES

int cut(int length);

// ----API----

// DB/TABLE

int index_init_db(int buf_num);
int index_shutdown_db();

int index_open_table(char * pathname);
int index_close_table(int table_id);

// FIND

pagenum_t index_find_leaf(int table_id, int64_t key);
int index_find(int table_id, int64_t key, char * ret_val);

// INSERTION

record index_make_record(int64_t key, char * value);
int index_make_page(page_t* page);
int index_make_leaf(page_t* page);
int index_get_left_index(buffer_block_t* parent_buffer, pagenum_t left_pagenum);
int index_insert_into_leaf(buffer_block_t* leaf_buffer, record new_record);
int index_insert_into_leaf_after_splitting(buffer_block_t* leaf_buffer, record new_record);
int index_insert_into_internal(buffer_block_t* parent_buffer, int left_index, int64_t key, pagenum_t right_pagenum);
int index_insert_into_internal_after_splitting(buffer_block_t* parent_buffer, int left_index, int64_t key, pagenum_t right_pagenum);
int index_insert_into_parent(buffer_block_t* left_buffer, int64_t key, buffer_block_t* right_buffer);
int index_insert_into_new_root(buffer_block_t* left_buffer, int64_t key, buffer_block_t* right_buffer);
int index_start_new_tree(int table_id, record new_record);
int index_insert(int table_id, int64_t key, char * value);

// DELETION
pagenum_t index_find_left_leaf(buffer_block_t *current_buffer);
int index_get_index(buffer_block_t *current_buffer, buffer_block_t *parent_buffer);
int index_get_neighbor_index(buffer_block_t *current_buffer, buffer_block_t *parent_buffer);
int index_adjust_root(buffer_block_t *header_buffer, buffer_block_t *root_buffer);
int index_remove_entry_from_internal(buffer_block_t *current_buffer);
int index_remove_record_from_leaf(page_t* leaf_page, int64_t key);
int index_merge_pages(buffer_block_t *current_buffer, buffer_block_t *neighbor_buffer, buffer_block_t *parent_buffer,
        int neighbor_index, int k_prime_index, int64_t k_prime);
int index_redistribute_pages(buffer_block_t *current_buffer, buffer_block_t *neighbor_buffer, buffer_block_t *parent_buffer,
        int neighbor_index, int k_prime_index, int64_t k_prime);
int index_delete_entry(buffer_block_t *current_buffer);
int index_delete_record(int table_id, pagenum_t leaf_pagenum, int64_t key);
int index_delete(int table_id, int64_t key);

#endif // __INDEX_MANAGER_H__