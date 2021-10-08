#ifndef __INDEX_MANAGER_H__
#define __INDEX_MANAGER_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "disk_space_manager.h"

// Return values.

#define SUCCESS 0
#define PAGE_NOT_OPENED -1
#define ERROR -5

// For db_find_left_leaf
#define FIND_LEFT_LEAF_LEFTMOST -1
#define FIND_LEFT_LEAF_CANNOT_FIND -2

// For db_find.
#define FIND_EMPTY_TREE -2
#define FIND_NOT_EXISTS -3

// For db_insert.
#define INSERT_DUPLICATED -2

// For db_get_index.
#define GET_INDEX_LEFTMOST -1
#define GET_INDEX_ERROR -3

// For db_get_neighbor_index.
#define GET_NEIGHBOR_INDEX_LEFTMOST -2
#define GET_NEIGHBOR_INDEX_ERROR -3

// For db_delete.
#define DELETE_NOT_EXISTS -1

// UTILITIES

int cut(int length);
pagenum_t db_find_left_leaf(pagenum_t pagenum);

// ----API----

// OPEN

int open_table(char * pathname);

// FIND

pagenum_t db_find_leaf(int64_t key);
int db_find(int64_t key, char * ret_val);

// INSERTION

record db_make_record(int64_t key, char * value);
page_t db_make_page();
page_t db_make_leaf();
int db_get_left_index(pagenum_t parent_pagenum, pagenum_t left_pagenum);
int db_insert_into_leaf(pagenum_t pagenum, record new_record);
int db_insert_into_leaf_after_splitting(pagenum_t pagenum, record new_record);
int db_insert_into_internal(pagenum_t parent_pagenum, int left_index, int64_t key, pagenum_t right_pagenum);
int db_insert_into_internal_after_splitting(pagenum_t parent_pagenum, int left_index, int64_t key, pagenum_t right_pagenum);
int db_insert_into_parent(pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum);
int db_insert_into_new_root(pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum);
int db_start_new_tree(record new_record);
int db_insert(int64_t key, char * value);

// DELETION

int db_get_index(pagenum_t pagenum);
int db_get_neighbor_index(pagenum_t pagenum);
int db_adjust_root();
int db_remove_entry_from_internal(pagenum_t pagenum);
int db_remove_record_from_leaf(pagenum_t leaf_pagenum, int64_t key);
int db_merge_pages(pagenum_t current_pagenum, pagenum_t neighbor_pagenum,
        int neighbor_index, int k_prime_index, int64_t k_prime);
int db_redistribute_pages(pagenum_t current_pagenum, pagenum_t neighbor_pagenum,
        int neighbor_index, int k_prime_index, int64_t k_prime);
int db_delete_entry(pagenum_t pagenum);
int db_delete_record(pagenum_t leaf_pagenum, int64_t key);
int db_delete(int64_t key);

#endif // __INDEX_MANAGER_H__