#ifndef __DISK_SPACE_MANAGER_H__
#define __DISK_SPACE_MANAGER_H__

#include <stdio.h>
#include <stdint.h>
#include <string.h> // for memset.+@.
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include "definition.h"

typedef uint64_t pagenum_t; // Start from 0. Pagenum of header page is 0.

// In-memory record structure.
typedef struct record { // 128 (8 + 120) Bytes.
    int64_t key;
    char value[SIZE_RECORD_VALUE];
} record;

// In-memory entry structure.
typedef struct entry { // 16 (8 + 8) Bytes.
    int64_t key;
    pagenum_t pagenum;
} entry;

// In-memory header page structure.
typedef struct header_page_t {
    pagenum_t free_page_num; // [0-7]
    pagenum_t root_page_num; // [8-15]
    uint64_t num_of_pages; // [16-23]
    char reserved[SIZE_HEADER_PAGE_RESERVED]; // [24-4095]
} header_page_t;

// In-memory leaf/internal/free page structure.
typedef struct page_t {
    // Page Header [0-127]
    pagenum_t parent_or_next_free_page_num; // [0-7]
    int32_t is_leaf; // [8-11]
    uint32_t num_of_keys; // [12-15]
    char reserved[SIZE_PAGE_HEADER_RESERVED]; // [16-119]
    pagenum_t right_sibling_or_one_more_page_num; // [120-127]

    union {
        record records[ORDER_LEAF - 1]; // [128-4095] (128 * 31 = 3968)
        entry entries[ORDER_INTERNAL - 1]; // [128-4095] (16 * 248 = 3968)
    } childs;
} page_t;

// int fd; // File descriptor.

// API

// Open a file(table).
int file_open_table(char* pathname);

// Close a file(table).
int file_close_table(int fd);

// Read an on-disk page into the in-memory page structure(dest).
int file_read_page(int fd, pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page.
int file_write_page(int fd, pagenum_t pagenum, const page_t* src);

#endif // __DISK_SPACE_MANAGER_H__