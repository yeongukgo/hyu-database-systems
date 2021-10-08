#ifndef __DISK_SPACE_MANAGER_H__
#define __DISK_SPACE_MANAGER_H__

#include <stdio.h>
#include <string.h> // for memset.+@.
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#define PAGE_SIZE 4096
#define HEADER_PAGE_RESERVED_SIZE 4072
#define FREE_PAGE_RESERVED_SIZE 4088
#define PAGE_HEADER_RESERVED_SIZE 104
#define HEADER_PAGE_NUM 0
#define RECORD_SIZE 128
#define ENTRY_SIZE 16
#define RECORD_VALUE_SIZE 120
#define LEAF_ORDER 32
#define INTERNAL_ORDER 249

typedef uint64_t pagenum_t; // Start from 0. Pagenum of header page is 0.

// In-memory record structure.
typedef struct record { // 128 (8 + 120) Bytes.
    int64_t key;
    char value[RECORD_VALUE_SIZE];
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
    char reserved[HEADER_PAGE_RESERVED_SIZE]; // [24-4095]
} header_page_t;

// In-memory leaf/internal/free page structure.
typedef struct page_t {
    // Page Header [0-127]
    pagenum_t parent_or_next_free_page_num; // [0-7]
    int32_t is_leaf; // [8-11]
    uint32_t num_of_keys; // [12-15]
    char reserved[PAGE_HEADER_RESERVED_SIZE]; // [16-119]
    pagenum_t right_sibling_or_one_more_page_num; // [120-127]

    union {
        record records[LEAF_ORDER - 1]; // [128-4095] (128 * 31 = 3968)
        entry entries[INTERNAL_ORDER - 1]; // [128-4095] (16 * 248 = 3968)
    } childs;
} page_t;

int fd; // File descriptor.

// UTILITIES
pagenum_t get_root_page_num();
uint64_t get_num_of_pages();
void set_root_page_num(pagenum_t pagenum);
pagenum_t offset_to_pagenum(__off_t offset);
off_t pagenum_to_offset(pagenum_t pagenum);

// API

// Open a file(table).
int file_open(char* pathname);

// Allocate an on-disk page from the free page list.
pagenum_t file_alloc_page();

// Free an on-disk page to the free page list.
void file_free_page(pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest).
void file_read_page(pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page.
void file_write_page(pagenum_t pagenum, const page_t* src);

#endif // __DISK_SPACE_MANAGER_H__