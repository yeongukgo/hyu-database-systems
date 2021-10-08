#include "disk_space_manager.h"

// UTILITIES

// Return root page number.
pagenum_t get_root_page_num() {
    header_page_t header_page;
    file_read_page(HEADER_PAGE_NUM, (page_t*)&header_page);

    return header_page.root_page_num;
}

// Return number of pages.
uint64_t get_num_of_pages() {
    header_page_t header_page;
    file_read_page(HEADER_PAGE_NUM, (page_t*)&header_page);

    return header_page.num_of_pages;
}

// Set root page number.
void set_root_page_num(pagenum_t pagenum) {
    header_page_t header_page;
    file_read_page(HEADER_PAGE_NUM, (page_t*)&header_page);
    header_page.root_page_num = pagenum;
    file_write_page(HEADER_PAGE_NUM, (page_t*)&header_page);
}

// Convert offset to pagenum.
pagenum_t offset_to_pagenum(off_t offset) {
    return offset / PAGE_SIZE;
}

// Convert pagenum to offset.
off_t pagenum_to_offset(pagenum_t pagenum) {
    return pagenum * PAGE_SIZE;
}

// API

// Open a file(table).
int file_open(char* pathname) {
    header_page_t header_page;
    
    // Use O_SYNC flag to read/write directly at disk.
    // To Do not use kernel buffer caching.

    // If file exists, just open.
    if((fd = open(pathname, O_RDWR | O_SYNC)) >= 0) {
        return fd;
    }
    
    // If file doesn't exists, create and initialize.
    if((fd = open(pathname, O_RDWR | O_SYNC | O_CREAT, 0644)) >= 0) {
        memset(&header_page, 0, PAGE_SIZE);
        header_page.free_page_num = 0;
        header_page.root_page_num = 0;
        header_page.num_of_pages = 1;
        file_write_page(HEADER_PAGE_NUM, (page_t*)&header_page);
    }

    // If failed to open and create, return -1.
    return fd;
}

// Allocate an on-disk page from the free page list.
pagenum_t file_alloc_page() {
    pagenum_t new_pagenum;
    page_t new_page;
    header_page_t header_page;

    file_read_page(HEADER_PAGE_NUM, (page_t*)&header_page);

    // If free page doesn't exists, make a page.
    if(header_page.free_page_num == 0) {
        new_pagenum = header_page.num_of_pages;
        memset(&new_page, 0, PAGE_SIZE);
        header_page.num_of_pages++;

        file_write_page(HEADER_PAGE_NUM, (page_t*)&header_page);
        file_write_page(new_pagenum, &new_page);
    }
    // If free page exists, allocate the first free page from the free page list.
    else {
        new_pagenum = header_page.free_page_num;
        file_read_page(new_pagenum, &new_page);

        header_page.free_page_num = new_page.parent_or_next_free_page_num;
        new_page.parent_or_next_free_page_num = 0;

        file_write_page(HEADER_PAGE_NUM, (page_t*)&header_page);
        file_write_page(new_pagenum, &new_page); 
    }
    
    // Add exception handling.

    return new_pagenum;
}

// Free an on-disk page to the free page list.
void file_free_page(pagenum_t pagenum) {
    page_t new_free_page;
    header_page_t header_page;

    file_read_page(HEADER_PAGE_NUM, (page_t*)&header_page);
    file_read_page(pagenum, &new_free_page); // It is not necessary.
    memset(&new_free_page, 0, PAGE_SIZE);

    new_free_page.parent_or_next_free_page_num = header_page.free_page_num;
    header_page.free_page_num = pagenum;

    file_write_page(HEADER_PAGE_NUM, (page_t*)&header_page);
    file_write_page(pagenum, &new_free_page); 
}

// Read an on-disk page into the in-memory page structure(dest).
void file_read_page(pagenum_t pagenum, page_t* dest) {
    off_t offset = pagenum_to_offset(pagenum);
    pread(fd, dest, PAGE_SIZE, offset);
}

// Write an in-memory page(src) to the on-disk page.
void file_write_page(pagenum_t pagenum, const page_t* src) {
    off_t offset = pagenum_to_offset(pagenum);
    pwrite(fd, src, PAGE_SIZE, offset);
    // fflush or fsync are not necessary,
    // because O_SYNC flag was used when file opened.
}
