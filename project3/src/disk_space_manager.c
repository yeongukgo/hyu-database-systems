#include "disk_space_manager.h"

// API

// Open a file(table).
int file_open_table(char* pathname) {
    int fd;
    header_page_t header_page;
    
    // Use O_SYNC flag to read/write directly at disk.
    // To Do not use kernel buffer caching.

    // If file exists, just open.
    if((fd = open(pathname, O_RDWR | O_SYNC)) >= 0) {
        return fd;
    }
    
    // If file doesn't exists, create and initialize.
    if((fd = open(pathname, O_RDWR | O_SYNC | O_CREAT, 0644)) >= 0) {
        memset(&header_page, 0, SIZE_PAGE);
        header_page.free_page_num = 0;
        header_page.root_page_num = 0;
        header_page.num_of_pages = 1;
        file_write_page(fd, HEADER_PAGE_NUM, (page_t*)&header_page);
    }

    // If failed to open and create, return -1.
    return fd;
}

// Close a file(table).
int file_close_table(int fd) {
    return close(fd);
}
/*
// Allocate an on-disk page from the free page list.
pagenum_t file_alloc_page(int fd) {
    pagenum_t new_pagenum;
    page_t new_page;
    header_page_t header_page;

    file_read_page(fd, HEADER_PAGE_NUM, (page_t*)&header_page);

    // If free page doesn't exists, make a page.
    if(header_page.free_page_num == 0) {
        new_pagenum = header_page.num_of_pages;
        memset(&new_page, 0, SIZE_PAGE);
        header_page.num_of_pages++;

        file_write_page(fd, HEADER_PAGE_NUM, (page_t*)&header_page);
        file_write_page(fd, new_pagenum, &new_page);
    }
    // If free page exists, allocate the first free page from the free page list.
    else {
        new_pagenum = header_page.free_page_num;
        file_read_page(fd, new_pagenum, &new_page);

        header_page.free_page_num = new_page.parent_or_next_free_page_num;
        new_page.parent_or_next_free_page_num = 0;

        file_write_page(fd, HEADER_PAGE_NUM, (page_t*)&header_page);
        file_write_page(fd, new_pagenum, &new_page); 
    }
    
    // Add exception handling.

    return new_pagenum;
}

// Free an on-disk page to the free page list.
int file_free_page(int fd, pagenum_t pagenum) {
    page_t new_free_page;
    header_page_t header_page;
    int result = 0;

    result = file_read_page(fd, HEADER_PAGE_NUM, (page_t*)&header_page);
    // result = file_read_page(fd, pagenum, &new_free_page); // It is not necessary.
    memset(&new_free_page, 0, SIZE_PAGE);

    if(result < 0) {
        return -1; // Error occured during file_read_page.
    }

    new_free_page.parent_or_next_free_page_num = header_page.free_page_num;
    header_page.free_page_num = pagenum;

    result = file_write_page(fd, HEADER_PAGE_NUM, (page_t*)&header_page);
    result = file_write_page(fd, pagenum, &new_free_page);

    if(result < 0) {
        return -2; // Error occured during file_write_page.
    }

    return 0;
}
*/
// Read an on-disk page into the in-memory page structure(dest).
int file_read_page(int fd, pagenum_t pagenum, page_t* dest) {
    ssize_t result;
    off_t offset = PAGENUM_TO_OFFSET( pagenum );
    result = pread(fd, dest, SIZE_PAGE, offset);
    
    if(result == SIZE_PAGE) {
        return 0;
    }
    else {
        return -1; // Error occured during pread.
    }
}

// Write an in-memory page(src) to the on-disk page.
int file_write_page(int fd, pagenum_t pagenum, const page_t* src) {
    ssize_t result;
    off_t offset = PAGENUM_TO_OFFSET( pagenum );
    result = pwrite(fd, src, SIZE_PAGE, offset);
    // fflush or fsync are not necessary,
    // because O_SYNC flag was used when file opened.

    if(result == SIZE_PAGE) {
        return 0;
    }
    else {
        return -1; // Error occured during pread.
    }
}
