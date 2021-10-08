#include "disk_space_manager.h"

// API

// Open a file(table).
int disk_open_table(char* pathname) {
    int fd;
    hdr_page_t hdr_page;
    
    // Use O_SYNC flag to read/write directly at disk.
    // To Do not use kernel buffer caching.

    // If file exists, just open.
    if((fd = open(pathname, O_RDWR | O_SYNC)) >= 0) {
        return fd;
    }
    
    // If file doesn't exists, create and initialize.
    if((fd = open(pathname, O_RDWR | O_CREAT | O_SYNC, 0644)) >= 0) {
        memset(&hdr_page, 0, SIZE_PAGE);
        hdr_page.free_page_num = 0;
        hdr_page.root_page_num = 0;
        hdr_page.num_of_pages = 1;
        disk_write_page(fd, HEADER_PAGE_NUM, (page_t*)&hdr_page);
    }

    // If failed to open and create, return -1.
    return fd;
}

// Close a file(table).
int disk_close_table(int fd) {
    return close(fd);
}

// Read an on-disk page into the in-memory page structure(dest).
int disk_read_page(int fd, pagenum_t pagenum, page_t* dest) {
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
int disk_write_page(int fd, pagenum_t pagenum, const page_t* src) {
    ssize_t result;
    off_t offset = PAGENUM_TO_OFFSET( pagenum );
    result = pwrite(fd, src, SIZE_PAGE, offset);
    // fsync(fd);
    // fflush or fsync are not necessary,
    // because O_SYNC flag was used when file opened.

    if(result == SIZE_PAGE) {
        return 0;
    }
    else {
        return -1; // Error occured during pread.
    }
}
