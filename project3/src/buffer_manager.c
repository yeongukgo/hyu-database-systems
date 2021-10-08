#include "buffer_manager.h"

//buffer_header_t* buf_header = NULL;

bool db_opened = false;
int buffer_size = 0; // Size of buffer pool. Maximum number of buffer blocks.
int table_num = 0; // Number of opened tables.
int block_num = 0; // Number of allocated buffer blocks.
buffer_table_t buf_tables[MAX_NUM_OF_TABLE_ID];
buffer_block_t* head_of_LRU_list = NULL;
buffer_block_t* tail_of_LRU_list = NULL;

// This will be not used by buffer block pointer.
// This will be used by buffer block array.
buffer_block_t* buf_pool = NULL;


// UTILITIES

int find_table_index(int table_id) {
    int i, table_index = -1;

    if(!db_opened) {
        return -2; // DB is not initialized.
    }

    for(i = 0; i < table_num; i++) {
        if(buf_tables[i].table_id == table_id) {
            table_index = i;
            break;
        }
    }

    // If not found, return -1.
    return table_index;
}
/*
int find_buffer_block_index(buffer_block_t* buf_block) {
    int i, buf_block_index = -1;

    if(!db_opened) {
        return -2; // DB is not initialized.
    }

    for(i = 0; i < block_num; i++) {
        if(&(buf_pool[i]) == buf_block) {
            buf_block_index = i;
            break;
        }
    }

    // If not found, return -1.
    return buf_block_index;
}
*/
// Call this function after checking the block can be evicted.
int evict_buffer_block(buffer_block_t* buf_block) {
    int buffer_block_index, i, result = 0;
    buffer_table_t* buf_table = &(buf_tables[find_table_index(buf_block->table_id)]);

    // If it is dirty, rewrite file.
    if(buf_block->is_dirty) {
        result = file_write_page(buf_table->fd, buf_block->page_num, &buf_block->frame);
    }

    if(result < 0) {
        return -1; // Error occured during file_write_page.
    }

    // Update links of LRU list.
    if(buf_block->prev_of_LRU == NULL) { // This is head of LRU list.
        //if(buf_block != head_of_LRU_list) {printf("error\n");}
        head_of_LRU_list = buf_block->next_of_LRU;
    }
    else { // This is not head of LRU list.
        buf_block->prev_of_LRU->next_of_LRU = buf_block->next_of_LRU;
    }

    if(buf_block->next_of_LRU == NULL) { // This is tail of LRU list.
        //if(buf_block != tail_of_LRU_list) {printf("error\n");}
        tail_of_LRU_list = buf_block->prev_of_LRU;
    }
    else { // This is not tail of LRU list.
        buf_block->next_of_LRU->prev_of_LRU = buf_block->prev_of_LRU;
    }

    // Update links of table.
    if(buf_block->prev_of_table == NULL) { // This is head of table.
        buf_table->head_of_table = buf_block->next_of_table;
    }
    else { // This is not head of table.
        buf_block->prev_of_table->next_of_table = buf_block->next_of_table;
    }

    if(buf_block->next_of_table != NULL) { // This is not last block of table.
        buf_block->next_of_table->prev_of_table = buf_block->prev_of_table;
    }
    
    // Remove current block from buffer pool.
    /*
    buffer_block_index = find_buffer_block_index(buf_block);
    for(i = buffer_block_index; i < (block_num - 1); i++) {
        buf_pool[i] = buf_pool[i + 1];
    }*/
    buf_block->table_id = 0;
    block_num--;
    buf_table->block_num--;

    return 0;
}

int create_buffer_block(int table_id, pagenum_t pagenum, page_t buf_page, buffer_block_t **dest) {
    int table_index, i, result = 0;
    buffer_table_t* buf_table = NULL;
    buffer_block_t* buf_block = NULL;
    table_index = find_table_index(table_id);

    if(table_index == -1) {
        return -2; // Given table id not exists.
    }

    buf_table = &(buf_tables[table_index]);
    // Buffer pool is full. LRU page eviction.
    if(block_num == buffer_size) {
        // LRU page eviction
        buf_block = tail_of_LRU_list;
        while(true) {
            if(buf_block->is_pinned == false) {
                break;
            }
            buf_block = buf_block->prev_of_LRU;

            if(buf_block == NULL) {
                // Every blocks of buffer are in use.
                return -1;
            }
        }

        result = evict_buffer_block(buf_block);

        if(result < 0) { // Error occured during evicting.
            return -2;
        }
    }
    // Buffer pool is not full. Just find empty block.
    else {
        for(i = 0; i < buffer_size; i++) {
            if(buf_pool[i].table_id == 0) { // A block that is empty.
                buf_block = &(buf_pool[i]);
                break;
            }
        }

        if(buf_block == NULL) {
            return -3; // Buffer pool is not full, but there are not empty block.
        }
    } 

    // Initialize block.
    buf_block->is_pinned = 1;
    buf_block->frame = buf_page;
    buf_block->table_id = buf_table->table_id;
    buf_block->page_num = pagenum;
    buf_block->is_dirty = false;

    // Set it to head of LRU list.
    if(buf_block != head_of_LRU_list) {
        buf_block->prev_of_LRU = NULL;
        buf_block->next_of_LRU = head_of_LRU_list;
        if(head_of_LRU_list != NULL) {
            head_of_LRU_list->prev_of_LRU = buf_block;
        }
        else { // head of LRU list is NULL. this is first.
            tail_of_LRU_list = buf_block;
        }
        head_of_LRU_list = buf_block;
    }
    // Set it to head of table.
    if(buf_block != buf_table->head_of_table) {
        buf_block->prev_of_table = NULL;
        buf_block->next_of_table = buf_table->head_of_table;
        if(buf_table->head_of_table != NULL) {
            buf_table->head_of_table->prev_of_table = buf_block;
        }
        buf_table->head_of_table = buf_block;
    }
    

    block_num++;
    buf_table->block_num++;

    // return.
    *dest = buf_block;

    return 0;
}

// API

int buffer_init_db(int buf_num) {
    int i;

    if(db_opened) { //  DB is already opened.
        return -1;
    }

    buf_pool = (buffer_block_t*)malloc(sizeof(buffer_block_t) * buf_num);
    if(buf_pool == NULL) { // Fail to allocate.
        //printf("");
        return -2;
    }

    for(i = 0; i < buf_num; i++) {
        buf_pool[i].table_id = 0; // Set blocks empty.
    }

    db_opened = true;
    buffer_size= buf_num;
    table_num = 0;
    block_num = 0;
    head_of_LRU_list = NULL;
    tail_of_LRU_list = NULL;

    return 0;
}

int buffer_shutdown_db(void) {
    int i, result = 0;

    if(!db_opened) {
        return -1; // DB is not initialized.
    }
    
    for(i = 0; i < table_num; i++) {
        result += buffer_close_table(buf_tables[i].table_id);
    }

    // Error checking.
    if(result < 0) {
        return -2; // Error occurred during buffer_close.
    }

    free(buf_pool);
    db_opened = false;

    return 0;
}

int buffer_open_table(char* pathname) {
    int i, fd, table_id;
    buffer_table_t* buf_table;

    if(!db_opened) {
        return -1; // DB is not initialized.
    }

    if(table_num == MAX_NUM_OF_TABLE_ID) {
        return -2; // Full opened tables.
    }

    fd = file_open_table(pathname);

    if(fd < 0) {
        return -3; // Fail to open file.
    }

    // It will be changed.
    table_id = fd + 1;

    buf_table = &(buf_tables[table_num]);
    buf_table->table_id = table_id;
    buf_table->fd = fd;
    buf_table->block_num = 0;
    buf_table->head_of_table = NULL;

    table_num++;

    return table_id;
}

int buffer_close_table(int table_id) {
    int table_index, i, result = 0;
    buffer_table_t* buf_table;
    buffer_block_t* buf_block;

    if(!db_opened) {
        return -1; // DB is not initialized.
    }

    table_index = find_table_index(table_id);

    if(table_index == -1) {
        return -2; // Given table id not exists.
    }

    buf_table = &(buf_tables[table_index]);

    // If pinned block exists, can not close table.
    // Check that and pin every blocks.
    buf_block = buf_table->head_of_table;
    while(buf_block != NULL) {
        if(buf_block->is_pinned) {
            // Cannot close table.
            // Unpin pinned blocks that are pinned in this for loop.
            // After that, return error.
            buf_block = buf_block->prev_of_table;
            while(buf_block != NULL) {
                buf_block->is_pinned = 0;
                buf_block = buf_block->prev_of_table;
            }
            return -3; // Block in given table is pinned.
        }
        buf_block->is_pinned = 1;
        buf_block = buf_block->next_of_table;
    }

    while(buf_table->head_of_table != NULL) { // Evict every blocks of table.
        result = evict_buffer_block(buf_table->head_of_table);
        //buf_table->block_num--;
    }

    if(result < 0) { // Error occured during evicting buffer blocks.
        return -4;
    }

    // Close file.
    result = file_close_table(buf_table->fd);

    if(result < 0) { // Error occured during closing file.
        return -5;
    }

    // Remove current table from table array of buffer header.
    for(i = table_index;  i < table_num - 1; i++) {
        buf_tables[i] = buf_tables[i - 1];
    }
    table_num--;

    return 0;
}

pagenum_t buffer_alloc_page(int table_id) {
    int result;
    buffer_block_t *header, *buf_block;
    header_page_t *header_page;
    pagenum_t new_pagenum;
    page_t* new_page;
    page_t temp_page;

    result = buffer_request_page(table_id, HEADER_PAGE_NUM, &header);
    
    if(result < 0) { // Error occured during request. (header)
        return -1;
    }

    header_page = (header_page_t*)&(header->frame);
    // If free page doesn't exists, make a page.
    if(header_page->free_page_num == 0) {
        new_pagenum = header_page->num_of_pages;
        memset(&temp_page, 0, SIZE_PAGE);
        //result = create_buffer_block(table_id, new_pagenum, temp_page, &buf_block);
        buffer_table_t* buf_table = &(buf_tables[find_table_index(table_id)]);
        result = file_write_page(buf_table->fd, new_pagenum, &temp_page);
        if(result < 0) {
            return -1; // Error occured during file_write_page.
        }
        result = buffer_request_page(table_id, new_pagenum, &buf_block);

        if(result < 0) {
            return -2; // Error occured during request. (maked empty page)
        }
        header->is_dirty = true;
        buf_block->is_dirty = true;

        header_page->num_of_pages++;
    }
    // If free page exists, allocate the first free page from the free page list.
    else {
        new_pagenum = header_page->free_page_num;
        result = buffer_request_page(table_id, new_pagenum, &buf_block);

        if(result < 0) { // Error occured during request. (free)
            buffer_release_page(header, false);
            return -3;
        }
        new_page = &(buf_block->frame);
        header->is_dirty = true;
        buf_block->is_dirty = true;

        header_page->free_page_num = new_page->parent_or_next_free_page_num;
        memset(new_page, 0, SIZE_PAGE);
    }

    buffer_release_page(header, true);
    buffer_release_page(buf_block, true);

    return new_pagenum;
}

int buffer_free_page(int table_id, pagenum_t pagenum) {
    int result;
    buffer_block_t *header, *buf_block;
    header_page_t *header_page;
    page_t* page;

    result = buffer_request_page(table_id, HEADER_PAGE_NUM, &header);
    if(result < 0) { // Error occured during request. (header)
        return -1;
    }

    result = buffer_request_page(table_id, pagenum, &buf_block);
    if(result < 0) { // Error occured during request. (header)
        return -2;
    }

    header_page = (header_page_t*)&(header->frame);
    page = &(buf_block->frame);
    header->is_dirty = true;
    buf_block->is_dirty = true;

    memset(page, 0, SIZE_PAGE);
    page->parent_or_next_free_page_num = header_page->free_page_num;
    header_page->free_page_num = pagenum;

    buffer_release_page(header, true);
    buffer_release_page(buf_block, true);

    return 0;
}

int buffer_request_page(int table_id, pagenum_t pagenum, buffer_block_t** dest) {
    int table_index, block_index, i, result = 0;
    buffer_table_t* buf_table = NULL;
    buffer_block_t* buf_block = NULL;
    page_t buf_page;

    if(!db_opened) {
        return -1; // DB is not initialized.
    }
    table_index = find_table_index(table_id);
    if(table_index == -1) {
        return -2; // Given table id not exists.
    }

    buf_table = &(buf_tables[table_index]);
    // Find the page from buffer pool.
    buf_block = buf_table->head_of_table;
    while(buf_block != NULL) {
        // Page is found.
        if(buf_block->page_num == pagenum) {
            if(buf_block->is_pinned) {
                // Use is_writing with pin to allow multiple reading?
                return -3; // The page is in use.
            }
            break;
        }
        buf_block = buf_block->next_of_table; 
    }
    // Page is not found in buffer. Read file.
    if(buf_block == NULL) { 
        result = file_read_page(buf_table->fd, pagenum, &buf_page);
        // Error occured during file reading.
        if(result < 0) {
            return -4;
        }
        result = create_buffer_block(table_id, pagenum, buf_page, &buf_block);
        // Error occured during creating buffer block.
        if(result < 0) {
            return -5;
        }
    }
    // Pin and return.
    if(buf_block == NULL) {
        printf("buf_block is NULL\n");
    }
    buf_block->is_pinned = 1;
    *dest = buf_block;
    return 0;
}

int buffer_release_page(buffer_block_t* buf_block, bool is_writed) {
    
    if(!buf_block->is_pinned) {
        return -1; // Error. Cannot release unpinned block.
    }

    if(is_writed) {
        buf_block->is_dirty = true;
    }
    
    // Set it to head of LRU list.
    if(buf_block != head_of_LRU_list) {
        if(buf_block->prev_of_LRU != NULL) {
            buf_block->prev_of_LRU->next_of_LRU = buf_block->next_of_LRU;
        }
        if(buf_block->next_of_LRU != NULL) {
            buf_block->next_of_LRU->prev_of_LRU = buf_block->prev_of_LRU;
        }
        else { // This is tail of LRU list
            tail_of_LRU_list = buf_block->prev_of_LRU;
        }
        buf_block->prev_of_LRU = NULL;
        buf_block->next_of_LRU = head_of_LRU_list;
        if(head_of_LRU_list != NULL) {
            head_of_LRU_list->prev_of_LRU = buf_block;
        }
        head_of_LRU_list = buf_block;
    }

    

    // Unpin and return.
    buf_block->is_pinned = 0;

    return 0;
}