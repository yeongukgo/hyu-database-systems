#include "buffer_manager.h"

bool db_opened = false;
int buf_size = 0; // Size of buffer pool. Maximum number of buffer blocks.
int table_num = 0; // Number of opened tables.
int block_num = 0; // Number of allocated buffer blocks.
buf_table_t buf_tables[MAX_NUM_OF_TABLE_ID];
buf_block_t* head_of_LRU_list = NULL;
buf_block_t* tail_of_LRU_list = NULL;

// This will be not used by buffer block pointer.
// This will be used by buffer block array.
buf_block_t* buf_pool = NULL;

hash_bkt_t* hash_table = NULL;
int hash_size = 0;

pthread_mutex_t buf_pool_latch;

/*** UTILITIES ***/

int buf_find_table_idx(int table_id) {
    int i, table_idx = -1;

    if(!db_opened) {
        return -2; // DB is not initialized.
    }

    for(i = 0; i < table_num; i++) {
        if(buf_tables[i].table_id == table_id) {
            table_idx = i;
            break;
        }
    }

    // If not found, return -1.
    return table_idx;
}

// Call this function after checking the block can be evicted.
int buf_evict_buf_block(buf_block_t* buf_block) {
    int buf_block_idx, i, result = 0;
    buf_table_t* buf_table = &(buf_tables[buf_find_table_idx(buf_block->table_id)]);

    // If it is dirty, rewrite file.
    if(buf_block->is_dirty) {
        result = disk_write_page(buf_table->fd, buf_block->page_num, &buf_block->frame);
    }

    if(result < 0) {
        return -1; // Error occured during disk_write_page.
    }

    // Update links of LRU list.
    if(buf_block->prev_of_LRU == NULL) { // This is head of LRU list.
        head_of_LRU_list = buf_block->next_of_LRU;
    }
    else { // This is not head of LRU list.
        buf_block->prev_of_LRU->next_of_LRU = buf_block->next_of_LRU;
    }

    if(buf_block->next_of_LRU == NULL) { // This is tail of LRU list.
        tail_of_LRU_list = buf_block->prev_of_LRU;
    }
    else { // This is not tail of LRU list.
        buf_block->next_of_LRU->prev_of_LRU = buf_block->prev_of_LRU;
    }

    buf_hash_delete(buf_block->table_id, buf_block->page_num);

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
    
    buf_block->table_id = 0;
    block_num--;
    buf_table->block_num--;

    return 0;
}

int buf_set_buf_block(int table_id, pagenum_t pagenum, page_t buf_page, buf_block_t **dest) {
    int table_idx, i, result = 0;
    buf_table_t* buf_table = NULL;
    buf_block_t* buf_block = NULL;
    table_idx = buf_find_table_idx(table_id);

    if(table_idx == -1) {
        return -2; // Given table id not exists.
    }

    buf_table = &(buf_tables[table_idx]);
    // Buffer pool is full. LRU page eviction.
    if(block_num == buf_size) {
        // LRU page eviction
        buf_block = tail_of_LRU_list;
        while(true) {
            if(buf_block->is_pinned == 0) {
                break;
            }
            buf_block = buf_block->prev_of_LRU;

            if(buf_block == NULL) {
                // Every blocks of buffer are in use.
                return -1;
            }
        }

        result = buf_evict_buf_block(buf_block);

        if(result < 0) { // Error occured during evicting.
            return -2;
        }
    }
    // Buffer pool is not full. Just find empty block.
    else {
        for(i = 0; i < buf_size; i++) {
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
    buf_block->is_pinned = 1; // Add pin.
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

    // Insert it in hash table.
    buf_hash_insert(table_id, pagenum, buf_block);

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

int buf_hash_insert(int table_id, pagenum_t pagenum, buf_block_t *buf_block) {
    hash_bkt_t* hash_bkt;
    uint64_t key;
    int pos;

    // Cantor pairing function.
    key = ((table_id + pagenum)*(table_id + pagenum + 1))/2 + pagenum;

    pos = key % hash_size;
    hash_bkt = &(hash_table[pos]);

    if(hash_bkt->table_id != -1) {
        while(hash_bkt->next != NULL) {
            hash_bkt = hash_bkt->next;
        }
        hash_bkt->next = (hash_bkt_t*)malloc(sizeof(hash_bkt_t));
        hash_bkt = hash_bkt->next;
    }
    hash_bkt->table_id = table_id;
    hash_bkt->pagenum = pagenum;
    hash_bkt->buf_block = buf_block;
    hash_bkt->next = NULL;

    return 0;
}

buf_block_t* find_from_hash_table(int table_id, pagenum_t pagenum) {
    hash_bkt_t *hash_bkt;
    uint64_t key;
    int pos;

    // Cantor pairing function.
    key = ((table_id + pagenum)*(table_id + pagenum + 1))/2 + pagenum;

    pos = key % hash_size;
    hash_bkt = &(hash_table[pos]);

    if(hash_bkt->table_id == -1) {
        return NULL; // not exist.
    }

    while(hash_bkt->table_id != table_id || hash_bkt->pagenum != pagenum) {
        hash_bkt = hash_bkt->next;
        if(hash_bkt == NULL) {
            return NULL; // not exist.
        }
    }

    // Add pin.
    hash_bkt->buf_block->is_pinned++;

    return hash_bkt->buf_block;
}

int buf_hash_delete(int table_id, pagenum_t pagenum) {
    hash_bkt_t* hash_bkt, *hash_bkt_temp;
    uint64_t key;
    int pos;

    // Cantor pairing function.
    key = ((table_id + pagenum)*(table_id + pagenum + 1))/2 + pagenum;

    pos = key % hash_size;
    hash_bkt = &(hash_table[pos]);

    if(hash_bkt->table_id == -1) {
        return -1; // not exist.
    }

    if(hash_bkt->table_id == table_id && hash_bkt->pagenum == pagenum) {
        if(hash_bkt->next != NULL) {
            hash_bkt_temp = hash_bkt->next;
            hash_table[pos] = *hash_bkt_temp;
            free(hash_bkt_temp);
        }
        else {
            hash_bkt->table_id = -1;
            hash_bkt->buf_block = NULL;
        }
    }
    else {
        if(hash_bkt->next == NULL) {
            return -1; // not exist.
        }
        while(hash_bkt->next->table_id != table_id || hash_bkt->next->pagenum != pagenum) {
            hash_bkt = hash_bkt->next;
            if(hash_bkt->next == NULL) {
                return -1; // not exist.
            }
        }
        hash_bkt_temp = hash_bkt->next;
        hash_bkt->next = hash_bkt_temp->next;
        free(hash_bkt_temp);
    }
    
    return 0;
}

void buf_acquire_buf_pool_latch() {
    pthread_mutex_lock(&buf_pool_latch);
}

void buf_release_buf_pool_latch() {
    pthread_mutex_unlock(&buf_pool_latch);
}

/*** API ***/

int buf_init_db(int buf_num) {
    int i;

    if(db_opened) { //  DB is already opened.
        return -1;
    }

    buf_pool = (buf_block_t*)malloc(sizeof(buf_block_t) * buf_num);
    if(buf_pool == NULL) { // Fail to allocate.
        return -2;
    }

    hash_size = buf_num;
    hash_table = (hash_bkt_t*)malloc(sizeof(hash_bkt_t) * hash_size);
    if(hash_table == NULL) { // Fail to allocate.
        return -2;
    }

    for(i = 0; i < buf_num; i++) {
        buf_pool[i].table_id = 0; // Set blocks empty.
        pthread_mutex_init(&(buf_pool[i].buf_page_latch), NULL);
    }

    for(i = 0; i < hash_size; i++) {
        hash_table[i].table_id = -1;
        hash_table[i].buf_block = NULL;
        hash_table[i].next = NULL;
    }

    db_opened = true;
    buf_size= buf_num;
    table_num = 0;
    block_num = 0;
    head_of_LRU_list = NULL;
    tail_of_LRU_list = NULL;

    pthread_mutex_init(&buf_pool_latch, NULL);

    return 0;
}

int buf_shutdown_db(void) {
    int i, result = 0, repeat;

    if(!db_opened) {
        return -1; // DB is not initialized.
    }

    repeat = table_num;
    for(i = 0; i < repeat; i++) {
        result += buf_close_table(buf_tables[0].table_id);
    }

    // Error checking.
    if(result < 0) {
        return -2; // Error occurred during buf_close.
    }

    free(buf_pool);

    // Linked hash buckets was deallocated already during closing table.
    free(hash_table);

    db_opened = false;

    return 0;
}

int buf_open_table(char* pathname) {
    int i, fd, table_id;
    buf_table_t* buf_table;

    if(!db_opened) {
        return -1; // DB is not initialized.
    }

    if(table_num == MAX_NUM_OF_TABLE_ID) {
        return -2; // Full opened tables.
    }

    fd = disk_open_table(pathname);

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

int buf_close_table(int table_id) {
    int table_idx, i, result = 0;
    buf_table_t* buf_table;
    buf_block_t* buf_block;

    if(!db_opened) {
        return -1; // DB is not initialized.
    }

    table_idx = buf_find_table_idx(table_id);

    if(table_idx == -1) {
        return -2; // Given table id not exists.
    }

    buf_table = &(buf_tables[table_idx]);

    // If pinned block exists, can not close table.
    // Check that and pin every blocks.
    buf_block = buf_table->head_of_table;
    while(buf_block != NULL) {
        if(buf_block->is_pinned > 0) {
            // Cannot close table.
            // Unpin pinned blocks that are pinned in this for loop.
            // After that, return error.
            buf_block = buf_block->prev_of_table;
            while(buf_block != NULL) {
                buf_block->is_pinned--;
                buf_block = buf_block->prev_of_table;
            }
            return -3; // Block in given table is pinned.
        }
        buf_block->is_pinned++;
        buf_block = buf_block->next_of_table;
    }

    while(buf_table->head_of_table != NULL) { // Evict every blocks of table.
        result = buf_evict_buf_block(buf_table->head_of_table);
        //buf_table->block_num--;
    }

    if(result < 0) { // Error occured during evicting buffer blocks.
        return -4;
    }

    // Close file.
    result = disk_close_table(buf_table->fd);

    if(result < 0) { // Error occured during closing file.
        return -5;
    }

    // Remove current table from table array of buffer hdr.
    for(i = table_idx;  i < table_num - 1; i++) {
        buf_tables[i] = buf_tables[i + 1];
    }
    table_num--;

    return 0;
}

pagenum_t buf_alloc_page(int table_id) {
    int result;
    buf_block_t *hdr_buf, *cur_buf;
    hdr_page_t *hdr_page;
    pagenum_t new_pagenum;
    page_t* new_page;
    page_t temp_page;

    result = buf_request_page(table_id, HEADER_PAGE_NUM, &hdr_buf);
    
    if(result < 0) { // Error occured during request. (header)
        return -1;
    }

    hdr_page = (hdr_page_t*)&(hdr_buf->frame);
    // If free page doesn't exists, make a page.
    if(hdr_page->free_page_num == 0) {
        new_pagenum = hdr_page->num_of_pages;
        memset(&temp_page, 0, SIZE_PAGE);
        //result = buf_set_buf_block(table_id, new_pagenum, temp_page, &buf_block);
        buf_table_t* buf_table = &(buf_tables[buf_find_table_idx(table_id)]);
        result = disk_write_page(buf_table->fd, new_pagenum, &temp_page);
        if(result < 0) {
            return -1; // Error occured during disk_write_page.
        }
        result = buf_request_page(table_id, new_pagenum, &cur_buf);

        if(result < 0) {
            return -2; // Error occured during request. (maked empty page)
        }
        hdr_buf->is_dirty = true;
        cur_buf->is_dirty = true;

        hdr_page->num_of_pages++;
    }
    // If free page exists, allocate the first free page from the free page list.
    else {
        new_pagenum = hdr_page->free_page_num;
        result = buf_request_page(table_id, new_pagenum, &cur_buf);

        if(result < 0) { // Error occured during request. (free)
            buf_release_page(hdr_buf, false);
            return -3;
        }
        new_page = &(cur_buf->frame);
        hdr_buf->is_dirty = true;
        cur_buf->is_dirty = true;

        hdr_page->free_page_num = new_page->parent_or_next_free_page_num;
        memset(new_page, 0, SIZE_PAGE);
    }

    buf_release_page(hdr_buf, true);
    buf_release_page(cur_buf, true);

    return new_pagenum;
}

int buf_free_page(int table_id, pagenum_t pagenum) {
    int result;
    buf_block_t *hdr_buf, *cur_buf;
    hdr_page_t *hdr_page;
    page_t* page;

    result = buf_request_page(table_id, HEADER_PAGE_NUM, &hdr_buf);
    if(result < 0) { // Error occured during request. (hdr_buf)
        return -1;
    }

    result = buf_request_page(table_id, pagenum, &cur_buf);
    if(result < 0) { // Error occured during request. (hdr_buf)
        return -2;
    }

    hdr_page = (hdr_page_t*)&(hdr_buf->frame);
    page = &(cur_buf->frame);
    hdr_buf->is_dirty = true;
    cur_buf->is_dirty = true;

    memset(page, 0, SIZE_PAGE);
    page->parent_or_next_free_page_num = hdr_page->free_page_num;
    hdr_page->free_page_num = pagenum;

    buf_release_page(hdr_buf, true);
    buf_release_page(cur_buf, true);

    return 0;
}

int buf_request_page(int table_id, pagenum_t pagenum, buf_block_t** dest) {
    int table_idx, block_idx, i, result = 0;
    buf_table_t* buf_table = NULL;
    buf_block_t* buf_block = NULL;
    page_t buf_page;

    if(!db_opened) {
        return -1; // DB is not initialized.
    }
    table_idx = buf_find_table_idx(table_id);
    if(table_idx == -1) {
        return -2; // Given table id not exists.
    }

    buf_table = &(buf_tables[table_idx]);

    buf_block = find_from_hash_table(table_id, pagenum);

    // Page is not found in buffer. Read file.
    if(buf_block == NULL) { 
        result = disk_read_page(buf_table->fd, pagenum, &buf_page);
        // Error occured during file reading.
        if(result < 0) {
            return -4;
        }
        result = buf_set_buf_block(table_id, pagenum, buf_page, &buf_block);
        if(result < 0) {
            return -5;
        }
    }
    // Return.
    if(buf_block == NULL) {
        printf("buf_block is NULL\n");
    }
    //buf_block->is_pinned++;
    // Pin was added at find_from_hash_table or buf_set_buf_block.
    *dest = buf_block;
    return 0;
}

int buf_release_page(buf_block_t* buf_block, bool is_writed) {
    
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
    //buf_block->is_pinned = 0;
    buf_block->is_pinned--;

    return 0;
}