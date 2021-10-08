#include "index_manager.h"
#include <pthread.h>

// Wrapper functions.

// Wrapper function of trx_init_db().
int idx_init_db(int buf_num) {
    return trx_init_db(buf_num);
}

// Wrapper function of trx_shutdown_db().
int idx_shutdown_db() {
    return trx_shutdown_db();
}

// Wrapper function of buf_open_table().
int idx_open_table(char * pathname) {
    return buf_open_table(pathname);
}

// Wrapper function of buf_close_table().
int idx_close_table(int table_id) {
    return buf_close_table(table_id);
}

// Wrapper function of buf_request_page().
int idx_request_page(int table_id, pagenum_t pagenum, buf_block_t** dest) {
    return buf_request_page(table_id, pagenum, dest);
}

// Wrapper function of buf_release_page().
int idx_release_page(buf_block_t* buf_block, bool is_writed) {
    return buf_release_page(buf_block, is_writed);
}

// UTILITIES

/* 
 * Find the appropriate place to split a page.
 */
int idx_cut(int length) {
    if(length % 2 == 0) return length/2;
    else return length/2 + 1;
}

/*
 *
 */
// parent_buf : Requested input, do not release it in here.
int idx_get_idx(buf_block_t *parent_buf, pagenum_t cur_pagenum) {
    page_t *parent_page = &(parent_buf->frame);
    int i;

    if(parent_page->right_sibling_or_one_more_page_num == cur_pagenum) {
        return GET_INDEX_LEFTMOST; // Given page is the leftmost child.
    }
    for(i = 0; i < parent_page->num_of_keys; i++) {
        if(parent_page->childs.entries[i].pagenum == cur_pagenum) {
            return i;
        }
    }

    // error.
    return GET_INDEX_ERROR;
}

/*
 * Find leftmost leaf page.
 */
buf_block_t* idx_find_leftmost_leaf(buf_block_t *header_buf) {
    pagenum_t cur_pagenum;
    buf_block_t *cur_buf;
    header_page_t *header_page;
    page_t *cur_page;
    int table_id = header_buf->table_id;

    header_page = (header_page_t*)&(header_buf->frame);

    cur_pagenum = header_page->root_page_num;
    
    if(cur_pagenum == 0) {
        return NULL; // Empty table.
    }

    buf_request_page(table_id, cur_pagenum, &cur_buf);
    cur_page = &(cur_buf->frame);

    // Find leftmost leaf page in table.
    while(!cur_page->is_leaf) {
        cur_pagenum = cur_page->right_sibling_or_one_more_page_num;
        buf_release_page(cur_buf, false);
        buf_request_page(table_id, cur_pagenum, &cur_buf);
        cur_page = &(cur_buf->frame);
    }

    return cur_buf;
}

/*** FIND ***/

/* Trace the path from the root to a leaf, searching by key.
 * Return buffer block of the leaf containing the given key.
 */
buf_block_t* idx_find_leaf(int table_id, int64_t key) {
    pagenum_t pagenum;
    buf_block_t *header_buf, *cur_buf;
    header_page_t *header_page;
    page_t *cur_page;
    int i;
    int result;
    //int left, right, mid;

    result = buf_request_page(table_id, HEADER_PAGE_NUM, &header_buf);
    if(result < 0) { return NULL; }

    header_page = (header_page_t*)&(header_buf->frame);
    pagenum = header_page->root_page_num;
    result = buf_release_page(header_buf, false);
    if(result < 0) { return NULL; }
    // If root doesn't exists, return 0.
    if(pagenum == 0) {
        return NULL;
    }

    result = buf_request_page(table_id, pagenum, &cur_buf);
    if(result < 0) { return NULL; }
    cur_page = &(cur_buf->frame);
    
    // Trace the path from the root to a leaf, searching by key.
    while(!cur_page->is_leaf) {
        // Binary search.
        /*
        left = 0;
        right = cur_page->num_of_keys - 1;
        while(left <= right) {
            mid = (left + right) / 2;
            if(key >= cur_page->childs.entries[mid].key) {
                left = mid + 1;
            }
            else {
                right = mid - 1;
            }
        }*/
        //i = left;
        
        // Linear search.
        
        for(i = 0; i < cur_page->num_of_keys;) {
            if(key >= cur_page->childs.entries[i].key) i++;
            else break;
        }
        if(i == 0) pagenum = cur_page->right_sibling_or_one_more_page_num;
        else pagenum = cur_page->childs.entries[i-1].pagenum;

        result = buf_release_page(cur_buf, false);
        if(result < 0) { return NULL; }

        result = buf_request_page(table_id, pagenum, &cur_buf);
        if(result < 0) { return NULL; }
        cur_page = &(cur_buf->frame);
    }

    /*result = buf_release_page(cur_buf, false);
    if(result < 0) { return NULL; }*/

    return cur_buf;
}

/* Find the record which given key refers.
 * Store the value of the record to given char pointer.
 * If successful, return 0.
 */
int idx_find(int table_id, int64_t key, char * ret_val) {
    //pagenum_t pagenum;
    buf_block_t* cur_buf;
    page_t* cur_page;
    int i, result;
    // Table is not opened or db is not initialized.
    if(buf_find_table_idx(table_id) < 0) {
        printf("Please open table first.\n");
        return PAGE_NOT_OPENED;
    }

    cur_buf = idx_find_leaf(table_id, key);

    // Empty tree.
    if(cur_buf == NULL) return FIND_EMPTY_TREE;
    
    // Find the record.
    cur_page = &(cur_buf->frame);
    for(i = 0; i < cur_page->num_of_keys; i++) {
        if(cur_page->childs.records[i].key == key) break;
    }

    // Record which given key refers doesn't exists.
    if(i == cur_page->num_of_keys) {
        result = buf_release_page(cur_buf, false);
        if(result < 0) { return -1; }
        return FIND_NOT_EXISTS;
    }
    else { // Record which given key refers exists.
        memcpy(ret_val, cur_page->childs.records[i].value, SIZE_RECORD_VALUE);
        result = buf_release_page(cur_buf, false);
        if(result < 0) { return -1; }
        return SUCCESS;
    }
}

int idx_find(int table_id, int64_t key, char * ret_val, int trx_id) {
    trx_t* cur_trx;
    buf_block_t* cur_buf;
    page_t* cur_page;
    int i, result;

    // 0. Run current transaction.
    trx_acquire_trx_mgr_latch();
    cur_trx = trx_get_trx(trx_id);
    if(cur_trx == NULL) {
        trx_release_trx_mgr_latch();
        return -5; // invalid input.
    }
    pthread_mutex_lock(&(cur_trx->trx_latch));
    trx_release_trx_mgr_latch();
    cur_trx->state = RUNNING;
    pthread_mutex_unlock(&(cur_trx->trx_latch));
    
    while(true) {
        // 1. Acquire the buffer pool latch.
        buf_acquire_buf_pool_latch();

        // 2. Find a leaf page containing the given record(key).
        cur_buf = idx_find_leaf(table_id, key);
        if(cur_buf == NULL) {
            buf_release_buf_pool_latch();
            return FIND_EMPTY_TREE;
        }

        // 3. Try to acquire the buffer page latch.
        if(pthread_mutex_trylock(&(cur_buf->buf_page_latch)) != 0) {
            buf_release_page(cur_buf, false);
            buf_release_buf_pool_latch();
            continue;
        }

        // 4. Release the buffer pool latch.
        buf_release_buf_pool_latch();

        // 4.1. If the record does not exist, stop.
        cur_page = &(cur_buf->frame);
        for(i = 0; i < cur_page->num_of_keys; i++) {
            if(cur_page->childs.records[i].key == key) break;
        }
        if(i == cur_page->num_of_keys) { // The record does not exist.
            pthread_mutex_unlock(&(cur_buf->buf_page_latch));
            buf_release_page(cur_buf, false);
            return FIND_NOT_EXISTS;
        }

        // The record exists.
        // 5. Try to acquire record lock.
        result = trx_acquire_record_lock(table_id, cur_buf->page_num, key, trx_id, SHARED);
        if(result == DEADLOCK) {
            pthread_mutex_unlock(&(cur_buf->buf_page_latch));
            buf_release_page(cur_buf, false);
            trx_abort_trx(trx_id);
            return ABORTED;
        }
        if(result == CONFLICT) {
            pthread_mutex_unlock(&(cur_buf->buf_page_latch));
            buf_release_page(cur_buf, false);
            
            trx_acquire_trx_mgr_latch();
            cur_trx = trx_get_trx(trx_id);
            pthread_mutex_lock(&(cur_trx->trx_latch));
            trx_release_trx_mgr_latch();
            cur_trx->state = WAITING;
            pthread_cond_wait(&(cur_trx->trx_cond), &(cur_trx->trx_latch));
            pthread_mutex_unlock(&(cur_trx->trx_latch));
        }
        else {
            break;
        }
    }

    // 6. Find. (It was already found. Just copy.)
    memcpy(ret_val, cur_page->childs.records[i].value, SIZE_RECORD_VALUE);
    
    // 7. Release the buffer page latch.
    pthread_mutex_unlock(&(cur_buf->buf_page_latch));
    buf_release_page(cur_buf, false);

    // 7.1. Make trx state IDLE.
    trx_acquire_trx_mgr_latch();
    cur_trx = trx_get_trx(trx_id);
    pthread_mutex_lock(&(cur_trx->trx_latch));
    trx_release_trx_mgr_latch();
    cur_trx->state = IDLE;
    pthread_mutex_unlock(&(cur_trx->trx_latch));

    // 8. Return SUCCESS.
    return SUCCESS;

}

/*** UPDATE ***/

int idx_update(int table_id, int64_t key, char* values, int trx_id) {
    trx_t* cur_trx;
    buf_block_t* cur_buf;
    page_t* cur_page;
    int i, result;

    while(true) {
        // 0. Run current transaction.
        trx_acquire_trx_mgr_latch();
        cur_trx = trx_get_trx(trx_id);
        if(cur_trx == NULL) {
            trx_release_trx_mgr_latch();
            return -5; // invalid input.
        }
        pthread_mutex_lock(&(cur_trx->trx_latch));
        trx_release_trx_mgr_latch();
        cur_trx->state = RUNNING;
        pthread_mutex_unlock(&(cur_trx->trx_latch));

        // 1. Acquire the buffer pool latch.
        buf_acquire_buf_pool_latch();

        // 2. Find a leaf page containing the given record(key).
        cur_buf = idx_find_leaf(table_id, key);
        if(cur_buf == NULL) {
            buf_release_buf_pool_latch();
            return FIND_EMPTY_TREE;
        }

        // 3. Try to acquire the buffer page latch.
        pthread_mutex_trylock(&(cur_buf->buf_page_latch));
        /*
        if(pthread_mutex_trylock(&(cur_buf->buf_page_latch)) != 0) {
            buf_release_buf_pool_latch();
            continue;
        }*/

        // 4. Release the buffer pool latch.
        buf_release_buf_pool_latch();

        // 4.1. If the record does not exist, stop.
        cur_page = &(cur_buf->frame);
        for(i = 0; i < cur_page->num_of_keys; i++) {
            if(cur_page->childs.records[i].key == key) break;
        }
        if(i == cur_page->num_of_keys) { // The record does not exist.
            pthread_mutex_unlock(&(cur_buf->buf_page_latch));
            buf_release_page(cur_buf, false);
            return FIND_NOT_EXISTS;
        }

        // The record exists.
        // 5. Try to acquire record lock.
        result = trx_acquire_record_lock(table_id, cur_buf->page_num, key, trx_id, EXCLUSIVE);
        if(result == DEADLOCK) {
            pthread_mutex_unlock(&(cur_buf->buf_page_latch));
            buf_release_page(cur_buf, false);
            trx_abort_trx(trx_id);
            return ABORTED;
        }
        if(result == CONFLICT) {
            pthread_mutex_unlock(&(cur_buf->buf_page_latch));
            buf_release_page(cur_buf, false);
            
            trx_acquire_trx_mgr_latch();
            cur_trx = trx_get_trx(trx_id);
            pthread_mutex_lock(&(cur_trx->trx_latch));
            trx_release_trx_mgr_latch();
            cur_trx->state = WAITING;
            pthread_cond_wait(&(cur_trx->trx_cond), &(cur_trx->trx_latch));
            pthread_mutex_unlock(&(cur_trx->trx_latch));
        }
        else {
            break;
        }
    }

    // 6. Update.
    trx_acquire_trx_mgr_latch();
    cur_trx = trx_get_trx(trx_id);
    pthread_mutex_lock(&(cur_trx->trx_latch));
    trx_release_trx_mgr_latch();
    undo_log_t * undo_log = (undo_log_t*)malloc(sizeof(undo_log_t));
    undo_log->table_id = table_id;
    undo_log->pagenum = cur_buf->page_num;
    undo_log->key = key;
    memcpy(undo_log->old_value, cur_page->childs.records[i].value, SIZE_RECORD_VALUE);
    cur_trx->undo_log_list.push_back(undo_log);
    pthread_mutex_unlock(&(cur_trx->trx_latch));

    memcpy(cur_page->childs.records[i].value, values, SIZE_RECORD_VALUE);

    // 7. Release the buffer page latch.
    pthread_mutex_unlock(&(cur_buf->buf_page_latch));
    buf_release_page(cur_buf, true);

    // 7.1. Make trx state IDLE.
    trx_acquire_trx_mgr_latch();
    cur_trx = trx_get_trx(trx_id);
    pthread_mutex_lock(&(cur_trx->trx_latch));
    trx_release_trx_mgr_latch();
    cur_trx->state = IDLE;
    pthread_mutex_unlock(&(cur_trx->trx_latch));

    // 8. Return SUCCESS.
    return SUCCESS;
}

/*** INSERTION ***/

/* Create and initialize a new record with given key/value.
 * Return maked record.
 */
record idx_make_record(int64_t key, char * value) {
    record new_record;

    memset(&new_record, 0, SIZE_RECORD);
    new_record.key = key;
    //strcpy(new_record.value, value);
    memcpy(new_record.value, value, SIZE_RECORD_VALUE);

    return new_record;
}

/* Create and initialize a new page.
 * Return maked page.
 */
int idx_make_page(page_t* page) {
    if(page == NULL) {
        return -1;
    }
    memset(page, 0, SIZE_PAGE);
    page->parent_or_next_free_page_num = -1;
    page->is_leaf = 0;
    page->num_of_keys = 0;

    return SUCCESS;
}

/* Create and initialize a new leaf page.
 * Return maked leaf page.
 */
int idx_make_leaf(page_t* page) {
    int result;
    
    result = idx_make_page(page);
    if(result < 0) { return -1; }
    
    page->is_leaf = 1;

    return SUCCESS;
}

/*
 *
 */
int idx_insert_into_leaf(buf_block_t* leaf_buf, record new_record) {
    page_t *leaf_page = &(leaf_buf->frame);
    int i, insertion_point, result;

    insertion_point = 0;
    while(insertion_point < leaf_page->num_of_keys && leaf_page->childs.records[insertion_point].key < new_record.key) {
        insertion_point++;
    }

    for(i = leaf_page->num_of_keys; i > insertion_point; i--) {
        leaf_page->childs.records[i] = leaf_page->childs.records[i-1];
    }
    leaf_page->childs.records[insertion_point] = new_record;
    leaf_page->num_of_keys++;

    result = buf_release_page(leaf_buf, true);
    if(result < 0) { return -1; }
    
    return SUCCESS;
}

/*
 *
 */
int idx_insert_into_leaf_after_splitting(buf_block_t* leaf_buf, record new_record) {
    pagenum_t new_pagenum;
    buf_block_t *new_leaf_buf;
    page_t *leaf_page, *new_leaf_page;
    int64_t new_key;
    record temp_record[ORDER_LEAF];
    int insertion_point, i, j, split, result;

    //printf("idx_insert_into_leaf_after_splitting_start\n");
    
    //
    leaf_page = &(leaf_buf->frame);

    //
    new_pagenum = buf_alloc_page(leaf_buf->table_id);
    if(new_pagenum < 0) { return -1; }

    //
    result = buf_request_page(leaf_buf->table_id, new_pagenum, &new_leaf_buf);
    if(result < 0) { return -1; }
    
    new_leaf_page = &(new_leaf_buf->frame);
    result = idx_make_leaf(new_leaf_page);
    if(result < 0) { return -1; }

    //
    insertion_point = 0;
    while(insertion_point < ORDER_LEAF - 1 && leaf_page->childs.records[insertion_point].key < new_record.key) {
        insertion_point++;
    }

    for(i = 0, j = 0; i < leaf_page->num_of_keys; i++, j++) {
        if(j == insertion_point) j++;
        temp_record[j] = leaf_page->childs.records[i];
    }
    temp_record[insertion_point] = new_record;

    leaf_page->num_of_keys = 0;

    split = idx_cut(ORDER_LEAF - 1);

    for(i = 0; i < split; i++) {
        leaf_page->childs.records[i] = temp_record[i];
        leaf_page->num_of_keys++;
    }

    for(i = split, j = 0; i < ORDER_LEAF; i++, j++) {
        new_leaf_page->childs.records[j] = temp_record[i];
        new_leaf_page->num_of_keys++;
    }

    new_leaf_page->right_sibling_or_one_more_page_num = leaf_page->right_sibling_or_one_more_page_num;
    leaf_page->right_sibling_or_one_more_page_num = new_pagenum;

    memset(&(leaf_page->childs.records[leaf_page->num_of_keys]), 0, (ORDER_LEAF-1-leaf_page->num_of_keys)*SIZE_RECORD);
    
    new_leaf_page->parent_or_next_free_page_num = leaf_page->parent_or_next_free_page_num;
    new_key = new_leaf_page->childs.records[0].key;

    // Releasing must be executed at that function.

    return idx_insert_into_parent(leaf_buf, new_key, new_leaf_buf);
}

/*
 *
 */
int idx_insert_into_internal(buf_block_t* parent_buf, int left_idx, int64_t key, pagenum_t right_pagenum) {
    page_t *parent_page;
    int i, result;

    //
    parent_page = &(parent_buf->frame);

    // Given bpt code sets,
    // key :        0   1   2   3   4
    // pointer :  0   1   2   3   4   5
    // this code sets,
    // key :        0   1   2   3   4
    // pagenum : -1   0   1   2   3   4
    // So use left_idx + 1.
    for(i = parent_page->num_of_keys; i > left_idx + 1; i--) {
        parent_page->childs.entries[i] = parent_page->childs.entries[i-1];
    }
    parent_page->childs.entries[left_idx+1].key = key;
    parent_page->childs.entries[left_idx+1].pagenum = right_pagenum;
    parent_page->num_of_keys++;

    // write page.
    result = buf_release_page(parent_buf, true);
    if(result < 0) { return -1; }

    return SUCCESS;
}

/*
 *
 */
int idx_insert_into_internal_after_splitting(buf_block_t* parent_buf, int left_idx, int64_t key, pagenum_t right_pagenum) {
    pagenum_t new_pagenum;
    buf_block_t *new_internal_buf, *temp_buf;
    page_t *internal_page, *new_internal_page, *temp_page;
    pagenum_t temp_one_more_pagenum;
    entry temp_entry[ORDER_INTERNAL];
    int64_t k_prime;
    int i, j, split, result, table_id;
    
    //printf("idx_insert_into_internal_after_splitting_start\n");

    //
    internal_page = &(parent_buf->frame);
    table_id = parent_buf->table_id;

    //
    new_pagenum = buf_alloc_page(table_id);
    if(new_pagenum < 0) { return -1; }

    //
    result = buf_request_page(table_id, new_pagenum, &new_internal_buf);
    if(result < 0) { return -1; }

    new_internal_page = &(new_internal_buf->frame);
    result = idx_make_page(new_internal_page);

    //
    temp_one_more_pagenum = internal_page->right_sibling_or_one_more_page_num;
    for(i = 0, j = 0; i < internal_page->num_of_keys; i++, j++) {
        if(j == left_idx+1) j++;
        temp_entry[j] = internal_page->childs.entries[i];
    }
    temp_entry[left_idx+1].key = key;
    temp_entry[left_idx+1].pagenum = right_pagenum;

    //
    split = idx_cut(ORDER_INTERNAL);
    internal_page->num_of_keys = 0;
    internal_page->right_sibling_or_one_more_page_num = temp_one_more_pagenum;
    for(i = 0; i < split - 1; i++) {
        internal_page->childs.entries[i] = temp_entry[i];
        internal_page->num_of_keys++;
    }
    k_prime = temp_entry[i].key;
    new_internal_page->right_sibling_or_one_more_page_num = temp_entry[i].pagenum;
    for(++i, j = 0; i < ORDER_INTERNAL; i++, j++) {
        new_internal_page->childs.entries[j] = temp_entry[i];
        new_internal_page->num_of_keys++;
    }

    //
    new_internal_page->parent_or_next_free_page_num = internal_page->parent_or_next_free_page_num;

    memset(&(internal_page->childs.entries[internal_page->num_of_keys]), 0, (ORDER_INTERNAL-1-internal_page->num_of_keys)*SIZE_ENTRY);

    result = buf_request_page(table_id, new_internal_page->right_sibling_or_one_more_page_num, &temp_buf);
    if(result < 0) { return -1; }
    temp_page = &(temp_buf->frame);
    temp_page->parent_or_next_free_page_num = new_pagenum;
    result = buf_release_page(temp_buf, true);
    if(result < 0) { return -1; }
    for(i = 0; i < new_internal_page->num_of_keys; i++) {
        result = buf_request_page(table_id, new_internal_page->childs.entries[i].pagenum, &temp_buf);
        if(result < 0) { return -1; }
        temp_page = &(temp_buf->frame);
        temp_page->parent_or_next_free_page_num = new_pagenum;
        result = buf_release_page(temp_buf, true);
        if(result < 0) { return -1; }
    }

    // Releasing must be executed at that function.

    return idx_insert_into_parent(parent_buf, k_prime, new_internal_buf);
}

/*
 *
 */
int idx_insert_into_parent(buf_block_t* left_buf, int64_t key, buf_block_t* right_buf) {
    pagenum_t parent_pagenum, right_pagenum;
    buf_block_t *parent_buf;
    page_t *left_page, *right_page, *parent_page;
    int left_idx, result;

    //printf("idx_insert_into_parent_start\n");

    //
    left_page = &(left_buf->frame);
    right_page = &(right_buf->frame);
    parent_pagenum = left_page->parent_or_next_free_page_num;
    
    // Parent page is header page. It means left page was root page.
    // Make new root.
    if(parent_pagenum == 0) {
        //printf("Make new root.\n");
        // Releasing must be executed at that function.
        return idx_insert_into_new_root(left_buf, key, right_buf);
    }

    //
    result = buf_request_page(left_buf->table_id, parent_pagenum, &parent_buf);
    if(result < 0) { return -1; }
    parent_page = &(parent_buf->frame);

    left_idx = idx_get_idx(parent_buf, left_buf->page_num);

    // Releasing. From now, they will be not changed.
    right_pagenum = right_buf->page_num;
    result = buf_release_page(left_buf, true);
    result = buf_release_page(right_buf, true);
    if(result < 0) { return -1; }

    // if parent has room.
    if(parent_page->num_of_keys < ORDER_INTERNAL - 1) {
        return idx_insert_into_internal(parent_buf, left_idx, key, right_pagenum);
    }

    // parent need split.
    return idx_insert_into_internal_after_splitting(parent_buf, left_idx, key, right_pagenum);
}

/*
 *
 */
int idx_insert_into_new_root(buf_block_t* left_buf, int64_t key, buf_block_t* right_buf) {
    pagenum_t root_pagenum;
    buf_block_t *header_buf, *root_buf;
    header_page_t *header_page;
    page_t *left_page, *right_page, *root_page;
    int table_id, result;

    //printf("idx_insert_into_new_root_start\n");

    //
    left_page = &(left_buf->frame);
    right_page = &(right_buf->frame);
    table_id = left_buf->table_id;

    //
    root_pagenum = buf_alloc_page(table_id);
    if(root_pagenum < 0) { return -1; }

    //
    result = buf_request_page(table_id, root_pagenum, &root_buf);
    if(result < 0) { return -1; }

    root_page = &(root_buf->frame);
    result = idx_make_page(root_page);
    if(result < 0) { return -1; }
    root_page->parent_or_next_free_page_num = 0;
    root_page->num_of_keys++;
    root_page->right_sibling_or_one_more_page_num = left_buf->page_num;
    root_page->childs.entries[0].key = key;
    root_page->childs.entries[0].pagenum = right_buf->page_num;

    result = buf_release_page(root_buf, true);
    if(result < 0) { return -1; }

    //
    left_page->parent_or_next_free_page_num = root_pagenum;
    right_page->parent_or_next_free_page_num = root_pagenum;
    result = buf_release_page(left_buf, true);
    result = buf_release_page(right_buf, true);
    if(result < 0) { return -1; }

    //
    result = buf_request_page(table_id, HEADER_PAGE_NUM, &header_buf);
    if(result < 0) { return -1; }

    header_page = (header_page_t*)&(header_buf->frame);
    header_page->root_page_num = root_pagenum;
    
    result = buf_release_page(header_buf, true);
    if(result < 0) { return -1; }

    return SUCCESS;
}

/*
 *
 */
int idx_start_new_tree(int table_id, record new_record) {
    pagenum_t pagenum;
    buf_block_t *header_buf, *cur_buf;
    header_page_t *header_page;
    page_t *cur_page;
    int result;
    
    //
    pagenum = buf_alloc_page(table_id);
    if(pagenum < 0) { return -1; }

    //
    result = buf_request_page(table_id, pagenum, &cur_buf);
    if(result < 0) { return -1; }

    cur_page = &(cur_buf->frame);
    result = idx_make_leaf(cur_page);
    if(result < 0) { return -1; }
    cur_page->parent_or_next_free_page_num = 0; // Parent is header page. This page is root page.
    cur_page->num_of_keys = 1;
    cur_page->right_sibling_or_one_more_page_num = 0;
    cur_page->childs.records[0] = new_record;

    result = buf_release_page(cur_buf, true);
    if(result < 0) { return -1; }

    //
    result = buf_request_page(table_id, HEADER_PAGE_NUM, &header_buf);
    if(result < 0) { return -1; }
    header_page = (header_page_t*)&(header_buf->frame);
    header_page->root_page_num = pagenum;
    result = buf_release_page(header_buf, true);
    if(result < 0) { return -1; }

    return SUCCESS;
}

/*
 *
 */
int idx_insert(int table_id, int64_t key, char * value) {
    record new_record, temp;
    //pagenum_t pagenum;
    buf_block_t *leaf_buf;
    page_t *leaf_page;
    int result;

    // printf("insert_start. key : %ld, value : %s\n", key, value);

    // Table is not opened or db is not initialized.
    if(buf_find_table_idx(table_id) < 0) {
        printf("Please open table first.\n");
        return PAGE_NOT_OPENED;
    }

    result = idx_find(table_id, key, temp.value);

    if(result == 0) { // Duplicated.
        return INSERT_DUPLICATED;
    }

    new_record = idx_make_record(key, value);

    if(result == -2) { // Empty tree.
        return idx_start_new_tree(table_id, new_record);
    }

    leaf_buf = idx_find_leaf(table_id, key);

    leaf_page = &(leaf_buf->frame);

    // Releasing must be executed at those functions.

    if(leaf_page->num_of_keys < ORDER_LEAF - 1) { // Leaf has room.
        return idx_insert_into_leaf(leaf_buf, new_record);
    }

    // Leaf need split.
    return idx_insert_into_leaf_after_splitting(leaf_buf, new_record);
}

// DELETION.

/*
 *
 */
// header_buf : Requested input, release it in here.
// root_buf : Requested input, release it in here.
int idx_adjust_root(buf_block_t *header_buf, buf_block_t *root_buf) {
    pagenum_t root_pagenum, temp_pagenum;
    buf_block_t *temp_buf;
    header_page_t *header_page;
    page_t *root_page, *temp_page;
    int result, table_id;

    // printf("adjust_root_start\n");

    // Get root page num.
    header_page = (header_page_t*)&(header_buf->frame);
    root_page = &(root_buf->frame);
    root_pagenum = root_buf->page_num;
    table_id = header_buf->table_id;

    // Nonempty root.
    if(root_page->num_of_keys > 0) {
        result = buf_release_page(header_buf, false);
        result = buf_release_page(root_buf, true);
        if(result < 0) { return -1; }
        return 0;
    }

    // Empty root.
    if(!root_page->is_leaf) { // If root is internal page,
        temp_pagenum = root_page->right_sibling_or_one_more_page_num;

        // Set root page num.
        header_page->root_page_num = temp_pagenum;

        result = buf_request_page(table_id, temp_pagenum, &temp_buf);
        if(result < 0) { return -1; }
        temp_page = &(temp_buf->frame);
        temp_page->parent_or_next_free_page_num = 0;

        result = buf_release_page(temp_buf, true);
        result = buf_release_page(header_buf, true);
        if(result < 0) { return -1; }
    }
    else { // If root is leaf page,
        // Set root page num.
        header_page->root_page_num = 0; // header.
        result = buf_release_page(header_buf, true);
        if(result < 0) { return -1; }
    }
    buf_release_page(root_buf, true);
    buf_free_page(table_id, root_pagenum);

    // Add error checking.

    return SUCCESS;
}

/*
 *
 */
// cur_buf : Requested input, release it in here.
int idx_remove_entry_from_internal(buf_block_t *cur_buf) {
    pagenum_t cur_pagenum, parent_pagenum, sib_pagenum;
    buf_block_t *parent_buf, *sib_buf;
    page_t *cur_page, *parent_page, *sib_page;
    int cur_idx, sib_idx, i, table_id;

    // printf("idx_remove_entry_from_internal_start\n");

    cur_page = &(cur_buf->frame);
    cur_pagenum = cur_buf->page_num;
    table_id = cur_buf->table_id;

    parent_pagenum = cur_page->parent_or_next_free_page_num;
    buf_request_page(cur_buf->table_id, parent_pagenum, &parent_buf);
    parent_page = &(parent_buf->frame);
    cur_idx = idx_get_idx(parent_buf, cur_pagenum);

    if(cur_idx <= -3) { // error
        //printf("cur_idx == -3\n");
        return -1;
    }

    if(cur_page->is_leaf) { // Link right_sibling_page_num.
        if(cur_idx == -1) { // If current page is leftmost child of parent page,
            // copy everything(records, right_sibling_page_num) of right sibling page to current page,
            // and change right sibling page to the page will be delete.

            // Copy datas from right sibling page to current page.
            sib_idx = 0; // right sibling page.
            sib_pagenum = parent_page->childs.entries[sib_idx].pagenum;
            buf_request_page(table_id, sib_pagenum, &sib_buf);
            sib_page = &(sib_buf->frame);
            *cur_page = *sib_page;
            buf_release_page(sib_buf, false);

            // Change right sibling page to the page will be delete.
            buf_release_page(cur_buf, true);
            cur_idx = sib_idx;
            cur_pagenum = sib_pagenum;
            buf_request_page(table_id, cur_pagenum, &cur_buf);
            cur_page = &(cur_buf->frame);
        }
        else { // If current page is not leftmost child,
            sib_idx = cur_idx - 1; // left sibling page.
            if(sib_idx == -1) { // If left sibling page is leftmost child of parent page,
                sib_pagenum = parent_page->right_sibling_or_one_more_page_num;
            }
            else { // If left sibling page is not leftmost child,
                sib_pagenum = parent_page->childs.entries[sib_idx].pagenum;
            }
            buf_request_page(table_id, sib_pagenum, &sib_buf);
            sib_page = &(sib_buf->frame);
            sib_page->right_sibling_or_one_more_page_num = cur_page->right_sibling_or_one_more_page_num;
            buf_release_page(sib_buf, true);
        }
    }

    if(cur_idx == -1) { // If current page is leftmost child of parent page,
        // Delete current page and pack the childs of parent page.
        parent_page->right_sibling_or_one_more_page_num = parent_page->childs.entries[0].pagenum;
        for(i = 1; i < parent_page->num_of_keys; i++) {
            parent_page->childs.entries[i - 1] = parent_page->childs.entries[i];
        }   
    }
    else { // If current page is not leftmost child,
        for(i = cur_idx + 1; i < parent_page->num_of_keys; i++) {
            parent_page->childs.entries[i - 1] = parent_page->childs.entries[i];
        }
    }
    parent_page->num_of_keys--;
    // Set needless space to 0 for tidness.
    memset(&(parent_page->childs.entries[parent_page->num_of_keys]), 0, (ORDER_INTERNAL-1-parent_page->num_of_keys)*SIZE_ENTRY);
    buf_release_page(cur_buf, true);
    buf_free_page(table_id, cur_pagenum);

    if(parent_page->num_of_keys == 0) {
        return idx_delete_entry(parent_buf);
    }

    buf_release_page(parent_buf, true);

    // Add error checking.

    return SUCCESS;
}


int idx_remove_record_from_leaf(page_t* leaf_page, int64_t key) {
    int i;

    // printf("remove_record_from_leaf_start\n");
    
    // Find the idx of given key.
    i = 0;
    while(leaf_page->childs.records[i].key != key) i++;

    // Remove the record and shift other records accordingly.
    for(++i; i < leaf_page->num_of_keys; i++) {
        leaf_page->childs.records[i - 1] = leaf_page->childs.records[i];
    }

    // One key fewer.
    leaf_page->num_of_keys--;

    // Set needless space to 0 for tidiness.
    memset(&(leaf_page->childs.records[leaf_page->num_of_keys]), 0, (ORDER_LEAF-1-leaf_page->num_of_keys)*SIZE_RECORD);

    // Add error check.

    return SUCCESS;
}

/* current : unique case. (redistribute first)
 * general case : current page has 0 key, and neighbor page has some keys < maximum.
 * unique case : current page has 0 key, and neighbor page has only 1 key.
 */
// funtions for general case. this can also cover unique case.
/*
int idx_merge_pages(buf_block_t *cur_buf, buf_block_t *neighbor_buf, buf_block_t *parent_buf,
        int neighbor_idx, int k_prime_idx, int64_t k_prime) {
    pagenum_t neighbor_pagenum;
    buf_block_t *temp_buf;
    page_t *cur_page, *neighbor_page, *parent_page, *temp_page;
    int i, table_id;
    //int cur_idx;

    // printf("idx_merge_pages_start\n");

    cur_page = &(cur_buf->frame);
    neighbor_page = &(neighbor_buf->frame);
    parent_page = & (parent_buf->frame);
    neighbor_pagenum = neighbor_buf->page_num;
    table_id = cur_buf->table_id;

    // If current page is leftmost page,
    if(neighbor_idx == -2) { // current | (k_prime) | neighbor
        //cur_idx = -1;
        
        if(!cur_page->is_leaf) {
            for(i = neighbor_page->num_of_keys; i > 0; i--) {
                neighbor_page->childs.entries[i] = neighbor_page->childs.entries[i - 1];
            }
            neighbor_page->childs.entries[0].pagenum = neighbor_page->right_sibling_or_one_more_page_num;
            neighbor_page->childs.entries[0].key = k_prime;
            neighbor_page->right_sibling_or_one_more_page_num = cur_page->right_sibling_or_one_more_page_num;
            buf_request_page(table_id, neighbor_page->right_sibling_or_one_more_page_num, &temp_buf);
            temp_page = &(temp_buf->frame);
            temp_page->parent_or_next_free_page_num = neighbor_pagenum;
            buf_release_page(temp_buf, true);
            neighbor_page->num_of_keys++;
        }
    }
    // If current page has a neighbor to the left,
    else { // neighbor | (k_prime) | current
        //cur_idx = k_prime_idx;
        if(!cur_page->is_leaf) {
            neighbor_page->childs.entries[neighbor_page->num_of_keys].key = k_prime;
            neighbor_page->childs.entries[neighbor_page->num_of_keys].pagenum = cur_page->right_sibling_or_one_more_page_num;
            buf_request_page(table_id, neighbor_page->childs.entries[neighbor_page->num_of_keys].pagenum, &temp_buf);
            temp_page = &(temp_buf->frame);
            temp_page->parent_or_next_free_page_num = neighbor_pagenum;
            buf_release_page(temp_buf, true);
            neighbor_page->num_of_keys++;
        }
    }

    buf_release_page(neighbor_buf, true);
    buf_release_page(parent_buf, true);
    
    return idx_remove_entry_from_internal(cur_buf);
    //return idx_delete_entry(cur_buf);
}*/
// funtions for unique case. it is faster than the function for general case.
int idx_merge_pages(buf_block_t *cur_buf, buf_block_t *neighbor_buf, buf_block_t *parent_buf,
        int neighbor_idx, int k_prime_idx, int64_t k_prime) {
    pagenum_t neighbor_pagenum;
    buf_block_t *temp_buf;
    page_t *cur_page, *neighbor_page, *parent_page, *temp_page;
    int cur_idx, table_id;

    // printf("idx_merge_pages_start\n");

    cur_page = &(cur_buf->frame);
    neighbor_page = &(neighbor_buf->frame);
    parent_page = & (parent_buf->frame);
    neighbor_pagenum = neighbor_buf->page_num;
    table_id = cur_buf->table_id;

    //exit(5);
    // If current page is leftmost page,
    if(neighbor_idx == -2) { // current | (k_prime) | neighbor
        cur_idx = -1;
        
        if(!cur_page->is_leaf) {
            neighbor_page->childs.entries[1] = neighbor_page->childs.entries[0];
            neighbor_page->childs.entries[0].pagenum = neighbor_page->right_sibling_or_one_more_page_num;
            neighbor_page->childs.entries[0].key = k_prime;
            neighbor_page->right_sibling_or_one_more_page_num = cur_page->right_sibling_or_one_more_page_num;
            buf_request_page(table_id, neighbor_page->right_sibling_or_one_more_page_num, &temp_buf);
            temp_page = &(temp_buf->frame);
            temp_page->parent_or_next_free_page_num = neighbor_pagenum;
            buf_release_page(temp_buf, true);
            neighbor_page->num_of_keys++;
            //exit(5);
            // Do we have to Change parent's key?
            // k_prime_idx = 0
            // parent_page->childs.entries[k_prime_idx].key = 
        }
    }
    // If current page has a neighbor to the left,
    else { // neighbor | (k_prime) | current
        cur_idx = k_prime_idx;
        if(!cur_page->is_leaf) {
            neighbor_page->childs.entries[1].key = k_prime;
            neighbor_page->childs.entries[1].pagenum = cur_page->right_sibling_or_one_more_page_num;
            buf_request_page(table_id, neighbor_page->childs.entries[1].pagenum, &temp_buf);
            temp_page = &(temp_buf->frame);
            temp_page->parent_or_next_free_page_num = neighbor_pagenum;
            buf_release_page(temp_buf, true);
            neighbor_page->num_of_keys++;
        }
    }

    buf_release_page(neighbor_buf, true);
    buf_release_page(parent_buf, true);
    
    return idx_remove_entry_from_internal(cur_buf);
    //return idx_delete_entry(cur_buf);
}

/* current : general case. (redistribute first)
 * general case : current page has 0 key, and neighbor page has some keys > 1.
 * unique case : current page has 0 key, and neighbor page has maximum keys.
 */
int idx_redistribute_pages(buf_block_t *cur_buf, buf_block_t *neighbor_buf, buf_block_t *parent_buf,
        int neighbor_idx, int k_prime_idx, int64_t k_prime) {
    pagenum_t cur_pagenum;
    buf_block_t *temp_buf;
    page_t *cur_page, *neighbor_page, *parent_page, *temp_page;
    int split_point, i, j, temp, table_id;
    
    // printf("idx_redistribute_pages\n");

    cur_page = &(cur_buf->frame);
    neighbor_page = &(neighbor_buf->frame);
    parent_page = & (parent_buf->frame);
    cur_pagenum = cur_buf->page_num;
    table_id = cur_buf->table_id;

    split_point = idx_cut(neighbor_page->num_of_keys);
    
    // If current page is leftmost page,
    // pull the half of the neighbor's childs
    // from the neighbor's left side to current's right end.
    if(neighbor_idx == -2) { // current | (k_prime) | neighbor
        cur_page->childs.entries[0].key = k_prime;
        cur_page->childs.entries[0].pagenum = neighbor_page->right_sibling_or_one_more_page_num;
        buf_request_page(table_id, cur_page->childs.entries[0].pagenum, &temp_buf);
        temp_page = &(temp_buf->frame);
        temp_page->parent_or_next_free_page_num = cur_pagenum;
        buf_release_page(temp_buf, true);
        cur_page->num_of_keys++;
        for(i = 1; i < split_point; i++) {
            cur_page->childs.entries[i] = neighbor_page->childs.entries[i - 1];
            buf_request_page(table_id, cur_page->childs.entries[i].pagenum, &temp_buf);
            temp_page = &(temp_buf->frame);
            temp_page->parent_or_next_free_page_num = cur_pagenum;
            buf_release_page(temp_buf, true);
            cur_page->num_of_keys++;
        }

        // Set parent's key appropriately.
        parent_page->childs.entries[k_prime_idx].key = neighbor_page->childs.entries[split_point - 1].key;

        // Adjust neighbor.
        temp = neighbor_page->num_of_keys;
        neighbor_page->num_of_keys = 0;
        neighbor_page->right_sibling_or_one_more_page_num = neighbor_page->childs.entries[split_point - 1].pagenum;
        for(i = split_point, j = 0; i < temp; i++, j++) {
            neighbor_page->childs.entries[j] = neighbor_page->childs.entries[i];
            neighbor_page->num_of_keys++;
        }

        // Set needless space to 0 for tidiness.
        memset(&(neighbor_page->childs.entries[neighbor_page->num_of_keys]), 0, (ORDER_INTERNAL-1-neighbor_page->num_of_keys)*SIZE_ENTRY);
    }

    // If current page has a neighbor to the left,
    // pull the half of the neighbor's childs
    // from the neighbor's right side to current's left end.
    else { // neighbor | (k_prime) | current
    //printf("fuck!!\n");exit(1);

        // Copy entries from neighbor to current.
        temp = neighbor_page->num_of_keys;
        cur_page->childs.entries[temp - 1 - split_point].key = k_prime;
        cur_page->childs.entries[temp - 1 - split_point].pagenum = cur_page->right_sibling_or_one_more_page_num;
        cur_page->num_of_keys++;
        cur_page->right_sibling_or_one_more_page_num = neighbor_page->childs.entries[split_point].pagenum;
        buf_request_page(table_id, cur_page->right_sibling_or_one_more_page_num, &temp_buf);
        temp_page = &(temp_buf->frame);
        temp_page->parent_or_next_free_page_num = cur_pagenum;
        buf_release_page(temp_buf, true);
        
        for(i = split_point + 1, j = 0; i < temp; i++, j++) {
            cur_page->childs.entries[j] = neighbor_page->childs.entries[i];
            buf_request_page(table_id, cur_page->childs.entries[j].pagenum, &temp_buf);
            temp_page = &(temp_buf->frame);
            temp_page->parent_or_next_free_page_num = cur_pagenum;
            buf_release_page(temp_buf, true);
            cur_page->num_of_keys++;
            neighbor_page->num_of_keys--;
        }
        neighbor_page->num_of_keys--;

        // Set parent's key appropriately.
        parent_page->childs.entries[k_prime_idx].key = neighbor_page->childs.entries[split_point].key;

        // Set needless space to 0 for tidiness.
        memset(&(neighbor_page->childs.entries[neighbor_page->num_of_keys]), 0, (ORDER_INTERNAL-1-neighbor_page->num_of_keys)*SIZE_ENTRY);
    }

    buf_release_page(cur_buf, true);
    buf_release_page(neighbor_buf, true);
    buf_release_page(parent_buf, true);

    // Add error checking.

    return SUCCESS;
}

/*
 *
 */
int idx_delete_entry(buf_block_t *cur_buf) {
    pagenum_t cur_pagenum, parent_pagenum, neighbor_pagenum;
    buf_block_t *header_buf, *parent_buf, *neighbor_buf;
    header_page_t *header_page;
    page_t *cur_page, *parent_page, *neighbor_page;
    int64_t k_prime;
    int neighbor_idx, k_prime_idx, table_id;

    // printf("idx_delte_entry : %ldpage\n", pagenum);

    cur_page = &(cur_buf->frame);
    cur_pagenum = cur_buf->page_num;
    table_id = cur_buf->table_id;

    // remove entry from internal page.
    if(cur_page->is_leaf) {
        return idx_remove_entry_from_internal(cur_buf);
    }

    buf_request_page(table_id, HEADER_PAGE_NUM, &header_buf);
    header_page = (header_page_t*)&(header_buf->frame);
    
    if(cur_buf->page_num == header_page->root_page_num) {
        return idx_adjust_root(header_buf, cur_buf);
    }

    buf_release_page(header_buf, false);
    
    parent_pagenum = cur_page->parent_or_next_free_page_num;
    buf_request_page(table_id, parent_pagenum, &parent_buf);
    parent_page = &(parent_buf->frame);

    // Find neighbor page.
    neighbor_idx = idx_get_idx(parent_buf, cur_pagenum) - 1;
    if(neighbor_idx < -2) return -2; // error.

    if(neighbor_idx == -2) { // Current page is leftmost child of parent page.
        k_prime_idx = 0;
        neighbor_pagenum = parent_page->childs.entries[0].pagenum;
    }
    else if(neighbor_idx == -1) { // Left neighbor page is leftmost child.
        k_prime_idx = 0;
        neighbor_pagenum = parent_page->right_sibling_or_one_more_page_num;
    }
    else { // idx of left neighbor page >= 0.
        k_prime_idx = neighbor_idx + 1;
        neighbor_pagenum = parent_page->childs.entries[neighbor_idx].pagenum;
    }
    k_prime = parent_page->childs.entries[k_prime_idx].key;

    // Delay merge. If merge isn't necessary, just excute redistribution.
    buf_request_page(table_id, neighbor_pagenum, &neighbor_buf);
    neighbor_page = &neighbor_buf->frame;
    
    
    // Redistribute first.
    if(neighbor_page->num_of_keys <= 1) { // If merge is necessary,
        // current page has 0 key, and neighbor page has only 1 key.
        return idx_merge_pages(cur_buf, neighbor_buf, parent_buf, neighbor_idx, k_prime_idx, k_prime);
    }
    else { // If merge isn't necessary,
        // current page has 0 key, and neighbor page has some keys > 1.
        return idx_redistribute_pages(cur_buf, neighbor_buf, parent_buf, neighbor_idx, k_prime_idx, k_prime);
    }
    /*
    // Merge first.
    if(neighbor_page->num_of_keys < ORDER_INTERNAL - 1) { // If redistribution isn't necessary,
        // current page has 0 key, and neighbor page has some key < maximum.
        return idx_merge_pages(cur_buf, neighbor_buf, parent_buf, neighbor_idx, k_prime_idx, k_prime);
    }
    else { // If redistribution is necessary,
        // current page has 0 key, and neighbor page has maximum keys.
        return idx_redistribute_pages(cur_buf, neighbor_buf, parent_buf, neighbor_idx, k_prime_idx, k_prime);
    }*/
}

/*
 *
 */
int idx_delete_record(int table_id, buf_block_t *leaf_buf, int64_t key) {
    pagenum_t leaf_pagenum;
    buf_block_t *header_buf;
    header_page_t *header_page;
    page_t *leaf_page;
    int i, result;
    uint32_t num_of_keys;

    // printf("idx_delte_record\n");

    // Remove record from leaf page.
    leaf_page = &(leaf_buf->frame);
    leaf_pagenum = leaf_buf->page_num;

    idx_remove_record_from_leaf(leaf_page, key);

    num_of_keys = leaf_page->num_of_keys;
    //buf_release_page(leaf_buf, true);

    // Deletion from the root.
    result = buf_request_page(table_id, HEADER_PAGE_NUM, &header_buf);
    if(result < 0) { return -1; }
    header_page = (header_page_t*)&(header_buf->frame);
    if(leaf_pagenum == header_page->root_page_num) {
        // Releasing must be executed at that function.
        return idx_adjust_root(header_buf, leaf_buf);
    }
    result = buf_release_page(header_buf, false);
    if(result < 0) { return -1; }

    // Deletion from a page below root.
    // Delayed merge. If there are at least one key, don't merge.
    if(num_of_keys != 0) {
        result = buf_release_page(leaf_buf, true);
        if(result < 0) { return -1; }
        return SUCCESS;
    }
    else {
        return idx_delete_entry(leaf_buf);
    }
}

/*
 *
 */
int idx_delete(int table_id, int64_t key) {
    buf_block_t* cur_buf;
    //pagenum_t key_leaf_pagenum;
    record temp;
    int find_result;

    // printf("idx_delte_start : %ld\n", key);
    
    // Table is not opened or db is not initialized.
    if(buf_find_table_idx(table_id) < 0) {
        printf("Please open table first.\n");
        return PAGE_NOT_OPENED;
    }

    cur_buf = idx_find_leaf(table_id, key);
    find_result = idx_find(table_id, key, temp.value);
    if(cur_buf != NULL && find_result == 0) {
        return idx_delete_record(table_id, cur_buf, key);
    }

    return DELETE_NOT_EXISTS; // Not exists.
}