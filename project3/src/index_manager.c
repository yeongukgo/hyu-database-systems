#include "index_manager.h"

// char opened = 0;

/* Find the appropriate place to split a page.
 */
int cut(int length) {
    if(length % 2 == 0) return length/2;
    else return length/2 + 1;
}

// ----API----

// DB/TABLE

// Wrapper function of buffer_init_db().
int index_init_db(int buf_num) {
    return buffer_init_db(buf_num);
}

// Wrapper function of buffer_shutdown_db().
int index_shutdown_db() {
    return buffer_shutdown_db();
}

// Wrapper function of buffer_open_table().
int index_open_table(char * pathname) {
    return buffer_open_table(pathname);
}

// Wrapper function of buffer_close_table().
int index_close_table(int table_id) {
    return buffer_close_table(table_id);
}

// FIND

/* Trace the path from the root to a leaf, searching by key.
 * Return pagenum of the leaf containing the given key.
 */
pagenum_t index_find_leaf(int table_id, int64_t key) {
    pagenum_t pagenum;
    buffer_block_t *header_buffer, *current_buffer;
    header_page_t *header_page;
    page_t *current_page;
    int i, result;

    //file_read_page(HEADER_PAGE_NUM, &header);
    //pagenum = header.root_page_num;
    result = buffer_request_page(table_id, HEADER_PAGE_NUM, &header_buffer);
    if(result < 0) { return -1; }

    header_page = (header_page_t*)&(header_buffer->frame);
    pagenum = header_page->root_page_num;
    result = buffer_release_page(header_buffer, false);
    if(result < 0) { return -1; }
    // If root doesn't exists, return 0.
    if(pagenum == 0) {
        return pagenum;
    }

    //file_read_page(pagenum, &page);
    result = buffer_request_page(table_id, pagenum, &current_buffer);
    if(result < 0) { return -1; }
    current_page = &(current_buffer->frame);
    
    // Trace the path from the root to a leaf, searching by key.
    while(!current_page->is_leaf) {
        for(i = 0; i < current_page->num_of_keys;) {
            if(key >= current_page->childs.entries[i].key) i++;
            else break;
        }
        if(i == 0) pagenum = current_page->right_sibling_or_one_more_page_num;
        else pagenum = current_page->childs.entries[i-1].pagenum;

        result = buffer_release_page(current_buffer, false);
        if(result < 0) { return -1; }

        result = buffer_request_page(table_id, pagenum, &current_buffer);
        if(result < 0) { return -1; }
        current_page = &(current_buffer->frame);
    }

    result = buffer_release_page(current_buffer, false);
    if(result < 0) { return -1; }

    return pagenum;
}

/* Find the record which given key refers.
 * Store the value of the record to given char pointer.
 * If successful, return 0.
 */
int index_find(int table_id, int64_t key, char * ret_val) {
    pagenum_t pagenum;
    buffer_block_t* current_buffer;
    page_t* current_page;
    int i, result;
    // Table is not opened or db is not initialized.
    if(find_table_index(table_id) < 0) {
        printf("Please open table first.\n");
        return PAGE_NOT_OPENED;
    }

    pagenum = index_find_leaf(table_id, key);

    // Empty tree.
    if(pagenum <= 0) return FIND_EMPTY_TREE;
    
    // Find the record.
    result = buffer_request_page(table_id, pagenum, &current_buffer);
    if(result < 0) { return -1; }
    current_page = &(current_buffer->frame);
    for(i = 0; i < current_page->num_of_keys; i++) {
        if(current_page->childs.records[i].key == key) break;
    }

    // Record which given key refers doesn't exists.
    if(i == current_page->num_of_keys) {
        result = buffer_release_page(current_buffer, false);
        if(result < 0) { return -1; }
        return FIND_NOT_EXISTS;
    }
    else { // Record which given key refers exists.
        memcpy(ret_val, current_page->childs.records[i].value, SIZE_RECORD_VALUE);
        result = buffer_release_page(current_buffer, false);
        if(result < 0) { return -1; }
        return SUCCESS;
    }
}

// INSERTION

/* Create and initialize a new record with given key/value.
 * Return maked record.
 */
record index_make_record(int64_t key, char * value) {
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
int index_make_page(page_t* page) {
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
int index_make_leaf(page_t* page) {
    int result;
    
    result = index_make_page(page);
    if(result < 0) { return -1; }
    
    page->is_leaf = 1;

    return SUCCESS;
}

/*
 *
 */
int index_get_left_index(buffer_block_t* parent_buffer, pagenum_t left_pagenum) {
    page_t* parent_page = &(parent_buffer->frame);
    int left_index = -1;

    if(parent_page->right_sibling_or_one_more_page_num == left_pagenum) {
        return left_index;
    }

    left_index = 0;
    while(left_index <= parent_page->num_of_keys &&
            parent_page->childs.entries[left_index].pagenum != left_pagenum) {
        left_index++;
    }
    return left_index;
}

/*
 *
 */
int index_insert_into_leaf(buffer_block_t* leaf_buffer, record new_record) {
    page_t *leaf_page = &(leaf_buffer->frame);
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

    result = buffer_release_page(leaf_buffer, true);
    if(result < 0) { return -1; }
    
    return SUCCESS;
}

/*
 *
 */
int index_insert_into_leaf_after_splitting(buffer_block_t* leaf_buffer, record new_record) {
    pagenum_t new_pagenum;
    buffer_block_t *new_leaf_buffer;
    page_t *leaf_page, *new_leaf_page;
    int64_t new_key;
    record temp_record[ORDER_LEAF];
    int insertion_point, i, j, split, result;

    //printf("index_insert_into_leaf_after_splitting_start\n");
    
    //
    leaf_page = &(leaf_buffer->frame);

    //
    new_pagenum = buffer_alloc_page(leaf_buffer->table_id);
    if(new_pagenum < 0) { return -1; }

    //
    result = buffer_request_page(leaf_buffer->table_id, new_pagenum, &new_leaf_buffer);
    if(result < 0) { return -1; }
    
    new_leaf_page = &(new_leaf_buffer->frame);
    result = index_make_leaf(new_leaf_page);
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

    split = cut(ORDER_LEAF - 1);

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

    return index_insert_into_parent(leaf_buffer, new_key, new_leaf_buffer);
}

/*
 *
 */
int index_insert_into_internal(buffer_block_t* parent_buffer, int left_index, int64_t key, pagenum_t right_pagenum) {
    page_t *parent_page;
    int i, result;

    //
    parent_page = &(parent_buffer->frame);

    // Given bpt code sets,
    // key :        0   1   2   3   4
    // pointer :  0   1   2   3   4   5
    // this code sets,
    // key :        0   1   2   3   4
    // pagenum : -1   0   1   2   3   4
    // So use left_index + 1. 
    // If it must be changed, get_left_index must be changed.
    for(i = parent_page->num_of_keys; i > left_index + 1; i--) {
        parent_page->childs.entries[i] = parent_page->childs.entries[i-1];
    }
    parent_page->childs.entries[left_index+1].key = key;
    parent_page->childs.entries[left_index+1].pagenum = right_pagenum;
    parent_page->num_of_keys++;

    // write page.
    result = buffer_release_page(parent_buffer, true);
    if(result < 0) { return -1; }

    return SUCCESS;
}

/*
 *
 */
int index_insert_into_internal_after_splitting(buffer_block_t* parent_buffer, int left_index, int64_t key, pagenum_t right_pagenum) {
    pagenum_t new_pagenum;
    buffer_block_t *new_internal_buffer, *temp_buffer;
    page_t *internal_page, *new_internal_page, *temp_page;
    pagenum_t temp_one_more_pagenum;
    entry temp_entry[ORDER_INTERNAL];
    int64_t k_prime;
    int i, j, split, result, table_id;
    
    //printf("index_insert_into_internal_after_splitting_start\n");

    //
    internal_page = &(parent_buffer->frame);
    table_id = parent_buffer->table_id;

    //
    new_pagenum = buffer_alloc_page(table_id);
    if(new_pagenum < 0) { return -1; }

    //
    result = buffer_request_page(table_id, new_pagenum, &new_internal_buffer);
    if(result < 0) { return -1; }

    new_internal_page = &(new_internal_buffer->frame);
    result = index_make_page(new_internal_page);

    //
    temp_one_more_pagenum = internal_page->right_sibling_or_one_more_page_num;
    for(i = 0, j = 0; i < internal_page->num_of_keys; i++, j++) {
        if(j == left_index+1) j++;
        temp_entry[j] = internal_page->childs.entries[i];
    }
    temp_entry[left_index+1].key = key;
    temp_entry[left_index+1].pagenum = right_pagenum;

    //
    split = cut(ORDER_INTERNAL);
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

    result = buffer_request_page(table_id, new_internal_page->right_sibling_or_one_more_page_num, &temp_buffer);
    if(result < 0) { return -1; }
    temp_page = &(temp_buffer->frame);
    temp_page->parent_or_next_free_page_num = new_pagenum;
    result = buffer_release_page(temp_buffer, true);
    if(result < 0) { return -1; }
    for(i = 0; i < new_internal_page->num_of_keys; i++) {
        result = buffer_request_page(table_id, new_internal_page->childs.entries[i].pagenum, &temp_buffer);
        if(result < 0) { return -1; }
        temp_page = &(temp_buffer->frame);
        temp_page->parent_or_next_free_page_num = new_pagenum;
        result = buffer_release_page(temp_buffer, true);
        if(result < 0) { return -1; }
    }

    // Releasing must be executed at that function.

    return index_insert_into_parent(parent_buffer, k_prime, new_internal_buffer);
}

/*
 *
 */
int index_insert_into_parent(buffer_block_t* left_buffer, int64_t key, buffer_block_t* right_buffer) {
    pagenum_t parent_pagenum, right_pagenum;
    buffer_block_t *parent_buffer;
    page_t *left_page, *right_page, *parent_page;
    int left_index, result;

    //printf("index_insert_into_parent_start\n");

    //
    left_page = &(left_buffer->frame);
    right_page = &(right_buffer->frame);
    parent_pagenum = left_page->parent_or_next_free_page_num;
    
    // Parent page is header page. It means left page was root page.
    // Make new root.
    if(parent_pagenum == 0) {
        //printf("Make new root.\n");
        // Releasing must be executed at that function.
        return index_insert_into_new_root(left_buffer, key, right_buffer);
    }

    //
    result = buffer_request_page(left_buffer->table_id, parent_pagenum, &parent_buffer);
    if(result < 0) { return -1; }
    parent_page = &(parent_buffer->frame);

    left_index = index_get_left_index(parent_buffer, left_buffer->page_num);

    // Releasing. From now, they will be not changed.
    right_pagenum = right_buffer->page_num;
    result = buffer_release_page(left_buffer, true);
    result = buffer_release_page(right_buffer, true);
    if(result < 0) { return -1; }

    // if parent has room.
    if(parent_page->num_of_keys < ORDER_INTERNAL - 1) {
        return index_insert_into_internal(parent_buffer, left_index, key, right_pagenum);
    }

    // parent need split.
    return index_insert_into_internal_after_splitting(parent_buffer, left_index, key, right_pagenum);
}

/*
 *
 */
int index_insert_into_new_root(buffer_block_t* left_buffer, int64_t key, buffer_block_t* right_buffer) {
    pagenum_t root_pagenum;
    buffer_block_t *header_buffer, *root_buffer;
    header_page_t *header_page;
    page_t *left_page, *right_page, *root_page;
    int table_id, result;

    //printf("index_insert_into_new_root_start\n");

    //
    left_page = &(left_buffer->frame);
    right_page = &(right_buffer->frame);
    table_id = left_buffer->table_id;

    //
    root_pagenum = buffer_alloc_page(table_id);
    if(root_pagenum < 0) { return -1; }

    //
    result = buffer_request_page(table_id, root_pagenum, &root_buffer);
    if(result < 0) { return -1; }

    root_page = &(root_buffer->frame);
    result = index_make_page(root_page);
    if(result < 0) { return -1; }
    root_page->parent_or_next_free_page_num = 0;
    root_page->num_of_keys++;
    root_page->right_sibling_or_one_more_page_num = left_buffer->page_num;
    root_page->childs.entries[0].key = key;
    root_page->childs.entries[0].pagenum = right_buffer->page_num;

    result = buffer_release_page(root_buffer, true);
    if(result < 0) { return -1; }

    //
    left_page->parent_or_next_free_page_num = root_pagenum;
    right_page->parent_or_next_free_page_num = root_pagenum;
    result = buffer_release_page(left_buffer, true);
    result = buffer_release_page(right_buffer, true);
    if(result < 0) { return -1; }

    //
    result = buffer_request_page(table_id, HEADER_PAGE_NUM, &header_buffer);
    if(result < 0) { return -1; }

    header_page = (header_page_t*)&(header_buffer->frame);
    header_page->root_page_num = root_pagenum;
    
    result = buffer_release_page(header_buffer, true);
    if(result < 0) { return -1; }

    return SUCCESS;
}

/*
 *
 */
int index_start_new_tree(int table_id, record new_record) {
    pagenum_t pagenum;
    buffer_block_t *header_buffer, *current_buffer;
    header_page_t *header_page;
    page_t *current_page;
    int result;
    
    //
    pagenum = buffer_alloc_page(table_id);
    if(pagenum < 0) { return -1; }

    //
    result = buffer_request_page(table_id, pagenum, &current_buffer);
    if(result < 0) { return -1; }

    current_page = &(current_buffer->frame);
    result = index_make_leaf(current_page);
    if(result < 0) { return -1; }
    current_page->parent_or_next_free_page_num = 0; // Parent is header page. This page is root page.
    current_page->num_of_keys = 1;
    current_page->right_sibling_or_one_more_page_num = 0;
    current_page->childs.records[0] = new_record;

    result = buffer_release_page(current_buffer, true);
    if(result < 0) { return -1; }

    //
    result = buffer_request_page(table_id, HEADER_PAGE_NUM, &header_buffer);
    if(result < 0) { return -1; }
    header_page = (header_page_t*)&(header_buffer->frame);
    header_page->root_page_num = pagenum;
    result = buffer_release_page(header_buffer, true);
    if(result < 0) { return -1; }

    return SUCCESS;
}

/*
 *
 */
int index_insert(int table_id, int64_t key, char * value) {
    record new_record, temp;
    pagenum_t pagenum;
    buffer_block_t *leaf_buffer;
    page_t *leaf_page;
    int result;

    // printf("insert_start. key : %ld, value : %s\n", key, value);

    // Table is not opened or db is not initialized.
    if(find_table_index(table_id) < 0) {
        printf("Please open table first.\n");
        return PAGE_NOT_OPENED;
    }

    result = index_find(table_id, key, temp.value);

    if(result == 0) { // Duplicated.
        return INSERT_DUPLICATED;
    }

    new_record = index_make_record(key, value);

    if(result == -2) { // Empty tree.
        return index_start_new_tree(table_id, new_record);
    }

    pagenum = index_find_leaf(table_id, key);

    result = buffer_request_page(table_id, pagenum, &leaf_buffer);
    if(result < 0) { return -1; }

    leaf_page = &(leaf_buffer->frame);

    // Releasing must be executed at those functions.

    if(leaf_page->num_of_keys < ORDER_LEAF - 1) { // Leaf has room.
        return index_insert_into_leaf(leaf_buffer, new_record);
    }

    // Leaf need split.
    return index_insert_into_leaf_after_splitting(leaf_buffer, new_record);
}

// DELETION.

/* Find the left leaf page of a page that has given pagenum.
 * If given page is leftmost page of tree, return -1.
 * If function can't find given page, return -2. (error)
 */
// current_buffer : Requested input, do not release it in here.
pagenum_t index_find_left_leaf(buffer_block_t *current_buffer) {
    pagenum_t current_pagenum, left_pagenum;
    buffer_block_t *header_buffer, *left_buffer;
    header_page_t *header_page;
    page_t *current_page, *left_page;
    int i, table_id = current_buffer->table_id;

    current_pagenum = current_buffer->page_num;
    buffer_request_page(table_id, HEADER_PAGE_NUM, &header_buffer);
    header_page = (header_page_t*)&(header_buffer->frame);

    left_pagenum = header_page->root_page_num;
    if(left_pagenum == current_pagenum) { // If given page is leftmost page of tree, return -1.
        buffer_release_page(header_buffer, false);
        return FIND_LEFT_LEAF_LEFTMOST;
    }
    buffer_request_page(table_id, left_pagenum, &left_buffer);
    left_page = &(left_buffer->frame);

    // Find leftmost leaf page in DB.
    while(!left_page->is_leaf) {
        left_pagenum = left_page->right_sibling_or_one_more_page_num;
        buffer_release_page(left_buffer, false);
        if(left_pagenum == current_pagenum) { // If given page is leftmost page of tree, return -1.
            buffer_release_page(header_buffer, false);
            return FIND_LEFT_LEAF_LEFTMOST;
        }
        buffer_request_page(table_id, left_pagenum, &left_buffer);
        left_page = &(left_buffer->frame);
    }
/*
    // If given page is leftmost page of tree, return -1.
    if(left_pagenum == current_pagenum) {
        buffer_release_page(header_buffer, false);
        buffer_release_page(left_buffer, false);
        return FIND_LEFT_LEAF_LEFTMOST;
    }
*/
    // Find left page of given page.
    while(!(left_page->right_sibling_or_one_more_page_num == current_pagenum)) {
        left_pagenum = left_page->right_sibling_or_one_more_page_num;
        buffer_release_page(left_buffer, false);
        buffer_request_page(table_id, left_pagenum, &left_buffer);
        left_page = &(left_buffer->frame);
        // If function can't find given page, return -2.
        if(left_pagenum == 0) {
            buffer_release_page(header_buffer, false);
            buffer_release_page(left_buffer, false);
            return FIND_LEFT_LEAF_CANNOT_FIND;
        }
    }

    buffer_release_page(header_buffer, false);
    buffer_release_page(left_buffer, false);
    return left_pagenum;
}

/*
 *
 */
// current_buffer : Requested input, do not release it in here.
// parent_buffer : Requested input, do not release it in here.
int index_get_index(buffer_block_t *current_buffer, buffer_block_t *parent_buffer) {
    page_t *parent_page;
    int i;

    // printf("index_get_index_start\n");
    parent_page = &(parent_buffer->frame);

    if(parent_page->right_sibling_or_one_more_page_num == current_buffer->page_num) {
        return GET_INDEX_LEFTMOST; // Given page is the leftmost child.
    }
    for(i = 0; i < parent_page->num_of_keys; i++) {
        if(parent_page->childs.entries[i].pagenum == current_buffer->page_num) {
            return i;
        }
    }

    // error.
    return GET_INDEX_ERROR;
}

/*
 *
 */
// current_buffer : Requested input, do not release it in here.
// parent_buffer : Requested input, do not release it in here.
int index_get_neighbor_index(buffer_block_t *current_buffer, buffer_block_t *parent_buffer) {
    page_t *parent_page;
    int i;

    // printf("index_get_neighbor_index_start\n");

    parent_page = &(parent_buffer->frame);

    if(parent_page->right_sibling_or_one_more_page_num == current_buffer->page_num) {
        return GET_NEIGHBOR_INDEX_LEFTMOST; // Given page is the leftmost child.
    }
    for(i = 0; i < parent_page->num_of_keys; i++) {
        if(parent_page->childs.entries[i].pagenum == current_buffer->page_num) {
            return i - 1;
        }
    }

    // error.
    return GET_NEIGHBOR_INDEX_ERROR;
}

/*
 *
 */
// header_buffer : Requested input, release it in here.
// root_buffer : Requested input, release it in here.
int index_adjust_root(buffer_block_t *header_buffer, buffer_block_t *root_buffer) {
    pagenum_t root_pagenum, temp_pagenum;
    buffer_block_t *temp_buffer;
    header_page_t *header_page;
    page_t *root_page, *temp_page;
    int result, table_id;

    // printf("adjust_root_start\n");

    // Get root page num.
    header_page = (header_page_t*)&(header_buffer->frame);
    root_page = &(root_buffer->frame);
    root_pagenum = root_buffer->page_num;
    table_id = header_buffer->table_id;

    // Nonempty root.
    if(root_page->num_of_keys > 0) {
        result = buffer_release_page(header_buffer, false);
        result = buffer_release_page(root_buffer, true);
        if(result < 0) { return -1; }
        return 0;
    }

    // Empty root.
    if(!root_page->is_leaf) { // If root is internal page,
        temp_pagenum = root_page->right_sibling_or_one_more_page_num;

        // Set root page num.
        //file_read_page(HEADER_PAGE_NUM, &header);
        header_page->root_page_num = temp_pagenum;

        result = buffer_request_page(table_id, temp_pagenum, &temp_buffer);
        if(result < 0) { return -1; }
        temp_page = &(temp_buffer->frame);
        temp_page->parent_or_next_free_page_num = 0;

        result = buffer_release_page(temp_buffer, true);
        result = buffer_release_page(header_buffer, true);
        if(result < 0) { return -1; }
    }
    else { // If root is leaf page,
        // Set root page num.
        header_page->root_page_num = 0; // header.
        result = buffer_release_page(header_buffer, true);
        if(result < 0) { return -1; }
    }
    buffer_release_page(root_buffer, true);
    buffer_free_page(table_id, root_pagenum);

    // Add error checking.

    return SUCCESS;
}

/*
 *
 */
// current_buffer : Requested input, release it in here.
int index_remove_entry_from_internal(buffer_block_t *current_buffer) {
    pagenum_t current_pagenum, parent_pagenum, left_pagenum;
    buffer_block_t *parent_buffer, *left_buffer;
    page_t *current_page, *parent_page, *left_page;
    int current_index, i, table_id;

    // printf("index_remove_entry_from_internal_start\n");

    current_page = &(current_buffer->frame);
    current_pagenum = current_buffer->page_num;
    table_id = current_buffer->table_id;
    
    if(current_page->is_leaf) { // Link right_sibling_page_num.
        left_pagenum = index_find_left_leaf(current_buffer);
        if(left_pagenum == FIND_LEFT_LEAF_CANNOT_FIND) {
            return ERROR;
        }
        else if(left_pagenum == FIND_LEFT_LEAF_LEFTMOST) {
            // Do nothing.
        }
        else {
            buffer_request_page(table_id, left_pagenum, &left_buffer);
            left_page = &(left_buffer->frame);
            left_page->right_sibling_or_one_more_page_num = current_page->right_sibling_or_one_more_page_num;
            buffer_release_page(left_buffer, true);
        }
        
    }

    parent_pagenum = current_page->parent_or_next_free_page_num;
    buffer_request_page(current_buffer->table_id, parent_pagenum, &parent_buffer);
    parent_page = &(parent_buffer->frame);
    current_index = index_get_index(current_buffer, parent_buffer);

    if(current_index == -3) {
        printf("current_index == -3\n");
        exit(5);
    }

    // If current page is leftmost child of parent page,
    if(current_index == -1) {
        parent_page->right_sibling_or_one_more_page_num = parent_page->childs.entries[0].pagenum;
        for(i = 1; i < parent_page->num_of_keys; i++) {
            parent_page->childs.entries[i - 1] = parent_page->childs.entries[i];
        }
        
    }
    // If current page is not leftmost child,
    else {
        for(i = current_index + 1; i < parent_page->num_of_keys; i++) {
            parent_page->childs.entries[i - 1] = parent_page->childs.entries[i];
        }
    }
    parent_page->num_of_keys--;
    // Set needless space to 0 for tidness.
    memset(&(parent_page->childs.entries[parent_page->num_of_keys]), 0, (ORDER_INTERNAL-1-parent_page->num_of_keys)*SIZE_ENTRY);
    buffer_release_page(current_buffer, true);
    buffer_free_page(table_id, current_pagenum);

    if(parent_page->num_of_keys == 0) {
        return index_delete_entry(parent_buffer);
    }

    buffer_release_page(parent_buffer, true);

    // Add error checking.

    return SUCCESS;
}


int index_remove_record_from_leaf(page_t* leaf_page, int64_t key) {
    int i;

    // printf("remove_record_from_leaf_start\n");
    
    // Find the index of given key.
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

/* current page has 0 key, neighbor page has 1 key.
 *
 */
int index_merge_pages(buffer_block_t *current_buffer, buffer_block_t *neighbor_buffer, buffer_block_t *parent_buffer,
        int neighbor_index, int k_prime_index, int64_t k_prime) {
    pagenum_t neighbor_pagenum;
    buffer_block_t *temp_buffer;
    page_t *current_page, *neighbor_page, *parent_page, *temp_page;
    int current_index, table_id;

    // printf("index_merge_pages_start\n");

    current_page = &(current_buffer->frame);
    neighbor_page = &(neighbor_buffer->frame);
    parent_page = & (parent_buffer->frame);
    neighbor_pagenum = neighbor_buffer->page_num;
    table_id = current_buffer->table_id;

    //exit(5);
    // If current page is leftmost page,
    if(neighbor_index == -2) { // current | (k_prime) | neighbor
        current_index = -1;
        
        if(!current_page->is_leaf) {
            neighbor_page->childs.entries[1] = neighbor_page->childs.entries[0];
            neighbor_page->childs.entries[0].pagenum = neighbor_page->right_sibling_or_one_more_page_num;
            neighbor_page->childs.entries[0].key = k_prime;
            neighbor_page->right_sibling_or_one_more_page_num = current_page->right_sibling_or_one_more_page_num;
            buffer_request_page(table_id, neighbor_page->right_sibling_or_one_more_page_num, &temp_buffer);
            temp_page = &(temp_buffer->frame);
            temp_page->parent_or_next_free_page_num = neighbor_pagenum;
            buffer_release_page(temp_buffer, true);
            neighbor_page->num_of_keys++;
            //exit(5);
            // Do we have to Change parent's key?
            // k_prime_index = 0
            // parent_page->childs.entries[k_prime_index].key = 
        }
    }
    // If current page has a neighbor to the left,
    else { // neighbor | (k_prime) | current
        current_index = k_prime_index;
        if(!current_page->is_leaf) {
            neighbor_page->childs.entries[1].key = k_prime;
            neighbor_page->childs.entries[1].pagenum = current_page->right_sibling_or_one_more_page_num;
            buffer_request_page(table_id, neighbor_page->childs.entries[1].pagenum, &temp_buffer);
            temp_page = &(temp_buffer->frame);
            temp_page->parent_or_next_free_page_num = neighbor_pagenum;
            buffer_release_page(temp_buffer, true);
            neighbor_page->num_of_keys++;
        }
    }

    buffer_release_page(neighbor_buffer, true);
    buffer_release_page(parent_buffer, true);
    
    return index_remove_entry_from_internal(current_buffer);
    //return index_delete_entry(current_buffer);
}


/* current page has 0 key, neighbor page has some keys > 1.
 *
 */
int index_redistribute_pages(buffer_block_t *current_buffer, buffer_block_t *neighbor_buffer, buffer_block_t *parent_buffer,
        int neighbor_index, int k_prime_index, int64_t k_prime) {
    pagenum_t current_pagenum;
    buffer_block_t *temp_buffer;
    page_t *current_page, *neighbor_page, *parent_page, *temp_page;
    int split_point, i, j, temp, table_id;
    
    // printf("index_redistribute_pages\n");

    current_page = &(current_buffer->frame);
    neighbor_page = &(neighbor_buffer->frame);
    parent_page = & (parent_buffer->frame);
    current_pagenum = current_buffer->page_num;
    table_id = current_buffer->table_id;

    split_point = cut(neighbor_page->num_of_keys);
    
    // If current page is leftmost page,
    // pull the half of the neighbor's childs
    // from the neighbor's left side to current's right end.
    if(neighbor_index == -2) { // current | (k_prime) | neighbor
        current_page->childs.entries[0].key = k_prime;
        current_page->childs.entries[0].pagenum = neighbor_page->right_sibling_or_one_more_page_num;
        buffer_request_page(table_id, current_page->childs.entries[0].pagenum, &temp_buffer);
        temp_page = &(temp_buffer->frame);
        temp_page->parent_or_next_free_page_num = current_pagenum;
        buffer_release_page(temp_buffer, true);
        current_page->num_of_keys++;
        for(i = 1; i < split_point; i++) {
            current_page->childs.entries[i] = neighbor_page->childs.entries[i - 1];
            buffer_request_page(table_id, current_page->childs.entries[i].pagenum, &temp_buffer);
            temp_page = &(temp_buffer->frame);
            temp_page->parent_or_next_free_page_num = current_pagenum;
            buffer_release_page(temp_buffer, true);
            current_page->num_of_keys++;
        }

        // Set parent's key appropriately.
        parent_page->childs.entries[k_prime_index].key = neighbor_page->childs.entries[split_point - 1].key;

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
        current_page->childs.entries[temp - 1 - split_point].key = k_prime;
        current_page->childs.entries[temp - 1 - split_point].pagenum = current_page->right_sibling_or_one_more_page_num;
        current_page->num_of_keys++;
        current_page->right_sibling_or_one_more_page_num = neighbor_page->childs.entries[split_point].pagenum;
        buffer_request_page(table_id, current_page->right_sibling_or_one_more_page_num, &temp_buffer);
        temp_page = &(temp_buffer->frame);
        temp_page->parent_or_next_free_page_num = current_pagenum;
        buffer_release_page(temp_buffer, true);
        
        for(i = split_point + 1, j = 0; i < temp; i++, j++) {
            current_page->childs.entries[j] = neighbor_page->childs.entries[i];
            buffer_request_page(table_id, current_page->childs.entries[j].pagenum, &temp_buffer);
            temp_page = &(temp_buffer->frame);
            temp_page->parent_or_next_free_page_num = current_pagenum;
            buffer_release_page(temp_buffer, true);
            current_page->num_of_keys++;
            neighbor_page->num_of_keys--;
        }
        neighbor_page->num_of_keys--;

        // Set parent's key appropriately.
        parent_page->childs.entries[k_prime_index].key = neighbor_page->childs.entries[split_point].key;

        // Set needless space to 0 for tidiness.
        memset(&(neighbor_page->childs.entries[neighbor_page->num_of_keys]), 0, (ORDER_INTERNAL-1-neighbor_page->num_of_keys)*SIZE_ENTRY);
    }

    buffer_release_page(current_buffer, true);
    buffer_release_page(neighbor_buffer, true);
    buffer_release_page(parent_buffer, true);

    // Add error checking.

    return SUCCESS;
}

/*
 *
 */
int index_delete_entry(buffer_block_t *current_buffer) {
    pagenum_t current_pagenum, parent_pagenum, neighbor_pagenum;
    buffer_block_t *header_buffer, *parent_buffer, *neighbor_buffer;
    header_page_t *header_page;
    page_t *current_page, *parent_page, *neighbor_page;
    int64_t k_prime;
    int neighbor_index, k_prime_index, table_id;

    // printf("index_delte_entry : %ldpage\n", pagenum);

    current_page = &(current_buffer->frame);
    current_pagenum = current_buffer->page_num;
    table_id = current_buffer->table_id;

    // remove entry from internal page.
    if(current_page->is_leaf) {
        return index_remove_entry_from_internal(current_buffer);
    }

    buffer_request_page(table_id, HEADER_PAGE_NUM, &header_buffer);
    header_page = (header_page_t*)&(header_buffer->frame);
    
    if(current_buffer->page_num == header_page->root_page_num) {
        return index_adjust_root(header_buffer, current_buffer);
    }

    buffer_release_page(header_buffer, false);
    
    parent_pagenum = current_page->parent_or_next_free_page_num;
    buffer_request_page(table_id, parent_pagenum, &parent_buffer);
    parent_page = &(parent_buffer->frame);

    // Find neighbor page.
    neighbor_index = index_get_neighbor_index(current_buffer, parent_buffer);
    if(neighbor_index == -3) return -2; // error.

    if(neighbor_index == -2) { // Current page is leftmost child of parent page.
        k_prime_index = 0;
        neighbor_pagenum = parent_page->childs.entries[0].pagenum;
    }
    else if(neighbor_index == -1) { // Left neighbor page is leftmost child.
        k_prime_index = 0;
        neighbor_pagenum = parent_page->right_sibling_or_one_more_page_num;
    }
    else { // Index of left neighbor page >= 0.
        k_prime_index = neighbor_index + 1;
        neighbor_pagenum = parent_page->childs.entries[neighbor_index].pagenum;
    }
    k_prime = parent_page->childs.entries[k_prime_index].key;

    // Delay merge. If merge isn't necessary, just excute redistribution.
    buffer_request_page(table_id, neighbor_pagenum, &neighbor_buffer);
    neighbor_page = &neighbor_buffer->frame;
    
    // Redistribute first.
    if(neighbor_page->num_of_keys <= 1) { // If merge is necessary,
        // current page has 0 key, and neighbor page has 1 key.
        return index_merge_pages(current_buffer, neighbor_buffer, parent_buffer, neighbor_index, k_prime_index, k_prime);
    }
    else { // If merge isn't necessary,
        // current page has 0 key, and neighbor page has some keys > 1.
        return index_redistribute_pages(current_buffer, neighbor_buffer, parent_buffer, neighbor_index, k_prime_index, k_prime);
    }
    /*
    // Redistribute first.
    if(neighbor_page->num_of_keys < ORDER_INTERNAL - 1) { // If merge can 
        // current page has 0 key, and neighbor page has 1 key.
        return index_merge_pages(current_buffer, neighbor_buffer, parent_buffer, neighbor_index, k_prime_index, k_prime);
    }
    else { // If redistribution is necessary,
        // current page has 0 key, and neighbor page has some keys > 1.
        return index_redistribute_pages(current_buffer, neighbor_buffer, parent_buffer, neighbor_index, k_prime_index, k_prime);
    }
    */
}

/*
 *
 */
int index_delete_record(int table_id, pagenum_t leaf_pagenum, int64_t key) {
    buffer_block_t *header_buffer, *leaf_buffer;
    header_page_t *header_page;
    page_t *leaf_page;
    int i, result;
    uint32_t num_of_keys;

    // printf("index_delte_record\n");

    // Remove record from leaf page.
    result = buffer_request_page(table_id, leaf_pagenum, &leaf_buffer);
    if(result < 0) { return -1; }
    leaf_page = &(leaf_buffer->frame);

    index_remove_record_from_leaf(leaf_page, key);

    num_of_keys = leaf_page->num_of_keys;
    //buffer_release_page(leaf_buffer, true);

    // Deletion from the root.
    result = buffer_request_page(table_id, HEADER_PAGE_NUM, &header_buffer);
    if(result < 0) { return -1; }
    header_page = (header_page_t*)&(header_buffer->frame);
    if(leaf_pagenum == header_page->root_page_num) {
        // Releasing must be executed at that function.
        return index_adjust_root(header_buffer, leaf_buffer);
    }
    result = buffer_release_page(header_buffer, false);
    if(result < 0) { return -1; }

    // Deletion from a page below root.
    // Delayed merge. If there are at least one key, don't merge.
    if(num_of_keys != 0) {
        result = buffer_release_page(leaf_buffer, true);
        if(result < 0) { return -1; }
        return SUCCESS;
    }
    else {
        return index_delete_entry(leaf_buffer);
    }
}

/*
 *
 */
int index_delete(int table_id, int64_t key) {
    pagenum_t key_leaf_pagenum;
    record temp;
    int find_result;

    // printf("index_delte_start : %ld\n", key);
    
    // Table is not opened or db is not initialized.
    if(find_table_index(table_id) < 0) {
        printf("Please open table first.\n");
        return PAGE_NOT_OPENED;
    }

    key_leaf_pagenum = index_find_leaf(table_id, key);
    find_result = index_find(table_id, key, temp.value);
    if(key_leaf_pagenum != 0 && find_result == 0) {
        return index_delete_record(table_id, key_leaf_pagenum, key);
    }

    return DELETE_NOT_EXISTS; // Not exists.
}