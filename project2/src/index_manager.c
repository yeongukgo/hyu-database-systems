#include "index_manager.h"

char opened = 0;

/* Find the appropriate place to split a page.
 */
int cut(int length) {
    if(length % 2 == 0) return length/2;
    else return length/2 + 1;
}

/* Find the left leaf page of a page that has given pagenum.
 * If given page is leftmost page of tree, return -1.
 * If function can't find given page, return -2. (error)
 */
pagenum_t db_find_left_leaf(pagenum_t pagenum) {
    pagenum_t left_pagenum;
    page_t left;
    int i;

    left_pagenum = get_root_page_num();
    file_read_page(left_pagenum, &left);

    // Find leftmost leaf page in DB.
    while(!left.is_leaf) {
        left_pagenum = left.right_sibling_or_one_more_page_num;
        file_read_page(left_pagenum, &left);
    }

    // If given page is leftmost page of tree, return -1.
    if(left_pagenum == pagenum) {
        return FIND_LEFT_LEAF_LEFTMOST;
    }

    // Find left page of given page.
    while(!(left.right_sibling_or_one_more_page_num == pagenum)) {
        left_pagenum = left.right_sibling_or_one_more_page_num;
        file_read_page(left_pagenum, &left);
        // If function can't find given page, return -2.
        if(left_pagenum == 0) {
            return FIND_LEFT_LEAF_CANNOT_FIND;
        }
    }

    return left_pagenum;
}

// ----API----

// OPEN 

/* Open a table by using file_open function in disk space manager.
 * Return fd value as unique table id. (temporary measure)
 */
int open_table(char * pathname) {
    int unique_table_id = file_open(pathname);
    
    if(unique_table_id >= 0) { // Success.
        opened = 1;
    }
    
    return unique_table_id;
}

// FIND

/* Trace the path from the root to a leaf, searching by key.
 * Return pagenum of the leaf containing the given key.
 */
pagenum_t db_find_leaf(int64_t key) {
    pagenum_t pagenum;
    page_t page;
    int i;

    pagenum = get_root_page_num();

    // If root doesn't exists, return 0.
    if(pagenum == 0) {
        return pagenum;
    }

    file_read_page(pagenum, &page);
    // Trace the path from the root to a leaf, searching by key.
    while(!page.is_leaf) {
        for(i = 0; i < page.num_of_keys;) {
            if(key >= page.childs.entries[i].key) i++;
            else break;
        }
        if(i == 0) pagenum = page.right_sibling_or_one_more_page_num;
        else pagenum = page.childs.entries[i-1].pagenum;

        file_read_page(pagenum, &page);
    }

    return pagenum;
}

/* Find the record which given key refers.
 * Store the value of the record to given char pointer.
 * If successful, return 0.
 */
int db_find(int64_t key, char * ret_val) {
    pagenum_t pagenum;
    page_t page;
    int i;

    if(opened == 0) { // Table is not opened.
        printf("Please open table first.\n");
        return PAGE_NOT_OPENED;
    }

    pagenum = db_find_leaf(key);

    // Empty tree.
    if(pagenum == 0) return FIND_EMPTY_TREE;
    
    // Find the record.
    file_read_page(pagenum, &page);
    for(i = 0; i < page.num_of_keys; i++) {
        if(page.childs.records[i].key == key) break;
    }

    // Record which given key refers doesn't exists.
    if(i == page.num_of_keys) return FIND_NOT_EXISTS;
    else { // Record which given key refers exists.
        //strcpy(ret_val, page.childs.records[i].value);
        memcpy(ret_val, page.childs.records[i].value, RECORD_VALUE_SIZE);
        return SUCCESS;
    }
}

// INSERTION

/* Create and initialize a new record with given key/value.
 * Return maked record.
 */
record db_make_record(int64_t key, char * value) {
    record new_record;

    memset(&new_record, 0, RECORD_SIZE);
    new_record.key = key;
    //strcpy(new_record.value, value);
    memcpy(new_record.value, value, RECORD_VALUE_SIZE);

    return new_record;
}

/* Create and initialize a new page.
 * Return maked page.
 */
page_t db_make_page() {
    page_t page;

    memset(&page, 0, PAGE_SIZE);
    page.parent_or_next_free_page_num = -1;
    page.is_leaf = 0;
    page.num_of_keys = 0;
    
    return page;
}

/* Create and initialize a new leaf page.
 * Return maked leaf page.
 */
page_t db_make_leaf() {
    page_t page = db_make_page();
    page.is_leaf = 1;

    return page;
}

/*
 *
 */
int db_get_left_index(pagenum_t parent_pagenum, pagenum_t left_pagenum) {
    page_t parent_page;
    int left_index = -1;

    file_read_page(parent_pagenum, &parent_page);

    if(parent_page.right_sibling_or_one_more_page_num == left_pagenum) {
        return left_index;
    }

    left_index = 0;
    while(left_index <= parent_page.num_of_keys &&
            parent_page.childs.entries[left_index].pagenum != left_pagenum) {
        left_index++;
    }
    return left_index;
}

/*
 *
 */
int db_insert_into_leaf(pagenum_t pagenum, record new_record) {
    page_t leaf;
    int i, insertion_point;

    file_read_page(pagenum, &leaf);
    insertion_point = 0;
    while(insertion_point < leaf.num_of_keys && leaf.childs.records[insertion_point].key < new_record.key) {
        insertion_point++;
    }

    for(i = leaf.num_of_keys; i > insertion_point; i--) {
        leaf.childs.records[i] = leaf.childs.records[i-1];
    }
    leaf.childs.records[insertion_point] = new_record;
    leaf.num_of_keys++;

    file_write_page(pagenum, &leaf);

    // Add error checking.
    
    return SUCCESS;
}

/*
 *
 */
int db_insert_into_leaf_after_splitting(pagenum_t pagenum, record new_record) {
    pagenum_t new_pagenum;
    page_t leaf, new_leaf;
    int64_t new_key;
    record temp_record[LEAF_ORDER];
    int insertion_point, i, j, split;

    //printf("db_insert_into_leaf_after_splitting_start\n");
    
    file_read_page(pagenum, &leaf);
    new_leaf = db_make_leaf();

    insertion_point = 0;
    while(insertion_point < LEAF_ORDER - 1 && leaf.childs.records[insertion_point].key < new_record.key) {
        insertion_point++;
    }

    for(i = 0, j = 0; i < leaf.num_of_keys; i++, j++) {
        if(j == insertion_point) j++;
        temp_record[j] = leaf.childs.records[i];
    }
    temp_record[insertion_point] = new_record;

    leaf.num_of_keys = 0;

    split = cut(LEAF_ORDER - 1);

    for(i = 0; i < split; i++) {
        leaf.childs.records[i] = temp_record[i];
        leaf.num_of_keys++;
    }

    for(i = split, j = 0; i < LEAF_ORDER; i++, j++) {
        new_leaf.childs.records[j] = temp_record[i];
        new_leaf.num_of_keys++;
    }

    new_pagenum = file_alloc_page();

    new_leaf.right_sibling_or_one_more_page_num = leaf.right_sibling_or_one_more_page_num;
    leaf.right_sibling_or_one_more_page_num = new_pagenum;

    memset(&leaf.childs.records[leaf.num_of_keys], 0, (LEAF_ORDER-1-leaf.num_of_keys)*RECORD_SIZE);
    
    new_leaf.parent_or_next_free_page_num = leaf.parent_or_next_free_page_num;
    new_key = new_leaf.childs.records[0].key;

    file_write_page(pagenum, &leaf);
    file_write_page(new_pagenum, &new_leaf);

    return db_insert_into_parent(pagenum, new_key, new_pagenum);
}

/*
 *
 */
int db_insert_into_internal(pagenum_t parent_pagenum, int left_index, int64_t key, pagenum_t right_pagenum) {
    page_t parent_page;
    int i;

    // read page.
    file_read_page(parent_pagenum, &parent_page);

    // Given bpt code sets,
    // key :        0   1   2   3   4
    // pointer :  0   1   2   3   4   5
    // this code sets,
    // key :        0   1   2   3   4
    // pagenum : -1   0   1   2   3   4
    // So use left_index + 1. 
    // If it must be changed, get_left_index must be changed.
    for(i = parent_page.num_of_keys; i > left_index + 1; i--) {
        parent_page.childs.entries[i] = parent_page.childs.entries[i-1];
    }
    parent_page.childs.entries[left_index+1].key = key;
    parent_page.childs.entries[left_index+1].pagenum = right_pagenum;
    parent_page.num_of_keys++;

    // write page.
    file_write_page(parent_pagenum, &parent_page);
    
    // Add error checking.

    return SUCCESS;
}

/*
 *
 */
int db_insert_into_internal_after_splitting(pagenum_t parent_pagenum, int left_index, int64_t key, pagenum_t right_pagenum) {
    pagenum_t new_pagenum;
    page_t internal, new_internal, temp_page; // Is the temp page used?
    pagenum_t temp_one_more_pagenum;
    entry temp_entry[INTERNAL_ORDER];
    int64_t k_prime;
    int i, j, split;
    
    //printf("db_insert_into_internal_after_splitting_start\n");

    file_read_page(parent_pagenum, &internal);

    temp_one_more_pagenum = internal.right_sibling_or_one_more_page_num;
    for(i = 0, j = 0; i < internal.num_of_keys; i++, j++) {
        if(j == left_index+1) j++;
        temp_entry[j] = internal.childs.entries[i];
    }
    temp_entry[left_index+1].key = key;
    temp_entry[left_index+1].pagenum = right_pagenum;

    split = cut(INTERNAL_ORDER);
    new_internal = db_make_page();
    internal.num_of_keys = 0;
    internal.right_sibling_or_one_more_page_num = temp_one_more_pagenum;
    for(i = 0; i < split - 1; i++) {
        internal.childs.entries[i] = temp_entry[i];
        internal.num_of_keys++;
    }
    k_prime = temp_entry[i].key;
    new_internal.right_sibling_or_one_more_page_num = temp_entry[i].pagenum;
    for(++i, j = 0; i < INTERNAL_ORDER; i++, j++) {
        new_internal.childs.entries[j] = temp_entry[i];
        new_internal.num_of_keys++;
    }

    new_internal.parent_or_next_free_page_num = internal.parent_or_next_free_page_num;

    memset(&internal.childs.entries[internal.num_of_keys], 0, (INTERNAL_ORDER-1-internal.num_of_keys)*ENTRY_SIZE);

    file_write_page(parent_pagenum, &internal);
    new_pagenum = file_alloc_page();
    file_write_page(new_pagenum, &new_internal);

    file_read_page(new_internal.right_sibling_or_one_more_page_num, &temp_page);
    temp_page.parent_or_next_free_page_num = new_pagenum;
    file_write_page(new_internal.right_sibling_or_one_more_page_num, &temp_page);
    for(i = 0; i < new_internal.num_of_keys; i++) {
        file_read_page(new_internal.childs.entries[i].pagenum, &temp_page);
        temp_page.parent_or_next_free_page_num = new_pagenum;
        file_write_page(new_internal.childs.entries[i].pagenum, &temp_page);
    }

    return db_insert_into_parent(parent_pagenum, k_prime, new_pagenum);
}

/*
 *
 */
int db_insert_into_parent(pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum) {
    pagenum_t parent_pagenum;
    page_t left_page, right_page, parent_page;
    int left_index;

    //printf("db_insert_into_parent_start\n");

    file_read_page(left_pagenum, &left_page);
    file_read_page(right_pagenum, &right_page);
    parent_pagenum = left_page.parent_or_next_free_page_num;
    file_read_page(parent_pagenum, &parent_page);

    // Parent page is header page. It means left page was root page.
    // Make new root.
    if(parent_pagenum == 0) {
        //printf("Make new root.\n");
        return db_insert_into_new_root(left_pagenum, key, right_pagenum);
    }

    left_index = db_get_left_index(parent_pagenum, left_pagenum);

    // if parent has room.
    if(parent_page.num_of_keys < INTERNAL_ORDER - 1) {
        return db_insert_into_internal(parent_pagenum, left_index, key, right_pagenum);
    }

    // parent need split.
    return db_insert_into_internal_after_splitting(parent_pagenum, left_index, key, right_pagenum);
}

/*
 *
 */
int db_insert_into_new_root(pagenum_t left_pagenum, int64_t key, pagenum_t right_pagenum) {
    pagenum_t root_pagenum;
    page_t root = db_make_page();
    page_t left_page, right_page;

    //printf("db_insert_into_new_root_start\n");

    file_read_page(left_pagenum, &left_page);
    file_read_page(right_pagenum, &right_page);

    root.parent_or_next_free_page_num = 0;
    root.num_of_keys++;
    root.right_sibling_or_one_more_page_num = left_pagenum;
    root.childs.entries[0].key = key;
    root.childs.entries[0].pagenum = right_pagenum;
    root_pagenum = file_alloc_page();

    set_root_page_num(root_pagenum);

    file_write_page(root_pagenum, &root);

    left_page.parent_or_next_free_page_num = root_pagenum;
    right_page.parent_or_next_free_page_num = root_pagenum;

    file_write_page(left_pagenum, &left_page);
    file_write_page(right_pagenum, &right_page);
 
    // Add error checking.

    return SUCCESS;
}

/*
 *
 */
int db_start_new_tree(record new_record) {
    pagenum_t pagenum;
    page_t page;

    pagenum = file_alloc_page();

    page = db_make_leaf();
    page.parent_or_next_free_page_num = 0; // Parent Header page. this page is root page.
    page.num_of_keys = 1;
    page.right_sibling_or_one_more_page_num = 0;
    page.childs.records[0] = new_record;

    set_root_page_num(pagenum);

    file_write_page(pagenum, &page);

    // Add error checking.

    return SUCCESS;
}

/*
 *
 */
int db_insert(int64_t key, char * value) {
    record new_record;
    pagenum_t pagenum;
    page_t leaf;
    char temp[RECORD_VALUE_SIZE];
    int find_result;

    // printf("insert_start. key : %ld, value : %s\n", key, value);

    if(opened == 0) { // Table is not opened.
        printf("Please open table first.\n");
        return PAGE_NOT_OPENED;
    }

    find_result = db_find(key, temp);

    if(find_result == 0) { // Duplicated.
        return INSERT_DUPLICATED;
    }

    new_record = db_make_record(key, value);

    if(find_result == -2) { // Empty tree.
        return db_start_new_tree(new_record);
    }

    pagenum = db_find_leaf(key);
    file_read_page(pagenum, &leaf);

    if(leaf.num_of_keys < LEAF_ORDER - 1) { // Leaf has room.
        return db_insert_into_leaf(pagenum, new_record);
    }

    // Leaf need split.
    return db_insert_into_leaf_after_splitting(pagenum, new_record);
}

// DELETION.

/*
 *
 */
int db_get_index(pagenum_t pagenum) {
    pagenum_t parent_pagenum;
    page_t page, parent_page;
    int i;

    // printf("db_get_index_start\n");

    file_read_page(pagenum, &page);
    parent_pagenum = page.parent_or_next_free_page_num;
    file_read_page(parent_pagenum, &parent_page);
    if(parent_page.right_sibling_or_one_more_page_num == pagenum) {
        return GET_INDEX_LEFTMOST; // Given page is the leftmost child.
    }
    for(i = 0; i < parent_page.num_of_keys; i++) {
        if(parent_page.childs.entries[i].pagenum == pagenum) {
            return i;
        }
    }

    // error.
    return GET_INDEX_ERROR;
}

/*
 *
 */
int db_get_neighbor_index(pagenum_t pagenum) {
    pagenum_t parent_pagenum;
    page_t page, parent_page;
    int i;

    // printf("db_get_neighbor_index_start\n");

    file_read_page(pagenum, &page);
    parent_pagenum = page.parent_or_next_free_page_num;
    file_read_page(parent_pagenum, &parent_page);

    if(parent_page.right_sibling_or_one_more_page_num == pagenum) {
        return GET_NEIGHBOR_INDEX_LEFTMOST; // Given page is the leftmost child.
    }
    for(i = 0; i < parent_page.num_of_keys; i++) {
        if(parent_page.childs.entries[i].pagenum == pagenum) {
            return i - 1;
        }
    }

    // error.
    return GET_NEIGHBOR_INDEX_ERROR;
}

/*
 *
 */
int db_adjust_root() {
    pagenum_t root_pagenum, temp_pagenum;
    page_t root, temp;

    // printf("adjust_root_start\n");

    root_pagenum = get_root_page_num();
    file_read_page(root_pagenum, &root);

    // Nonempty root.
    if(root.num_of_keys > 0) {
        return 0;
    }

    // Empty root.
    if(!root.is_leaf) { // If root is internal page,
        temp_pagenum = root.right_sibling_or_one_more_page_num;
        set_root_page_num(temp_pagenum);
        file_read_page(temp_pagenum, &temp);
        temp.parent_or_next_free_page_num = 0;
        file_write_page(temp_pagenum, &temp);
    }
    else { // If root is leaf page,
        set_root_page_num(0); // Header.
    }
    file_free_page(root_pagenum);

    // Add error checking.

    return SUCCESS;
}

/*
 *
 */
int db_remove_entry_from_internal(pagenum_t current_pagenum) {
    pagenum_t parent_pagenum, left_pagenum;
    page_t current, parent, left;
    int current_index, i;

    // printf("db_remove_entry_from_internal_start\n");

    file_read_page(current_pagenum, &current);
    parent_pagenum = current.parent_or_next_free_page_num;
    file_read_page(parent_pagenum, &parent);
    current_index = db_get_index(current_pagenum);

    if(current.is_leaf) { // Link right_sibling_page_num.
        left_pagenum = db_find_left_leaf(current_pagenum);
        if(left_pagenum == FIND_LEFT_LEAF_CANNOT_FIND) {
            return ERROR;
        }
        else if(left_pagenum == FIND_LEFT_LEAF_LEFTMOST) {
            // Do nothing.
        }
        else {
            file_read_page(left_pagenum, &left);
            left.right_sibling_or_one_more_page_num = current.right_sibling_or_one_more_page_num;
            file_write_page(left_pagenum, &left);
        }
        
    }

    // If current page is leftmost child of parent page,
    if(current_index == -1) {
        parent.right_sibling_or_one_more_page_num = parent.childs.entries[0].pagenum;
        for(i = 1; i < parent.num_of_keys; i++) {
            parent.childs.entries[i - 1] = parent.childs.entries[i];
        }
        
    }
    // If current page is not leftmost child,
    else {
        for(i = current_index + 1; i < parent.num_of_keys; i++) {
            parent.childs.entries[i - 1] = parent.childs.entries[i];
        }
    }
    parent.num_of_keys--;
    // Set needless space to 0 for tidness.
    memset(&parent.childs.entries[parent.num_of_keys], 0, (INTERNAL_ORDER-1-parent.num_of_keys)*ENTRY_SIZE);
    file_write_page(parent_pagenum, &parent);
    file_free_page(current_pagenum);
    if(parent.num_of_keys == 0) {
        return db_delete_entry(parent_pagenum);
    }

    // Add error checking.

    return SUCCESS;
}

/*
 *
 */
int db_remove_record_from_leaf(pagenum_t leaf_pagenum, int64_t key) {
    page_t leaf;
    int i;

    // printf("remove_record_from_leaf_start\n");

    file_read_page(leaf_pagenum, &leaf);
    
    // Find the index of given key.
    i = 0;
    while(leaf.childs.records[i].key != key) i++;

    // Remove the record and shift other records accordingly.
    for(++i; i < leaf.num_of_keys; i++) {
        leaf.childs.records[i - 1] = leaf.childs.records[i];
    }

    // One key fewer.
    leaf.num_of_keys--;

    // Set needless space to 0 for tidiness.
    memset(&leaf.childs.records[leaf.num_of_keys], 0, (LEAF_ORDER-1-leaf.num_of_keys)*RECORD_SIZE);

    file_write_page(leaf_pagenum, &leaf);

    // Add error check.

    return SUCCESS;
}

/* current page has 0 key, neighbor page has 1 key.
 *
 */
int db_merge_pages(pagenum_t current_pagenum, pagenum_t neighbor_pagenum,
        int neighbor_index, int k_prime_index, int64_t k_prime) {
    page_t current, neighbor, temp;
    int current_index;

    // printf("db_merge_pages_start\n");

    file_read_page(current_pagenum, &current);
    file_read_page(neighbor_pagenum, &neighbor);

    // If current page is leftmost page,
    if(neighbor_index == -2) { // current | (k_prime) | neighbor
        current_index = -1;
        
        if(!current.is_leaf) {
            neighbor.childs.entries[1] = neighbor.childs.entries[0];
            neighbor.childs.entries[0].pagenum = neighbor.right_sibling_or_one_more_page_num;
            neighbor.childs.entries[0].key = k_prime;
            neighbor.right_sibling_or_one_more_page_num = current.right_sibling_or_one_more_page_num;
            file_read_page(neighbor.right_sibling_or_one_more_page_num, &temp);
            temp.parent_or_next_free_page_num = neighbor_pagenum;
            file_write_page(neighbor.right_sibling_or_one_more_page_num, &temp);
            neighbor.num_of_keys++;
        }
    }
    // If current page has a neighbor to the left,
    else { // neighbor | (k_prime) | current
        current_index = k_prime_index;
        if(!current.is_leaf) {
            neighbor.childs.entries[1].key = k_prime;
            neighbor.childs.entries[1].pagenum = current.right_sibling_or_one_more_page_num;
            file_read_page(neighbor.childs.entries[1].pagenum, &temp);
            temp.parent_or_next_free_page_num = neighbor_pagenum;
            file_write_page(neighbor.childs.entries[1].pagenum, &temp);
            neighbor.num_of_keys++;
        }
    }

    file_write_page(neighbor_pagenum, &neighbor);
    
    return db_remove_entry_from_internal(current_pagenum);
}


/* current page has 0 key, neighbor page has some keys > 1.
 *
 */
int db_redistribute_pages(pagenum_t current_pagenum, pagenum_t neighbor_pagenum,
        int neighbor_index, int k_prime_index, int64_t k_prime) {
    pagenum_t parent_pagenum;
    page_t current, neighbor, parent, temp_page;
    int split_point, i, j, temp;
    
    // printf("db_redistribute_pages\n");

    file_read_page(current_pagenum, &current);
    file_read_page(neighbor_pagenum, &neighbor);
    parent_pagenum = current.parent_or_next_free_page_num;
    file_read_page(parent_pagenum, &parent);

    split_point = cut(neighbor.num_of_keys);
    
    // If current page is leftmost page,
    // pull the half of the neighbor's childs
    // from the neighbor's left side to current's right end.
    if(neighbor_index == -2) { // current | (k_prime) | neighbor
        /* Pull only one.
        printf("redistribute_nonleaf\n");
        current.childs.entries[0].key = k_prime;
        current.childs.entries[0].pagenum = neighbor.right_sibling_or_one_more_page_num;
        file_read_page(current.childs.entries[0].pagenum, &temp_page);
        temp_page.parent_or_next_free_page_num = current_pagenum;
        file_write_page(current.childs.entries[0].pagenum, &temp_page);
        current.num_of_keys++;
        
        // Set parent's key appropriately.
        parent.childs.entries[k_prime_index].key = neighbor.childs.entries[0].key;

        temp = neighbor.num_of_keys;
        neighbor.num_of_keys = 0;
        neighbor.right_sibling_or_one_more_page_num = neighbor.childs.entries[0].pagenum;
        for(i = 1, j = 0; i < temp; i++, j++) {
            neighbor.childs.entries[j] = neighbor.childs.entries[i];
            neighbor.num_of_keys++;
        }
        // Set needless space to 0 for tidiness.
        memset(&neighbor.childs.entries[neighbor.num_of_keys], 0, (INTERNAL_ORDER-1-neighbor.num_of_keys)*ENTRY_SIZE);
        */

        current.childs.entries[0].key = k_prime;
        current.childs.entries[0].pagenum = neighbor.right_sibling_or_one_more_page_num;
        file_read_page(current.childs.entries[0].pagenum, &temp_page);
        temp_page.parent_or_next_free_page_num = current_pagenum;
        file_write_page(current.childs.entries[0].pagenum, &temp_page);
        current.num_of_keys++;
        for(i = 1; i < split_point; i++) {
            current.childs.entries[i] = neighbor.childs.entries[i - 1];
            file_read_page(current.childs.entries[i].pagenum, &temp_page);
            temp_page.parent_or_next_free_page_num = current_pagenum;
            file_write_page(current.childs.entries[i].pagenum, &temp_page);
            current.num_of_keys++;
        }

        // Set parent's key appropriately.
        parent.childs.entries[k_prime_index].key = neighbor.childs.entries[split_point - 1].key;

        // Adjust neighbor.
        temp = neighbor.num_of_keys;
        neighbor.num_of_keys = 0;
        neighbor.right_sibling_or_one_more_page_num = neighbor.childs.entries[split_point - 1].pagenum;
        for(i = split_point, j = 0; i < temp; i++, j++) {
            neighbor.childs.entries[j] = neighbor.childs.entries[i];
            neighbor.num_of_keys++;
        }

        // Set needless space to 0 for tidiness.
        memset(&neighbor.childs.entries[neighbor.num_of_keys], 0, (INTERNAL_ORDER-1-neighbor.num_of_keys)*ENTRY_SIZE);
    }

    // If current page has a neighbor to the left,
    // pull the half of the neighbor's childs
    // from the neighbor's right side to current's left end.
    else { // neighbor | (k_prime) | current
    //printf("fuck!!\n");exit(1);

        // Copy entries from neighbor to current.
        temp = neighbor.num_of_keys;
        current.childs.entries[temp - 1 - split_point].key = k_prime;
        current.childs.entries[temp - 1 - split_point].pagenum = current.right_sibling_or_one_more_page_num;
        current.num_of_keys++;
        current.right_sibling_or_one_more_page_num = neighbor.childs.entries[split_point].pagenum;
        file_read_page(current.right_sibling_or_one_more_page_num, &temp_page);
        temp_page.parent_or_next_free_page_num = current_pagenum;
        file_write_page(current.right_sibling_or_one_more_page_num, &temp_page);
        
        for(i = split_point + 1, j = 0; i < temp; i++, j++) {
            current.childs.entries[j] = neighbor.childs.entries[i];
            file_read_page(current.childs.entries[j].pagenum, &temp_page);
            temp_page.parent_or_next_free_page_num = current_pagenum;
            file_write_page(current.childs.entries[j].pagenum, &temp_page);
            current.num_of_keys++;
            neighbor.num_of_keys--;
        }
        neighbor.num_of_keys--;

        // Set parent's key appropriately.
        parent.childs.entries[k_prime_index].key = neighbor.childs.entries[split_point].key;

        // Set needless space to 0 for tidiness.
        memset(&neighbor.childs.entries[neighbor.num_of_keys], 0, (INTERNAL_ORDER-1-neighbor.num_of_keys)*ENTRY_SIZE);
    }

    file_write_page(current_pagenum, &current);
    file_write_page(neighbor_pagenum, &neighbor);
    file_write_page(parent_pagenum, &parent);

    // Add error checking.

    return SUCCESS;
}

/*
 *
 */
int db_delete_entry(pagenum_t pagenum) {
    pagenum_t parent_pagenum, neighbor_pagenum;
    page_t current, parent, neighbor;
    int64_t k_prime;
    int neighbor_index, k_prime_index;

    // printf("db_delte_entry : %ldpage\n", pagenum);

    file_read_page(pagenum, &current);

    // remove entry from internal page.
    if(current.is_leaf) {
        return db_remove_entry_from_internal(pagenum);
    }

    if(pagenum == get_root_page_num()) {
        return db_adjust_root();
    }
    
    // Find neighbor page.
    neighbor_index = db_get_neighbor_index(pagenum);
    if(neighbor_index == -3) return -2; // error.

    parent_pagenum = current.parent_or_next_free_page_num;
    file_read_page(parent_pagenum, &parent);
    if(neighbor_index == -2) { // Current page is leftmost child of parent page.
        k_prime_index = 0;
        neighbor_pagenum = parent.childs.entries[0].pagenum;
    }
    else if(neighbor_index == -1) { // Left neighbor page is leftmost child.
        k_prime_index = 0;
        neighbor_pagenum = parent.right_sibling_or_one_more_page_num;
    }
    else { // Index of left neighbor page >= 0.
        k_prime_index = neighbor_index + 1;
        neighbor_pagenum = parent.childs.entries[neighbor_index].pagenum;
    }
    k_prime = parent.childs.entries[k_prime_index].key;

    // Delay merge. If merge isn't necessary, just excute redistribution.
    file_read_page(neighbor_pagenum, &neighbor);
    if(neighbor.num_of_keys <= 1) { // If merge is necessary,
        // current page has 0 key, and neighbor page has 1 key.
        return db_merge_pages(pagenum, neighbor_pagenum, neighbor_index, k_prime_index, k_prime);
    }
    else { // If merge isn't necessary,
        // current page has 0 key, and neighbor page has some keys > 1.
        return db_redistribute_pages(pagenum, neighbor_pagenum, neighbor_index, k_prime_index, k_prime);
    }
}

/*
 *
 */
int db_delete_record(pagenum_t leaf_pagenum, int64_t key) {
    page_t leaf;

    // printf("db_delte_record\n");

    // Remove record from leaf page.
    db_remove_record_from_leaf(leaf_pagenum, key);

    // Deletion from the root.
    if(leaf_pagenum == get_root_page_num()) {
        return db_adjust_root();
    }

    // Deletion from a page below root.
    file_read_page(leaf_pagenum, &leaf);
    // Delayed merge. If there are at least one key, don't merge.
    if(leaf.num_of_keys != 0) {
        return SUCCESS;
    }
    else {
        return db_delete_entry(leaf_pagenum);
    }
}

/*
 *
 */
int db_delete(int64_t key) {
    pagenum_t key_leaf_pagenum;
    record temp;
    int find_result;

    // printf("db_delte_start : %ld\n", key);
    
    if(opened == 0) { // Table is not opened.
        printf("Please open table first.\n");
        return PAGE_NOT_OPENED;
    }

    key_leaf_pagenum = db_find_leaf(key);
    find_result = db_find(key, temp.value);
    if(key_leaf_pagenum != 0 && find_result == 0) {
        return db_delete_record(key_leaf_pagenum, key);
    }

    return DELETE_NOT_EXISTS; // Not exists.
}