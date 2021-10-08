#include "relational_set_operator.h"

/*** Wrapper functions ***/

int rso_init_db(int buf_num) {
    return index_init_db(buf_num);
}

int rso_shutdown_db(void) {
    return index_shutdown_db();
}

int rso_open_table(char* pathname) {
    return index_open_table(pathname);
}

int rso_close_table(int table_id) {
    return index_close_table(table_id);
}

int rso_insert(int table_id, int64_t key, char* value) {
    return index_insert(table_id, key, value);
}

int rso_find(int table_id, int64_t key, char* ret_val) {
    return index_find(table_id, key, ret_val);
}

int rso_delete(int table_id, int64_t key) {
    return index_delete(table_id, key);
}

/*** Utility ***/

/*
 *
 */
int advance(int *idx, buffer_block_t **cur_buf, page_t **cur_page) {
    pagenum_t cur_pagenum;
    int table_id = (*cur_buf)->table_id;

    if(*idx < (*cur_page)->num_of_keys - 1) {
        (*idx)++;
    }
    else {
        cur_pagenum = (*cur_page)->right_sibling_or_one_more_page_num;
        if(cur_pagenum == 0) { // last page of table.
            return -1;
        }
        index_release_page(*cur_buf, false);
        index_request_page(table_id, cur_pagenum, cur_buf);
        *cur_page = &((*cur_buf)->frame);
        *idx = 0;
    }
    
    return 0;
}

/*** Relational set operator ***/

/*
 *
 */
int rso_join_table(int table_id_1, int table_id_2, char* pathname) {
    pagenum_t cur_pagenum_1, cur_pagenum_2;
    buffer_block_t *hd_buf_1, *hd_buf_2, *cur_buf_1, *cur_buf_2;
    header_page_t *hd_page_1, *hd_page_2;
    page_t *cur_page_1, *cur_page_2;
    int fd, idx1, idx2, result = 0;
    bool repeat = true;

    if((fd = open(pathname, O_WRONLY | O_CREAT | O_TRUNC, 0644)) < 0) {
        return -1; //
    }

    result = index_request_page(table_id_1, HEADER_PAGE_NUM, &hd_buf_1);
    if(result < 0) {
        close(fd);
        return -2; // The table has table_id_1 does not exist. / Or other error.
    }
    result = index_request_page(table_id_2, HEADER_PAGE_NUM, &hd_buf_2);
    if(result < 0) {
        index_release_page(hd_buf_1, false);
        close(fd);
        return -2; // The table has table_id_2 does not exist. / Or other error.
    }
    hd_page_1 = (header_page_t*)&(hd_buf_1->frame);
    hd_page_2 = (header_page_t*)&(hd_buf_2->frame);

    cur_buf_1 = find_leftmost_leaf(hd_buf_1);
    if(cur_buf_1 == NULL) { // Table 1 is empty.
        index_release_page(hd_buf_1, false);
        index_release_page(hd_buf_2, false);
        close(fd);
        return -3;
    }
    cur_buf_2 = find_leftmost_leaf(hd_buf_2);
    if(cur_buf_2 == NULL) { // Table 2 is empty.
        index_release_page(hd_buf_1, false);
        index_release_page(hd_buf_2, false);
        index_release_page(cur_buf_1, false);
        close(fd);
        return -3;
    }
    cur_page_1 = &(cur_buf_1->frame);
    cur_page_2 = &(cur_buf_2->frame);
    idx1 = 0;
    idx2 = 0;

    while(repeat) {
        // advance 1.
        while(cur_page_1->childs.records[idx1].key < cur_page_2->childs.records[idx2].key) {
            if(advance(&idx1, &cur_buf_1, &cur_page_1) < 0) {
                repeat = false;
                break;
            }
        }

        // advance 2.
        while(cur_page_1->childs.records[idx1].key > cur_page_2->childs.records[idx2].key) {
            if(advance(&idx2, &cur_buf_2, &cur_page_2) < 0) {
                repeat = false;
                break;
            }
        }

        // .
        if(cur_page_1->childs.records[idx1].key == cur_page_2->childs.records[idx2].key) {
            dprintf(fd, "%ld,%s,%ld,%s\n", cur_page_1->childs.records[idx1].key, cur_page_1->childs.records[idx1].value,
                cur_page_2->childs.records[idx2].key, cur_page_2->childs.records[idx2].value);

            // advance 1.
            if(advance(&idx1, &cur_buf_1, &cur_page_1) < 0) {
                break;
            }
            // advance 2.
            if(advance(&idx2, &cur_buf_2, &cur_page_2) < 0) {
                break;
            }
        }        
    }
    fsync(fd);

    index_release_page(hd_buf_1, false);
    index_release_page(hd_buf_2, false);
    index_release_page(cur_buf_1, false);
    index_release_page(cur_buf_2, false);

    close(fd);

    return 0;
}