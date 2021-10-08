#include "transaction_manager.h"

bool db_opened2 = false;

//trx_mgr_t* trx_mgr = NULL;
unordered_map<int, trx_t> trx_table; // Mapping trx id to trx structure.
int next_trx_id; // Next trx id.
pthread_mutex_t trx_mgr_latch = PTHREAD_MUTEX_INITIALIZER; // Trx manager latch.

//lock_mgr_t* lock_mgr = NULL;
unordered_map<pagenum_t, lock_hash_bkt_t> lock_table;
pthread_mutex_t lock_mgr_latch = PTHREAD_MUTEX_INITIALIZER;


/*** API using lower layer ***/

int trx_init_db(int buf_num) {
    int result = buf_init_db(buf_num);
    if(result != 0) { return result; }

    //trx_mgr = (trx_mgr_t*)malloc(sizeof(trx_mgr_t));
    //lock_mgr = (lock_mgr_t*)malloc(sizeof(lock_mgr_t));

    //trx_mgr->trx_table.clear();
    trx_table.clear();
    next_trx_id = 1;
    pthread_mutex_init(&trx_mgr_latch, NULL);
    lock_table.clear();
    pthread_mutex_init(&lock_mgr_latch, NULL);

    db_opened2 = true;

    return 0;
}

int trx_shutdown_db(void) {
    int result = buf_shutdown_db();
    if(result != 0) { return result; }

    trx_table.clear();
    next_trx_id = 1;
    pthread_mutex_init(&trx_mgr_latch, NULL);
    lock_table.clear();
    pthread_mutex_init(&lock_mgr_latch, NULL);

    db_opened2 = false;

    //free(trx_mgr);
    //free(lock_mgr);

    return 0;
}


/*** Transaction API ***/

int trx_begin_trx(void) {
    int trx_id = 0;

    if(!db_opened2) { return 0; } // DB is not initialized.

    // 1. Acquire the transaction system latch.
    pthread_mutex_lock(&trx_mgr_latch);

    if(trx_table.size() == __INT_MAX__) { return 0; } // Table size over.

    // 2. Get a new transaction id.
    while(true) {
        while(trx_table.find(next_trx_id) != trx_table.end()) {
            next_trx_id++;
        }
        if(next_trx_id < 0) { next_trx_id = 1; }
        else { break; }
    }
    trx_id = next_trx_id;
    next_trx_id++;

    // 3. Insert a new transaction structure in the transaction table.
    trx_table.insert({trx_id, trx_t{trx_id, IDLE, list<lock_t*>(0), NULL, list<lock_t*>(0),
        PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER, list<undo_log_t*>(0)}});
    
    // 4. Release the transaction system latch.
    pthread_mutex_unlock(&(trx_mgr_latch));

    // 5. Return the new transaction id.
    return trx_id;
}

int trx_end_trx(int tid) {
    lock_t *lock_temp;

    if(!db_opened2) { return 0; } // DB is not initialized.

    // 1. Acquire the lock table latch.
    pthread_mutex_lock(&lock_mgr_latch);

    // 4. Acquire the transaction system latch.
    pthread_mutex_lock(&(trx_mgr_latch));

    if(trx_table.find(tid) == trx_table.end()) {
        pthread_mutex_unlock(&lock_mgr_latch);
        pthread_mutex_unlock(&(trx_mgr_latch));
        return -5;
    }

    // 2. Release all acquired locks and wake up threads who wait on this transaction.(lock node).
    for(lock_t *lock : (trx_table.find(tid)->second.acquired_locks)) {
        lock_temp = lock->next;
        while(lock_temp != NULL) {
            trx_t* trx;
            trx = lock_temp->trx;
            pthread_mutex_lock(&(trx->trx_latch));
            if(find(trx->wait_locks.begin(), trx->wait_locks.end(), lock) !=
                trx->wait_locks.end()) { // If the trx has current lock,
                trx->wait_locks.remove(lock);
                pthread_cond_broadcast(&(trx->trx_cond));
            }
            pthread_mutex_unlock(&(trx->trx_latch));
            lock_temp = lock_temp->next;
        }
/*
        for(trx_t* trx : *(lock->waiting_trxs)) {
            pthread_mutex_lock(&(trx->trx_latch));
            trx->wait_locks.remove(lock);
            pthread_cond_signal(&(trx->trx_cond));
            pthread_mutex_unlock(&(trx->trx_latch));
        }*/

        // Release locks.
        if(lock->prev != NULL) {
            lock->prev->next = lock->next;
        } 
        if(lock->next != NULL) {
            lock->next->prev = lock->prev;
        }
        //delete lock->waiting_trxs;
        delete lock;
    }

    // 3. Release the lock table latch.
    pthread_mutex_unlock(&lock_mgr_latch);

    // 5. Delete the transaction from the transaction table.
    if(!trx_table.erase(tid)) { // Fail to end. Transaction not found.
        pthread_mutex_unlock(&(trx_mgr_latch));
        return 0;
    }

    // 6. Release the transaction system latch.
    pthread_mutex_unlock(&(trx_mgr_latch));

    // 7. Return the transaction id.
    return tid;
}

/*** Other functions ***/

void trx_acquire_trx_mgr_latch(void) {
    pthread_mutex_lock(&(trx_mgr_latch));
}

void trx_release_trx_mgr_latch(void) {
    pthread_mutex_unlock(&(trx_mgr_latch));
}

trx_t* trx_get_trx(int trx_id) {
    if(trx_table.count(trx_id)) {
        return &(trx_table.find(trx_id)->second);
    }

    return NULL;
}

int trx_acquire_record_lock(int table_id, pagenum_t pagenum, int64_t key, int trx_id, enum lock_mode mode) {
    trx_t *cur_trx, *trx_temp;
    lock_t *cur_lock, *lock_temp;
    lock_hash_bkt_t *lock_bkt = NULL;
    list<lock_t*> lock_nodes;
    int checking_trx_id;
    
    // Acquire the lock table latch.
    pthread_mutex_lock(&lock_mgr_latch);

    // Get trx latch.
    pthread_mutex_lock(&(trx_mgr_latch));
    cur_trx = &(trx_table.find(trx_id)->second);
    pthread_mutex_lock(&(cur_trx->trx_latch));
    pthread_mutex_unlock(&(trx_mgr_latch));

    /* 0. If this is re-trying, */
    cur_lock = cur_trx->acquiring_lock;
    if(cur_lock != NULL) {
        // A wait_lock will be released, and it was only wait_lock for this.
        // The wait_lock was deleted in the list, at releasing the lock.
        if(cur_trx->wait_locks.empty()) {
            cur_lock->acquired = true;
            cur_trx->acquired_locks.push_back(cur_lock);
            cur_trx->acquiring_lock = NULL;

            pthread_mutex_unlock(&(cur_trx->trx_latch));
            pthread_mutex_unlock(&lock_mgr_latch);
            return SUCCESS;
        }
        // A wait_lock will be released (because the lock waked up this),
        // but there are other wait_locks.
        else { // It means, CONFLICT again.
            pthread_mutex_unlock(&(cur_trx->trx_latch));
            pthread_mutex_unlock(&lock_mgr_latch);
            return CONFLICT;
        }
    }

    /* 1. If the trx has lock of given key already, */
    cur_lock = NULL;
    for(lock_t *lock : (cur_trx->acquired_locks)) {
        if(lock->table_id == table_id && lock->key == key) {
            cur_lock = lock;
            break;
        } 
    }
    if(cur_lock != NULL) { // It must be acquired.
        if(cur_lock->mode == EXCLUSIVE ||
        (cur_lock->mode == SHARED && mode == SHARED)) {
            pthread_mutex_unlock(&(cur_trx->trx_latch));
            pthread_mutex_unlock(&lock_mgr_latch);
            return SUCCESS;
        }
        else { // original lock : SHARED & this lock : EXCLUSIVE
            // The original lock will be not included wait_locks below.
        }
    }

    // Make lock structure.
    cur_lock = new lock_t;

    //lock_t lock_tmp2 = {table_id, pagenum, key, cur_trx, list<trx_t*>(0), false, mode, NULL, NULL};
    //memcpy(cur_lock, &lock_tmp2, sizeof(lock_t));
    //table_id = table_id

    cur_lock->table_id = table_id;
    cur_lock->pagenum = pagenum;
    cur_lock->key = key;
    cur_lock->trx = cur_trx;
    //cur_lock->waiting_trxs = new list<trx_t*>(0);
    //(*(cur_lock->waiting_trxs)).clear();
    cur_lock->mode = mode;
    cur_lock->prev = NULL;
    cur_lock->next = NULL;

    // Check the linked list.

    /* 2. If lock bucket does not exist, */
    if(!(lock_table.count(pagenum))) {
        cur_lock->acquired = true;
        lock_table.insert({pagenum, lock_hash_bkt_t{cur_lock, cur_lock}});
        cur_trx->acquired_locks.push_back(cur_lock);

        pthread_mutex_unlock(&(cur_trx->trx_latch));
        pthread_mutex_unlock(&lock_mgr_latch);

        return SUCCESS;
    }

    lock_bkt = &(lock_table.find(pagenum)->second); // head
    lock_temp = lock_bkt->head;
    // Find lock nodes of given key in the list.
    while(lock_temp != NULL) {
        if(lock_temp->table_id == table_id && lock_temp->key == key) {
            pthread_mutex_lock(&(trx_mgr_latch));
            pthread_mutex_lock(&(lock_temp->trx->trx_latch));
            pthread_mutex_unlock(&(trx_mgr_latch));
            if(lock_temp->trx->trx_id == trx_id) {
                // original lock : SHARED & this lock : EXCLUSIVE
                // The original lock will be not included wait_locks.
                // Don't care about original shared lock.
            }
            else {
                lock_nodes.push_back(lock_temp);
            }
            pthread_mutex_unlock(&(lock_temp->trx->trx_latch));
        }
        lock_temp = lock_temp->next;
    }
    /* 3. If lock does not exist, */
    if(lock_nodes.empty()) {
        cur_lock->acquired = true;
        lock_bkt->tail->next = cur_lock;
        cur_lock->prev = lock_bkt->tail;
        lock_bkt->tail = cur_lock;
        cur_trx->acquired_locks.push_back(cur_lock);

        pthread_mutex_unlock(&(cur_trx->trx_latch));
        pthread_mutex_unlock(&lock_mgr_latch);

        return SUCCESS;
    }

    lock_temp = lock_nodes.back();

    /* 4. If other lock exists, and this is SHARED, */
    if(mode == SHARED) {
        // S(?, acquired) <- S(this)
        if(lock_temp->mode == SHARED && lock_temp->acquired) {
            cur_lock->acquired = true;
            lock_bkt->tail->next = cur_lock;
            cur_lock->prev = lock_bkt->tail;
            lock_bkt->tail = cur_lock;
            cur_trx->acquired_locks.push_back(cur_lock);

            pthread_mutex_unlock(&(cur_trx->trx_latch));
            pthread_mutex_unlock(&lock_mgr_latch);

            return SUCCESS;
        }
    }
    /* 5. If other lock exists, and this is EXCLUSIVE, */
    else { // mode == EXCLUSIVE
        // ... <- S(?) x N <- X(this)
        // Wait for last shared nodes. Check DEADLOCK.
        if(lock_temp->mode == SHARED) {
            // Find last shared nodes.
            list<lock_t*> lock_nodes_temp;
            for(lock_t *lock : lock_nodes) {
                if(lock->mode == EXCLUSIVE) {
                    lock_nodes_temp.clear();
                }
                else {
                    lock_nodes_temp.push_back(lock);
                }
            }

            // Check DEADLOCK.
            for(lock_t *lock : lock_nodes_temp) {
                pthread_mutex_lock(&(trx_mgr_latch));
                trx_temp = lock_temp->trx;
                pthread_mutex_lock(&(trx_temp->trx_latch));
                pthread_mutex_unlock(&(trx_mgr_latch));

                checking_trx_id = trx_temp->trx_id;
                pthread_mutex_lock(&trx_mgr_latch);
                pthread_mutex_unlock(&(trx_temp->trx_latch));
                pthread_mutex_unlock(&(cur_trx->trx_latch));
                if(trx_check_deadlock(checking_trx_id, trx_id) == DEADLOCK) {
                    pthread_mutex_unlock(&trx_mgr_latch);
                    pthread_mutex_unlock(&lock_mgr_latch);
                    return DEADLOCK;
                }
            }

            // else : CONFLICT
            cur_lock->acquired = false;
            lock_bkt->tail->next = cur_lock;
            cur_lock->prev = lock_bkt->tail;
            lock_bkt->tail = cur_lock;
            cur_trx->acquiring_lock = cur_lock;
            for(lock_t *lock : lock_nodes_temp) {
                cur_trx->wait_locks.push_back(lock);
                //(*(lock->waiting_trxs)).push_back(cur_trx);
            }

            pthread_mutex_unlock(&(trx_temp->trx_latch));
            pthread_mutex_unlock(&(cur_trx->trx_latch));
            pthread_mutex_unlock(&lock_mgr_latch);
            return CONFLICT;
        }
    }

    // X(?) <- S/X(this) or
    // X(?) <- ... <- S(not acquired) <- S(this)

    // Find the X.
    lock_temp = NULL;
    for(lock_t *lock : lock_nodes) {
        if(lock->mode == EXCLUSIVE) {
            lock_temp = lock;
        }
    }
    if(lock_temp == NULL) {
        // invalid linked list.
        pthread_mutex_unlock(&(cur_trx->trx_latch));
        pthread_mutex_unlock(&lock_mgr_latch);
        return -10;
    }
    pthread_mutex_lock(&(trx_mgr_latch));
    trx_temp = lock_temp->trx;
    pthread_mutex_lock(&(trx_temp->trx_latch));
    pthread_mutex_unlock(&(trx_mgr_latch));

    // Check DEADLOCK.
    checking_trx_id = trx_temp->trx_id;
    pthread_mutex_lock(&trx_mgr_latch);
    pthread_mutex_unlock(&(trx_temp->trx_latch));
    pthread_mutex_unlock(&(cur_trx->trx_latch));
    if(trx_check_deadlock(checking_trx_id, trx_id) == DEADLOCK) {
        pthread_mutex_unlock(&trx_mgr_latch);
        pthread_mutex_unlock(&lock_mgr_latch);
        return DEADLOCK;
    }
    pthread_mutex_unlock(&trx_mgr_latch);

    // else : CONFLICT
    cur_lock->acquired = false;
    lock_bkt->tail->next = cur_lock;
    cur_lock->prev = lock_bkt->tail;
    lock_bkt->tail = cur_lock;

    cur_trx->acquiring_lock = cur_lock;
    cur_trx->wait_locks.push_back(lock_temp);
    
    //(*(lock_temp->waiting_trxs)).push_back(cur_trx);

    pthread_mutex_unlock(&(trx_temp->trx_latch));
    pthread_mutex_unlock(&(cur_trx->trx_latch));
    pthread_mutex_unlock(&lock_mgr_latch);
    return CONFLICT;
    

    //return SUCCESS;
}

int trx_check_deadlock(int checking_trx_id, int cur_trx_id) {
    trx_t *checking_trx, *trx_temp;
    int result, checking_trx_id_2;

    checking_trx = &(trx_table.find(checking_trx_id)->second);
    pthread_mutex_lock(&(checking_trx->trx_latch));

    if(checking_trx->state == WAITING) {
        if(checking_trx_id == cur_trx_id) { // DEADLOCK
            pthread_mutex_unlock(&(checking_trx->trx_latch));
            return DEADLOCK;
        }
        for(lock_t *lock : checking_trx->wait_locks) {
            trx_temp = lock->trx;
            pthread_mutex_lock(&(trx_temp->trx_latch));
            checking_trx_id_2 = trx_temp->trx_id;
            pthread_mutex_unlock(&(trx_temp->trx_latch));
            if(checking_trx_id_2 == cur_trx_id) { // DEADLOCK
                pthread_mutex_unlock(&(checking_trx->trx_latch));
                return DEADLOCK;
            }
            if(trx_check_deadlock(checking_trx_id_2, cur_trx_id) == DEADLOCK) { // DEADLOCK
                pthread_mutex_unlock(&(checking_trx->trx_latch));
                return DEADLOCK;
            }
        }
    }
    pthread_mutex_unlock(&(checking_trx->trx_latch));

    return SUCCESS; // Not DEADLOCK.
}

int trx_release_record_lock() {

    return 0;
}

int trx_abort_trx(int tid) {
    trx_t* cur_trx;
    undo_log_t* undo_log;
    buf_block_t* cur_buf;
    page_t* cur_page;
    int i;

    // Get trx latch.
    pthread_mutex_lock(&trx_mgr_latch);
    cur_trx = &(trx_table.find(tid)->second);
    pthread_mutex_lock(&(cur_trx->trx_latch));
    pthread_mutex_unlock(&trx_mgr_latch);

    // 1. Iterate reversely undo logs of the transaction.
    while(!cur_trx->undo_log_list.empty()) {
        undo_log = cur_trx->undo_log_list.back();
        
        while(true) {
            // 1-1. Acquire the buffer pool latch.
            buf_acquire_buf_pool_latch();
            // 1-2. Find a leaf page containing the given record(key).
            buf_request_page(undo_log->table_id, undo_log->pagenum, &cur_buf);
            // 1-3. Try to acquire the buffer page latch.
            if(pthread_mutex_trylock(&(cur_buf->buf_page_latch)) == 0) { // Success.
                break;
            }
            // 1-3-1. If fail to acquire, release the buffer pool latch and go to 1-1.
            buf_release_page(cur_buf, false);
            buf_release_buf_pool_latch();
            continue;
        }
        // 1-4. Release the buffer pool latch.
        buf_release_buf_pool_latch();

        // 1-5. Rollback one undo log.
        cur_page = &(cur_buf->frame);
        // Find the record.
        for(i = 0; i < cur_page->num_of_keys; i++) {
            if(cur_page->childs.records[i].key == undo_log->key) break;
        }
        if(i == cur_page->num_of_keys) {
            // Record is not found. It is error.
            // Transactions of current implement don't include insert/delete.
            // So, The record must be found.
            pthread_mutex_unlock(&(cur_buf->buf_page_latch));
            buf_release_page(cur_buf, false);
            pthread_mutex_unlock(&(cur_trx->trx_latch));
            return -5;
        }
        memcpy(cur_page->childs.records[i].value, undo_log->old_value, SIZE_RECORD_VALUE);
        cur_trx->undo_log_list.pop_back();

        // 1-6. Release the buffer page latch.
        pthread_mutex_unlock(&(cur_buf->buf_page_latch));
        buf_release_page(cur_buf, true);
    }
    pthread_mutex_unlock(&(cur_trx->trx_latch));

    // 2. End transaction.
    return trx_end_trx(tid);
}