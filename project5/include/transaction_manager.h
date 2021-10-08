#ifndef __TRANSACTION_MANAGER_H__
#define __TRANSACTION_MANAGER_H__

#include "buffer_manager.h"
#include <list>
#include <unordered_map>
#include <algorithm>

#define CONFLICT -1
#define DEADLOCK -2

#define ABORTED -4

using namespace std;

enum trx_state {IDLE, RUNNING, WAITING};
enum lock_mode {SHARED, EXCLUSIVE};

typedef struct trx_t trx_t;
typedef struct trx_mgr_t trx_mgr_t;
typedef struct undo_log_t undo_log_t;

typedef struct lock_t lock_t;
typedef struct lock_hash_bkt_t lock_hash_bkt_t;
typedef struct lock_mgr_t lock_mgr_t;

// Transaction structure.
struct trx_t {
    int trx_id; // Trx id.
    enum trx_state state; // Trx state.

    list<lock_t*> acquired_locks; // Lock node list which this trx acquire. 
    lock_t* acquiring_lock; // A Lock node which this trx is acquiring. (not yet acquired)
    list<lock_t*> wait_locks; // Lock node list which this trx wait for.

    // Latch and cond var to wait & wakeup.
    pthread_mutex_t trx_latch;
    pthread_cond_t trx_cond;

    list<undo_log_t*> undo_log_list; // Undo log list.
};

/*
// Transaction system manager structure.
struct trx_mgr_t {
    unordered_map<int, trx_t> trx_table; // Mapping trx id to trx structure.
    int next_trx_id; // Next trx id.
    pthread_mutex_t trx_mgr_latch; // Trx manager latch.
};
*/

// Undo log structure.
struct undo_log_t {
    int table_id;
    pagenum_t pagenum;
    int64_t key;
    char old_value[SIZE_RECORD_VALUE];
};

// Lock node structure.
struct lock_t {
    int table_id;
    pagenum_t pagenum;
    int64_t key;

    trx_t* trx; // The trx has this lock.
    //list<trx_t*>* waiting_trxs; // The trxs are waiting for this lock.

    bool acquired;
    enum lock_mode mode;

    lock_t* prev;
    lock_t* next;
};

// Lock hash bucket structure.
struct lock_hash_bkt_t {
    lock_t* head;
    lock_t* tail;
};

/*
// Lock node manager structure.
struct lock_mgr_t {
    unordered_map<pagenum_t, lock_hash_bkt_t> lock_table;
    pthread_mutex_t lock_mgr_latch;
};
*/

// API using lower layer.
int trx_init_db(int buf_num);
int trx_shutdown_db(void);

// Transaction API.
int trx_begin_trx(void);
int trx_end_trx(int tid);

// Other functions.
void trx_acquire_trx_mgr_latch(void);
void trx_release_trx_mgr_latch(void);
trx_t* trx_get_trx(int trx_id);
int trx_acquire_record_lock(int table_id, pagenum_t pagenum, int64_t key, int trx_id, enum lock_mode mode);
int trx_check_deadlock(int checking_trx_id, int cur_trx_id);
int trx_release_record_lock();
int trx_abort_trx(int tid);

#endif // __TRANSACTION_MANAGER_H__