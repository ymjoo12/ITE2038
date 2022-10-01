#ifndef DB_TRX_H__
#define DB_TRX_H__

/// Includes
#include "trx_type.h"

#include <stdint.h>
#include <pthread.h>
#include <iostream>


/// Transaction Manager
class TransactionManager
{
public:
    // Fields
    // Transaction table
    pthread_mutex_t trx_manager_latch;
    trx_table_t trx_table;
    int next_trx_id;

    // Lock table
    pthread_mutex_t lock_manager_latch;
    lock_table_t lock_table;

    // Transaction functions (protected by trx manager latch)
    bool is_active_trx(int trx_id);
    trx_t* get_trx(int trx_id);
    int remove_trx(int trx_id);
    bool is_deadlock(int trx_id);

    // Lock functions (private)
    bool is_empty_entry(page_key_t page);
    lock_t* create_lock(page_key_t page, int record_id, int trx_id, int lock_mode);
    lock_t* compress_lock(page_key_t page, int record_id, int trx_id, int lock_mode);
    int remove_lock(lock_t* lock_obj);
    int broadcast_lock(lock_t* lock_obj);

public:
    // Constructor
    TransactionManager();

    // Initializers
    int init();

    // Member functions
    int alloc_trx();
    int release_all_locks(int trx_id);
    int abort_trx(int trx_id);
    int add_undo_log(int trx_id, undo_log_t log);
    
    // Functions protected by lock manager latch
    int acquire_lock(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int old_trx_id, int lock_mode);
    int commit_trx(int trx_id);
};


/// APIs for Transaction manager
namespace TRX
{
    // Global transaction manager
    extern TransactionManager trx_manager;

    // Initialize transaction manager
    int init_trx_manager();

    // Acquire lock
    int acquire_lock(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int old_trx_id, int lock_mode);

    // Add undo log
    int save_log(int trx_id, int64_t table_id, pagenum_t page_id, int64_t key, std::string old_value, int old_trx_id);
}

// Allocate transaction
int trx_begin(void);

// Commit transaction
int trx_commit(int trx_id);

// Abort transaction
int trx_abort(int trx_id);


#endif /* DB_TRX_H__ */
