#include "trx.h"


/// Transaction Manager
// Constructor
TransactionManager::TransactionManager() 
    : next_trx_id(1)
{}


/// Transaction functions
// Check if a transaction is active
bool TransactionManager::is_active_trx(int trx_id)
{
    int flag;

    // Acquire the trx manager latch
    flag = pthread_mutex_lock(&this->trx_manager_latch);
    if (flag != 0) return false;

    // Check if the transaction id is valid
    if (trx_table.find(trx_id) == trx_table.end()) {
        // Release the trx manager latch and return false
        pthread_mutex_unlock(&this->trx_manager_latch);
        return false;
    }
    
    // Release the trx manager latch and return true
    pthread_mutex_unlock(&this->trx_manager_latch);
    return true;
}

trx_t* TransactionManager::get_trx(int trx_id)
{
    int flag;
    trx_t* trx_obj;

    // Acquire the trx manager latch
    flag = pthread_mutex_lock(&this->trx_manager_latch);
    if (flag != 0) return NULL;

    // Check if the transaction id is valid
    if (trx_table.find(trx_id) == trx_table.end()) {
        // Release the trx manager latch and return NULL
        pthread_mutex_unlock(&this->trx_manager_latch);
        return NULL;
    }

    // Get the transaction object
    trx_obj = &trx_table[trx_id];

    // Release the trx manager latch and return the trx
    pthread_mutex_unlock(&this->trx_manager_latch);
    return trx_obj;
}

// Deallocate a transaction
int TransactionManager::remove_trx(int trx_id) 
{
    int flag;

    // Acquire the trx manager latch
    flag = pthread_mutex_lock(&this->trx_manager_latch);
    if (flag != 0) return 0;

    // Check if the transaction id is valid
    if (this->trx_table.find(trx_id) == this->trx_table.end()) {
        // Release the trx manager latch and return 0
        pthread_mutex_unlock(&this->trx_manager_latch);
        return 0;
    }

    // Deallocate the transaction
    this->trx_table.erase(trx_id);

    // Release the trx manager latch
    pthread_mutex_unlock(&this->trx_manager_latch);
    return trx_id;
}

// Detect deadlock
bool TransactionManager::is_deadlock(int trx_id)
{
    int cur_id, next_id;
    trx_t *trx_obj, *cur_obj;
    std::stack<int> to_check, cur_stack;

    // Get the transaction object
    trx_obj = this->get_trx(trx_id);
    if (trx_obj == NULL) return false;

    // Get the waiting stack
    to_check = trx_obj->get_waiting_stack();

    // Detect cycle in waiting-for graph
    while (!to_check.empty()) {
        // Get the current transaction id
        cur_id = to_check.top();
        to_check.pop();

        // Get the current transaction object
        cur_obj = this->get_trx(cur_id);
        if (cur_obj == NULL) continue;

        // Get the waiting stack
        cur_stack = cur_obj->get_waiting_stack();
        
        // Push other transactions to the stack
        while (!cur_stack.empty()) {
            // Get the next transaction id
            next_id = cur_stack.top();
            cur_stack.pop();

            // Get the current transaction object
            if (!this->is_active_trx(next_id)) {
                cur_obj->pop_waiting_list(next_id);
                continue;
            }

            // Check already checked transaction
            if (trx_obj->in_waiting_list(next_id)) continue;

            // Check if the current transaction is waiting for the this transaction
            if (next_id == trx_id) return true;

            // Push the next transaction id to the stack
            trx_obj->add_waiting_list(next_id);
            to_check.push(next_id);
        }
    }

    return false;
}


/// Lock functions
// Check if a lock table entry is empty
bool TransactionManager::is_empty_entry(page_key_t page)
{
    lock_table_t::iterator it = lock_table.find(page);
    
    // Check if the page is in the lock table
    if (it == lock_table.end()) return true;

    // Check if the lock table entry is empty
    if (it->second.is_empty()) return true;

    return false;
}

// Create a new lock object
lock_t* TransactionManager::create_lock(page_key_t page, int record_id, int trx_id, int lock_mode)
{
    int flag, conflict;
    lock_t* lock_obj;
    trx_t* trx_obj;

    // If the lock table entry is not exist, create a new lock table entry
    if (this->lock_table.find(page) == this->lock_table.end()) {
        this->lock_table[page] = lock_table_entry_t(page);
    }

    // Allocate a new lock structure
    lock_obj = this->lock_table[page].append_lock(record_id, trx_id, lock_mode);
    if (lock_obj == NULL) return NULL;

    printf("[INFO][%s][trx_id: %d] Allocate lock (MUST): %d, %d, %d, %d\n", __func__, trx_id, trx_id, page.second, record_id, lock_mode);
    printf("[INFO][%s][trx_id: %d] Allocate lock (REAL): %d, %d, %d, %d\n", __func__, trx_id, lock_obj->trx_id, page.second, lock_obj->record_id, lock_obj->lock_mode);

    // Get the transaction object
    trx_obj = this->get_trx(trx_id);
    if (trx_obj == NULL) {
        this->lock_table[page].remove_lock(lock_obj);
        return NULL;
    }

    // Append the lock to the transaction
    flag = trx_obj->append_lock(lock_obj);
    if (flag != 0) {
        this->lock_table[page].remove_lock(lock_obj);
        return NULL;
    }

    // Check if it acquires the lock immediately (Create wait-for graph)
    for (lock_t* cur = lock_obj; cur != NULL; cur = cur->prev) {
        
        printf("[INFO][%s][trx_id: %d] Current lock: %d, %d, %d, %d, %d\n", __func__, trx_id, cur->trx_id, page.second, cur->record_id, cur->lock_mode, cur->bitmap.test(cur->record_id));
        // Check if the new lock is conflict with the current lock
        conflict = cur->is_conflict(record_id, trx_id, lock_mode);
        printf("[INFO][%s][trx_id: %d] Conflict: %d\n", __func__, trx_id, conflict);
        if (conflict) trx_obj->add_waiting_list(cur->trx_id);
        if (conflict > 1) break;

        // If the new lock don't need to wait for any lock, set the lock as acquired
        if (cur->prev == NULL) lock_obj->set_acquired();
    }

    return lock_obj;
}

// Compress the lock
lock_t* TransactionManager::compress_lock(page_key_t page, int record_id, int trx_id, int lock_mode)
{
    int flag;
    lock_t* lock_obj;

    // Check if the lock table entry exists
    if (this->lock_table.find(page) == this->lock_table.end()) {
        return NULL;
    }

    // Check if the transaction has the lock
    lock_obj = this->lock_table[page].compress_lock(record_id, trx_id, lock_mode);
    if (lock_obj == NULL) return NULL;

    return lock_obj;
}

// Remove a lock
int TransactionManager::remove_lock(lock_t* lock_obj)
{
    int flag;
    lock_table_entry_t* entry;

    // Check if the lock is valid
    if (lock_obj == NULL) return FLAG::FAILURE;

    // Remove the lock from the lock table
    entry = lock_obj->sentinel;
    flag = entry->remove_lock(lock_obj);
    return flag;
}

// Signal all locks waiting for the lock
int TransactionManager::broadcast_lock(lock_t* lock_obj)
{
    int flag;
    lock_table_entry_t* entry;

    // Check if the lock is valid
    if (lock_obj == NULL || lock_obj->sentinel == NULL) return FLAG::FAILURE;

    // Broadcast the lock
    entry = lock_obj->sentinel;
    flag = entry->broadcast_lock(lock_obj);
    if (flag != 0) return FLAG::FAILURE;

    return FLAG::SUCCESS;
}


/// Public functions
// Initializer
int TransactionManager::init()
{
    int flag;

    // Initialize the transaction table
    this->trx_table.clear();
    flag = pthread_mutex_init(&this->trx_manager_latch, NULL);
    if (flag != 0) return FLAG::FAILURE;

    // Initialize the lock table
    this->lock_table.clear();
    flag = pthread_mutex_init(&this->lock_manager_latch, NULL);
    if (flag != 0) return FLAG::FAILURE;

    return FLAG::SUCCESS;
}

// Allocate a new transaction id
int TransactionManager::alloc_trx() 
{
    int trx_id, flag;

    
    printf("[INFO][%s] Begin\n", __func__);

    // Acquire the trx manager latch
    flag = pthread_mutex_lock(&this->trx_manager_latch);
    if (flag != 0) return 0;

    
    printf("[INFO][%s] Acquire the trx manager latch\n", __func__);

    // Allocate a new transaction id
    trx_id = this->next_trx_id++;
    trx_table[trx_id] = trx_t(trx_id);
    
    // Release the trx manager latch and return the new transaction id
    pthread_mutex_unlock(&this->trx_manager_latch);

    
    printf("[INFO][%s] Release the trx manager latch\n", __func__);

    return trx_id;
}

// Release all locks of the transaction
int TransactionManager::release_all_locks(int trx_id)
{
    int flag;
    trx_t* trx_obj;

    printf("[INFO][%s] Begin\n", __func__);

    // Get the transaction object
    trx_obj = this->get_trx(trx_id);
    if (trx_obj == NULL) return FLAG::FAILURE;

    // Release all locks of the transaction
    for (lock_t* cur = trx_obj->first; cur != NULL; cur = trx_obj->first) {
        // Signal the locks waiting for this lock to be released
        flag = this->broadcast_lock(cur);
        if (flag != 0) return FLAG::FAILURE;

        // Link the next lock to first
        trx_obj->first = cur->next_trx_lock;

        // Deallocate the lock object
        flag = this->remove_lock(cur);
        if (flag != 0) return FLAG::FAILURE;
    }

    // Reset the last lock
    trx_obj->last = NULL;

    return FLAG::SUCCESS;
}

// Abort the transaction
int TransactionManager::abort_trx(int trx_id)
{
    int flag;
    trx_t* trx_obj;

    printf("[INFO][%s] Begin\n", __func__);

    // Get the transaction object
    trx_obj = this->get_trx(trx_id);
    if (trx_obj == NULL) return FLAG::FAILURE;

    // Rollback the transaction
    flag = trx_obj->rollback();
    if (flag != 0) return FLAG::FAILURE;

    // Release all the locks held by the transaction
    flag = this->release_all_locks(trx_id);
    if (flag != 0) return FLAG::FAILURE;

    // Deallocate the transaction object
    flag = this->remove_trx(trx_id);
    if (flag == 0) return FLAG::FAILURE;

    return FLAG::SUCCESS;
}

// Add undo log
int TransactionManager::add_undo_log(int trx_id, undo_log_t log)
{
    int flag;
    trx_t* trx_obj;

    // Get the transaction object
    trx_obj = this->get_trx(trx_id);
    if (trx_obj == NULL) return FLAG::FAILURE;

    // Add the undo log to the transaction
    trx_obj->add_undo_log(log);
    return FLAG::SUCCESS;
}

// Acquire the lock (Protected by the lock_manager_latch)
int TransactionManager::acquire_lock(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int old_trx_id, int lock_mode)
{
    int flag, record_id;
    lock_t* lock_obj;
    page_key_t page;

    printf("[INFO][acquire_lock][trx_id: %d] Begin acquire lock %ld %ld %ld %d %d\n", trx_id, table_id, page_id, key, trx_id, lock_mode);

    // Acquire the lock manager latch
    flag = pthread_mutex_lock(&this->lock_manager_latch);
    if (flag != 0) return FLAG::FATAL;

    printf("[INFO][acquire_lock][trx_id: %d] Acquire lock manager latch %ld %ld %ld %d %d\n", trx_id, table_id, page_id, key, trx_id, lock_mode);

    // Get page and record information
    page = {table_id, page_id};
    // std::tie(old_trx_id, record_id) = BPT::find_record(table_id, page_id, key);
    // if (old_trx_id < 0 && record_id < 0) {
    //     printf("[ERROR][%s][trx_id: %d] the record does not exist (table_id: %ld, page_id: %ld, key: %ld)\n", __func__, trx_id, table_id, page_id, key);
    //     pthread_mutex_unlock(&this->lock_manager_latch);
    //     return FLAG::FAILURE;
    // }

    // Check implicit locking
    if (this->is_empty_entry(page)) {
        // Case 1: The lock is already implicitly acquired by another transaction
        if (old_trx_id != trx_id && this->is_active_trx(old_trx_id)) {
            printf("[INFO][%s][trx_id: %d] already implicitly acquired by another transaction %ld\n", __func__, trx_id, old_trx_id);
            lock_obj = this->create_lock(page, record_id, old_trx_id, LOCK_MODE_EXCLUSIVE);
            if (lock_obj == NULL) {
                printf("[ERROR][%s][trx_id: %d] lock object is NULL of %d\n", __func__, trx_id, old_trx_id);
                pthread_mutex_unlock(&this->lock_manager_latch);
                return FLAG::FATAL;
            }
        }
        // Case 2: The lock is already implicitly acquired by the same transaction
        else if (lock_mode == LOCK_MODE_EXCLUSIVE) {
            printf("[INFO][%s][trx_id: %d] implicitly acquired by this transaction\n", __func__, trx_id);
            printf("[INFO][%s][trx_id: %d] Lock is acquired (page_id: %ld, record_id: %d, lock_mode: %d)\n", __func__, trx_id, page_id, record_id, lock_mode);
            pthread_mutex_unlock(&this->lock_manager_latch);
            return FLAG::SUCCESS;
        }
    }

    // Check if lock compression is possible
    lock_obj = this->compress_lock(page, record_id, trx_id, lock_mode);
    if (lock_obj != NULL) {
        printf("[INFO][%s][trx_id: %d] Success to compress lock\n", __func__, trx_id);
        printf("[INFO][%s][trx_id: %d] Lock is acquired (page_id: %ld, record_id: %d, lock_mode: %d)\n", __func__, trx_id, page_id, record_id, lock_mode);
        pthread_mutex_unlock(&this->lock_manager_latch);
        return FLAG::SUCCESS;
    }

    // Allocate a new lock object
    lock_obj = this->create_lock(page, record_id, trx_id, lock_mode);
    if (lock_obj == NULL) {
        printf("[ERROR][%s][trx_id: %d]  lock object is NULL\n", __func__, trx_id);
        pthread_mutex_unlock(&this->lock_manager_latch);
        return FLAG::FATAL;
    }

    // Deadlock detection
    if (this->is_deadlock(trx_id)) {
        printf("[INFO][%s][trx_id: %d] Deadlock detection Begin\n", __func__, trx_id);
        flag = this->abort_trx(trx_id);
        pthread_mutex_unlock(&this->lock_manager_latch);
            
        if (flag == FLAG::FATAL) printf("[ERROR][%s][trx_id: %d] Deadlock detection FATAL Error\n", __func__, trx_id);
        return flag ? FLAG::FATAL : FLAG::ABORTED;
    }

    // Wait for the conflict locks to be released
    // lock_obj->wait(&this->lock_manager_latch);
    if (!lock_obj->is_acquired()) {
        printf("[INFO][%s][trx_id: %d] Wait begin (page_id: %ld, record_id: %d, lock_mode: %d)\n", __func__, trx_id, page_id, record_id, lock_mode);
        pthread_cond_wait(&lock_obj->cond, &this->lock_manager_latch);
        printf("[INFO][%s][trx_id: %d] Wake (record_id: %d)\n", __func__, trx_id, record_id);
    }
    printf("[INFO][%s][trx_id: %d] Lock is acquired (page_id: %ld, record_id: %d, lock_mode: %d)\n", __func__, trx_id, page_id, record_id, lock_mode);

    // Release the lock manager latch
    pthread_mutex_unlock(&this->lock_manager_latch);
    
    printf("[INFO][acquire_lock][trx_id: %d] Release lock manager latch %ld %ld %ld %d %d\n", trx_id, table_id, page_id, key, trx_id, lock_mode);

    return FLAG::SUCCESS;
}

// Commit the transaction (Protected by the lock_manager_latch)
int TransactionManager::commit_trx(int trx_id)
{
    int flag;
    page_key_t page;

    printf("[INFO][commit_trx][trx_id: %d] Begin %ld\n", trx_id);

    // Acquire the lock manager latch
    flag = pthread_mutex_lock(&this->lock_manager_latch);
    if (flag != 0) return 0;

    printf("[INFO][commit_trx][trx_id: %d] Acquire lock manager latch\n", trx_id);

    // Check if the transaction is valid
    if (!this->is_active_trx(trx_id)) {
        pthread_mutex_unlock(&this->lock_manager_latch);
        return 0;
    }

    // Release all the locks held by the transaction
    flag = this->release_all_locks(trx_id);
    if (flag != 0) {
        pthread_mutex_unlock(&this->lock_manager_latch);
        return 0;
    }

    // Deallocate the transaction object
    flag = this->remove_trx(trx_id);
    if (flag == 0) {
        pthread_mutex_unlock(&this->lock_manager_latch);
        return 0;
    }

    // Release the lock manager latch
    pthread_mutex_unlock(&this->lock_manager_latch);

    printf("[INFO][commit_trx][trx_id: %d] Release lock manager latch\n", trx_id);

    return trx_id;
}


/// APIs for lock manager
namespace TRX
{
    // Global transaction manager
    TransactionManager trx_manager;

    // Initialize transaction manager
    int init_trx_manager()
    {
        return trx_manager.init();
    }

    // Acquire lock
    int acquire_lock(int64_t table_id, pagenum_t page_id, int64_t key, int trx_id, int old_trx_id, int lock_mode) 
    {
        int flag;

        // Acquire the lock for the transaction
        flag = trx_manager.acquire_lock(table_id, page_id, key, trx_id, old_trx_id, lock_mode);
        if (flag == FLAG::ABORTED) printf("[INFO][acquire_lock][trx_id: %d] Abort transaction %d\n", trx_id, trx_id);
        else if (flag == FLAG::SUCCESS) printf("[INFO][acquire_lock][trx_id: %d] Acquire lock for transaction %d\n", trx_id, trx_id);
        else printf("[INFO][acquire_lock][trx_id: %d] Failed to acquire lock for transaction %d\n", trx_id, trx_id);

        return flag;
    }

    // Add undo log
    int save_log(int trx_id, int64_t table_id, pagenum_t page_id, int64_t key, std::string old_value, int old_trx_id)
    {
        int flag;
        undo_log_t log;

        // Create and add the undo log
        log = undo_log_t(table_id, page_id, key, old_value, old_trx_id);
        flag = trx_manager.add_undo_log(trx_id, log);
        if (flag) printf("[INFO] Failed to add undo log for transaction %d\n", trx_id);
        return flag;
    }
}


/// APIs for transaction manager
// Allocate a transaction structure and initialize it
int trx_begin(void)
{
    int trx_id;

    printf("[INFO][%s] Global Begin\n", __func__);

    // Allocate a transaction id
    trx_id = TRX::trx_manager.alloc_trx();
    if (trx_id > 0) printf("[INFO][trx_begin][trx_id: %d] Allocate transaction %d\n", trx_id, trx_id);
    else printf("[ERROR][trx_begin] Failed to allocate transaction\n");
    return trx_id;
}

// Commit a transaction
int trx_commit(int trx_id)
{
    int flag;

    // Commit the transaction
    flag = TRX::trx_manager.commit_trx(trx_id);
    if (flag > 0) printf("[INFO][trx_commit][trx_id: %d] Commit transaction %d\n", trx_id, trx_id);
    else printf("[ERROR][trx_commit][trx_id: %d]  Failed to commit transaction %d\n", trx_id, trx_id);
    return flag;
}

// Abort a transaction
int trx_abort(int trx_id)
{
    int flag;

    // Abort the transaction
    flag = TRX::trx_manager.abort_trx(trx_id);
    if (flag > 0) printf("[INFO][trx_abort][trx_id: %d] Abort transaction %d\n", trx_id, trx_id);
    else printf("[ERROR][trx_abort][trx_id: %d]  Failed to abort transaction %d\n", trx_id, trx_id);
    return flag;
}