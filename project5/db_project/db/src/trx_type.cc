#include "trx_type.h"


/// Structures
// Undo log structure
undo_log_t::undo_log_t()
    : table_id(0), page_id(0), key(0), old_value(""), old_trx_id(0) 
{}

undo_log_t::undo_log_t(int64_t table_id, pagenum_t page_id, int64_t key, std::string old_value, int old_trx_id)
    : table_id(table_id), page_id(page_id), key(key), old_value(old_value), old_trx_id(old_trx_id)
{}

int undo_log_t::rollback()
{
    int flag;
    Record record_rollback;

    // Reverse the operation
    flag = BPT::update(
        this->table_id, this->page_id, this->key, 
        record_rollback, this->old_value, this->old_trx_id
    );
    return flag;
}


// Transaction structure
trx_t::trx_t()
    : trx_id(0), first(NULL), last(NULL)
{
    // Initialize the waiting for latch
    pthread_mutex_init(&this->waiting_list_latch, NULL);
}

trx_t::trx_t(int trx_id)
    : trx_id(trx_id), first(NULL), last(NULL)
{
    // Initialize the waiting for latch
    pthread_mutex_init(&this->waiting_list_latch, NULL);
}

int trx_t::append_lock(lock_t* lock_obj)
{
    // Append the lock to the end of the list
    if (this->first == NULL && this->last == NULL) {
        this->first = this->last = lock_obj;
    } else if (this->first != NULL && this->last != NULL) {
        this->last->next_trx_lock = lock_obj;
        this->last = lock_obj;
    } else {
        return FLAG::FAILURE;
    }

    return FLAG::SUCCESS;
}

bool trx_t::in_waiting_list(int trx_id)
{
    int flag;

    // Acquire the waiting list latch
    flag = pthread_mutex_lock(&this->waiting_list_latch);
    if (flag != 0) return false;

    // Check if this transaction is waiting for the given transaction
    flag = this->waiting_list.find(trx_id) != this->waiting_list.end();

    // Release the waiting list latch
    pthread_mutex_unlock(&this->waiting_list_latch);
    return flag;
}

void trx_t::add_waiting_list(int trx_id)
{
    int flag;

    // Acquire the waiting list latch
    flag = pthread_mutex_lock(&this->waiting_list_latch);
    if (flag != 0) return;

    // Add the trx_id to the waiting set
    this->waiting_list.insert(trx_id);

    // Release the waiting list latch
    pthread_mutex_unlock(&this->waiting_list_latch);
}

void trx_t::pop_waiting_list(int trx_id)
{
    int flag;

    // Acquire the waiting list latch
    flag = pthread_mutex_lock(&this->waiting_list_latch);
    if (flag != 0) return;

    // Remove the trx_id from the waiting set
    if (this->waiting_list.find(trx_id) != this->waiting_list.end()) {
        this->waiting_list.erase(trx_id);
    }

    // Release the waiting list latch
    pthread_mutex_unlock(&this->waiting_list_latch);
}

std::stack<int> trx_t::get_waiting_stack()
{
    std::stack<int> waiting_stack;
    int flag;

    // Acquire the waiting list latch
    flag = pthread_mutex_lock(&this->waiting_list_latch);
    if (flag != 0) return waiting_stack;

    // Get the waiting stack
    for (int trx_id : this->waiting_list) {
        waiting_stack.push(trx_id);
    }

    // Release the waiting list latch
    pthread_mutex_unlock(&this->waiting_list_latch);
    return waiting_stack;
}

void trx_t::add_undo_log(undo_log_t log)
{
    // Add the undo log to the top of the stack
    this->undo_logs.push(log);
}

int trx_t::rollback()
{
    int flag;
    undo_log_t log;

    // Rollback the transaction
    while (!this->undo_logs.empty()) {
        // Pop the top undo log
        log = this->undo_logs.top();
        this->undo_logs.pop();

        // Perform the rollback
        flag = log.rollback();
        if (flag) return FLAG::FAILURE;
    }
    return FLAG::SUCCESS;
}

void trx_t::clear()
{
    // Clear the transaction
    this->waiting_list.clear();
    this->undo_logs = std::stack<undo_log_t>();
}


// Lock structure
lock_t::lock_t()
    : sentinel(NULL), prev(NULL), next(NULL), next_trx_lock(NULL),
    record_id(0), trx_id(0), lock_mode(LOCK_MODE_SHARED), acquired(false)
{
    // Initialize the condition variable
    pthread_cond_init(&this->cond, NULL);

    // Reset bitmap
    this->bitmap.reset();
}

lock_t::lock_t(lock_table_entry_t* sentinel, int64_t record_id, int trx_id, int lock_mode)
    : sentinel(sentinel), prev(NULL), next(NULL), next_trx_lock(NULL),
    record_id(record_id), trx_id(trx_id), lock_mode(lock_mode), acquired(false)
{
    // Initialize the condition variable
    pthread_cond_init(&this->cond, NULL);
    
    // Reset bitmap
    this->bitmap.reset();
}

int lock_t::is_contained(int record_id, int trx_id, int lock_mode)
{
    // If trx_id is not the same, return 0
    if (trx_id && this->trx_id != trx_id) return 0;

    // If record_id is contain in this, return 2 (acquired)
    if (this->bitmap[record_id]) return 2;

    // If record_id is equal to this, return 1 (waiting)
    if (this->record_id == record_id) return 1;

    return 0;
}

bool lock_t::is_compressable(int trx_id, int lock_mode)
{
    // Check if the lock is replaceable
    return this->trx_id == trx_id && this->lock_mode >= lock_mode;
}

int lock_t::is_conflict(int record_id, int trx_id, int lock_mode)
{
    // Check if the trx_id is equal to this
    if (this->trx_id == trx_id) return 0;

    // Check if the record_id is equal to this
    if (!this->bitmap.test(record_id) && this->record_id != record_id) return 0;

    // If lock_mode is conflict with this, return non-zero
    if (this->lock_mode == LOCK_MODE_EXCLUSIVE) return 2;
    return lock_mode == LOCK_MODE_EXCLUSIVE;
}

bool lock_t::is_acquired()
{
    // If this lock is acquired, return true
    return this->acquired;
}

void lock_t::add_record(int record_id)
{
    // Add record_id to the bitmap
    this->bitmap[record_id] = true;
}

void lock_t::set_acquired()
{
    // Set the acquired bit
    this->acquired = true;
    this->bitmap.set(this->record_id, 1);
}

void lock_t::wait(pthread_mutex_t* lock_manager_latch)
{
    // Wait for the condition variable
    if (!this->acquired) {
        printf("[INFO][%s][trx_id: %d] Wait begin (record_id: %d)\n", __func__, this->trx_id, this->record_id);
        pthread_cond_wait(&this->cond, lock_manager_latch);
    }
}

void lock_t::signal()
{
    // Signal the condition variable
    pthread_cond_signal(&this->cond);
}


page_key_t lock_t::get_page_key()
{
    // Return the page_key_t of this lock
    return {this->sentinel->table_id, this->sentinel->page_id};
}


/// Lock table entry structure
lock_table_entry_t::lock_table_entry_t()
    : table_id(0), page_id(0), head(NULL), tail(NULL)
{}

lock_table_entry_t::lock_table_entry_t(page_key_t page)
    : table_id(page.first), page_id(page.second), head(NULL), tail(NULL)
{}

// Create a new lock
lock_t* lock_table_entry_t::append_lock(int record_id, int trx_id, int lock_mode) 
{
    int flag;

    // Create a new lock
    lock_t* lock = new lock_t(this, record_id, trx_id, lock_mode);

    // Update the lock table entry
    if (this->head == NULL && this->tail == NULL) {
        // No locks exist
        this->head = lock;
    } else if (this->head != NULL && this->tail != NULL) {
        // Require to wait for the lock
        this->tail->next = lock;
    } else {
        // This should never happen
        delete lock;
        return NULL;
    }

    // Link the lock to the tail of the lock list
    lock->prev = this->tail;
    this->tail = lock;

    // Return the lock
    return lock;
}

// Try compress the lock
lock_t* lock_table_entry_t::compress_lock(int record_id, int trx_id, int lock_mode)
{
    bool compressable = true;
    int flag;

    for (lock_t* lock = this->tail; lock != NULL; lock = lock->prev) {
        // Cheack if the lock is already acquired
        if (lock->is_contained(record_id, trx_id, lock_mode)) return lock;

        // Check if the lock is compressable
        if (compressable && lock->is_compressable(trx_id, lock_mode)) {
            lock->add_record(record_id);
            return lock;
        }

        // Check if the lock is conflict with the new lock
        if (lock->is_conflict(record_id, trx_id, lock_mode)) {
            // Case 1: The conflictable lock is already acquired
            if (lock->is_acquired()) return NULL;
            // Case 2: The conflictable lock is waiting
            compressable = false;
        }
    }

    return NULL;
}

// Remove the head lock
int lock_table_entry_t::remove_lock(lock_t* lock) 
{
    int flag;

    // Check if the lock is valid
    if (lock == NULL || lock->sentinel != this) return FLAG::FAILURE;
    if (this->head == NULL || this->tail == NULL) return FLAG::FAILURE;

    // Destroy the condition variable
    pthread_cond_destroy(&lock->cond);

    // Update the lock table entry
    if (this->head == lock) this->head = lock->next;
    if (this->tail == lock) this->tail = lock->prev;
    if (lock->next != NULL) lock->next->prev = lock->prev;
    if (lock->prev != NULL) lock->prev->next = lock->next;

    // Destroy the lock
    delete lock;
    return FLAG::SUCCESS;
}

// Check this lock table entry is empty
bool lock_table_entry_t::is_empty()
{
    // Check if the head and tail lock is NULL
    return this->head == NULL && this->tail == NULL;
}

int lock_table_entry_t::broadcast_lock(lock_t* lock_obj)
{
    int conflict;
    std::bitset<RECORD_MAX_COUNT> bitmap, xbitmap;

    // Reset bitmaps
    bitmap.reset();
    xbitmap.reset();

    // Check if the lock is valid
    if (lock_obj == NULL) return FLAG::FAILURE;

    printf("[INFO][%s][trx_id: %d] This lock: %d, %d, %d, %d\n", __func__, lock_obj->trx_id, lock_obj->trx_id, lock_obj->sentinel->page_id, lock_obj->record_id, lock_obj->lock_mode);

    // Check lock mode
    if (lock_obj->lock_mode == LOCK_MODE_EXCLUSIVE) {
        // Case 1: Exclusive lock
        for (lock_t* cur = lock_obj; cur != NULL; cur = cur->next) {
            // Check if current lock is conflict with lock to be released

            printf("[INFO][%s][trx_id: %d] Current lock: %d, %d, %d, %d\n", __func__, lock_obj->trx_id, cur->trx_id, cur->sentinel->page_id, cur->record_id, cur->lock_mode);

            conflict = cur->is_conflict(lock_obj->record_id, lock_obj->trx_id, lock_obj->lock_mode);

            printf("[INFO][%s][trx_id: %d] Conflict: %d\n", __func__, lock_obj->trx_id, conflict);
            
            if (conflict == 1) {
                if (xbitmap.test(cur->record_id) == 0) cur->signal();
                bitmap.set(cur->record_id, 1);
            }
            
            if (conflict == 2) {
                if (bitmap.test(cur->record_id) == 0) cur->signal();
                bitmap.set(cur->record_id, 1);
                xbitmap.set(cur->record_id, 1);
            }
        }
    } else if (lock_obj->lock_mode == LOCK_MODE_SHARED) {
        // Case 2: Shared lock
        for (lock_t* cur = this->head; cur != NULL; cur = cur->next) {

            printf("[INFO][%s][trx_id: %d] Current lock: %d, %d, %d, %d\n", __func__, lock_obj->trx_id, cur->trx_id, cur->sentinel->page_id, cur->record_id, cur->lock_mode);

            // Check if current x lock is conflict with bitmap accumulated OR
            if (cur->trx_id == lock_obj->trx_id) continue;
            else if (cur->lock_mode == LOCK_MODE_SHARED && cur->is_acquired()) {
                bitmap |= cur->bitmap;
            }
            else if (cur->lock_mode == LOCK_MODE_EXCLUSIVE && !cur->is_acquired()) {
                if (bitmap.test(cur->record_id) == 0) cur->signal();
            }
        }
    }

    return FLAG::SUCCESS;
}

