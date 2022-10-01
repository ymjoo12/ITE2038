#include "lock_table.h"

/// Global variables
lock_table_t lock_table;
pthread_mutex_t lock_table_latch;


/// Lock table entry
// Constructors and destructor
lock_t::lock_t()
    : prev(NULL), next(NULL), sentinel(NULL), cond(NULL)
{}

lock_t::lock_t(lock_table_entry_t* sentinel)
    : prev(NULL), next(NULL), sentinel(sentinel), cond(new pthread_cond_t)
{}

lock_t::~lock_t()
{
    if (this->cond != NULL) delete this->cond;
}

lock_table_entry_t::lock_table_entry_t()
    : table_id(0), key(0), head(NULL), tail(NULL)
{}

lock_table_entry_t::lock_table_entry_t(record_key_t record)
    : table_id(record.first), key(record.second), head(NULL), tail(NULL)
{}

// Create a new lock
lock_t* lock_table_entry_t::append_lock() 
{
    int flag;

    // Create a new lock
    lock_t* lock = new lock_t(this);

    // Initialize the condition variable
    flag = pthread_cond_init(lock->cond, NULL);
    if (flag != 0) {
        delete lock;
        return NULL;
    }

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
    lock->prev = this->tail;
    this->tail = lock;

    // Return the lock
    return lock;
}

// Remove the head lock
int lock_table_entry_t::remove_lock(lock_t* lock) 
{
    int flag;

    // Check if the lock is valid
    if (lock == NULL || lock->sentinel != this) return -1;
    if (this->head != lock || this->tail == NULL) return -1;

    // Destroy the condition variable
    flag = pthread_cond_destroy(lock->cond);
    if (flag != 0) return -1;

    // Update the lock table entry
    if (this->tail == lock) {
        // Only one lock exists
        this->head = NULL;
        this->tail = NULL;
    } else {
        // The lock is not tail
        this->head = lock->next;
        this->head->prev = NULL;
    }

    // Destroy the lock
    delete lock;
    return 0;
}


/// APIs for lock table
// Initialize the lock table
int init_lock_table(void) 
{
    int flag;

    // initialize lock table
    lock_table.clear();

    // initialize mutex
    flag = pthread_mutex_init(&lock_table_latch, NULL);
    if (flag != 0) return -1;

    return 0;
}

// Acquire a lock by table id and key
lock_t* lock_acquire(int64_t table_id, int64_t key) 
{
    int flag;
    lock_t* lock_obj;
    record_key_t record;

    // Acquire the lock table mutex
    flag = pthread_mutex_lock(&lock_table_latch);
    if (flag != 0) return NULL;

    // Get record information
    record = {table_id, key};

    // Find the lock table entry
    if (lock_table.find(record) == lock_table.end()) {
        lock_table[record] = lock_table_entry_t(record);
    }

    // Get the lock
    lock_obj = lock_table[record].append_lock();
    if (!lock_obj) {
        // Failed to get the lock
        pthread_mutex_unlock(&lock_table_latch);
        return NULL;
    }

    // Wait for the lock
    while (lock_obj->prev != NULL) {
        pthread_cond_wait(lock_obj->cond, &lock_table_latch);
    }

    // Unlock the lock table mutex and return the lock
    pthread_mutex_unlock(&lock_table_latch);
    return lock_obj;
}

// Release the lock
int lock_release(lock_t* lock_obj) 
{
    int flag;
    record_key_t record;

    // Acquire the lock table mutex
    flag = pthread_mutex_lock(&lock_table_latch);
    if (flag != 0) return -1;

    // Get record information
    record = {lock_obj->sentinel->table_id, lock_obj->sentinel->key};

    // Find the lock table entry
    if (lock_table.find(record) == lock_table.end()) {
        pthread_mutex_unlock(&lock_table_latch);
        return -1;
    }

    // Release the lock
    if (lock_table[record].remove_lock(lock_obj) != 0) {
        pthread_mutex_unlock(&lock_table_latch);
        return -1;
    }

    // Signal the next lock
    if (lock_table[record].head != NULL) {
        pthread_cond_signal(lock_table[record].head->cond);
    }

    // Unlock the lock table mutex
    pthread_mutex_unlock(&lock_table_latch);
    return 0;
}
