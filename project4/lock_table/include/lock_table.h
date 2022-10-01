#ifndef __LOCK_TABLE_H__
#define __LOCK_TABLE_H__

/// Includes
#include <stdint.h>
#include <pthread.h>
#include <unordered_map>
#include <vector>
#include <string>


/// Type definitions
struct lock_t;
struct lock_table_entry_t;
struct hash_record_t;
using record_key_t = std::pair<int64_t, int64_t>;
using lock_table_t = std::unordered_map<record_key_t, lock_table_entry_t, hash_record_t>;


/// Structures
struct lock_t 
{
    lock_t* prev;
    lock_t* next;
    lock_table_entry_t* sentinel;
    pthread_cond_t* cond;

    // Constructor and destructor
    lock_t();
    lock_t(lock_table_entry_t* sentinel);
    ~lock_t();
};

struct lock_table_entry_t 
{
    int64_t table_id;
    int64_t key;
    lock_t* tail;
    lock_t* head;

    // Constructors
    lock_table_entry_t();
    lock_table_entry_t(record_key_t record);

    // Member functions
    lock_t* append_lock();
    int remove_lock(lock_t* lock);
};

struct hash_record_t
{
    size_t operator()(const record_key_t& k) const
    {
        std::size_t h1 = std::hash<std::string>()(std::to_string(k.first));
        std::size_t h2 = std::hash<std::string>()(std::to_string(k.second));
        return h1 ^ (h2 << 1);
    }
};


/// Global variables
extern lock_table_t lock_table;
extern pthread_mutex_t lock_table_latch;


/// APIs for lock table
int init_lock_table(void);
lock_t *lock_acquire(int64_t table_id, int64_t key);
int lock_release(lock_t* lock_obj);


#endif /* __LOCK_TABLE_H__ */
