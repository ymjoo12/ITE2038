#ifndef DB_TRX_TYPE_H__
#define DB_TRX_TYPE_H__


// Includes
#include "page.h"
#include "bpt.h"

#include <stdint.h>
#include <pthread.h>

#include <iostream>
#include <unordered_map>
#include <vector>
#include <string>
#include <bitset>
#include <set>
#include <stack>
#include <iterator>
#include <tuple>


/// Constants
constexpr int LOCK_MODE_SHARED = 0;
constexpr int LOCK_MODE_EXCLUSIVE = 1;


/// Types
struct undo_log_t;
struct trx_t;
struct lock_t;
struct lock_table_entry_t;
struct hash_page_t;
using page_key_t = std::pair<int64_t, pagenum_t>;
using lock_table_t = std::unordered_map<page_key_t, lock_table_entry_t, hash_page_t>;
using trx_table_t = std::unordered_map<int, trx_t>;


/// Structures
struct undo_log_t
{
private:
    // Fields
    int64_t table_id;
    pagenum_t page_id;
    int64_t key;
    std::string old_value;
    int old_trx_id;

public:
    // Constructors
    undo_log_t();
    undo_log_t(int64_t table_id, pagenum_t page_id, int64_t key, std::string old_value, int old_trx_id);

    // Member functions
    int rollback();
};

struct trx_t
{
private:
    // Fields (private)
    int trx_id;
    std::stack<undo_log_t> undo_logs;
    pthread_mutex_t waiting_list_latch;

public:
    // Fields (public)
    std::set<int> waiting_list;
    lock_t *first, *last;

    // Constructor and destructor
    trx_t();
    trx_t(int trx_id);

    // Member functions
    int append_lock(lock_t* lock_obj);

    bool in_waiting_list(int trx_id);
    void add_waiting_list(int trx_id);
    void pop_waiting_list(int trx_id);
    std::stack<int> get_waiting_stack();

    void add_undo_log(undo_log_t undo_log);
    int rollback();

    void clear();
};

struct lock_t 
{
public:
    // Fields 
    int record_id;
    int trx_id;
    int lock_mode;
    bool acquired;
    std::bitset<RECORD_MAX_COUNT> bitmap;
    pthread_cond_t cond;

    lock_table_entry_t *sentinel;
    lock_t *prev, *next, *next_trx_lock;

    // Constructor and destructor
    lock_t();
    lock_t(lock_table_entry_t* sentinel, int64_t record_id, int trx_id, int lock_mode = LOCK_MODE_SHARED);

    // Member functions
    int is_contained(int record_id, int trx_id = 0, int lock_mode = LOCK_MODE_SHARED);
    bool is_compressable(int trx_id, int lock_mode);
    int is_conflict(int record_id, int trx_id, int lock_mode);
    bool is_acquired();

    void add_record(int record_id);
    void set_acquired();

    void wait(pthread_mutex_t* lock_manager_latch);
    void signal();

    page_key_t get_page_key();

    friend struct lock_table_entry_t;
};

struct lock_table_entry_t 
{
private:
    // Fields
    int64_t table_id;
    pagenum_t page_id;
    lock_t *tail, *head;

public:
    // Constructors
    lock_table_entry_t();
    lock_table_entry_t(page_key_t page);

    // Member functions
    lock_t* append_lock(int record_id, int trx_id, int lock_mode);
    lock_t* compress_lock(int record_id, int trx_id, int lock_mode);
    int remove_lock(lock_t* lock);
    bool is_empty();
    int broadcast_lock(lock_t* lock);

    friend struct lock_t;
};

struct hash_page_t
{
    size_t operator()(const page_key_t& k) const
    {
        std::size_t h1 = std::hash<std::string>()(std::to_string(k.first));
        std::size_t h2 = std::hash<std::string>()(std::to_string(k.second));
        return h1 ^ (h2 << 1);
    }
};


#endif /* DB_TRX_H__ */
