#ifndef DB_BUFFER_H_
#define DB_BUFFER_H_

/// Includes
#include "page.h"
#include "file.h"
#include "log_util.h"

#include <pthread.h>
#include <assert.h>
#include <set>
#include <map>
#include <vector>
#include <tuple>
#include <algorithm>


/// Type
using link_pair = std::pair<int ,int>;


/// Buffer manager
class BufferManager
{
private:
    // Buffer structure
    struct buffer_t
    {
        // Metadata
        int frame_id;       // point to frame, linked lru, eviction priority
        int64_t table_id;
        pagenum_t pg_num;
        bool is_dirty;

        // Page latch
        pthread_mutex_t page_latch;

        // Constructors
        buffer_t();
        buffer_t(int index, int64_t table_id, pagenum_t pg_num, bool pin = false, bool dirty = false);
    };

    // Fields
    int num_buf, num_used;
    int oldest[3], latest[3];

    // Buffer indexes, Control blocks, Frames, LRU linked list
    std::map<std::pair<int64_t, pagenum_t>, int> index_map;      // map
    std::vector<buffer_t> pool;                                  // fixed and aligned
    std::vector<page_t> frames;                                  // fixed and aligned
    std::vector<link_pair> lru, eviction_priority;               // fixed and aligned

    // Buffer manager latch
    pthread_mutex_t buffer_latch;
    pthread_mutex_t list_latch;

    // Latch Manager
    int page_latch_acquire(int index);
    int page_latch_release(int index);

    // Disk accessor
    void flush_page(int index);
    void load_page(int index, int table_id, int pg_num);

    // Buffer index allocators
    void update_list(std::vector<link_pair>& list, int index, int pin = 2);
    void update_index(int index, bool pin = false);
    int get_new_index(bool pin = false);
    int get_victim_index();

    // Buffer page index mappers
    int assign_index(int64_t table_id, pagenum_t pg_num, bool load = true);

public:
    // Constructor
    BufferManager();

    // Initializers
    int init(int num_buf);
    void clear();
    void flush_all_pages();

    // Member functions (Page access manager)
    page_t get_page(int64_t table_id, pagenum_t pg_num, int& index);
    void set_dirty_page(int index, const page_t& pg_img);
    int pin_page(int64_t table_id, pagenum_t pg_num);
    void unpin_page(int index);
};

/// Buffer Manager APIs
namespace BUF
{
    /// Global buffer
    extern BufferManager buffer;

    /// Buffer initializers
    int init_buffer(int num_buf);
    int clear_buffer();
    
    /// Page controllers
    page_t read_page(int64_t table_id, pagenum_t pg_num, int& pin_id, bool pin = false);
    void write_page(int pin_id, const page_t& pg_img);
    int pin_page(int64_t table_id, pagenum_t pg_num);
    void unpin_page(int pin_id);

    /// Table controllers (processing with header page in buffer)
    pagenum_t alloc_page(int64_t table_id);
    void free_page(int64_t table_id, pagenum_t pg_num, int pin_id);
    bool is_valid_page(int64_t table_id, pagenum_t pg_num);
}

#endif  // DB_BUFFER_H_
