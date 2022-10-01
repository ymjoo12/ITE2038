#ifndef DB_BUFFER_H_
#define DB_BUFFER_H_

/// Includes
#include "page.h"
#include "file.h"
#include "log_util.h"

#include <assert.h>
#include <set>
#include <map>
#include <vector>
#include <tuple>
#include <algorithm>


/// Type
using link_pair = std::pair<int ,int>;


/// Buffer manager
class BufferStorage
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
        int pin_count;

        // Constructor
        buffer_t();
        buffer_t(int index, int64_t table_id, pagenum_t pg_num, bool pin = false, bool dirty = false);
    };

    // Fields
    int num_buf, num_used;
    int oldest[3], latest[3];

    // Buffer indexes, Control blocks, Frames, LRU linked list
    std::map<std::pair<int64_t, pagenum_t>, int> index_map;      // hash table
    std::vector<buffer_t> pool;                                  // fixed and aligned
    std::vector<page_t> frames;                                  // fixed and aligned
    std::vector<link_pair> lru, eviction_priority;               // fixed and aligned

    // Buffer index allocators
    void update_list(std::vector<link_pair>& list, int index, int is_pinned = 2);
    void update_index(int index, bool pin = false);
    int get_new_index(bool pin = false);
    int get_victim_index();

    // Buffer page index mappers
    int get_page_index(int64_t table_id, pagenum_t pg_num);
    int replace_page(int64_t table_id, pagenum_t pg_num, const page_t& pg_img, bool pin = false, bool dirty = false);

public:
    // Constructor
    BufferStorage();

    // Initializers
    int init(int num_buf);
    void clear();
    void flush_all_pages();

    // Member functions (Page access manager)
    page_t get_page(int64_t table_id, pagenum_t pg_num, bool& pinned, bool pin = false);
    void set_dirty_page(int64_t table_id, pagenum_t pg_num, const page_t& pg_img, bool unpin = true);
    void set_pin_page(int64_t table_id, pagenum_t pg_num, int pin_delta);
};

/// Buffer Manager APIs
namespace BufferManager
{
    /// Global buffer
    extern BufferStorage buffer;

    /// Buffer initializers
    int init_buffer(int num_buf);
    int clear_buffer();
    
    /// Page controllers
    page_t read_page(int64_t table_id, pagenum_t pg_num, bool& pinned, bool pin = false);
    void write_page(int64_t table_id, pagenum_t pg_num, const page_t& pg_img, bool unpin = true);
    void unpin_page(int64_t table_id, pagenum_t pg_num, bool unpin = true);

    /// Table controllers (processing with header page in buffer)
    pagenum_t alloc_page(int64_t table_id);
    void free_page(int64_t table_id, pagenum_t pg_num, bool unpin = true);
    bool is_valid_page(int64_t table_id, pagenum_t pg_num);
}

#endif  // DB_BUFFER_H_
