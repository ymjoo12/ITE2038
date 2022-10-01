#include "buffer.h"


/// Buffer Storage
BufferStorage::BufferStorage()
    : num_buf(0), num_used(0)
{}


/// Buffer structure
BufferStorage::buffer_t::buffer_t()
    : frame_id(), table_id(0), pg_num(0), 
    is_dirty(false), pin_count(0)
{}

BufferStorage::buffer_t::buffer_t(int index, int64_t table_id, pagenum_t pg_num, bool pin, bool dirty)
    : frame_id(index), table_id(table_id), pg_num(pg_num),
    is_dirty(dirty), pin_count(pin)
{}  


/// Member functions
// Initializer API
int BufferStorage::init(int num_buf)
{
    // Check whether the number of buffer is valid
    if (num_buf < 0) return 1;

    // Initialize field data
    this->num_buf = num_buf;
    this->num_used = 0;
    std::fill(this->oldest,this->oldest+3,-1);
    std::fill(this->latest,this->latest+3,-1);

    this->index_map.clear();
    this->pool.reserve(this->num_buf);
    this->frames.reserve(this->num_buf);
    this->lru.resize(this->num_buf, {-1,-1});
    this->eviction_priority.resize(this->num_buf, {-1,-1});

    return 0;
}

void BufferStorage::clear()
{
    this->num_buf = 0;
    this->num_used = 0;
    std::fill(this->oldest,this->oldest+3,-1);
    std::fill(this->latest,this->latest+3,-1);

    this->index_map.clear();
    this->pool.clear();
    this->frames.clear();
    this->lru.clear();
    this->eviction_priority.clear();
}


/// Index Allocators
void BufferStorage::update_list(std::vector<link_pair>& list, int index, int is_pinned)
{
    bool old_pin;
    int prev, next;

    // Check if there is nothing to update
    if (index == this->latest[is_pinned]) return;
    
    // Update neighbor index
    std::tie(prev, next) = list[index];
    if (prev >= 0) list[prev].second = next;
    if (next >= 0) list[next].first = prev;

    // Update oldest, latest index
    if (is_pinned > 1) {
        if (index == this->oldest[is_pinned]) this->oldest[is_pinned] = next;
    } else if (index < this->num_used) {
        old_pin = this->pool[index].pin_count > 0;
        if (index == this->oldest[old_pin]) this->oldest[old_pin] = next;
        if (index == this->latest[!is_pinned]) this->latest[!is_pinned] = prev;
    }

    // Update this index
    list[index] = {this->latest[is_pinned], -1};
    if (this->latest[is_pinned] >= 0) list[this->latest[is_pinned]].second = index;
    this->latest[is_pinned] = index;
    if (this->oldest[is_pinned] == -1) this->oldest[is_pinned] = index;
}

void BufferStorage::update_index(int index, bool pin)
{
    // Check whether index is valid
    if (index >= this->num_used || index < 0) return;

    // Update linked lists
    this->update_list(this->lru, index);
    this->update_list(this->eviction_priority, index, pin);
}

// Allocate new index of buffer for a page
int BufferStorage::get_new_index(bool pin)
{
    int new_index = this->num_used;

    // Check if buffer is full
    if (this->num_used >= this->num_buf) return -1;

    // Update linked lists
    this->update_list(this->lru, new_index);
    this->update_list(this->eviction_priority, new_index, pin);

    // Increas the number of used buffer frame
    this->num_used++;

    return new_index;
}

int BufferStorage::get_victim_index()
{
    // Return the index oldest and unpinned (If it doesn't exist, return -1)
    return this->oldest[0];
}


/// Buffer mamber functions
// 
int BufferStorage::get_page_index(int64_t table_id, pagenum_t pg_num)
{
    // Check if the page is in buffer
    if (this->index_map.find({table_id, pg_num}) != this->index_map.end()) {
        return this->index_map[{table_id, pg_num}];
    }

    // If page is not in buffer, return -1
    return -1;
}

// Fetch page image into buffer frame
int BufferStorage::replace_page(int64_t table_id, pagenum_t pg_num, const page_t& pg_img, bool pin, bool dirty)
{
    std::cout << "[replace_page] ( table_id: " << table_id << ", pg_num: " << pg_num << ", pin: " << pin << ", dirty: " << dirty << " )" << std::endl;

    int index;

    // Get index
    if (this->num_buf > this->num_used) index = this->get_new_index();
    else {
        // Eviction case
        index = this->get_victim_index();
        if (index < 0) {
            std::cout << "[BufferStorage::replace_page] Buffer space is not enough." << std::endl;
            return -1;
        }
        if (this->pool[index].is_dirty) {
            file_write_page(this->pool[index].table_id, this->pool[index].pg_num, &frames[index]);
            std::cout << "[FLUSH PAGE] ( table id : " << this->pool[index].table_id;
            std::cout << ", page number: " << this->pool[index].pg_num << " )" << std::endl;
        }
        this->index_map.erase({pool[index].table_id, this->pool[index].pg_num});
    }

    // Map index to page number
    this->index_map[{table_id, pg_num}] = index;
    this->update_index(index, pin);

    // Save data to buffer
    this->pool[index] = buffer_t(index, table_id, pg_num, pin, dirty);
    this->frames[index] = pg_img;

    // Return index
    return index;
}


page_t BufferStorage::get_page(int64_t table_id, pagenum_t pg_num, bool& pinned, bool pin)
{
    std::cout << "[get_page] ( table_id: " << table_id << ", pg_num: " << pg_num << ", pin: " << pin << " )" << std::endl;
    
    int index;
    page_t pg_img;

    // Check the buffer index of the page
    index = this->get_page_index(table_id, pg_num);
    if (index < 0) {
        // Case 1: the page is not in buffer - put it on to buffer
        std::cout << "[READ MISS] ( table_id: " << table_id << ", pg_num: " << pg_num << " )" << std::endl;
        file_read_page(table_id, pg_num, &pg_img);
        std::cout << "[FETCH PAGE] ( table_id : " << table_id << ", pg_num: " << pg_num << " )" << std::endl;
        index = this->replace_page(table_id, pg_num, pg_img, pin);
        if (index < 0) {
            // Case: the buffer space is not large enough
            pinned = false;
            return pg_img;
        }
    } else {
        // Case 2: the page is already in buffer
        std::cout << "[READ HIT] ( table_id: " << table_id << ", pg_num: " << pg_num << " )" << std::endl;
        this->pool[index].pin_count += pin;
        this->update_index(index, this->pool[index].pin_count);
    }

    // Return of the page data and pinned
    pinned = pin;
    return this->frames[index];
}


void BufferStorage::set_dirty_page(int64_t table_id, pagenum_t pg_num, const page_t& pg_img, bool unpin)
{
    std::cout << "[set_dirty_page] ( table_id: " << table_id << ", pg_num: " << pg_num << ", unpin: " << unpin << " )" << std::endl;

    int index; 

    // Check the buffer index of the page
    index = this->get_page_index(table_id, pg_num);
    if (index < 0) {
        // If the page is not in the buffer, put it on to buffer
        std::cout << "[WRITE MISS] ( table_id: " << table_id << ", pg_num: " << pg_num << " )" << std::endl;
        index = this->replace_page(table_id, pg_num, pg_img, false, true);
        if (index < 0) {
            file_write_page(table_id, pg_num, &pg_img);
            std::cout << "[FLUSH PAGE] ( table_id : " << table_id << ", pg_num: " << pg_num << " )" << std::endl;
            return;
        }
    } else {
        // Update page in buffer and set dirty bit
        std::cout << "[WRITE HIT] ( table_id: " << table_id << ", pg_num: " << pg_num << " )" << std::endl;
        this->frames[index] = pg_img;
        this->pool[index].is_dirty = true;
        if (this->pool[index].pin_count) this->pool[index].pin_count -= unpin;
        this->update_index(index, this->pool[index].pin_count);
    }
}

void BufferStorage::set_pin_page(int64_t table_id, pagenum_t pg_num, int pin_delta)
{
    int index;

    // Check the buffer index of the page
    index = this->get_page_index(table_id, pg_num);
    if (index < 0) return;

    // Set or unset pin of the page
    if (pin_delta >= 0 || pin_delta < 0 && this->pool[index].pin_count > 0) {
        this->pool[index].pin_count += pin_delta;
    }
    this->update_index(index, this->pool[index].pin_count);
}

void BufferStorage::flush_all_pages()
{
    std::cout << "[flush_all_pages]" << std::endl;

    // Flush buffer data
    for (int index = 0; index < num_buf; index++) {
        if (this->pool[index].is_dirty) {
            file_write_page(this->pool[index].table_id, this->pool[index].pg_num, &frames[pool[index].frame_id]);
            std::cout << "[FLUSH PAGE] ( table id : " << this->pool[index].table_id;
            std::cout << ", page number: " << this->pool[index].pg_num << " )" << std::endl;
        }
    }
}



/// Buffer Manager APIs
namespace BufferManager
{
    /// Global buffer
    BufferStorage buffer;

    /// Buffer initializers
    int init_buffer(int num_buf)
    {
        return buffer.init(num_buf);
    }

    int clear_buffer()
    {
        buffer.flush_all_pages();
        buffer.clear();

        return 0;
    }

    /// Page controllers
    page_t read_page(int64_t table_id, pagenum_t pg_num, bool& pinned, bool pin)
    {
        return buffer.get_page(table_id, pg_num, pinned, pin);
    }

    void write_page(int64_t table_id, pagenum_t pg_num, const page_t& pg_img, bool unpin)
    {
        buffer.set_dirty_page(table_id, pg_num, pg_img, unpin);
    }

    void unpin_page(int64_t table_id, pagenum_t pg_num, bool unpin)
    {
        buffer.set_pin_page(table_id, pg_num, -unpin);
    }


    /// Table controllers (processing with header page in buffer)
    pagenum_t alloc_page(int64_t table_id)
    {
        std::cout << "[alloc_page]" << std::endl;

        bool pinned_h, pinned_f;
        pagenum_t pg_num_to_alloc, num_pages;
        HeaderPage header_page, free_page;

        // Get header page data
        header_page = HeaderPage(buffer.get_page(table_id, 0, pinned_h, true));
        pg_num_to_alloc = header_page.next_free_page_number;
        num_pages = header_page.num_of_pages;

        // Extend file size 
        if (!pg_num_to_alloc) {
            // Update header page
            header_page.next_free_page_number = num_pages;
            header_page.num_of_pages = num_pages * 2;

            // Append free pages
            for (pagenum_t pg_num = num_pages; pg_num < num_pages * 2; pg_num++) {
                free_page.next_free_page_number = (pg_num + 1) % (num_pages * 2);
                buffer.set_dirty_page(table_id, pg_num, free_page, false);
            }

            // Get new first free page number again
            pg_num_to_alloc = num_pages;
        }

        // Get first free page
        free_page = HeaderPage(buffer.get_page(table_id, pg_num_to_alloc, pinned_f, true));

        // Update free page numbers
        header_page.next_free_page_number = free_page.next_free_page_number;
        free_page.next_free_page_number = pg_num_to_alloc;

        // Apply updated pages
        buffer.set_dirty_page(table_id, 0, header_page, pinned_h);
        buffer.set_dirty_page(table_id, pg_num_to_alloc, free_page, pinned_f);

        return pg_num_to_alloc;
    }

    void free_page(int64_t table_id, pagenum_t pg_num, bool unpin)
    {
        std::cout << "[free_page]" << std::endl;

        bool pinned_h;
        HeaderPage header_page, free_page;

        // Get header page data
        header_page = HeaderPage(buffer.get_page(table_id, 0, pinned_h, true));

        // Update free page numbers
        free_page.next_free_page_number = header_page.next_free_page_number;
        header_page.next_free_page_number = pg_num;

        // Apply updated pages
        buffer.set_dirty_page(table_id, 0, header_page, pinned_h);
        buffer.set_dirty_page(table_id, pg_num, free_page, unpin);
    }

    bool is_valid_page(int64_t table_id, pagenum_t pg_num)
    {
        bool pinned_h;
        HeaderPage header_page;

        // Get header page data
        header_page = HeaderPage(buffer.get_page(table_id, 0, pinned_h));

        // Return the page is exist in the table
        return pg_num < header_page.num_of_pages && pg_num >= 0;
    }
}