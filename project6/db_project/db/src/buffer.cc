#include "buffer.h"


/// Buffer Manager
BufferManager::BufferManager()
    : num_buf(0), num_used(0)
{}


/// Buffer structure
BufferManager::buffer_t::buffer_t()
    : frame_id(), table_id(0), pg_num(0), is_dirty(false)
{
    // initialize mutex
    pthread_mutex_init(&page_latch, NULL);
}

BufferManager::buffer_t::buffer_t(int index, int64_t table_id, pagenum_t pg_num, bool pin, bool dirty)
    : frame_id(index), table_id(table_id), pg_num(pg_num), is_dirty(dirty)
{
    // initialize mutex
    pthread_mutex_init(&page_latch, NULL);
}  


/// Member functions
// Initializer API
int BufferManager::init(int num_buf)
{
    int flag;

    // Check whether the number of buffer is valid
    if (num_buf < 0) return 1;

    // Initialize buffer manager latch
    flag = pthread_mutex_init(&this->buffer_latch, NULL);
    if (flag != 0) return 1;

    // Initialize list latch
    flag = pthread_mutex_init(&this->list_latch, NULL);
    if (flag != 0) return 1;

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

    for (int index = 0; index < this->num_buf; index++) {
        this->pool[index].frame_id = index;
        flag = pthread_mutex_init(&this->pool[index].page_latch, NULL);
        if (flag != 0) return 1;
    }

    return 0;
}

void BufferManager::clear()
{
    // Clear all data
    this->num_buf = 0;
    this->num_used = 0;
    std::fill(this->oldest,this->oldest+3,-1);
    std::fill(this->latest,this->latest+3,-1);

    for (int index = 0; index < this->num_buf; index++) {
        pthread_mutex_destroy(&this->pool[index].page_latch);
    }

    this->index_map.clear();
    this->pool.clear();
    this->frames.clear();
    this->lru.clear();
    this->eviction_priority.clear();

    // Destroy buffer manager latch
    pthread_mutex_destroy(&this->buffer_latch);
    pthread_mutex_destroy(&this->list_latch);
}

void BufferManager::flush_all_pages()
{
    std::cout << "[flush_all_pages]" << std::endl;

    // Acquire buffer manager latch
    pthread_mutex_lock(&this->buffer_latch);

    // Flush buffer data
    for (int index = 0; index < num_buf; index++) {
        pthread_mutex_lock(&this->pool[index].page_latch);
        this->flush_page(index);
        pthread_mutex_unlock(&this->pool[index].page_latch);
    }

    // Release buffer manager latch
    pthread_mutex_unlock(&this->buffer_latch);
}


/// Disk Accessor
// flush the page from buffer to disk
void BufferManager::flush_page(int index)
{
    // Check whether index is valid
    if (index < 0 || index >= this->num_used) return;

    // Page is dirty, write to disk
    if (this->pool[index].is_dirty) {
        file_write_page(this->pool[index].table_id, this->pool[index].pg_num, &this->frames[index]);
        std::cout << "[FLUSH PAGE] ( table id : " << this->pool[index].table_id;
        std::cout << ", page number: " << this->pool[index].pg_num << " )" << std::endl;
    }
}

// load the page from disk to buffer
void BufferManager::load_page(int index, int table_id, int pg_num)
{
    // Check whether index is valid
    if (index < 0 || index >= this->num_used) return;

    // Load page from disk
    file_read_page(table_id, pg_num, &this->frames[index]);
    std::cout << "[FETCH PAGE] ( table id : " << table_id << ", page number: " << pg_num << " )" << std::endl;
}


// Latch Manager
// Acquire page latch (Require buffer manager latch)
int BufferManager::page_latch_acquire(int index)
{
    int flag;

    
    #if DEBUG_MODE
    printf("[DEBUG][%s] Begin (index: %d)\n", __func__, index);
    #endif

    // Check whether index is valid
    if (index < 0 || index >= this->num_used) return 1;

    // Acquire page latch
    flag = pthread_mutex_lock(&this->pool[index].page_latch);

    
    #if DEBUG_MODE
    printf("[DEBUG][%s] Acquire the page latch (index: %d)\n", __func__, index);
    #endif

    if (flag != 0) {
        
        // pthread_mutex_unlock(&this->buffer_latch);
        return 1;
    }

    // Pin the page
    this->update_index(index, true);

    
    // // Release buffer manager latch
    // pthread_mutex_unlock(&this->buffer_latch);

    return 0;
}

// Release page latch
int BufferManager::page_latch_release(int index)
{
    int flag;

    
    #if DEBUG_MODE
    printf("[DEBUG][%s] Begin (index: %d)\n", __func__, index);
    #endif

    // Check whether index is valid
    if (index < 0 || index >= this->num_used) return 1;

    // // Acquire buffer manager latch
    // flag = pthread_mutex_lock(&this->buffer_latch);
    // if (flag != 0) return 1;

    
    // printf("[DEBUG][%s] Acquire the buffer latch (index: %d)\n", __func__, index);

    // Unpin the page
    this->update_index(index, false);

    // // Release buffer manager latch
    // pthread_mutex_unlock(&this->buffer_latch);

    
    // printf("[DEBUG][%s] Release the buffer latch (index: %d)\n", __func__, index);

    // Release page latch
    flag = pthread_mutex_unlock(&this->pool[index].page_latch);
    if (flag != 0) return 1;

    
    #if DEBUG_MODE
    printf("[DEBUG][%s] Release the page latch (index: %d)\n", __func__, index);
    #endif

    return 0;
}


/// Index Allocators
void BufferManager::update_list(std::vector<link_pair>& list, int index, int pin)
{
    bool old_pin;
    int prev, next;

    // Check if there is nothing to update
    if (index == this->latest[pin]) return;
    
    // Update neighbor index
    std::tie(prev, next) = list[index];
    if (prev >= 0) list[prev].second = next;
    if (next >= 0) list[next].first = prev;

    // Update oldest, latest index
    if (pin > 1) {
        if (index == this->oldest[pin]) this->oldest[pin] = next;
    } else if (index < this->num_used) {
        if (index == this->oldest[!pin]) this->oldest[!pin] = next;
        if (index == this->latest[!pin]) this->latest[!pin] = prev;
    }

    // Update this index
    list[index] = {this->latest[pin], -1};
    if (this->latest[pin] >= 0) list[this->latest[pin]].second = index;
    this->latest[pin] = index;
    if (this->oldest[pin] == -1) this->oldest[pin] = index;
}

// Set page to recent access
void BufferManager::update_index(int index, bool pin)
{
    int flag;

    // Acquire list latch
    flag = pthread_mutex_lock(&this->list_latch);
    if (flag != 0) return;

    // Check whether index is valid
    if (index >= this->num_used || index < 0) return;

    // Update linked lists
    this->update_list(this->lru, index);
    this->update_list(this->eviction_priority, index, pin);

    // Release list latch
    pthread_mutex_unlock(&this->list_latch);
}

// Allocate new index of buffer for a page
int BufferManager::get_new_index(bool pin)
{
    int new_index, flag;

    // Acquire list latch
    flag = pthread_mutex_lock(&this->list_latch);
    if (flag != 0) return -1;
    
    // New index is the number of used pages
    new_index = this->num_used;

    // Check if buffer is full
    if (this->num_used >= this->num_buf) return -1;

    // Update linked lists
    this->update_list(this->lru, new_index);
    this->update_list(this->eviction_priority, new_index, pin);

    // Increas the number of used buffer frame
    this->num_used++;

    // Release list latch
    pthread_mutex_unlock(&this->list_latch);

    return new_index;
}

// Find index of the victim page
int BufferManager::get_victim_index()
{
    int victim, flag;

    // Acquire list latch
    flag = pthread_mutex_lock(&this->list_latch);
    if (flag != 0) return -1;

    // Find the index oldest and unpinned (If it doesn't exist, it is -1)
    victim = this->oldest[0];

    // Release list latch
    pthread_mutex_unlock(&this->list_latch);

    return victim;
}

/// Buffer page index mapper
// Acquire page latch and get page index
int BufferManager::assign_index(int64_t table_id, pagenum_t pg_num, bool load)
{
    int flag, index;

    
    #if DEBUG_MODE
    printf("[DEBUG][%s] Begin (table id: %ld, page number: %d)\n", __func__, table_id, pg_num);
    #endif

    // Acquire buffer manager latch
    flag = pthread_mutex_lock(&this->buffer_latch);
    if (flag != 0) return -1;

    
    #if DEBUG_MODE
    printf("[DEBUG][%s] Acquire the buffer latch (table id: %ld, page number: %d)\n", __func__, table_id, pg_num);
    #endif

    if (this->index_map.find({table_id, pg_num}) != this->index_map.end()) {
        // Case 1: Page is already in buffer (HIT)
        index = this->index_map[{table_id, pg_num}];
        if (this->page_latch_acquire(index)) {
            // Release buffer manager latch
            pthread_mutex_unlock(&this->buffer_latch);
            return -1;
        }
    } else if (this->num_buf > this->num_used) {
        // Case 2: Page is not in buffer and buffer is not full (COLD MISS)
        index = this->get_new_index(true);
        if (this->page_latch_acquire(index)) {
            // Release buffer manager latch
            pthread_mutex_unlock(&this->buffer_latch);
            return -1;
        }

        // Load page from disk
        if (load) load_page(index, table_id, pg_num);
        this->index_map[{table_id, pg_num}] = index;
    } else {
        // Case 3: Page is not in buffer and buffer is full (MISS)
        index = this->get_victim_index();
        if (index < 0) {
            // Case 4: Buffer is full and no victim page
            pthread_mutex_unlock(&this->buffer_latch);
            std::cout << "[ERROR] Buffer is full and no victim page" << std::endl;
            exit(1);
        }
        if (this->page_latch_acquire(index)) {
            // Release buffer manager latch
            pthread_mutex_unlock(&this->buffer_latch);
            return -1;
        }

        // Evict page
        flush_page(index);
        this->index_map.erase({this->pool[index].table_id, this->pool[index].pg_num});

        // Load page from disk
        if (load) load_page(index, table_id, pg_num);
        this->index_map[{table_id, pg_num}] = index;
    }
    
    // Update page information
    this->pool[index].table_id = table_id;
    this->pool[index].pg_num = pg_num;

    // Release buffer manager latch
    pthread_mutex_unlock(&this->buffer_latch);

    
    #if DEBUG_MODE
    printf("[DEBUG][%s] Release the buffer latch (table id: %ld, page number: %d)\n", __func__, table_id, pg_num);
    #endif

    return index;
}


/// Buffer page accessors
// Read page image from buffer and acquire page latch
page_t BufferManager::get_page(int64_t table_id, pagenum_t pg_num, int& index)
{
    if (DebugUtil::DEBUG_MODE) std::cout << "[get_page] ( table_id: " << table_id << ", pg_num: " << pg_num << " )" << std::endl;

    // Get page index (Acquire page latch)
    index = this->assign_index(table_id, pg_num);
    if (index < 0) {
        std::cout << "[BufferManager::get_page] Page latch is not acquired." << std::endl;
        exit(1);
    }

    // Get page image
    return this->frames[index];
}

// Read page image from buffer when ALREADY acquired page latch
page_t BufferManager::get_page_by_idx(int index)
{
    return this->frames[index];
}

// Write page image to buffer and release page latch
void BufferManager::set_dirty_page(int index, const page_t& pg_img, bool unpin)
{
    if (DebugUtil::DEBUG_MODE) std::cout << "[set_dirty_page] ( index: " << index << " )" << std::endl;

    int flag;

    // Put page image into buffer
    this->frames[index] = pg_img;
    this->pool[index].is_dirty = true;

    // Release page latch
    if (!unpin) return;
    flag = this->page_latch_release(index);
    if (flag != 0) {
        std::cout << "[BufferManager::set_dirty_page] Failed to release page latch." << std::endl;
        exit(1);
    }
}

// Acquire page latch as pin
int BufferManager::pin_page(int64_t table_id, pagenum_t pg_num)
{
    if (DebugUtil::DEBUG_MODE) std::cout << "[pin_page] ( table_id: " << table_id << ", pg_num: " << pg_num << " )" << std::endl;

    int index;

    // Get page index (Acquire page latch)
    index = this->assign_index(table_id, pg_num, false);
    if (index < 0) {
        std::cout << "[BufferManager::get_page] Page latch is not acquired." << std::endl;
        exit(1);
    }

    // return pin id
    return index;
}

// Unpin page and release page latch
void BufferManager::unpin_page(int index)
{
    int flag;

    // Check if the index is valid
    if (index < 0 || index >= this->num_used) {
        std::cout << "[BufferManager::unpin_page] Invalid index." << std::endl;
        return;
    }

    // Release page latch
    flag = this->page_latch_release(index);
    if (flag != 0) {
        std::cout << "[BufferManager::set_dirty_page] Failed to release page latch." << std::endl;
        exit(1);
    }
}

/// Buffer Manager APIs
namespace BUF
{
    /// Global buffer
    BufferManager buffer;

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
    page_t read_page(int64_t table_id, pagenum_t pg_num, int& pin_id, bool pin, bool pinned)
    {
        page_t pg_img;

        // Check if the page is already pinned page
        if (pinned) {
            pg_img = buffer.get_page_by_idx(pin_id);
            return pg_img;
        }

        // Read page image
        pg_img = buffer.get_page(table_id, pg_num, pin_id);
        if (!pin) {
            buffer.unpin_page(pin_id);
            pin_id = -1;
        }

        return pg_img;
    }

    void write_page(int pin_id, const page_t& pg_img, bool unpin)
    {
        // Write page image
        buffer.set_dirty_page(pin_id, pg_img, unpin);
    }

    int pin_page(int64_t table_id, pagenum_t pg_num)
    {
        // Pin the page (not read)
        return buffer.pin_page(table_id, pg_num);
    }

    void unpin_page(int pin_id)
    {
        // Unpin the page
        buffer.unpin_page(pin_id);
    }

    /// Table controllers (processing with header page in buffer)
    pagenum_t alloc_page(int64_t table_id)
    {
        std::cout << "[alloc_page]" << std::endl;

        int pin_id_h, pin_id_f;
        pagenum_t pg_num_to_alloc, num_pages;
        HeaderPage header_page, free_page;

        // Get header page data
        header_page = HeaderPage(buffer.get_page(table_id, 0, pin_id_h));
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
                buffer.set_dirty_page(buffer.pin_page(table_id, pg_num), free_page);
            }

            // Get new first free page number again
            pg_num_to_alloc = num_pages;
        }

        // Get first free page
        free_page = HeaderPage(buffer.get_page(table_id, pg_num_to_alloc, pin_id_f));

        // Update free page numbers
        header_page.next_free_page_number = free_page.next_free_page_number;
        free_page.next_free_page_number = pg_num_to_alloc;

        // Apply updated pages
        buffer.set_dirty_page(pin_id_h, header_page);
        buffer.set_dirty_page(pin_id_f, free_page);

        return pg_num_to_alloc;
    }

    void free_page(int64_t table_id, pagenum_t pg_num, int pin_id)
    {
        std::cout << "[free_page]" << std::endl;

        int pin_id_h;
        HeaderPage header_page, free_page;

        // Get header page data
        header_page = HeaderPage(buffer.get_page(table_id, 0, pin_id_h));

        // Update free page numbers
        free_page.next_free_page_number = header_page.next_free_page_number;
        header_page.next_free_page_number = pg_num;

        // Apply updated pages
        buffer.set_dirty_page(pin_id_h, header_page);
        buffer.set_dirty_page(pin_id, free_page);
    }

    bool is_valid_page(int64_t table_id, pagenum_t pg_num)
    {
        int pin_id_h;
        HeaderPage header_page;

        // Get header page data
        header_page = HeaderPage(buffer.get_page(table_id, 0, pin_id_h));

        // Return the page is exist in the table
        return pg_num < header_page.num_of_pages && pg_num >= 0;
    }
}