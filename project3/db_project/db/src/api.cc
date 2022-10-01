#include "api.h"


/// Index Manager APIs
// Open existing data file using 'pathname' or create one if not existed
int64_t open_table(char* pathname)
{
    LogUtil::PrintMarker(__func__,"( pathname: " + std::string(pathname) + " )");

    int64_t table_id;

    // Get table id 
    table_id = file_open_table_file(pathname);

    return table_id;
}

// Insert input 'key/value' (record) with its size to data file at the right place
int db_insert(int64_t table_id, int64_t key, char* value, uint16_t val_size)
{
    LogUtil::PrintMarker(__func__,"( table_id: " + std::to_string(table_id) + ", key: " + std::to_string(key) + ", value: \"" + std::string(value) + "\" )");

    pagenum_t root_page_number;
    std::string value_str;
    char value_char[VALUE_MAX_SIZE+1];
    int flag;

    // If size is invalid return flag 1
    if (val_size < VALUE_MIN_SIZE || val_size > VALUE_MAX_SIZE) {
        return 1;
    }

    // Convert char* to std::string
    memset(&value_char, 0, val_size+1);
    memcpy(&value_char, value, val_size);
    value_str = std::string(value_char);

    // Get root page number
    root_page_number = BPT::get_root_page(table_id);

    // Call insert function
    flag = BPT::insert(table_id, root_page_number, key, value_str);
    if (flag) return 1;

    return 0;
}

// Find the record containing input 'key'
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size)
{
    LogUtil::PrintMarker(__func__,"( table_id: " + std::to_string(table_id) + ", key: " + std::to_string(key) + " )");

    pagenum_t root_page_number;
    std::string value;
    int flag;

    // Check if pointer to return is valid
    if (ret_val == NULL || val_size == NULL) return 1;

    // Get root page number
    root_page_number = BPT::get_root_page(table_id);
    if (root_page_number == 0) return 1;

    // Find the record corresponding to key
    flag = BPT::find(table_id, root_page_number, key, value);
    if (flag) return 1;

    // Assign the found value and size
    memset(ret_val, 0, value.size()+1);
    std::copy(value.begin(), value.begin()+value.size(), ret_val);
    *val_size = value.size();

    return 0;
}

// Find the matching record and delete it if found
int db_delete(int64_t table_id, int64_t key)
{
    LogUtil::PrintMarker(__func__,"( table_id: " + std::to_string(table_id) + ", key: " + std::to_string(key) + " )");

    pagenum_t root_page_number;
    int flag;

    // Get root page number
    root_page_number = BPT::get_root_page(table_id);
    if (root_page_number == 0) return 1;

    // Delete the record corresponding to key
    flag = BPT::db_delete(table_id, root_page_number, key);
    if (flag) return 1;

    return 0;
}

// Initialize database management system
int init_db(int num_buf)
{
    LogUtil::PrintMarker(__func__);

    int flag;

    // Buffer manager initialization
    flag = BufferManager::init_buffer(num_buf);
    if (flag) return 1;

    return 0;
}

// Shutdown database management system
int shutdown_db()
{
    LogUtil::PrintMarker(__func__);

    // Flush buffer data and close all opened table files
    BufferManager::clear_buffer();
    file_close_table_files();

    return 0;
}