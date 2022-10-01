#include "index.h"


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
        return FLAG::FAILURE;
    }

    // Convert char* to std::string
    memset(&value_char, 0, val_size+1);
    memcpy(&value_char, value, val_size);
    value_str = std::string(value_char);

    // Get root page number
    root_page_number = BPT::get_root_page(table_id);

    // Call insert function
    flag = BPT::insert(table_id, root_page_number, key, value_str);
    if (flag) return FLAG::FAILURE;

    return FLAG::SUCCESS;
}

// Find the record containing input 'key'
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size)
{
    LogUtil::PrintMarker(__func__,"( table_id: " + std::to_string(table_id) + ", key: " + std::to_string(key) + " )");

    pagenum_t root_page_number;
    std::string value;
    int flag;

    // Check if pointer to return is valid
    if (ret_val == NULL || val_size == NULL) return FLAG::FAILURE;

    // Get root page number
    root_page_number = BPT::get_root_page(table_id);
    if (root_page_number == 0) return FLAG::FAILURE;

    // Find the record corresponding to key
    flag = BPT::find(table_id, root_page_number, key, value);
    if (flag) return FLAG::FAILURE;

    // Assign the found value and size
    memset(ret_val, 0, value.size()+1);
    std::copy(value.begin(), value.begin()+value.size(), ret_val);
    *val_size = value.size();

    return FLAG::SUCCESS;
}

// Find the matching record and delete it if found
int db_delete(int64_t table_id, int64_t key)
{
    LogUtil::PrintMarker(__func__,"( table_id: " + std::to_string(table_id) + ", key: " + std::to_string(key) + " )");

    pagenum_t root_page_number;
    int flag;

    // Get root page number
    root_page_number = BPT::get_root_page(table_id);
    if (root_page_number == 0) return FLAG::FAILURE;

    // Delete the record corresponding to key
    flag = BPT::db_delete(table_id, root_page_number, key);
    if (flag) return FLAG::FAILURE;

    return FLAG::SUCCESS;
}

// Initialize database management system
int init_db(int num_buf)
{
    LogUtil::PrintMarker(__func__);

    int flag;

    // Transaction manager initialization
    flag = TRX::init_trx_manager();
    if (flag) return FLAG::FAILURE;

    // Buffer manager initialization
    flag = BUF::init_buffer(num_buf);
    if (flag) return FLAG::FAILURE;

    return FLAG::SUCCESS;
}

// Shutdown database management system
int shutdown_db()
{
    LogUtil::PrintMarker(__func__);

    // Flush buffer data and close all opened table files
    BUF::clear_buffer();
    file_close_table_files();

    return FLAG::SUCCESS;
}


/// Index Manager APIs with Transaction Control
// Read a value in the table with a matching key for the transaction having trx_id
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size, int trx_id)
{
    printf("[INFO][%s][trx_id: %d] ( table_id: %d, key: %d )\n", __func__, trx_id, table_id, key);

    lock_t* lock;
    pagenum_t root_page, key_page;
    std::string value;
    int flag, record_id;

    // Check if pointer to return is valid
    if (ret_val == NULL || val_size == NULL) return FLAG::FAILURE;

    // Get root page number
    root_page = BPT::get_root_page(table_id);
    if (root_page == 0) return FLAG::FAILURE;

    // Find the leaf page number
    key_page = BPT::find_leaf(table_id, root_page, key);
    if (key_page == 0) return FLAG::FAILURE;

    // Acquire lock
    flag = TRX::acquire_lock(table_id, key_page, key, trx_id, LOCK_MODE_SHARED);
    if (flag) return flag;

    // Find the record corresponding to key
    flag = BPT::find(table_id, root_page, key, value, key_page);
    if (flag) return FLAG::SUCCESS;

    // Assign the found value and size
    memset(ret_val, 0, value.size()+1);
    std::copy(value.begin(), value.begin()+value.size(), ret_val);
    *val_size = value.size();

    return FLAG::SUCCESS;
}

// Find the matching key and modify the values
int db_update(int64_t table_id, int64_t key, char* values, uint16_t val_size, uint16_t* old_val_size, int trx_id)
{
    printf("[INFO][%s][trx_id: %d] ( table_id: %d, key: %d )\n", __func__, trx_id, table_id, key);
    
    pagenum_t root_page, key_page;
    std::string value_old, value_new;
    Record record_old;
    char value_char[VALUE_MAX_SIZE+1];
    int flag, record_id;

    // Check if pointer to return is valid
    if (values == NULL || old_val_size == NULL) return FLAG::FAILURE;

    // Convert char* to std::string
    memset(&value_char, 0, val_size+1);
    memcpy(&value_char, values, val_size);
    value_new = std::string(value_char);

    // Get root page number
    root_page = BPT::get_root_page(table_id);
    if (root_page == 0) return FLAG::FAILURE;

    // Find the leaf page number
    key_page = BPT::find_leaf(table_id, root_page, key);
    if (key_page == 0) return FLAG::FAILURE;

    // Acquire lock
    flag = TRX::acquire_lock(table_id, key_page, key, trx_id, LOCK_MODE_EXCLUSIVE);
    if (flag) return flag;

    // Update the record corresponding to key
    flag = BPT::update(table_id, key_page, key, record_old, value_new, trx_id);
    if (flag) return FLAG::FAILURE;

    // Save undo log using old record
    flag = TRX::save_log(trx_id, table_id, key_page, key, record_old.value, record_old.trx_id);
    if (flag) return FLAG::FAILURE;

    // Assign old value size
    *old_val_size = record_old.value.size();

    return FLAG::SUCCESS;
}
