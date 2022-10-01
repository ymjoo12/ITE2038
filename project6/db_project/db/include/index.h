#ifndef DB_API_H_
#define DB_API_H_


/// Includes
#include "file.h"
#include "buffer.h"
#include "bpt.h"
#include "trx.h"


/// Index Manager APIs
// Open existing data file using 'pathname' or create one if not existed
int64_t open_table(char* pathname);

// Insert input 'key/value' (record) with its size to data file at the right place
int db_insert(int64_t table_id, int64_t key, char* value, uint16_t val_size);

// Find the record containing input 'key'
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size);

// Find the matching record and delete it if found
int db_delete(int64_t table_id, int64_t key);

// Initialize database management system
int init_db(int num_buf, int flag, int log_num, char* log_path, char* logmsg_path);

// Shutdown database management system
int shutdown_db();


/// Index Manager APIs with Transaction Control
// Read a value in the table with a matching key for the transaction having trx_id
int db_find(int64_t table_id, int64_t key, char* ret_val, uint16_t* val_size, int trx_id);

// Find the matching key and modify the values
int db_update(int64_t table_id, int64_t key, char* values, uint16_t val_size, uint16_t* old_val_size, int trx_id);


#endif  // DB_API_H_
