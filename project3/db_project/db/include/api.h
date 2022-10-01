#ifndef DB_API_H_
#define DB_API_H_


/// Includes
#include "file.h"
#include "buffer.h"
#include "bpt.h"


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
int init_db(int num_buf);

// Shutdown database management system
int shutdown_db();


#endif  // DB_API_H_
