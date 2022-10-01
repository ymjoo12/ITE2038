#ifndef DB_FILE_H_
#define DB_FILE_H_

/// Includes
#include "page.h"
#include "file_util.h"


/// Global variables
extern TableManager opened_tables;


/// Disk Space Manager APIs
// Open existing database file or create one if it doesn't exist
int64_t file_open_table_file(const char* pathname);

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id);

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum);

// Read an on-disk page into the in-memory page structure(dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest);

// Write an in-memory page(src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src);

// Close the database file
void file_close_table_files();


#endif  // DB_FILE_H_
