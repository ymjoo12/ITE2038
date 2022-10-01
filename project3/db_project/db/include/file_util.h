#ifndef DB_FILE_UTIL_H_
#define DB_FILE_UTIL_H_

/// Includes
#include "page.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string>
#include <iostream>
#include <vector>


/// Table manager for opened tables
class TableManager
{
private:
    // table list
    std::vector<std::pair<int,std::string>> tables;
public:
    // member functions
    int64_t push(int fd, const std::string pathname);
    int pop();
    int getFileDesc(int64_t table_id);
    int numOfTables();
};


/// Utils for Disk Space Manager
namespace FileUtil
{
    // Read a block from disk file
    void read_block(int fd, void* buffer, size_t block_size, off_t offset = 0);

    // Write a block to disk file
    void write_block(int fd, const void* buffer, size_t block_size, off_t offset = 0);

    // Initialize the database file (default size: 10MiB - A header page and 2559 free pages)
    void init_table_file(int fd, pagenum_t num_of_pages = INITIAL_DB_FILE_SIZE / PAGE_SIZE);

    // Double the space of the database file
    void extend_table_file(int fd);

    // Return that file size is invalid value
    bool is_valid_table_file(int fd);
};


#endif  // DB_FILE_UTIL_H_
