#include "file_util.h"

/// Table manager for opened tables
// push a table file info to table list
int64_t TableManager::push(int fd, const std::string pathname)
{
    int64_t table_id;

    // check given file is already opened file
    for (table_id = 0; table_id < this->tables.size(); table_id++) {
        if (this->tables[table_id].second == pathname) {
            return table_id;
        }
    }

    // push back the given file to table list and return the table id (list index)
    table_id = this->tables.size();
    this->tables.push_back({fd, pathname});
    return table_id;
}

// pop the last table file info from table list and return the file descriptor
int TableManager::pop()
{
    // if table list is empty, return -1
    if (this->tables.size() == 0) return -1;

    // pop and return the last table of the list
    int fd = this->tables.back().first;
    this->tables.pop_back();
    return fd;
}

// return the file descriptor of given table
int TableManager::getFileDesc(int64_t table_id)
{
    // if given table id is invalid, return -1
    if (table_id >= this->tables.size() || table_id < 0) return -1;

    // return the file descriptor of given table id
    int fd = this->tables[table_id].first;
    return fd;
}

// return the number of tables in table list
int TableManager::numOfTables()
{
    // return the size of table list
    int num_of_tables = this->tables.size();
    return num_of_tables;
}


/// Utils for Disk Space Manager
namespace FileUtil
{
    // Read a block from disk file
    void read_block(int fd, void* buffer, size_t block_size, off_t offset)
    {
        int flag;

        flag = pread(fd, buffer, block_size, offset);
        if (flag == 0) {
            std::cout << "[read_block] Tried to read the end of the file" << std::endl;
            exit(1);
        }
        if (flag != block_size || flag < 0) {
            std::cout << "[read_block] File read failed" << std::endl;
            exit(1);
        }
    }

    // Write a block to disk file
    void write_block(int fd, const void* buffer, size_t block_size, off_t offset)
    {
        int flag;

        flag = pwrite(fd, buffer, block_size, offset);
        if (flag != block_size || flag < 0) {
            std::cout << "[write_block] File write failed" << std::endl;
            exit(1);
        }
        
        flag = fsync(fd);
        if (flag < 0) {
            std::cout << "[write_block] File sync failed" << std::endl;
            exit(1);
        }
    }

    // Initialize the table file (initial size: 10MiB - A header page and 2559 free pages)
    void init_table_file(int fd, pagenum_t num_of_pages)
    {
        HeaderPage page_buffer;

        // Initialize header page
        page_buffer.num_of_pages = num_of_pages;
        page_buffer.next_free_page_number = 1;
        page_buffer.root_page_number = 0;
        write_block(fd, &page_buffer, PAGE_SIZE);

        // Initialize free pages
        page_buffer.num_of_pages = 0;
        for (int page_number = 1; page_number < num_of_pages; page_number++) {
            page_buffer.next_free_page_number = (page_number + 1) % num_of_pages;
            write_block(fd, &page_buffer, PAGE_SIZE, PAGE_SIZE * page_number);
        }
    }

    // Double the space of the table file
    void extend_table_file(int fd)
    {
        HeaderPage page_buffer;
        pagenum_t old_next_free_page_number;
        pagenum_t old_num_of_pages, new_num_of_pages;

        // Read header page
        read_block(fd, &page_buffer, PAGE_SIZE);
        old_next_free_page_number = page_buffer.next_free_page_number;
        old_num_of_pages = page_buffer.num_of_pages;
        new_num_of_pages = old_num_of_pages * 2;

        // Update header page
        page_buffer.next_free_page_number = old_num_of_pages;
        page_buffer.num_of_pages = new_num_of_pages;
        write_block(fd, &page_buffer, PAGE_SIZE);

        // Append free pages
        page_buffer.num_of_pages = 0;
        for (int page_number = old_num_of_pages; page_number < new_num_of_pages - 1; page_number++) {
            page_buffer.next_free_page_number = page_number + 1;
            write_block(fd, &page_buffer, PAGE_SIZE, PAGE_SIZE * page_number);
        }
        page_buffer.next_free_page_number = old_next_free_page_number;
        write_block(fd, &page_buffer, PAGE_SIZE, PAGE_SIZE * (new_num_of_pages - 1));
    }

    // Return that file size is invalid value
    bool is_valid_table_file(int fd)
    {
        HeaderPage header;
        off_t fsize;

        // Get the size of the database file
        fsize = lseek(fd, 0, SEEK_END);
        if (fsize < PAGE_SIZE || (fsize % PAGE_SIZE) != 0) return false;

        // Read header page
        read_block(fd, &header, PAGE_SIZE);

        // Return if file size is invalid
        return header.num_of_pages * PAGE_SIZE == fsize;
    }

    // Return that the page number is valid
    bool is_valid_page_number(int fd, pagenum_t page_number)
    {
        HeaderPage header;

        // Read header page
        read_block(fd, &header, PAGE_SIZE);

        // Return if this page is exist
        return page_number >= 0 && page_number < header.num_of_pages;
    }
}
