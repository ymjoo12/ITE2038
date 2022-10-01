#include "file.h"

/// Global variables (only used in Disk Space Manager)
TableManager opened_tables;


/// Disk Space Manager APIs
// Open existing database file or create one if not existed.
int64_t file_open_table_file(const char* pathname)
{
    int fd, table_id;
    const int flags = O_RDWR | O_CREAT | O_SYNC;
    const mode_t permission = 0777;

    // Open or create database file
    fd = open(pathname, flags, permission);
    if (fd < 0) {
        std::cout << "[file_open_table_file] Failed to open a file" << std::endl;
        exit(1);
    }

    // Check whether the table file is valid
    if (!FileUtil::is_valid_table_file(fd)) {
        // Re-create database file
        remove(pathname);
        fd = open(pathname, flags, permission);
        FileUtil::init_table_file(fd);
    }

    // Append opened table to opened_tables, dynamically assign and return the table_id
    table_id = opened_tables.push(fd, pathname);
    return table_id;
}

// Allocate an on-disk page from the free page list
pagenum_t file_alloc_page(int64_t table_id)
{
    HeaderPage header_buffer, page_buffer;
    pagenum_t page_number_to_alloc, new_next_free_page_number;
    int fd;

    // Get file descriptor of the table file
    fd = opened_tables.getFileDesc(table_id);
    if (fd < 0) {
        std::cout << "[file_alloc_page] Invalid table ";
        std::cout << "( table_id: " << table_id << " )" << std::endl;
        return 0;
    }

    // Read header page
    FileUtil::read_block(fd, &header_buffer, PAGE_SIZE);
    page_number_to_alloc = header_buffer.next_free_page_number;

    // Extend file size
    if (!page_number_to_alloc) {
        FileUtil::extend_table_file(fd);
        FileUtil::read_block(fd, &header_buffer, PAGE_SIZE);
        page_number_to_alloc = header_buffer.next_free_page_number;
    }

    // Read first free page
    FileUtil::read_block(fd, &page_buffer, PAGE_SIZE, PAGE_SIZE * page_number_to_alloc);
    new_next_free_page_number = page_buffer.next_free_page_number;

    // Update header page
    header_buffer.next_free_page_number = new_next_free_page_number;
    FileUtil::write_block(fd, &header_buffer, PAGE_SIZE);

    // Return and log page number to allocate
    std::cout << "[file_alloc_page] Allocating page success ";
    std::cout << "( page_number: " << page_number_to_alloc << " )" << std::endl;
    return page_number_to_alloc;
}

// Free an on-disk page to the free page list
void file_free_page(int64_t table_id, pagenum_t pagenum)
{
    HeaderPage page_buffer;
    pagenum_t old_next_free_page_number;
    int fd;

    // Get file descriptor of the table file
    fd = opened_tables.getFileDesc(table_id);
    if (fd < 0) {
        std::cout << "[file_free_page] The table doesn't exist ";
        std::cout << "( table_id: " << table_id << " )" << std::endl;
        return;
    }

    // Read header page
    FileUtil::read_block(fd, &page_buffer, PAGE_SIZE);
    old_next_free_page_number = page_buffer.next_free_page_number;

    // Update header page
    page_buffer.next_free_page_number = pagenum;
    FileUtil::write_block(fd, &page_buffer, PAGE_SIZE);

    // Update new free page (initialize reserved space)
    page_buffer = HeaderPage();
    page_buffer.next_free_page_number = old_next_free_page_number;
    FileUtil::write_block(fd, &page_buffer, PAGE_SIZE, PAGE_SIZE * pagenum);

    // Log page number freed
    std::cout << "[file_free_page] Freeing page success ";
    std::cout << "( page_number: " << pagenum << " )" << std::endl;
}

// Read an on-disk page into the in-memory page (dest)
void file_read_page(int64_t table_id, pagenum_t pagenum, page_t* dest)
{
    int fd;

    // Get file descriptor of the table file
    fd = opened_tables.getFileDesc(table_id);
    if (fd < 0) {
        std::cout << "[file_read_page] The table doesn't exist ";
        std::cout << "( table_id: " << table_id << " )" << std::endl;
        return;
    }

    // Read the page from disk to dest
    FileUtil::read_block(fd, dest, PAGE_SIZE, PAGE_SIZE * pagenum);
}

// Write an in-memory page (src) to the on-disk page
void file_write_page(int64_t table_id, pagenum_t pagenum, const page_t* src)
{
    int fd;

    // Get file descriptor of the table file
    fd = opened_tables.getFileDesc(table_id);
    if (fd < 0) {
        std::cout << "[file_write_page] The table doesn't exist ";
        std::cout << "( table_id: " << table_id << " )" << std::endl;
        return;
    }

    // Write the page from src to disk
    FileUtil::write_block(fd, src, PAGE_SIZE, PAGE_SIZE * pagenum);
}

// Stop referencing the database file
void file_close_table_files()
{
    int flag, fd;

    // Close each file descriptor of tables in opened_tables (LIFO)
    while (opened_tables.numOfTables() > 0) {
        fd = opened_tables.pop();
        if (fd < 0) {
            std::cout << "[file_close_table_files] There is no opened table" << std::endl;
            return;
        }
        flag = close(fd);
        if (flag < 0) {
            std::cout << "[file_close_table_files] Failed to close a file" << std::endl;
            exit(1);
        }
    }
}

