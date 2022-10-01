#include "file.h"
#include "test_util.h"
#include <gtest/gtest.h>
using namespace std;


/// Tests

// 1. File Initialization
/*
 * Tests file open/close APIs
 */
TEST(FileInitTest, CheckNumOfPages)
{
    remove(TestUtil::TEST_FILE_PATH.c_str());

    int64_t table_id = file_open_table_file(TestUtil::TEST_FILE_PATH.c_str());
    int table_file = opened_tables.getFileDesc(table_id);
    ASSERT_GE(table_file, 0);
    EXPECT_TRUE(TestUtil::IsValidOpenedFile(table_id));

    EXPECT_EQ(TestUtil::GetFileSize(table_id), INITIAL_DB_FILE_SIZE);
    EXPECT_EQ(TestUtil::GetNumberOfPages(table_id), INITIAL_DB_FILE_SIZE / PAGE_SIZE);

    file_close_table_files();
    EXPECT_TRUE(TestUtil::IsValidClosedFile(table_id));

    ASSERT_EQ(remove(TestUtil::TEST_FILE_PATH.c_str()),0);
}

TEST(FileInitTest, CheckManyTables)
{
    remove(TestUtil::TEST_FILE_PATH.c_str());

    std::string filepath;
    int64_t table_id;
    int table_file;
    const int NUM_OF_TABLES = 128;
    const bool verbose = false;

    for (int num = 1; num <= NUM_OF_TABLES; num++) {
        filepath = TestUtil::TEST_FILE_PATH.c_str() + to_string(num);
        table_id = file_open_table_file(filepath.c_str());
        table_id = file_open_table_file(filepath.c_str());
        table_file = opened_tables.getFileDesc(table_id);
        ASSERT_GE(table_file, 0);
        EXPECT_TRUE(TestUtil::IsValidOpenedFile(table_id));

        EXPECT_EQ(TestUtil::GetFileSize(table_id), INITIAL_DB_FILE_SIZE);
        EXPECT_EQ(TestUtil::GetNumberOfPages(table_id), INITIAL_DB_FILE_SIZE / PAGE_SIZE);

        if (!verbose) continue;

        cout << "[creating a table: " << num << "]\n";
        cout << "table_id: " << table_id << "\n";
        cout << "filepath: " << filepath << "\n";
        cout << "fd: " << table_file << "\n";
    }

    EXPECT_EQ(opened_tables.numOfTables(),NUM_OF_TABLES);

    file_close_table_files();

    for (table_id = 0; table_id < NUM_OF_TABLES; table_id++) {
        EXPECT_TRUE(TestUtil::IsValidClosedFile(table_id));
        filepath = TestUtil::TEST_FILE_PATH.c_str() + to_string(table_id+1);
        ASSERT_EQ(remove(filepath.c_str()),0);
    }
}


// 2. Page Management
class PageManagementTest : public ::testing::Test {
protected:
    int64_t table_id;
    void SetUp() override {
        remove(TestUtil::TEST_FILE_PATH.c_str());
        table_id = file_open_table_file(TestUtil::TEST_FILE_PATH.c_str());
        int table_file = opened_tables.getFileDesc(table_id);
        ASSERT_GT(table_file, 0);
        EXPECT_TRUE(TestUtil::IsValidOpenedFile(table_id));
    }
    void TearDown() override {
        file_close_table_files();
        EXPECT_TRUE(TestUtil::IsValidClosedFile(table_id));
    }
};

/* 
 * Tests file alloc/free APIs.
 */
TEST_F(PageManagementTest, CheckFileAllocFree)
{
    int next_free_page, allocated_page, freed_page;

    next_free_page = TestUtil::GetFirstFreePageNumber(table_id);
    allocated_page = file_alloc_page(table_id);
    EXPECT_EQ(next_free_page, allocated_page);
    EXPECT_FALSE(TestUtil::IsFreedPage(table_id, allocated_page));

    next_free_page = TestUtil::GetFirstFreePageNumber(table_id);
    freed_page = file_alloc_page(table_id);
    EXPECT_EQ(next_free_page, freed_page);
    EXPECT_FALSE(TestUtil::IsFreedPage(table_id, freed_page));

    EXPECT_NE(allocated_page, freed_page);

    file_free_page(table_id, freed_page);
    next_free_page = TestUtil::GetFirstFreePageNumber(table_id);
    EXPECT_EQ(next_free_page, freed_page);
    EXPECT_TRUE(TestUtil::IsFreedPage(table_id, freed_page));
}

/* 
 * Tests file alloc APIs when the file size needs to extend.
 */
TEST_F(PageManagementTest, CheckFileExtend)
{
    int old_num_of_pages, old_file_size, new_num_of_pages, new_file_size;

    old_num_of_pages = TestUtil::GetNumberOfPages(table_id);
    old_file_size = TestUtil::GetFileSize(table_id);
    EXPECT_EQ(old_num_of_pages, INITIAL_DB_FILE_SIZE / PAGE_SIZE);
    EXPECT_EQ(old_file_size, INITIAL_DB_FILE_SIZE);

    for (int i = 0; i < old_num_of_pages * 3/2; i++) {
        file_alloc_page(table_id);
    }

    new_num_of_pages = TestUtil::GetNumberOfPages(table_id);
    new_file_size = TestUtil::GetFileSize(table_id);
    EXPECT_EQ(new_num_of_pages, old_num_of_pages * 2);
    EXPECT_EQ(new_file_size, old_file_size * 2);
}


// 3. Page I/O
class PageIOTest : public ::testing::Test {
protected:
    int64_t table_id;
    pagenum_t allocated_page_number;
    const char TEST_VALUE = 'a';
    void SetUp() override {
        remove(TestUtil::TEST_FILE_PATH.c_str());
        table_id = file_open_table_file(TestUtil::TEST_FILE_PATH.c_str());
        int table_file = opened_tables.getFileDesc(table_id);
        ASSERT_GT(table_file, 0);
        EXPECT_TRUE(TestUtil::IsValidOpenedFile(table_id));
        allocated_page_number = file_alloc_page(table_id);
        EXPECT_FALSE(TestUtil::IsFreedPage(table_id, allocated_page_number));
    }
    void TearDown() override {
        file_close_table_files();
        EXPECT_TRUE(TestUtil::IsValidClosedFile(table_id));
    }
};

/* 
 * Tests file read/write page APIs (in-memory page type: char array)
 */
TEST_F(PageIOTest, CheckWriteArray)
{
    char page1[PAGE_SIZE], page2[PAGE_SIZE];

    std::fill(page1, page1 + PAGE_SIZE, TEST_VALUE);
    file_write_page(table_id, allocated_page_number, (page_t*)&page1);

    file_read_page(table_id, allocated_page_number, (page_t*)&page2);

    for (int byte = 0; byte < PAGE_SIZE; byte++) {
        EXPECT_EQ(page1[byte], page2[byte]);
    }
}

/* 
 * Tests file read/write page APIs (in-memory page type: HeaderPage structure)
 */
TEST_F(PageIOTest, CheckWriteStruct)
{
    HeaderPage page1, page2;

    page1.next_free_page_number = 1;
    page1.num_of_pages = 1;
    page1.root_page_number = 1;
    std::fill(page1.reserved, page1.reserved + PAGE_SIZE - 24, TEST_VALUE);
    file_write_page(table_id, allocated_page_number, (page_t*)&page1);

    file_read_page(table_id, allocated_page_number, (page_t*)&page2);

    EXPECT_EQ(page1.next_free_page_number, page2.next_free_page_number);
    EXPECT_EQ(page1.num_of_pages, page2.num_of_pages);
    EXPECT_EQ(page1.root_page_number, page2.root_page_number);
    for (int byte = 0; byte < PAGE_SIZE - 24; byte++) {
        EXPECT_EQ(page1.reserved[byte], page2.reserved[byte]);
    }
}

