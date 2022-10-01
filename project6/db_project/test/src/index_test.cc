#include "index.h"
#include "bpt.h"
#include "test_util.h"
#include <gtest/gtest.h>


/// Types
using TRecord = std::pair<int64_t, std::string>;


/// Settings
const bool START_FROM_EMPTY = true;
const bool UPDATE_DB_FILE = true;
const int NUM_KEY = 2e4;
const int BUFFER_SIZE = 15;
const std::string DB_FILE_PATH = "test00.db";


/// SetUp and TearDown
class DBTest : public ::testing::Test {
public:
    const bool init = START_FROM_EMPTY;
    const bool update = UPDATE_DB_FILE;

    int64_t table_id;
    pagenum_t page_number;

    void SetUp() override {
        ASSERT_EQ(init_db(BUFFER_SIZE, 0, 100, "logfile.data", "logmsg.txt"),0);
        remove(TestUtil::TEST_FILE_PATH.c_str());
        if (!init) TestUtil::LoadDB(DB_FILE_PATH);
        table_id = open_table(const_cast<char*>(TestUtil::TEST_FILE_PATH.c_str()));
        EXPECT_TRUE(TestUtil::IsValidOpenedFile(table_id));
    }

    void TearDown() override {
        EXPECT_EQ(shutdown_db(),0);
        EXPECT_TRUE(TestUtil::IsValidClosedFile(table_id));
        if (init || update) TestUtil::SaveDB(DB_FILE_PATH);
    }
};


/// Utility
namespace DBTestOperation
{
    void SequencialInsertionTest(int64_t table_id, int64_t first, int64_t last, int trx_id)
    {
        static const char* MAX_LEN_DATA = "123456789_123456789_123456789_123456789_123456789_123456789_123456789_123456789_123456789_123456789_12345678";
        static const char* MIN_LEN_DATA = "123456789_123456789_123456789_123456789_123456";

        int flag = 0;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        std::cout << "\n<< table id: " << table_id << " >> " << std::endl;
        for (int key = first; key <= last; key++) {
            std::cout << "\n<< current key: " << key << " >> " << std::endl;
            if (key%2) {
                flag = db_insert(table_id, key, const_cast<char*>(MAX_LEN_DATA), VALUE_MAX_SIZE);
                EXPECT_EQ(flag, 0);
                flag = db_insert(table_id, key, const_cast<char*>(MAX_LEN_DATA), VALUE_MAX_SIZE);
                EXPECT_EQ(flag, 1);
                flag = db_find(table_id, key, ret_val, &val_size, trx_id);
                EXPECT_EQ(flag, 0);
                EXPECT_EQ(string(ret_val), std::string(MAX_LEN_DATA));
                EXPECT_EQ(val_size, VALUE_MAX_SIZE);
                flag = db_find(table_id, key - 2'000'000, ret_val, &val_size, trx_id);
                EXPECT_EQ(flag, 1);
            } else {
                flag = db_insert(table_id, key, const_cast<char*>(MIN_LEN_DATA), VALUE_MIN_SIZE);
                EXPECT_EQ(flag, 0);
                flag = db_insert(table_id, key, const_cast<char*>(MIN_LEN_DATA), VALUE_MIN_SIZE);
                EXPECT_EQ(flag, 1);
                flag = db_find(table_id, key, ret_val, &val_size, trx_id);
                EXPECT_EQ(flag, 0);
                EXPECT_EQ(string(ret_val), std::string(MIN_LEN_DATA));
                EXPECT_EQ(val_size, VALUE_MIN_SIZE);
                flag = db_find(table_id, key - 2'000'000, ret_val, &val_size, trx_id);
                EXPECT_EQ(flag, 1);
            }
        }
    }

    void RandomInsertionTest(int64_t table_id, int max_count, int64_t min_key = numeric_limits<int64_t>::min(), int64_t max_key = numeric_limits<int64_t>::max())
    {
        int flag_find, flag_insert, trx_id = 1;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        int64_t key_to_insert;
        std::string value_to_insert;

        std::cout << "\n<< table id: " << table_id << " >> " << std::endl;
        for (int count = 0; count < max_count;) {
            value_to_insert = TestUtil::GenRandomValue();
            key_to_insert = TestUtil::GenRandomKey(min_key, max_key);

            flag_find = db_find(table_id, key_to_insert, ret_val, &val_size, trx_id);
            flag_insert = db_insert(table_id, key_to_insert, const_cast<char*>(value_to_insert.c_str()), value_to_insert.size());

            std::cout << "\n<< key: " << key_to_insert << " >> " << std::endl;
            std::cout << "<< value: \"" << value_to_insert << "\" >> " << std::endl;
            if (value_to_insert.size() < VALUE_MIN_SIZE || value_to_insert.size() > VALUE_MAX_SIZE) {
                EXPECT_EQ(flag_insert, 1);
                std::cout << "<< INVALID SIZE >>" << std::endl;
            } else if (flag_find == 0) {
                EXPECT_EQ(flag_insert, 1);
                std::cout << "<< ALREADY EXIST >>" << std::endl;
            } else if (flag_find == 1) {
                EXPECT_EQ(flag_insert, 0);

                flag_find = db_find(table_id, key_to_insert, ret_val, &val_size, trx_id);
                EXPECT_EQ(flag_find, 0);
                EXPECT_EQ(value_to_insert, std::string(ret_val));
                EXPECT_EQ(value_to_insert.size(), val_size);

                std::cout << "<< INSERT ( count: " << ++count << " ) >>" << std::endl;
            }
        }
    }

    void SequencialDeletionTest(int64_t table_id, int64_t first, int64_t last, int trx_id, bool reverse = false)
    {
        int flag_find, flag_delete;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;
        int64_t key;

        std::cout << "\n<< table id: " << table_id << " >> " << std::endl;

        key = reverse ? last : first;
        while (key <= last && key >= first) {
            std::cout << "\n<< current key: " << key << " >> " << std::endl;

            flag_find = db_find(table_id, key, ret_val, &val_size, trx_id);
            flag_delete = db_delete(table_id, key);
            EXPECT_EQ(flag_find, flag_delete);
            flag_find = db_find(table_id, key, ret_val, &val_size, trx_id);
            EXPECT_EQ(flag_find, 1);
            flag_delete = db_delete(table_id, key);
            EXPECT_EQ(flag_find, 1);

            if (!reverse) key++;
            else key--;
        }
    }

    void RandomDeletionTest(int64_t table_id, int max_count, int64_t min_key = -1e6, int64_t max_key = 1e6)
    {
        int flag_find, flag_delete;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        int64_t key_to_delete;

        std::cout << "\n<< table id: " << table_id << " >> " << std::endl;
        for (int count = 0; count < max_count;) {
            key_to_delete = TestUtil::GenRandomKey(min_key, max_key);

            flag_find = db_find(table_id, key_to_delete, ret_val, &val_size);
            flag_delete = db_delete(table_id, key_to_delete);
            EXPECT_EQ(flag_find, flag_delete);
            if (flag_find) continue;
            else {
                std::cout << "\n<< key: " << key_to_delete << " >> " << std::endl;
                std::cout << "<< DELETE SUCCESS ( count: " << ++count << " ) >>" << std::endl;
                flag_find = db_find(table_id, key_to_delete, ret_val, &val_size);
                EXPECT_EQ(flag_find, 1);
            }
        }
    }

    void QueueInsertionTest(int64_t table_id, std::queue<TRecord>& records, int trx_id, int max_count = 0)
    {
        int64_t min_key = numeric_limits<int64_t>::min();
        int64_t max_key = numeric_limits<int64_t>::max();

        int flag_find, flag_insert;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        int64_t key_to_insert;
        std::string value_to_insert;

        std::cout << "\n<< table id: " << table_id << " >> " << std::endl;
        for (int count = 0; count < max_count;) {
            value_to_insert = TestUtil::GenRandomValue();
            key_to_insert = TestUtil::GenRandomKey(min_key, max_key);

            std::cout << "\n<< key: " << key_to_insert << " >> " << std::endl;
            std::cout << "<< value: \"" << value_to_insert << "\" >> " << std::endl;

            flag_find = db_find(table_id, key_to_insert, ret_val, &val_size, trx_id);
            flag_insert = db_insert(table_id, key_to_insert, const_cast<char*>(value_to_insert.c_str()), value_to_insert.size());

            if (value_to_insert.size() < VALUE_MIN_SIZE || value_to_insert.size() > VALUE_MAX_SIZE) {
                EXPECT_EQ(flag_insert, 1);
                std::cout << "<< INVALID SIZE >>" << std::endl;
            } else if (flag_find == 0) {
                EXPECT_EQ(flag_insert, 1);
                std::cout << "<< ALREADY EXIST >>" << std::endl;
            } else if (flag_find == 1) {
                EXPECT_EQ(flag_insert, 0);

                flag_find = db_find(table_id, key_to_insert, ret_val, &val_size, trx_id);
                EXPECT_EQ(flag_find, 0);
                EXPECT_EQ(value_to_insert, std::string(ret_val));
                EXPECT_EQ(value_to_insert.size(), val_size);

                if (flag_insert == 0 && flag_find == 0) {
                    std::cout << "<< INSERT SUCCESS ( count: " << ++count << " ) >>" << std::endl;
                    records.push({key_to_insert, value_to_insert});
                }
            }
        }
    }

    void QueueInsertionTestAdv(int64_t table_id, std::queue<TRecord>& records, int trx_id, int max_count = 0)
    {
        bool initial = records.empty();

        int64_t min_key = numeric_limits<int64_t>::min();
        int64_t max_key = numeric_limits<int64_t>::max();

        int flag_find, flag_insert;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        int64_t key_to_insert;
        std::string value_to_insert;

        std::cout << "\n<< table id: " << table_id << " >> " << std::endl;

        for (int count = 0; count < max_count;) {
            if (!initial) {
                std::tie(key_to_insert, value_to_insert) = records.front();
                records.pop();
            } else {
                value_to_insert = TestUtil::GenRandomValue();
                key_to_insert = TestUtil::GenRandomKey(min_key, max_key);
            }

            std::cout << "\n<< key: " << key_to_insert << " >> " << std::endl;
            std::cout << "<< value: \"" << value_to_insert << "\" >> " << std::endl;

            flag_find = db_find(table_id, key_to_insert, ret_val, &val_size, trx_id);
            flag_insert = db_insert(table_id, key_to_insert, const_cast<char*>(value_to_insert.c_str()), value_to_insert.size());

            if (value_to_insert.size() < VALUE_MIN_SIZE || value_to_insert.size() > VALUE_MAX_SIZE) {
                EXPECT_EQ(flag_insert, 1);
                std::cout << "<< INVALID SIZE >>" << std::endl;
                ASSERT_TRUE(initial);
            } else if (flag_find == 0) {
                EXPECT_EQ(flag_insert, 1);
                std::cout << "<< ALREADY EXIST >>" << std::endl;
                ASSERT_TRUE(initial);
            } else if (flag_find == 1) {
                EXPECT_EQ(flag_insert, 0);

                flag_find = db_find(table_id, key_to_insert, ret_val, &val_size, trx_id);
                EXPECT_EQ(flag_find, 0);
                EXPECT_EQ(value_to_insert, std::string(ret_val));
                EXPECT_EQ(value_to_insert.size(), val_size);

                if (flag_insert == 0 && flag_find == 0) {
                    std::cout << "<< INSERT SUCCESS ( count: " << ++count << " ) >>" << std::endl;
                    records.push({key_to_insert, value_to_insert});
                }
            }
        }
    }

    void QueueFindTest(int64_t table_id, std::queue<TRecord>& records, int trx_id, int max_count = 0)
    {
        int flag_find;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        int64_t key_to_find;
        std::string value_to_find;
        if (max_count == 0) max_count = records.size();

        std::cout << "\n<< table id: " << table_id << " >> " << std::endl;
        for (int count = 0; count < max_count && !records.empty(); count++) {
            std::tie(key_to_find, value_to_find) = records.front();
            records.pop();
            records.push({key_to_find, value_to_find});

            std::cout << "\n<< key: " << key_to_find << " >> " << std::endl;

            flag_find = db_find(table_id, key_to_find, ret_val, &val_size, trx_id);
            EXPECT_EQ(flag_find, 0);
            EXPECT_EQ(value_to_find, std::string(ret_val));

            if (flag_find) {
                std::cout << "<< FIND FAILED ( count: " << count << " ) >>" << std::endl;
                DebugUtil::PrintTree(table_id);
            } else {
                std::cout << "<< FIND SUCCESS ( count: " << count << " ) >>" << std::endl;
            }
        }
    }

    void QueueUpdateTest(int64_t table_id, std::queue<TRecord>& records, int trx_id, int max_count = 0)
    {
        int flag_find, flag_update;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size, old_val_size;

        std::string value_to_update;
        int64_t key_to_update;
        if (max_count == 0) max_count = records.size();

        std::cout << "\n<< table id: " << table_id << " >> " << std::endl;
        for (int count = 0; count < max_count && !records.empty(); count++) {
            std::tie(key_to_update, value_to_update) = records.front();
            records.pop();

            std::cout << "\n<< key: " << key_to_update << " >> " << std::endl;

            flag_find = db_find(table_id, key_to_update, ret_val, &val_size, trx_id);
            EXPECT_EQ(flag_find, 0);
            EXPECT_NE(val_size, 0);
            value_to_update = TestUtil::GenRandomValue(val_size);
            records.push({key_to_update, value_to_update});

            flag_update = db_update(table_id, key_to_update, const_cast<char*>(value_to_update.c_str()), value_to_update.size(), &old_val_size, trx_id);
            EXPECT_EQ(flag_update, 0);

            flag_find = db_find(table_id, key_to_update, ret_val, &val_size, trx_id);
            EXPECT_EQ(flag_find, 0);
            EXPECT_EQ(std::string(ret_val), value_to_update);

            if (flag_update) {
                std::cout << "<< UPDATE FAILED ( count: " << count << " ) >>" << std::endl;
                DebugUtil::PrintTree(table_id);
            } else {
                std::cout << "<< UPDATE SUCCESS ( count: " << count << " ) >>" << std::endl;
            }
        }
    }

    void QueueDeletionTest(int64_t table_id, std::queue<TRecord>& records, int trx_id, int max_count = 0)
    {
        int flag_find, flag_delete;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        int64_t key_to_delete;
        std::string value_to_delete;
        if (max_count == 0) max_count = records.size();

        std::cout << "\n<< table id: " << table_id << " >> " << std::endl;
        for (int count = 0; count < max_count && !records.empty(); count++) {
            std::tie(key_to_delete, value_to_delete) = records.front();
            records.pop();

            std::cout << "\n<< key: " << key_to_delete << " >> " << std::endl;

            flag_find = db_find(table_id, key_to_delete, ret_val, &val_size, trx_id);
            flag_delete = db_delete(table_id, key_to_delete);
            EXPECT_EQ(flag_find, flag_delete);
            if (flag_delete) {
                std::cout << "<< DELETE FAILED ( count: " << count << " ) >>" << std::endl;
                DebugUtil::PrintTree(table_id);
            } else {
                flag_find = db_find(table_id, key_to_delete, ret_val, &val_size, trx_id);
                EXPECT_EQ(flag_find, 1);
                std::cout << "<< DELETE SUCCESS ( count: " << count << " ) >>" << std::endl;
            }
        }
    }

    void RandomOperationTest(int64_t table_id, int max_count, int64_t min_key = -1e6, int64_t max_key = 1e6)
    {
        int flag_find, flag_insert, flag_delete;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        int64_t random_key;
        std::string value_to_insert;

        std::cout << "\n<< table id: " << table_id << " >> " << std::endl;
        for (int count = 0; count < max_count; count++) {
            value_to_insert = TestUtil::GenRandomValue();
            random_key = TestUtil::GenRandomKey(min_key, max_key);
            std::cout << "\n<< key: " << random_key << " >> " << std::endl;

            flag_find = db_find(table_id, random_key, ret_val, &val_size);
            if (flag_find) {
                std::cout << "\n<< NOT FOUND - INSERT >> " << std::endl;
                flag_delete = db_delete(table_id, random_key);
                EXPECT_EQ(flag_delete, 1);
                flag_insert = db_insert(table_id, random_key, const_cast<char*>(value_to_insert.c_str()), value_to_insert.size());
                if (value_to_insert.size() >= VALUE_MIN_SIZE && value_to_insert.size() <= VALUE_MAX_SIZE) {
                    EXPECT_EQ(flag_insert, 0);
                    flag_find = db_find(table_id, random_key, ret_val, &val_size);
                    EXPECT_EQ(flag_find, 0);
                    EXPECT_EQ(value_to_insert, std::string(ret_val));
                    EXPECT_EQ(value_to_insert.size(), val_size);
                } else {
                    EXPECT_EQ(flag_insert, 1);
                }
            } else {
                std::cout << "\n<< FOUND - DELETE >> " << std::endl;
                flag_insert = db_insert(table_id, random_key, const_cast<char*>(value_to_insert.c_str()), value_to_insert.size());
                EXPECT_EQ(flag_insert, 1);
                flag_delete = db_delete(table_id, random_key);
                EXPECT_EQ(flag_delete, 0);
                flag_find = db_find(table_id, random_key, ret_val, &val_size);
                EXPECT_EQ(flag_find, 1);
            }
        }
    }
}


/// Tests

// One table test
TEST_F(DBTest, SequencialKeyTest)
{
    int num_records = NUM_KEY;
    int trx_id = trx_begin();

    ASSERT_GT(trx_id, 0);
    ASSERT_EQ(BPT::get_root_page(table_id), 0);

    // Insert and find records
    DBTestOperation::SequencialInsertionTest(table_id, 1, num_records, trx_id);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);

    // Delete and find records
    DBTestOperation::SequencialDeletionTest(table_id, 1, num_records, trx_id, true);
    EXPECT_EQ(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);

    // Commit transaction
    EXPECT_EQ(trx_commit(trx_id), trx_id);
}

TEST_F(DBTest, RandomKeyTest01)
{
    int num_records = NUM_KEY;
    std::queue<TRecord> records;
    int trx_id = trx_begin();

    ASSERT_GT(trx_id, 0);
    ASSERT_EQ(records.size(), 0);
    ASSERT_EQ(BPT::get_root_page(table_id), 0);

    // Insert records
    DBTestOperation::QueueInsertionTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);

    // Find records
    DBTestOperation::QueueFindTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);
    
    // Delete records
    DBTestOperation::QueueDeletionTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), 0);
    EXPECT_EQ(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);

    // Commit transaction
    EXPECT_EQ(trx_commit(trx_id), trx_id);
}

TEST_F(DBTest, RandomKeyTest02)
{
    int num_records = std::max(NUM_KEY,2);;
    int half = num_records/2;
    std::queue<TRecord> records;
    int trx_id = trx_begin();

    ASSERT_GT(trx_id, 0);
    ASSERT_EQ(records.size(), 0);
    ASSERT_EQ(BPT::get_root_page(table_id), 0);

    // Insert records
    DBTestOperation::QueueInsertionTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DBTestOperation::QueueFindTest(table_id, records, trx_id, num_records);
    DebugUtil::PrintTree(table_id);

    // Delete half of records
    DBTestOperation::QueueDeletionTest(table_id, records, trx_id, half);
    ASSERT_EQ(records.size(), num_records - half);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DBTestOperation::QueueFindTest(table_id, records, trx_id, num_records - half);
    DebugUtil::PrintTree(table_id);

    // Insert records again
    DBTestOperation::QueueInsertionTest(table_id, records, trx_id, half);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DBTestOperation::QueueFindTest(table_id, records, trx_id, num_records);
    DebugUtil::PrintTree(table_id);

    // Delete records again
    DBTestOperation::QueueDeletionTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), 0);
    EXPECT_EQ(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);

    // Commit transaction
    EXPECT_EQ(trx_commit(trx_id), trx_id);
}

TEST_F(DBTest, RandomKeyTest03)
{
    int num_records = NUM_KEY;
    std::queue<TRecord> records;
    int trx_id = trx_begin();

    ASSERT_GT(trx_id, 0);
    ASSERT_EQ(records.size(), 0);
    ASSERT_EQ(BPT::get_root_page(table_id), 0);

    // Insert records
    DBTestOperation::QueueInsertionTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);

    // Find records
    DBTestOperation::QueueFindTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);

    // Update records
    DBTestOperation::QueueUpdateTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);

    // Find records
    DBTestOperation::QueueFindTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);

    // Delete records
    DBTestOperation::QueueDeletionTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), 0);
    EXPECT_EQ(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);

    // Commit transaction
    EXPECT_EQ(trx_commit(trx_id), trx_id);
}

TEST_F(DBTest, RandomKeyTest04)
{
    int num_records = std::max(NUM_KEY,2);
    int half = num_records/2;
    std::queue<TRecord> records;
    int trx_id = trx_begin();

    ASSERT_GT(trx_id, 0);
    ASSERT_EQ(records.size(), 0);
    ASSERT_EQ(BPT::get_root_page(table_id), 0);

    // Insert records
    DBTestOperation::QueueInsertionTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DBTestOperation::QueueFindTest(table_id, records, trx_id, num_records);
    DebugUtil::PrintTree(table_id);

    // Update half of records
    DBTestOperation::QueueUpdateTest(table_id, records, trx_id, half);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DBTestOperation::QueueFindTest(table_id, records, trx_id, num_records);
    DebugUtil::PrintTree(table_id);

    // Delete half of records
    DBTestOperation::QueueDeletionTest(table_id, records, trx_id, half);
    ASSERT_EQ(records.size(), num_records - half);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DBTestOperation::QueueFindTest(table_id, records, trx_id, num_records - half);
    DebugUtil::PrintTree(table_id);

    // Insert records again
    DBTestOperation::QueueInsertionTest(table_id, records, trx_id, half);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DBTestOperation::QueueFindTest(table_id, records, trx_id, num_records);
    DebugUtil::PrintTree(table_id);

    // Update records again
    DBTestOperation::QueueUpdateTest(table_id, records, trx_id, half);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DBTestOperation::QueueFindTest(table_id, records, trx_id, num_records);
    DebugUtil::PrintTree(table_id);

    // Delete records again
    DBTestOperation::QueueDeletionTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), 0);
    EXPECT_EQ(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);

    // Commit transaction
    EXPECT_EQ(trx_commit(trx_id), trx_id);
}

// One table re-load test
// Init and shutdown DB Test with setting buffer size
void InitTest(int buffer_size = -1)
{
    int num_records = NUM_KEY, trx_id;
    std::queue<TRecord> records;
    int64_t table_id;

    // Init DB
    ASSERT_EQ(init_db(buffer_size == -1 ? 1 : buffer_size, 0, 100, "logfile.data", "logmsg.txt"),0);
    remove(TestUtil::TEST_FILE_PATH.c_str());
    table_id = open_table(const_cast<char*>(TestUtil::TEST_FILE_PATH.c_str()));
    EXPECT_TRUE(TestUtil::IsValidOpenedFile(table_id));
    ASSERT_EQ(table_id, 0);

    // Insert records
    trx_id = trx_begin();
    ASSERT_GT(trx_id, 0);
    DBTestOperation::QueueInsertionTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);
    EXPECT_EQ(trx_commit(trx_id), trx_id);

    // Shutdown DB
    EXPECT_EQ(shutdown_db(),0);
    EXPECT_TRUE(TestUtil::IsValidClosedFile(table_id));
    TestUtil::SaveDB(DB_FILE_PATH);


    // Init DB
    ASSERT_EQ(init_db(buffer_size == -1 ? 1 : buffer_size, 0, 100, "logfile.data", "logmsg.txt"),0);
    remove(TestUtil::TEST_FILE_PATH.c_str());
    TestUtil::LoadDB(DB_FILE_PATH);
    table_id = open_table(const_cast<char*>(TestUtil::TEST_FILE_PATH.c_str()));
    EXPECT_TRUE(TestUtil::IsValidOpenedFile(table_id));
    ASSERT_EQ(table_id, 0);

    // Update records
    trx_id = trx_begin();
    ASSERT_GT(trx_id, 0);
    DBTestOperation::QueueUpdateTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);
    EXPECT_EQ(trx_commit(trx_id), trx_id);

    // Shutdown DB
    EXPECT_EQ(shutdown_db(),0);
    EXPECT_TRUE(TestUtil::IsValidClosedFile(table_id));
    TestUtil::SaveDB(DB_FILE_PATH);


    // Init DB
    ASSERT_EQ(init_db(buffer_size == -1 ? 2 : buffer_size, 0, 100, "logfile.data", "logmsg.txt"),0);
    remove(TestUtil::TEST_FILE_PATH.c_str());
    TestUtil::LoadDB(DB_FILE_PATH);
    table_id = open_table(const_cast<char*>(TestUtil::TEST_FILE_PATH.c_str()));
    EXPECT_TRUE(TestUtil::IsValidOpenedFile(table_id));
    ASSERT_EQ(table_id, 0);

    // Find records
    trx_id = trx_begin();
    ASSERT_GT(trx_id, 0);
    DBTestOperation::QueueFindTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), num_records);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);
    EXPECT_EQ(trx_commit(trx_id), trx_id);

    // Shutdown DB
    EXPECT_EQ(shutdown_db(),0);
    EXPECT_TRUE(TestUtil::IsValidClosedFile(table_id));
    TestUtil::SaveDB(DB_FILE_PATH);


    // Init DB
    ASSERT_EQ(init_db(buffer_size == -1 ? 3 : buffer_size, 0, 100, "logfile.data", "logmsg.txt"),0);
    remove(TestUtil::TEST_FILE_PATH.c_str());
    TestUtil::LoadDB(DB_FILE_PATH);
    table_id = open_table(const_cast<char*>(TestUtil::TEST_FILE_PATH.c_str()));
    EXPECT_TRUE(TestUtil::IsValidOpenedFile(table_id));
    ASSERT_EQ(table_id, 0);

    // Delete records
    trx_id = trx_begin();
    ASSERT_GT(trx_id, 0);
    DBTestOperation::QueueDeletionTest(table_id, records, trx_id, num_records);
    ASSERT_EQ(records.size(), 0);
    EXPECT_EQ(BPT::get_root_page(table_id), 0);
    DebugUtil::PrintTree(table_id);
    EXPECT_EQ(trx_commit(trx_id), trx_id);

    // Shutdown DB
    EXPECT_EQ(shutdown_db(),0);
    EXPECT_TRUE(TestUtil::IsValidClosedFile(table_id));
    TestUtil::SaveDB(DB_FILE_PATH);
}

TEST(BufferTest, InitShutdownDB)
{
    ASSERT_EQ(init_db(-1, 0, 100, "logfile.data", "logmsg.txt"),1);
    ASSERT_EQ(init_db(0, 0, 100, "logfile.data", "logmsg.txt"),0);
    ASSERT_EQ(shutdown_db(),0);
}

// TEST(BufferTest, BufferBypass)
// {
//     InitTest(0);
// }

// TEST(BufferTest, BufferVariableSize)
// {
//     InitTest();
// }

// TEST(BufferTest, BufferFixedSize_1)
// {
//     InitTest(1);
// }

// TEST(BufferTest, BufferFixedSize_2)
// {
//     InitTest(2);
// }

// TEST(BufferTest, BufferFixedSize_3)
// {
//     InitTest(3);
// }

TEST(BufferTest, BufferFixedSize_4)
{
    InitTest(4);
}

TEST(BufferTest, BufferFixedSize_5)
{
    InitTest(5);
}

TEST(BufferTest, BufferFixedSize_10)
{
    InitTest(10);
}

TEST(BufferTest, BufferFixedSize_100)
{
    InitTest(100);
}

TEST(BufferTest, BufferFixedSize_1000)
{
    InitTest(1000);
}

TEST(BufferTest, BufferFixedSize_10000)
{
    InitTest(10000);
}


// File comparison test
TEST(BufferTest, TableComparison)
{
    int num_records = NUM_KEY;
    int half = num_records / 2;
    int trx_id;
    std::queue<TRecord> records, records_copy, records_original;
    int64_t table_id;

    std::vector<std::pair<int,std::string>> table_files = {
        // {0, "TableBypass.db"}, 
        // {1, "TableBuffer01.db"}, 
        // {2, "TableBuffer02.db"}, 
        // {3, "TableBuffer03.db"}, 
        {4, "TableBuffer04.db"}, 
        {5, "TableBuffer05.db"}, 
        {10, "TableBuffer10.db"}, 
        {100, "TableBuffer100.db"}
    };

    // Insertion
    for (int idx = 0; idx < table_files.size(); idx++) {
        std::cout << "\n<< Table: " << table_files[idx].second << " >>\n" << std::endl;

        // Init DB
        ASSERT_EQ(init_db(table_files[idx].first, 0, 100, "logfile.data", "logmsg.txt"),0);
        remove(table_files[idx].second.c_str());
        table_id = open_table(const_cast<char*>(table_files[idx].second.c_str()));
        EXPECT_TRUE(TestUtil::IsValidOpenedFile(table_id));
        ASSERT_EQ(table_id, 0);

        if (idx) EXPECT_FALSE(TestUtil::CompareFiles(table_files[0].second, table_files[idx].second));

        // Insert records
        trx_id = trx_begin();
        ASSERT_GT(trx_id, 0);
        DBTestOperation::QueueInsertionTestAdv(table_id, records_original, trx_id, num_records);
        ASSERT_EQ(records_original.size(), num_records);
        EXPECT_NE(BPT::get_root_page(table_id), 0);
        DebugUtil::PrintTree(table_id);
        EXPECT_EQ(trx_commit(trx_id), trx_id);

        // Shutdown DB
        EXPECT_EQ(shutdown_db(),0);
        EXPECT_TRUE(TestUtil::IsValidClosedFile(table_id));

        if (idx) EXPECT_TRUE(TestUtil::CompareFiles(table_files[0].second, table_files[idx].second));
    }

    // Deletion
    for (int i = 0; i < half; i++) {
        records.push(records_original.front());
        records_original.pop();
    }
    ASSERT_EQ(records.size(), half);

    for (int idx = 0; idx < table_files.size(); idx++) {
        std::cout << "\n<< Table: " << table_files[idx].second << " >>\n" << std::endl;

        // Init DB
        ASSERT_EQ(init_db(table_files[idx].first, 0, 100, "logfile.data", "logmsg.txt"),0);
        table_id = open_table(const_cast<char*>(table_files[idx].second.c_str()));
        EXPECT_TRUE(TestUtil::IsValidOpenedFile(table_id));
        ASSERT_EQ(table_id, 0);

        if (idx) EXPECT_FALSE(TestUtil::CompareFiles(table_files[0].second, table_files[idx].second));

        // Delete half of records
        trx_id = trx_begin();
        ASSERT_GT(trx_id, 0);
        records_copy = std::queue<TRecord>(records);
        DBTestOperation::QueueDeletionTest(table_id, records_copy, trx_id, half);
        ASSERT_EQ(records.size(), half);
        ASSERT_EQ(records_copy.size(), 0);
        EXPECT_NE(BPT::get_root_page(table_id), 0);
        DebugUtil::PrintTree(table_id);
        EXPECT_EQ(trx_commit(trx_id), trx_id);


        // Shutdown DB
        EXPECT_EQ(shutdown_db(),0);
        EXPECT_TRUE(TestUtil::IsValidClosedFile(table_id));

        if (idx) EXPECT_TRUE(TestUtil::CompareFiles(table_files[0].second, table_files[idx].second));
    }
}
