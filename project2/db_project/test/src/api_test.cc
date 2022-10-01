#include "api.h"
#include "bpt.h"
#include "test_util.h"
#include <gtest/gtest.h>
using namespace std;


/// Settings
const bool START_FROM_EMPTY = true;
const bool UPDATE_DB_FILE = true;
const std::string DB_FILE_PATH = "test00.db";


/// SetUp and TearDown
class DBTest : public ::testing::Test {
public:
    const bool init = START_FROM_EMPTY;
    const bool update = UPDATE_DB_FILE;

    int64_t table_id;
    pagenum_t page_number;

    void SetUp() override {
        EXPECT_EQ(init_db(),0);
        remove(TestUtil::TEST_FILE_PATH.c_str());
        if (!init) TestUtil::LoadDB(DB_FILE_PATH);
        table_id = open_table(TestUtil::TEST_FILE_PATH.c_str());
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
    void SequencialInsertionTest(int64_t table_id, int64_t first, int64_t last)
    {
        static const char* MAX_LEN_DATA = "123456789_123456789_123456789_123456789_123456789_123456789_123456789_123456789_123456789_123456789_123456789_12";
        static const char* MIN_LEN_DATA = "123456789_123456789_123456789_123456789_123456789_";

        int flag = 0;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        cout << "\n<< table id: " << table_id << " >> " << endl;
        for (int key = first; key <= last; key++) {
            cout << "\n<< current key: " << key << " >> " << endl;
            if (key%2) {
                flag = db_insert(table_id, key, const_cast<char*>(MAX_LEN_DATA), VALUE_MAX_SIZE);
                EXPECT_EQ(flag, 0);
                flag = db_insert(table_id, key, const_cast<char*>(MAX_LEN_DATA), VALUE_MAX_SIZE);
                EXPECT_EQ(flag, 1);
                flag = db_find(table_id, key, ret_val, &val_size);
                EXPECT_EQ(flag, 0);
                EXPECT_EQ(string(ret_val), string(MAX_LEN_DATA));
                EXPECT_EQ(val_size, VALUE_MAX_SIZE);
                flag = db_find(table_id, key - 2'000'000, ret_val, &val_size);
                EXPECT_EQ(flag, 1);
            } else {
                flag = db_insert(table_id, key, const_cast<char*>(MIN_LEN_DATA), VALUE_MIN_SIZE);
                EXPECT_EQ(flag, 0);
                flag = db_insert(table_id, key, const_cast<char*>(MIN_LEN_DATA), VALUE_MIN_SIZE);
                EXPECT_EQ(flag, 1);
                flag = db_find(table_id, key, ret_val, &val_size);
                EXPECT_EQ(flag, 0);
                EXPECT_EQ(string(ret_val), string(MIN_LEN_DATA));
                EXPECT_EQ(val_size, VALUE_MIN_SIZE);
                flag = db_find(table_id, key - 2'000'000, ret_val, &val_size);
                EXPECT_EQ(flag, 1);
            }
        }
    }

    void RandomInsertionTest(int64_t table_id, int max_count, int64_t min_key = numeric_limits<int64_t>::min(), int64_t max_key = numeric_limits<int64_t>::max())
    {
        int flag_find, flag_insert;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        int64_t key_to_insert;
        string value_to_insert;

        cout << "\n<< table id: " << table_id << " >> " << endl;
        for (int count = 0; count < max_count;) {
            value_to_insert = TestUtil::GenRandomValue();
            key_to_insert = TestUtil::GenRandomKey(min_key, max_key);

            flag_find = db_find(table_id, key_to_insert, ret_val, &val_size);
            flag_insert = db_insert(table_id, key_to_insert, const_cast<char*>(value_to_insert.c_str()), value_to_insert.size());

            cout << "\n<< key: " << key_to_insert << " >> " << endl;
            cout << "<< value: \"" << value_to_insert << "\" >> " << endl;
            if (value_to_insert.size() < VALUE_MIN_SIZE || value_to_insert.size() > VALUE_MAX_SIZE) {
                EXPECT_EQ(flag_insert, 1);
                cout << "<< INVALID SIZE >>" << endl;
            } else if (flag_find == 0) {
                EXPECT_EQ(flag_insert, 1);
                cout << "<< ALREADY EXIST >>" << endl;
            } else if (flag_find == 1) {
                EXPECT_EQ(flag_insert, 0);

                flag_find = db_find(table_id, key_to_insert, ret_val, &val_size);
                EXPECT_EQ(flag_find, 0);
                EXPECT_EQ(value_to_insert, string(ret_val));
                EXPECT_EQ(value_to_insert.size(), val_size);

                cout << "<< INSERT ( count: " << ++count << " ) >>" << endl;
            }
        }
    }

    void SequencialDeletionTest(int64_t table_id, int64_t first, int64_t last, bool reverse = false)
    {
        int flag_find, flag_delete;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;
        int64_t key;

        cout << "\n<< table id: " << table_id << " >> " << endl;

        key = reverse ? last : first;
        while (key <= last && key >= first) {
            cout << "\n<< current key: " << key << " >> " << endl;

            flag_find = db_find(table_id, key, ret_val, &val_size);
            flag_delete = db_delete(table_id, key);
            EXPECT_EQ(flag_find, flag_delete);
            flag_find = db_find(table_id, key, ret_val, &val_size);
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

        cout << "\n<< table id: " << table_id << " >> " << endl;
        for (int count = 0; count < max_count;) {
            key_to_delete = TestUtil::GenRandomKey(min_key, max_key);

            flag_find = db_find(table_id, key_to_delete, ret_val, &val_size);
            flag_delete = db_delete(table_id, key_to_delete);
            EXPECT_EQ(flag_find, flag_delete);
            if (flag_find) continue;
            else {
                cout << "\n<< key: " << key_to_delete << " >> " << endl;
                cout << "<< DELETE SUCCESS ( count: " << ++count << " ) >>" << endl;
                flag_find = db_find(table_id, key_to_delete, ret_val, &val_size);
                EXPECT_EQ(flag_find, 1);
            }
        }
    }

    void QueueInsertionTest(int64_t table_id, std::queue<int64_t>& keys, int max_count = 0)
    {
        int64_t min_key = numeric_limits<int64_t>::min();
        int64_t max_key = numeric_limits<int64_t>::max();

        int flag_find, flag_insert;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        int64_t key_to_insert;
        string value_to_insert;

        cout << "\n<< table id: " << table_id << " >> " << endl;
        for (int count = 0; count < max_count;) {
            value_to_insert = TestUtil::GenRandomValue();
            key_to_insert = TestUtil::GenRandomKey(min_key, max_key);

            cout << "\n<< key: " << key_to_insert << " >> " << endl;
            cout << "<< value: \"" << value_to_insert << "\" >> " << endl;

            flag_find = db_find(table_id, key_to_insert, ret_val, &val_size);
            flag_insert = db_insert(table_id, key_to_insert, const_cast<char*>(value_to_insert.c_str()), value_to_insert.size());

            if (value_to_insert.size() < VALUE_MIN_SIZE || value_to_insert.size() > VALUE_MAX_SIZE) {
                EXPECT_EQ(flag_insert, 1);
                cout << "<< INVALID SIZE >>" << endl;
            } else if (flag_find == 0) {
                EXPECT_EQ(flag_insert, 1);
                cout << "<< ALREADY EXIST >>" << endl;
            } else if (flag_find == 1) {
                EXPECT_EQ(flag_insert, 0);

                flag_find = db_find(table_id, key_to_insert, ret_val, &val_size);
                EXPECT_EQ(flag_find, 0);
                EXPECT_EQ(value_to_insert, string(ret_val));
                EXPECT_EQ(value_to_insert.size(), val_size);

                if (flag_insert == 0 && flag_find == 0) {
                    cout << "<< INSERT SUCCESS ( count: " << ++count << " ) >>" << endl;
                    keys.push(key_to_insert);
                }
            }
        }
    }

    void QueueFindTest(int64_t table_id, std::queue<int64_t>& keys, int max_count = 0)
    {
        int flag_find;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        int64_t key_to_find;
        if (max_count == 0) max_count = keys.size();

        cout << "\n<< table id: " << table_id << " >> " << endl;
        for (int count = 0; count < max_count && !keys.empty(); count++) {
            key_to_find = keys.front();
            keys.pop();
            keys.push(key_to_find);

            cout << "\n<< key: " << key_to_find << " >> " << endl;

            flag_find = db_find(table_id, key_to_find, ret_val, &val_size);
            EXPECT_EQ(flag_find, 0);

            if (flag_find) {
                cout << "<< FIND FAILED ( count: " << count << " ) >>" << endl;
            } else {
                cout << "<< FIND SUCCESS ( count: " << count << " ) >>" << endl;
            }
        }
    }

    void QueueDeletionTest(int64_t table_id, std::queue<int64_t>& keys, int max_count = 0)
    {
        int flag_find, flag_delete;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        int64_t key_to_delete;
        if (max_count == 0) max_count = keys.size();

        cout << "\n<< table id: " << table_id << " >> " << endl;
        for (int count = 0; count < max_count && !keys.empty(); count++) {
            key_to_delete = keys.front();
            keys.pop();

            cout << "\n<< key: " << key_to_delete << " >> " << endl;

            flag_find = db_find(table_id, key_to_delete, ret_val, &val_size);
            flag_delete = db_delete(table_id, key_to_delete);
            EXPECT_EQ(flag_find, flag_delete);
            if (flag_delete) {
                cout << "<< DELETE FAILED ( count: " << count << " ) >>" << endl;
                LogUtil::PrintTree(table_id);
            } else {
                cout << "<< DELETE SUCCESS ( count: " << count << " ) >>" << endl;
                flag_find = db_find(table_id, key_to_delete, ret_val, &val_size);
                EXPECT_EQ(flag_find, 1);
            }
        }
    }

    void RandomOperationTest(int64_t table_id, int max_count, int64_t min_key = -1e6, int64_t max_key = 1e6)
    {
        int flag_find, flag_insert, flag_delete;
        char ret_val[VALUE_MAX_SIZE+1]; 
        uint16_t val_size;

        int64_t random_key;
        string value_to_insert;

        cout << "\n<< table id: " << table_id << " >> " << endl;
        for (int count = 0; count < max_count; count++) {
            value_to_insert = TestUtil::GenRandomValue();
            random_key = TestUtil::GenRandomKey(min_key, max_key);
            cout << "\n<< key: " << random_key << " >> " << endl;

            flag_find = db_find(table_id, random_key, ret_val, &val_size);
            if (flag_find) {
                cout << "\n<< NOT FOUND - INSERT >> " << endl;
                flag_delete = db_delete(table_id, random_key);
                EXPECT_EQ(flag_delete, 1);
                flag_insert = db_insert(table_id, random_key, const_cast<char*>(value_to_insert.c_str()), value_to_insert.size());
                if (value_to_insert.size() >= VALUE_MIN_SIZE && value_to_insert.size() <= VALUE_MAX_SIZE) {
                    EXPECT_EQ(flag_insert, 0);
                    flag_find = db_find(table_id, random_key, ret_val, &val_size);
                    EXPECT_EQ(flag_find, 0);
                    EXPECT_EQ(value_to_insert, string(ret_val));
                    EXPECT_EQ(value_to_insert.size(), val_size);
                } else {
                    EXPECT_EQ(flag_insert, 1);
                }
            } else {
                cout << "\n<< FOUND - DELETE >> " << endl;
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
TEST_F(DBTest, RandomKeyTest01)
{
    int num_keys = 1e5;
    std::queue<int64_t> keys;

    ASSERT_EQ(keys.size(), 0);
    ASSERT_EQ(BPT::get_root_page(table_id), 0);

    // Insert keys
    DBTestOperation::QueueInsertionTest(table_id, keys, num_keys);
    ASSERT_EQ(keys.size(), num_keys);
    EXPECT_NE(BPT::get_root_page(table_id), 0);

    // Find keys
    DBTestOperation::QueueFindTest(table_id, keys, num_keys);
    ASSERT_EQ(keys.size(), num_keys);
    EXPECT_NE(BPT::get_root_page(table_id), 0);

    // Delete keys
    DBTestOperation::QueueDeletionTest(table_id, keys, num_keys);
    ASSERT_EQ(keys.size(), 0);
    EXPECT_EQ(BPT::get_root_page(table_id), 0);
}

TEST_F(DBTest, RandomKeyTest02)
{
    int num_keys = 1e5;
    int half = num_keys/2;
    std::queue<int64_t> keys;

    ASSERT_EQ(keys.size(), 0);
    ASSERT_EQ(BPT::get_root_page(table_id), 0);

    // Insert keys
    DBTestOperation::QueueInsertionTest(table_id, keys, num_keys);
    ASSERT_EQ(keys.size(), num_keys);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DBTestOperation::QueueFindTest(table_id, keys, num_keys);

    // Delete half of keys
    DBTestOperation::QueueDeletionTest(table_id, keys, half);
    ASSERT_EQ(keys.size(), num_keys - half);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DBTestOperation::QueueFindTest(table_id, keys, num_keys - half);

    // Insert keys again
    DBTestOperation::QueueInsertionTest(table_id, keys, half);
    ASSERT_EQ(keys.size(), num_keys);
    EXPECT_NE(BPT::get_root_page(table_id), 0);
    DBTestOperation::QueueFindTest(table_id, keys, num_keys);

    // Delete keys again
    DBTestOperation::QueueDeletionTest(table_id, keys, num_keys);
    ASSERT_EQ(keys.size(), 0);
    EXPECT_EQ(BPT::get_root_page(table_id), 0);
}