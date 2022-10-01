#include <gtest/gtest.h>
#include "test_util.h"

#include "index.h"
#include "trx.h"

#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <time.h>


/// Settings
const int NUM_BUF = 200;
const int NUM_KEY = 10;
const int NUM_THRD = 1000;
const int TIME_UNIT = 3;

/// Deadlock test
void* SimpleDeadlock(void* arg) 
{
    int trx_id, table_id, num_key, flag, time;
    std::string values = "123456789_123456789_123456789_123456789_123456789_";
    uint16_t val_size = values.size(), old_val_size;
    char ret_val[VALUE_MAX_SIZE+1];
    memset(ret_val, 0, sizeof(ret_val));

    trx_id = trx_begin();
    EXPECT_GT(trx_id, 0);

    table_id = ((int*)arg)[0];
    num_key = NUM_KEY;
    time = TIME_UNIT;

    if (trx_id == 2) sleep(time);

    flag = db_update(table_id, trx_id, const_cast<char*>(values.c_str()), val_size, &old_val_size, trx_id);
    // flag = db_find(table_id, trx_id, ret_val, &old_val_size, trx_id);
    EXPECT_EQ(flag, 0);

    if (trx_id == 1) sleep(time*2);

    flag = db_update(table_id, trx_id%2+1, const_cast<char*>(values.c_str()), val_size, &old_val_size, trx_id);
    if (trx_id == 1) EXPECT_EQ(flag, 0);
    else if (trx_id == 2) EXPECT_EQ(flag, FLAG::ABORTED);
    // ASSERT_TRUE(trx_id <= 2);

    // if (trx_id == 1) sleep(time*2);
    if (trx_id == 2) sleep(time*2);

    if (trx_id == 1) EXPECT_EQ(trx_commit(trx_id),0);
    if (trx_id == 2) EXPECT_EQ(trx_commit(trx_id),2);

    return NULL;
}


TEST(TransactionTest, SimpleDeadlock) 
{
    int table_id, flag;
    std::string value1 = "abcdefghi_abcdefghi_abcdefghi_abcdefghi_101010101_";
    std::string value2 = "abcdefghi_abcdefghi_abcdefghi_abcdefghi_202020202_";

    EXPECT_EQ(init_db(NUM_BUF), 0);

    if (std::remove("SimpleDeadlock.db") == 0) {
        printf("[INFO] File 'SimpleDeadlock.db' already exists. Deleting it.\n");
    }

    table_id = open_table("SimpleDeadlock.db");
    EXPECT_EQ(db_insert(table_id, 1, const_cast<char*>(value1.c_str()), value1.length()), 0);
    EXPECT_EQ(db_insert(table_id, 2, const_cast<char*>(value2.c_str()), value2.length()), 0);

    pthread_t thrd1, thrd2;

    pthread_create(&thrd1, NULL, SimpleDeadlock, (void*)&table_id);
    pthread_create(&thrd2, NULL, SimpleDeadlock, (void*)&table_id);

    pthread_join(thrd1, NULL);
    pthread_join(thrd2, NULL);

    EXPECT_EQ(shutdown_db(), 0);


    // int n = N;

    // for (int i = 1; i <= n; i++) {
    //     #if DEBUG_MODE
    //     std::cout << "[DEBUG] Inserting key = " << i << std::endl;
    //     #endif
    //     std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
    //     int res = db_insert(*table_id, i, const_cast<char*>(data.c_str()), data.length());
    //     EXPECT_EQ(res, 0);
    // }

    // std::cout << "[INFO] Population done, now testing SLockOnly Test" << std::endl;

    // int m = M;
    // uint16_t val_size;
    // pthread_t threads[m];
    // // pthread_attr_t attr;
    // // pthread_attr_setstacksize(&attr, 128 * 1024 * 1024);
    // for (int i = 0; i < m; i++) {
    //     #if DEBUG_MODE
    //     std::cout << "[DEBUG] Create thread " << i << std::endl;
    //     #endif
    //     pthread_create(&threads[i], NULL, slock_only, table_id);
    // }

    // for (int i = 0; i < m; i++) {
    //     #if DEBUG_MODE
    //     std::cout << "[DEBUG] Join thread start " << i << std::endl;
    //     #endif
    //     pthread_join(threads[i], NULL);
    //     #if DEBUG_MODE
    //     std::cout << "[DEBUG] Join thread end " << i << std::endl;
    //     #endif
    // }

    // free(table_id);

    // EXPECT_EQ(shutdown_db(), 0);
}