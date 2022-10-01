#include "index.h"
#include "trx.h"

#include <iostream>
#include <string>
#include <cstring>
#include <fstream>
#include <ctime>
#include <algorithm>
#include <vector>
#include <random>
#include <unistd.h>
#include <utility>
#include <string>

#define BUF_SIZE 200
#define DEBUG_MODE 1
#define N 20
#define M 2000

// using namespace std;

// static const std::string CHARACTERS {
// 	"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"};

// static const std::string BASE_TABLE_NAME { "table" };
// static constexpr int NUM_TABLES { 2 };
// static constexpr int NUM_KEYS { 10000 };
// static constexpr int BUFFER_SIZE { 10 };

// static constexpr int MIN_VAL_SIZE { 46 };
// static constexpr int MAX_VAL_SIZE { 108 };

// int main( int argc, char ** argv ) {

// 	int64_t table_id;
// 	int64_t key;
// 	char* value;
// 	uint16_t size;

// 	char ret_value[MAX_VAL_SIZE];
// 	uint16_t ret_size;

// 	std::vector<std::pair<int64_t, std::string>> key_value_pairs {};
// 	std::vector<std::pair<std::string, int64_t>> tables {};

// 	std::random_device rd;
// 	std::mt19937 gen(rd());
// 	std::uniform_int_distribution<uint16_t> len_dis(MIN_VAL_SIZE, MAX_VAL_SIZE);
// 	std::uniform_int_distribution<int> char_dis(1, CHARACTERS.size());

// 	std::default_random_engine rng(rd());

// 	tables.reserve(NUM_TABLES);
// 	key_value_pairs.reserve(NUM_KEYS);

// 	auto helper_function = [] (auto& gen, auto& cd, auto& size) -> std::string {
// 		std::string ret_str;
// 		int index;
// 		ret_str.reserve(size);

// 		for (int i = 0; i < size; ++i) {
// 			index = cd(gen) - 1;
// 			ret_str += CHARACTERS[index];
// 		}
// 		return ret_str;
// 	};

// 	for (int i = 0; i < NUM_TABLES; ++i)
// 		tables.emplace_back(BASE_TABLE_NAME + to_string(i), 0);

// 	for (int i = 1; i <= NUM_KEYS; ++i) {
// 		size = len_dis(gen);
// 		key_value_pairs.emplace_back(i, helper_function(gen, char_dis, size));
// 	}


// 	// Simple test code.
// 	std::cout << "[INIT START]\n";
// 	if (init_db(BUFFER_SIZE) != 0) {
// 		return 0;
// 	}
// 	std::cout << "[INIT END]\n\n";
// 	std::cout << "[OPEN TABLE START]\n";
// 	for (auto& t : tables) {
// 		remove(t.first.c_str());
// 		table_id = open_table(const_cast<char*>(t.first.c_str()));
// 		if (table_id < 0) {
// 			goto func_exit;
// 		} else {
// 			t.second = table_id;
// 		}
// 	}
// 	std::cout << "[OPEN TABLE END]\n\n";
// 	std::cout << "[TEST START]\n";
// 	for (const auto& t: tables) {
// 		std::cout << "[TABLE : " << t.first << " START]\n";
// 		std::shuffle(key_value_pairs.begin(), key_value_pairs.end(), rng);
// 		std::cout << "[INSERT START]\n";
// 		for (const auto& kv: key_value_pairs) {
// 			if (db_insert(t.second, kv.first, const_cast<char*>(kv.second.c_str()), kv.second.size()) != 0) {
// 				goto func_exit;
// 			}
// 		}
// 		std::cout << "[INSERT END]\n";
// 		std::cout << "[FIND START]\n";
// 		for (const auto& kv: key_value_pairs) {
// 			ret_size = 0;
// 			memset(ret_value, 0x00, MAX_VAL_SIZE);
// 			if (db_find(t.second, kv.first, ret_value, &ret_size, 1) != 0) {
// 				goto func_exit;
// 			} else if (kv.second.size() != ret_size ||
// 					kv.second != std::string(ret_value, ret_size)) {
// 				goto func_exit;
// 			}
// 		}
// 		std::cout << "[FIND END]\n";
// 		std::cout << "[DELETE START]\n";
// 		for (const auto& kv: key_value_pairs) {
// 			if (db_delete(t.second, kv.first) != 0) {
// 				goto func_exit;
// 			}
// 		}
// 		std::cout << "[DELETE END]\n";
// 		std::cout << "[FIND START AGAIN]\n";
// 		for (const auto& kv: key_value_pairs) {
// 			ret_size = 0;
// 			memset(ret_value, 0x00, MAX_VAL_SIZE);
// 			if (db_find(t.second, kv.first, ret_value, &ret_size, 1) == 0) {
// 				goto func_exit;
// 			}
// 		}
// 		std::cout << "[FIND END AGAIN]\n";
// 		std::cout << "[TABLE : " << t.first << " END]\n\n";
// 	}
// 	std::cout << "[TEST END]\n";

// func_exit:

// 	std::cout << "[SHUTDOWN START]\n";
// 	if (shutdown_db() != 0) {
// 		return 0;
// 	}
// 	std::cout << "[SHUTDOWN END]\n";
// 	return 0;
// }


void* xlock_only(void* arg) {
    int trx_id = trx_begin();
	assert(trx_id >= 0);

    int table_id = ((int*)arg)[0];
    int n = N;
    int err = 0;
    uint16_t val_size;
    std::string s[10] = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
    for (int i = 1; i <= n; i++) {
        // char ret_val[112] = "\0";
        // std::string data = s[i%10] + "2345678901234567890123456789012345678901234567890" + std::to_string(i);
        std::string data = s[trx_id%10] + "2345678901234567890123456789012345678901234567890" + std::to_string(i);
        uint16_t *old_val_size = (uint16_t*)malloc(sizeof(uint16_t));
        int res = db_update(table_id, i, const_cast<char*>(data.c_str()), data.length(), old_val_size, trx_id);
		printf("[[xlock_only]][%d] res: %d, old_val_size: %d\n", i, res, *old_val_size);
        free(old_val_size);
        // res = db_find(table_id, i, ret_val, &old_val_size);
        // EXPECT_EQ(res, 0);

        // for (int j = 0; j < val_size; j++) {
        //     EXPECT_EQ(ret_val[j], data[j]);
        // }
    }

    trx_commit(trx_id);
    return NULL;
}




int main()
{
    init_db(BUF_SIZE, 0, 100, "logfile.data", "logmsg.txt");

    if (std::remove("XLockOnly.dat") == 0) {
        std::cout << "[INFO] File 'XLockOnly.dat' already exists. Deleting it." << std::endl;
    }

    int table_id = open_table("XLockOnly.dat");

    int n = N;

    for (int i = 1; i <= n; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Inserting key = " << i << std::endl;
        #endif
        std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
        int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
		assert(res == 0);
    }

    std::cout << "[INFO] Population done, now testing XLockOnly Test" << std::endl;

    int m = M;
    uint16_t val_size;
    pthread_t threads[m];
    std::vector<int*> args;
    // pthread_attr_t attr;
    // pthread_attr_setstacksize(&attr, 128 * 1024 * 1024);
    for (int i = 0; i < m; i++) {
        #if DEBUG_MODE
        std::cout << "[DEBUG] Create thread " << i << std::endl;
        #endif
        int *arg = (int*)malloc(sizeof(int));
        *arg = table_id;
        args.push_back(arg);
        pthread_create(&threads[i], NULL, xlock_only, arg);
    }

    for (int i = 0; i < m; i++) {
        pthread_join(threads[i], NULL);
    }

    for(auto i:args){
        free(i);
    }
    
    shutdown_db();
}



void* xlock_only_disjoint(void* arg) {
    int trx_id = trx_begin();
    // EXPECT_GT(trx_id, 0);
	assert(trx_id > 0);

    int table_id = ((int*)arg)[0];
    int n = N;
    int err = 0;
    uint16_t val_size;
    int st = ((int*)arg)[1];
    // for (int i = st * (n / 10) + 1; i <= (st+1) * (n / 10); i++) {
    for (int i = 1; i <= n; i++) {
        // char ret_val[112] = "\0";
        std::string data = "12345678901234567890123456789012345678901234567890" + std::to_string(i);
        uint16_t old_val_size = 0;
        int res = db_update(table_id, i, const_cast<char*>(data.c_str()), data.length(), &old_val_size, trx_id);
        // EXPECT_EQ(res, 0);
        // res = db_find(table_id, i, ret_val, &old_val_size);
        // EXPECT_EQ(res, 0);

        // for (int j = 0; j < val_size; j++) {
        //     EXPECT_EQ(ret_val[j], data[j]);
        // }
    }

    trx_commit(trx_id);
    return NULL;
}

// int main()
// {
// 	init_db(BUF_SIZE);
//     if (std::remove("XLockOnly.dat") == 0)
//     {
//         std::cout << "[INFO] File 'XLockOnly.dat' already exists. Deleting it." << std::endl;
//     }

//     int table_id = open_table("XLockOnly.dat");

//     int n = N;

//     for (int i = 1; i <= n; i++) {
//         #if DEBUG_MODE
//         std::cout << "[DEBUG] Inserting key = " << i << std::endl;
//         #endif
//         std::string data = "01234567890123456789012345678901234567890123456789" + std::to_string(i);
//         int res = db_insert(table_id, i, const_cast<char*>(data.c_str()), data.length());
//     }

//     std::cout << "[INFO] Population done, now testing XLockOnly Test" << std::endl;

//     int m = M;
//     uint16_t val_size;
//     pthread_t threads[m];

//     std::vector<int*> args;
//     // pthread_attr_t attr;
//     // pthread_attr_setstacksize(&attr, 128 * 1024 * 1024);
//     for (int i = 0; i < m; i++) {
//         #if DEBUG_MODE
//         std::cout << "[DEBUG] Create thread " << i << std::endl;
//         #endif
//         int *arg = (int*)malloc(sizeof(int) * 2);
//         arg[0] = table_id;
//         arg[1] = i;
//         args.push_back(arg);
//         pthread_create(&threads[i], NULL, xlock_only_disjoint, arg);
//     }

//     for (int i = 0; i < m; i++) {
//         pthread_join(threads[i], NULL);
//     }

//     for(auto i:args){
//         free(i);
//     }
// }