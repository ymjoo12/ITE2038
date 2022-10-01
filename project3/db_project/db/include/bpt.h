#ifndef DB_BPT_H__
#define DB_BPT_H__


/// Includes
#include "page.h"
#include "file.h"
#include "log_util.h"
#include "buffer.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include <string>
#include <iostream>
#include <deque>
#include <algorithm>


/// B+Tree FUNCTION PROTOTYPES
namespace BPT
{   
    // Flags
    const int FLAG_SUCCESS = 0;
    const int FLAG_FAIL = 1;

    // Checker
    bool is_invalid_node_page(NodePage& node);
    bool is_correct_internal_index(int index, NodePage& node, pagenum_t page_number);

    // Node page IO (buffer IO)
    NodePage load_node_page(int64_t table_id, pagenum_t page_number, bool& pinned, bool pin = false);
    void set_node_page(int64_t table_id, pagenum_t page_number, NodePage& node, bool unpin = true, bool rollback = false);
    pagenum_t make_node_page(int64_t table_id, bool is_leaf = true);
    void free_node_page(int64_t table_id, pagenum_t page_number, bool unpin = true);

    // Getter and setter (buffer IO)
    pagenum_t get_root_page(int64_t table_id);
    void set_root_page(int64_t table_id, pagenum_t root);
    void set_parent_page(int64_t table_id, pagenum_t child, pagenum_t parent);
    void set_parent_pages(int64_t table_id, pagenum_t parent, int start_point = -1);

    // Update metadata of node page
    void update_node(NodePage& node, int start_point = 0);

    // Find
    template <typename T>
    int find_key_index(std::deque<T>& keys, int64_t key);
    pagenum_t find_leaf(int64_t table_id, pagenum_t root, int64_t key);
    int find(int64_t table_id, pagenum_t root, int64_t key, std::string& value);

    // Insertion
    template <typename T>
    int insert_node_key(std::deque<T>& dest, T& keypair);
    int split_leaf_slots(std::deque<Record>& right, std::deque<Record>& origin, int64_t& prime_key);
    int split_internal_edges(NodePage& right_node, std::deque<Edge>& origin, int64_t& prime_key);
    int insert_into_leaf(int64_t table_id, pagenum_t leaf, int64_t key, std::string value);
    int insert_into_leaf_after_splitting(int64_t table_id, pagenum_t leaf, int64_t key, std::string value);
    int insert_into_internal(int64_t table_id, pagenum_t internal, pagenum_t left, int64_t key, pagenum_t right);
    int insert_into_internal_after_splitting(int64_t table_id, pagenum_t internal, pagenum_t left, int64_t key, pagenum_t right);
    int insert_into_parent(int64_t table_id, pagenum_t left, int64_t key, pagenum_t right);
    int insert_into_new_root(int64_t table_id, pagenum_t left, int64_t key, pagenum_t right);
    int insert(int64_t table_id, pagenum_t root, int64_t key, std::string value);

    // Deletion
    template <typename T>
    int delete_node_key(std::deque<T>& dest, int64_t key);
    int remove_entry_from_node(int64_t table_id, pagenum_t key_page, int64_t key);
    int adjust_root(int64_t table_id, pagenum_t root);
    int merge_nodes(int64_t table_id, pagenum_t root, pagenum_t left, pagenum_t right, int64_t prime_key);
    int redistribute_nodes(int64_t table_id, pagenum_t left, pagenum_t right, int prime_key_index, int64_t prime_key);
    int delete_entry(int64_t table_id, pagenum_t root, pagenum_t page_number, int64_t key);
    int db_delete(int64_t table_id, pagenum_t root, int64_t key);

    // Destruction
    void destroy_tree_nodes(int64_t table_id, pagenum_t page_number);
    void destroy_tree(int64_t table_id, pagenum_t root);
}


#endif // DB_BPT_H
