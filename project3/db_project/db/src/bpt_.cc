#include "bpt.h"


/// B+Tree functions
namespace BPT
{
    /// Checkers

    bool is_valid_node_page(NodePage& node)
    {
        // Check page data
        if (node.header.is_leaf) {
            // Check the number of keys
            if (node.header.number_of_keys != node.slots.size()) {
                std::cout << "[is_valid_node_page] the number of keys is invalid" << std::endl;
                return false;
            }
            // Check the amount of free space
            uint64_t amount_of_free_space = PAGE_SIZE - (
                HEADER_SIZE + node.header.number_of_keys * SLOT_SIZE + 
                (node.header.number_of_keys > 0 ? PAGE_SIZE - node.slots.back().offset : 0)
            );
            if (node.header.amount_of_free_space != amount_of_free_space) {
                std::cout << "[is_valid_node_page] the amount of free space is invalid ";
                std::cout << "( expected: " << amount_of_free_space << " B";
                std::cout << ", real: " << node.header.amount_of_free_space << " B )" << std::endl;
                return false;
            }
        } else {
            // Check the number of keys
            if (node.header.number_of_keys != node.edges.size()) {
                std::cout << "[is_valid_node_page] the number of keys is invalid" << std::endl;
                return false;
            }
        }

        return true;
    }

    bool is_correct_internal_index(int index, NodePage& node, pagenum_t page_number)
    {
        // Check the node is internal
        if (node.header.is_leaf) return false;
        
        // Check the page numbers are equal
        if (index == -1) {
            return node.header.first_child_page_number == page_number;
        } else if (index < node.header.number_of_keys) {
            return node.edges[index].page_number == page_number;
        }

        return false;
    }


    /// Node page IO

    NodePage load_node_page(int64_t table_id, pagenum_t page_number, bool pin)
    {
        if (LogUtil::DEBUG_MODE) LogUtil::PrintMarker(__func__, "( page_number: " + std::to_string(page_number) + " )");

        page_t buf;
        NodePage node_to_return;

        // Check the page is header page
        if (page_number == 0) {
            std::cout << "[load_node_page] tried to read header page as node page" << std::endl;
            exit(1);
        }

        // Read given page as Node Page
        // TODO:
        file_read_page(table_id, page_number, &buf);
        node_to_return = NodePage(buf);
        // node_to_return = NodePage(buffer.read_page(table_id, page_number, pin));

        // Check the page is valid page
        if (!is_valid_node_page(node_to_return)) {
            std::cout << "[load_node_page] invalid page " << std::endl;
            std::cout << "( table_id: " << table_id << ", page_number: " << page_number << " )" << std::endl;
            exit(1);
        }

        return node_to_return;
    }

    void save_node_page(int64_t table_id, pagenum_t page_number, NodePage& node)
    {
        if (LogUtil::DEBUG_MODE) {
            LogUtil::PrintMarker(__func__, "( page_number: " + std::to_string(page_number) + " )");
            LogUtil::PrintPage(node,true);
        }

        page_t buf;

        // Check the page is valid page
        if (!is_valid_node_page(node)) {
            std::cout << "[load_node_page] invalid page " << std::endl;
            std::cout << "( table_id: " << table_id << ", page_number: " << page_number << " )" << std::endl;
            exit(1);
        }

        // convert node page to page_t and write page
        // TODO:
        buf = node;
        file_write_page(table_id, page_number, &buf);
        // buffer.write_page(table_id, page_number, node);
    }

    pagenum_t make_node_page(int64_t table_id, bool is_leaf)
    {
        if (LogUtil::DEBUG_MODE) LogUtil::PrintMarker(__func__);

        pagenum_t new_page_number;
        NodePage new_node;

        // allocate new page
        // TODO:
        new_page_number = file_alloc_page(table_id);
        // new_page_number = buffer.alloc_page(table_id);

        // create new node page and initialize the node page on disk
        new_node = NodePage(is_leaf);
        // TODO:
        save_node_page(table_id, new_page_number, new_node);
        // buffer.write_page(table_id, new_page_number, new_node);

        return new_page_number;
    }

    void free_node_page(int64_t table_id, pagenum_t page_number)
    {
        if (LogUtil::DEBUG_MODE) LogUtil::PrintMarker(__func__);

        // free given page
        file_free_page(table_id, page_number);
        // TODO:
        // buffer.free_page(table_id, page_number);
    }


    /// Getter and setter (Disk IO)

    // get root page number, if root page doesn't exist, create it
    pagenum_t get_root_page(int64_t table_id)
    {
        HeaderPage header;
        pagenum_t root;

        // Read header page and return root page number
        // TODO:
        file_read_page(table_id, 0, (page_t*)&header);
        // header = HeaderPage(buffer.read_page(table_id, 0));
        root = header.root_page_number;

        return root;
    }

    // set root page number to on-disk header page
    void set_root_page(int64_t table_id, pagenum_t root)
    {
        HeaderPage header;

        // Update header page
        // TODO:
        // header = HeaderPage(buffer.read_page(table_id, 0, true));
        file_read_page(table_id, 0, (page_t*)&header);
        header.root_page_number = root;
        file_write_page(table_id, 0, (page_t*)&header);
        // buffer.write_page(table_id, 0, header);
    }

    // set parent page number of child page
    void set_parent_page(int64_t table_id, pagenum_t child, pagenum_t parent)
    {
        NodePage child_node;

        // update parent page number of child page
        child_node = load_node_page(table_id, child, true);
        child_node.header.parent_page_number = parent;
        save_node_page(table_id, child, child_node);
    }

    // set parent page number of child pages of parent page (must have been updated child data in parent)
    void set_parent_pages(int64_t table_id, pagenum_t parent, int start_point) 
    {
        pagenum_t child;
        NodePage parent_node, child_node;

        // update parent of child node pages
        parent_node = load_node_page(table_id, parent);
        for (int idx = start_point; idx < (int)parent_node.header.number_of_keys; idx++) {
            if (idx == -1) child = parent_node.header.first_child_page_number;
            else child = parent_node.edges[idx].page_number;
            set_parent_page(table_id, child, parent);
        }
    }


    /// Updating functions

    // // update header data and offset value of each record of leaf node
    // void update_node(NodePage& leaf_node, int start_point)
    // {
    //     uint16_t offset;

    //     // Check the node is leaf
    //     if (!leaf_node.header.is_leaf) return;

    //     // set initial offset value
    //     if (start_point == 0) offset = PAGE_SIZE;
    //     else offset = leaf_node.slots[start_point-1].offset;

    //     // calculate and set each offset value of a leaf slots using each size value
    //     for (int idx = start_point; idx < leaf_node.slots.size(); idx++) {
    //         offset -= leaf_node.slots[idx].size;
    //         if (offset < HEADER_SIZE) {
    //             std::cout << "[update_leaf_offset] out of page space" << std::endl;
    //             exit(1);
    //         }
    //         leaf_node.slots[idx].offset = offset;
    //     }

    //     // update header data
    //     offset = leaf_node.slots.empty() ? PAGE_SIZE : leaf_node.slots.back().offset;
    //     leaf_node.header.number_of_keys = leaf_node.slots.size();
    //     leaf_node.header.amount_of_free_space = PAGE_SIZE - (HEADER_SIZE + SLOT_SIZE * leaf_node.slots.size() + (PAGE_SIZE - offset));
    // }
    
    // // update header data of internal node
    // void update_node(NodePage& internal_node)
    // {
    //     // Check the node is internal
    //     if (internal_node.header.is_leaf) return;

    //     // update header data
    //     internal_node.header.number_of_keys = internal_node.edges.size();
    // }

    // update header data and offset value
    void update_node(NodePage& node, int start_point)
    {
        uint16_t offset;

        if (node.header.is_leaf) {
            // Case: node is leaf
            // set initial offset value
            if (start_point == 0) offset = PAGE_SIZE;
            else offset = node.slots[start_point-1].offset;

            // calculate and set each offset value of a leaf slots using each size value
            for (int idx = start_point; idx < node.slots.size(); idx++) {
                offset -= node.slots[idx].size;
                if (offset < HEADER_SIZE) {
                    std::cout << "[update_leaf_offset] out of page space" << std::endl;
                    exit(1);
                }
                node.slots[idx].offset = offset;
            }

            // update header data
            offset = node.slots.empty() ? PAGE_SIZE : node.slots.back().offset;
            node.header.number_of_keys = node.slots.size();
            node.header.amount_of_free_space = PAGE_SIZE - (HEADER_SIZE + SLOT_SIZE * node.slots.size() + (PAGE_SIZE - offset));
        } else {
            // Case: node is internal
            // update header data
            node.header.number_of_keys = node.edges.size();
        }
    }


    /// Find

    // find candidate index having given key
    template <typename T>
    int find_key_index(std::deque<T>& keys, int64_t key)
    {
        int idx;

        // find the key index using binary search
        idx = std::upper_bound(keys.begin(), keys.end(), key) - keys.begin() - 1;

        return idx;
    }

    // Traces the path from the root to a leaf, searching
    pagenum_t find_leaf(int64_t table_id, pagenum_t root, int64_t key)
    {
        NodePage node;
        pagenum_t leaf;
        int idx;

        // load the given root page
        if (root == 0) return 0;
        node = load_node_page(table_id, root);

        // find the leaf page
        leaf = root;
        while (!node.header.is_leaf) {
            // find the candidate index having the key
            idx = find_key_index(node.edges, key);

            // load the page of the index found
            if (idx == -1) leaf = node.header.first_child_page_number;
            else leaf = node.edges[idx].page_number;
            node = load_node_page(table_id, leaf);
        }

        return leaf;
    }

    // Finds and returns the record to which a key refers
    int find(int64_t table_id, pagenum_t root, int64_t key, std::string& value)
    {
        int idx;
        pagenum_t leaf;
        NodePage leaf_node;

        // Initialize the return value to empty string
        value = "";

        // Find the leaf node having the key
        leaf = find_leaf(table_id, root, key);
        if (leaf == 0) return FLAG_FAIL;
        leaf_node = load_node_page(table_id, leaf);

        // Find the key in the leaf node
        idx = find_key_index(leaf_node.slots, key);
        if (idx >= 0 && idx < leaf_node.header.number_of_keys && leaf_node.slots[idx].key == key) {
            value = leaf_node.slots[idx].value;
            return FLAG_SUCCESS;
        }

        return FLAG_FAIL;
    }


    /// INSERTION

    template <typename T>
    int insert_node_key(std::deque<T>& dest, T& keypair)
    {
        int insertion_point;

        // find insertion point and insert the keypair
        insertion_point = find_key_index(dest, keypair.key)+1;
        if (insertion_point < 0 || insertion_point > dest.size()) {
            return -1;
        }
        dest.insert(dest.begin()+insertion_point, keypair);

        return insertion_point;
    }

    int split_leaf_slots(std::deque<Record>& right_slots, std::deque<Record>& origin, int64_t& prime_key)
    {
        int split_point;
        uint16_t size;
        const uint16_t HALF_BODY = BODY_SIZE/2;

        // find index to split and check the point is valid
        for (split_point = 0, size = 0; split_point < origin.size(); split_point++) {
            size += origin[split_point].size + SLOT_SIZE;
            if (size >= HALF_BODY) break;
        }
        if (split_point == 0) return FLAG_FAIL;

        // copy slots after split point to right_slots
        right_slots.clear();
        right_slots = std::deque<Record>(origin.begin()+split_point+1, origin.end());
        origin.erase(origin.begin()+split_point+1, origin.end());

        // Get first key of right node
        prime_key = right_slots[0].key;

        return FLAG_SUCCESS;
    }

    // split origin edge array and return the first key of right node
    int split_internal_edges(NodePage& right_node, std::deque<Edge>& origin, int64_t& prime_key)
    {
        int split_point;

        // find index to split and check the point is valid
        split_point = (origin.size()+1)/2;
        if (split_point == 0) return FLAG_FAIL;

        // copy slots after split point to right_node.edges
        right_node.edges.clear();
        right_node.edges = std::deque<Edge>(origin.begin()+split_point+1, origin.end());
        
        // Set first child of right node and get first key of right node
        right_node.header.first_child_page_number = origin[split_point].page_number;
        prime_key = origin[split_point].key;
        origin.erase(origin.begin()+split_point, origin.end());

        return FLAG_SUCCESS;
    }

    int insert_into_leaf(int64_t table_id, pagenum_t leaf, int64_t key, std::string value)
    {
        int index;
        NodePage leaf_node;
        Record record;

        // load leaf node to be inserted
        leaf_node = load_node_page(table_id, leaf);
        if (!leaf_node.header.is_leaf) return FLAG_FAIL;

        // make new record and insert the record
        record = Record(key, value);
        index = insert_node_key(leaf_node.slots, record);
        if (index < 0) return FLAG_FAIL;

        // update offsets of records and page header
        update_node(leaf_node, index);

        // flush the node
        save_node_page(table_id, leaf, leaf_node);

        return FLAG_SUCCESS;
    }

    // Inserts a new key into a leaf node with split the node into two
    int insert_into_leaf_after_splitting(int64_t table_id, pagenum_t leaf, int64_t key, std::string value)
    {
        pagenum_t new_leaf;
        NodePage new_leaf_node, leaf_node;
        Record record;
        int index, flag;
        int64_t new_key;

        // load leaf node
        leaf_node = load_node_page(table_id, leaf);
        if (!leaf_node.header.is_leaf) return FLAG_FAIL;

        // make new record and insert the record
        record = Record(key, value);
        index = insert_node_key(leaf_node.slots, record);

        // create new leaf node page
        new_leaf = make_node_page(table_id);
        new_leaf_node = load_node_page(table_id, new_leaf);

        // split array of slots
        flag = split_leaf_slots(new_leaf_node.slots, leaf_node.slots, new_key);
        if (flag) {
            file_free_page(table_id, new_leaf);
            return FLAG_FAIL;
        };

        // update sibling and parent page number
        new_leaf_node.header.parent_page_number = leaf_node.header.parent_page_number;
        new_leaf_node.header.right_sibling_page_number = leaf_node.header.right_sibling_page_number;
        leaf_node.header.right_sibling_page_number = new_leaf;

        // update offsets of records and page headers
        update_node(leaf_node);
        update_node(new_leaf_node);

        // flush the node pages
        save_node_page(table_id, leaf, leaf_node);
        save_node_page(table_id, new_leaf, new_leaf_node);

        return insert_into_parent(table_id, leaf, new_key, new_leaf);
    }

    // Inserts a new key into a internal node
    int insert_into_internal(int64_t table_id, pagenum_t internal, pagenum_t left, int64_t key, pagenum_t right)
    {
        int right_index;
        NodePage internal_node;
        Edge edge;

        // load internal node to be inserted
        internal_node = load_node_page(table_id, internal);
        if (internal_node.header.is_leaf) return FLAG_FAIL;

        // make new edge, and insert the edge
        edge = Edge(key, right);
        right_index = insert_node_key(internal_node.edges, edge);
        if (!is_correct_internal_index(right_index-1, internal_node, left)) return FLAG_FAIL;
        
        // update page header
        if (internal_node.edges.size() != internal_node.header.number_of_keys + 1) {
            return FLAG_FAIL;
        }
        update_node(internal_node);

        // flush the node
        save_node_page(table_id, internal, internal_node);

        return FLAG_SUCCESS;
    }

    // Inserts a new key into a internal node with split the node into two
    int insert_into_internal_after_splitting(int64_t table_id, pagenum_t internal, pagenum_t left, int64_t key, pagenum_t right) 
    {
        pagenum_t new_internal, child;
        NodePage new_internal_node, internal_node, child_node;
        Edge edge;
        int right_index, flag;
        int64_t new_key;

        // load internal node
        internal_node = load_node_page(table_id, internal);
        if (internal_node.header.is_leaf) return FLAG_FAIL;

        // make new edge and insert the edge
        edge = Edge(key, right);
        right_index = insert_node_key(internal_node.edges, edge);
        if (!is_correct_internal_index(right_index-1, internal_node, left)) return FLAG_FAIL;

        // create new internal node page
        new_internal = make_node_page(table_id, false);
        new_internal_node = load_node_page(table_id, new_internal);

        // split array of edges
        flag = split_internal_edges(new_internal_node, internal_node.edges, new_key);
        if (flag) {
            file_free_page(table_id, new_internal);
            return FLAG_FAIL;
        };

        // update parent page number
        new_internal_node.header.parent_page_number = internal_node.header.parent_page_number;
        update_node(internal_node);
        update_node(new_internal_node);

        // flush the node pages
        save_node_page(table_id, internal, internal_node);
        save_node_page(table_id, new_internal, new_internal_node);

        // update parent of child node pages
        set_parent_pages(table_id, new_internal);

        // insert a new Edge (new key, new internal node) into its parent node
        return insert_into_parent(table_id, internal, new_key, new_internal);
    }


    // Inserts a new node (leaf or internal node) into the B+ tree. 
    // Returns the root of the tree after insertion.
    int insert_into_parent(int64_t table_id, pagenum_t left, int64_t key, pagenum_t right)
    {
        int left_index;
        int64_t left_key;
        pagenum_t parent;
        NodePage parent_node, left_node;

        // load left node
        left_node = load_node_page(table_id, left);
        parent = left_node.header.parent_page_number;

        // Case: new root
        if (parent == 0) {
            return insert_into_new_root(table_id, left, key, right);
        }

        // Simple case: the new key fits into the node.
        parent_node = load_node_page(table_id, parent);
        if (parent_node.header.number_of_keys < EDGE_MAX_COUNT) {
            return insert_into_internal(table_id, parent, left, key, right);
        }

        // Harder case:  split a node in order to preserve the B+ tree properties.
        return insert_into_internal_after_splitting(table_id, parent, left, key, right);
    }


    // Creates a new root for two subtrees and inserts the appropriate key into the new root.
    int insert_into_new_root(int64_t table_id, pagenum_t left, int64_t key, pagenum_t right)
    {
        pagenum_t new_root;
        NodePage new_root_node, left_node, right_node;

        // create new root node
        new_root = make_node_page(table_id, false);
        new_root_node = load_node_page(table_id, new_root);
        new_root_node.edges.clear();

        // fill the data of new root page
        new_root_node.header.first_child_page_number = left;
        new_root_node.edges.push_back(Edge(key, right));
        new_root_node.header.number_of_keys = 1;
        save_node_page(table_id, new_root, new_root_node);
        
        // update root page number of header page
        set_root_page(table_id, new_root);

        // update parent page number of child pages
        set_parent_page(table_id, left, new_root);
        set_parent_page(table_id, right, new_root);

        return FLAG_SUCCESS;
    }

    // Master insertion function
    int insert(int64_t table_id, pagenum_t root, int64_t key, std::string value) 
    {
        NodePage leaf_node;
        pagenum_t leaf;
        std::string temp;

        // if the key is already in the table, return flag 1 (ignore duplicates)
        if (find(table_id, root, key, temp) == 0) {
            return FLAG_FAIL;
        }

        // Case 0: the tree does not exist yet. start a new tree.
        if (root == 0) {
            leaf = root = make_node_page(table_id);
            set_root_page(table_id, root);
            return insert_into_leaf(table_id, leaf, key, value);
        }

        // find leaf to insert given key
        leaf = find_leaf(table_id, root, key);
        if (leaf == 0) return FLAG_FAIL;
        leaf_node = load_node_page(table_id, leaf);

        // Case 1: leaf has enough free space to insert new record
        if (leaf_node.header.amount_of_free_space >= SLOT_SIZE + value.size()) {
            return insert_into_leaf(table_id, leaf, key, value);
        }

        // Case 2: No room for new record (leaf must be split)
        return insert_into_leaf_after_splitting(table_id, leaf, key, value);
    }


    /// DELETION

    template <typename T>
    int delete_node_key(std::deque<T>& dest, int64_t key)
    {
        int deletion_point;

        // find deletion point and delete the key
        deletion_point = find_key_index(dest, key);
        if (deletion_point < 0 || deletion_point > dest.size()) {
            return -1;
        }
        dest.erase(dest.begin()+deletion_point);

        return deletion_point;
    }

    int remove_entry_from_node(int64_t table_id, pagenum_t key_page, int64_t key)
    {
        int index;
        NodePage key_node;

        // load key node page to be inserted
        key_node = load_node_page(table_id, key_page);

        // find key index and delete the record or edge
        if (key_node.header.is_leaf) {
            index = delete_node_key(key_node.slots, key);
            if (index < 0) return FLAG_FAIL;
            update_node(key_node, index);
        } else {
            index = delete_node_key(key_node.edges, key);
            update_node(key_node);
        }

        // flush the node
        save_node_page(table_id, key_page, key_node);

        return FLAG_SUCCESS;
    }

    int adjust_root(int64_t table_id, pagenum_t root)
    {
        NodePage root_node, new_root_node;
        pagenum_t new_root;

        // load root page
        root_node = load_node_page(table_id, root);
        if (root_node.header.parent_page_number != 0) return FLAG_FAIL;

        // Case: nonempty root
        // Key and pointer have already been deleted, so nothing to do.
        if (root_node.header.number_of_keys > 0) return FLAG_SUCCESS;

        // Case: empty root
        if (root_node.header.is_leaf) {
            // If it is a leaf (has no children), then the whole tree is empty.
            set_root_page(table_id, 0);
        } else {
            // If it has a child, promote the first (only) child as the new root.
            new_root = root_node.header.first_child_page_number;
            set_parent_page(table_id, new_root, 0);
            set_root_page(table_id, new_root);
        }

        // free old root page
        free_node_page(table_id, root);

        return FLAG_SUCCESS;
    }

    template <typename T>
    int append_node_keys(std::deque<T>& dest, std::deque<T>& src)
    {
        int insertion_point;

        // Get insertion point and append src to dest
        insertion_point = dest.size();
        dest.insert(dest.end(), src.begin(), src.end());
        src.clear();
        
        return insertion_point;
    }

    // Merge
    int merge_nodes(int64_t table_id, pagenum_t root, pagenum_t left, pagenum_t right, int64_t prime_key)
    {
        int insertion_index;
        NodePage left_node, right_node;
        pagenum_t parent;

        // load node pages and get data
        left_node = load_node_page(table_id, left);
        right_node = load_node_page(table_id, right);
        insertion_index = left_node.header.number_of_keys;
        parent = left_node.header.parent_page_number;

        // Merge (Append right keys to left keys)
        if (left_node.header.is_leaf) {
            // Case: leaf node
            append_node_keys(left_node.slots, right_node.slots);
            left_node.header.right_sibling_page_number = right_node.header.right_sibling_page_number;
            update_node(left_node, insertion_index);
            save_node_page(table_id, left, left_node);
        } else {
            // Case: internal node
            left_node.edges.push_back(Edge(prime_key, right_node.header.first_child_page_number));
            append_node_keys(left_node.edges, right_node.edges);
            update_node(left_node);
            save_node_page(table_id, left, left_node);
            set_parent_pages(table_id, left, insertion_index);
        }

        // Free right node page
        free_node_page(table_id, right);

        return delete_entry(table_id, root, parent, prime_key);
    }


    // Redistribution
    int redistribute_nodes(int64_t table_id, pagenum_t left, pagenum_t right, int prime_key_index, int64_t prime_key) 
    {
        NodePage left_node, right_node, parent_node;
        pagenum_t parent;

        // Load node pages to be redistributed
        left_node = load_node_page(table_id, left);
        right_node = load_node_page(table_id, right);

        // Load parent page and check the prime key index is valid
        parent = left_node.header.parent_page_number;
        parent_node = load_node_page(table_id, parent);
        if (parent_node.edges[prime_key_index].page_number != right) return FLAG_FAIL;

        // Redistribute
        if (left_node.header.is_leaf) {
            // Case: leaf nodes
            if (left_node.header.amount_of_free_space < right_node.header.amount_of_free_space) {
                // Case: number of left records >= number of right records
                while (right_node.header.amount_of_free_space >= RECORD_THRESHOLD) {
                    right_node.slots.push_front(left_node.slots.back());
                    left_node.slots.pop_back();
                    update_node(right_node);
                }
                update_node(left_node, left_node.slots.size());
            } else {
                // Case: number of left records < number of right records
                while (left_node.header.amount_of_free_space >= RECORD_THRESHOLD) {
                    left_node.slots.push_back(right_node.slots.front());
                    right_node.slots.pop_front();
                    update_node(left_node, left_node.header.number_of_keys);
                }
                update_node(right_node);
            }
            // Update prime key
            prime_key = right_node.slots.front().key;
        } else {
            // Case: internal nodes
            if (left_node.header.number_of_keys >= right_node.header.number_of_keys) {
                // Case: number of left edges >= number of right edges
                if (right_node.header.number_of_keys < EDGE_MAX_COUNT/2) {
                    right_node.edges.push_front(Edge(prime_key, right_node.header.first_child_page_number));
                    prime_key = left_node.edges.back().key;
                    right_node.header.first_child_page_number = left_node.edges.back().page_number;
                    left_node.edges.pop_back();
                    set_parent_page(table_id, right_node.header.first_child_page_number, right);
                }
            } else {
                // Case: number of left edges < number of right edges
                if (left_node.header.number_of_keys < EDGE_MAX_COUNT/2) {
                    left_node.edges.push_back(Edge(prime_key, right_node.header.first_child_page_number));
                    prime_key = right_node.edges.front().key;
                    right_node.header.first_child_page_number = right_node.edges.front().page_number;
                    right_node.edges.pop_front();
                    set_parent_page(table_id, left_node.edges.back().page_number, left);
                }
            }
            // Update header of internal nodes
            update_node(left_node);
            update_node(right_node);
        }

        // Update and save parent node
        parent_node.edges[prime_key_index].key = prime_key;
        save_node_page(table_id, parent, parent_node);

        // Save redistributed nodes
        save_node_page(table_id, left, left_node);
        save_node_page(table_id, right, right_node);

        return FLAG_SUCCESS;
    }


    // Deletes an entry from the B+ tree
    int delete_entry(int64_t table_id, pagenum_t root, pagenum_t key_page, int64_t key) 
    {
        int flag, key_index;
        NodePage key_node, parent_node, neighbor_node;
        pagenum_t parent, left, right, neighbor;
        int64_t prime_key, first_key;

        // Remove a record having the key from given page
        flag = remove_entry_from_node(table_id, key_page, key);
        if (flag) return FLAG_FAIL;

        // Case 0: deletion from the root (there is no sibling)
        if (key_page == root) {
            return adjust_root(table_id, root);
        }

        // Case 1: amount of free space of key_node is less than RECORD_THRESHOLD (2500 Bytes)
        // Nothing to do (the simple case)
        key_node = load_node_page(table_id, key_page);
        if (key_node.header.number_of_keys == 0) return FLAG_FAIL;
        if (key_node.header.is_leaf) {
            if (key_node.header.amount_of_free_space < RECORD_THRESHOLD) return FLAG_SUCCESS;
            first_key = key_node.slots[0].key;
        } else {
            if (key_node.header.number_of_keys >= EDGE_MAX_COUNT/2) return FLAG_SUCCESS;
            first_key = key_node.edges[0].key;
        }

        // Case 2: The other cases (need to be merge or redistribute)
        // Get the index having the key
        parent = key_node.header.parent_page_number;
        parent_node = load_node_page(table_id, parent);
        key_index = find_key_index(parent_node.edges, first_key);
        if (!is_correct_internal_index(key_index, parent_node, key_page)) return FLAG_FAIL;

        // Set left/right node page number and prime key
        if (key_index <= 0) {
            left = parent_node.header.right_sibling_page_number;
            right = parent_node.edges[0].page_number;
            prime_key = parent_node.edges[0].key;
            neighbor = key_index < 0 ? right : left;
            key_index = 0;
        } else {
            left = neighbor = parent_node.edges[key_index-1].page_number;
            right = key_page;
            prime_key = parent_node.edges[key_index].key;
        }

        // Load neighbor node and check the case
        neighbor_node = load_node_page(table_id, neighbor);
        if (
            (key_node.header.is_leaf && key_node.header.amount_of_free_space + neighbor_node.header.amount_of_free_space > BODY_SIZE) ||
            (!key_node.header.is_leaf && key_node.header.number_of_keys + neighbor_node.header.number_of_keys < EDGE_MAX_COUNT)
        ) {
            // Case 2-1: Merge
            return merge_nodes(table_id, root, left, right, prime_key);
        } else {
            // Case 2-2: Redistribution
            return redistribute_nodes(table_id, left, right, key_index, prime_key);
        }
    }

    // Master deletion function
    int db_delete(int64_t table_id, pagenum_t root, int64_t key)
    {
        NodePage leaf_node;
        pagenum_t key_leaf;
        std::string temp;

        // if the key is not in the table, return flag 1
        if (find(table_id, root, key, temp) != 0) {
            return FLAG_FAIL;
        }

        // find leaf page to delete given key
        key_leaf = find_leaf(table_id, root, key);
        if (key_leaf == 0) return FLAG_FAIL;

        // delete given key from the leaf page
        return delete_entry(table_id, root, key_leaf, key);
    }


    /// Destruction

    void destroy_tree_nodes(int64_t table_id, pagenum_t page_number)
    {
        NodePage node;
        pagenum_t child;

        // load given page
        node = load_node_page(table_id, page_number);

        // free node pages
        if (node.header.is_leaf) {
            free_node_page(table_id, page_number);
        } else {
            for (int idx = -1; idx < node.edges.size(); idx++) {
                if (idx == -1) child = node.header.first_child_page_number;
                else child = node.edges[idx].page_number;
                destroy_tree_nodes(table_id, child);
            }
        }
    }

    void destroy_tree(int64_t table_id, pagenum_t root) 
    {
        // free pages of tree and set root page to zero
        destroy_tree_nodes(table_id, root);
        set_root_page(table_id, 0);
    }
}
