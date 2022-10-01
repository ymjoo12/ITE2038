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
                LogUtil::PrintPage(node);
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
                LogUtil::PrintPage(node);
                return false;
            }
        } else {
            // Check the number of keys
            if (node.header.number_of_keys != node.edges.size()) {
                std::cout << "[is_valid_node_page] the number of keys is invalid" << std::endl;
                LogUtil::PrintPage(node);
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


    // Node page IO (buffer IO)

    NodePage load_node_page(int64_t table_id, pagenum_t page_number, int& pin_id, bool pin)
    {
        if (LogUtil::DEBUG_MODE) {
            LogUtil::PrintMarker(__func__, "( page_number: " + std::to_string(page_number) + " )");
        }

        NodePage node_to_return;

        // Check the page is header page
        if (page_number == 0) {
            std::cout << "[load_node_page] tried to read header page as node page" << std::endl;
            exit(1);
        }

        // Read given page as Node Page
        node_to_return = NodePage(BUF::read_page(table_id, page_number, pin_id, pin));
        if (LogUtil::DEBUG_MODE) LogUtil::PrintPage(node_to_return);

        // Check the page is valid page
        if (!is_valid_node_page(node_to_return)) {
            std::cout << "[load_node_page] invalid page " << std::endl;
            std::cout << "( table_id: " << table_id << ", page_number: " << page_number << " )" << std::endl;
            exit(1);
        }
        return node_to_return;
    }

    void save_node_page(int64_t table_id, pagenum_t page_number, NodePage& node, int pin_id, bool cancel)
    {
        if (LogUtil::DEBUG_MODE) {
            LogUtil::PrintMarker(__func__, "( page_number: " + std::to_string(page_number) + " )");
            LogUtil::PrintPage(node, true);
        }

        // If cancel argument is true, unpin this page in buffer and skip to write
        if (cancel) {
            BUF::unpin_page(pin_id);
            return;
        }

        // Check the page is valid page
        if (!is_valid_node_page(node)) {
            std::cout << "[load_node_page] invalid page " << std::endl;
            std::cout << "( table_id: " << table_id << ", page_number: " << page_number << " )" << std::endl;
            exit(1);
        }

        // Convert node page to page_t and write page
        BUF::write_page(pin_id, node);
    }

    pagenum_t make_node_page(int64_t table_id, bool is_leaf)
    {
        if (LogUtil::DEBUG_MODE) LogUtil::PrintMarker(__func__);

        pagenum_t new_page_number;
        NodePage new_node;

        // allocate new page
        new_page_number = BUF::alloc_page(table_id);

        // create new node page and initialize the node page in buffer
        new_node = NodePage(is_leaf);
        BUF::write_page(BUF::pin_page(table_id, new_page_number), new_node);

        return new_page_number;
    }

    void free_node_page(int64_t table_id, pagenum_t page_number, int pin_id)
    {
        if (LogUtil::DEBUG_MODE) LogUtil::PrintMarker(__func__);

        // free given page
        BUF::free_page(table_id, page_number, pin_id);
    }


    /// Getter and setter (buffer IO)

    // get root page number, if root page doesn't exist, create it
    pagenum_t get_root_page(int64_t table_id)
    {
        int pin_id;
        pagenum_t root;
        HeaderPage header;

        // Read header page and return root page number
        header = HeaderPage(BUF::read_page(table_id, 0, pin_id));
        root = header.root_page_number;

        return root;
    }

    // set root page number to on-disk header page
    void set_root_page(int64_t table_id, pagenum_t root)
    {
        int pin_id;
        HeaderPage header;

        // Update header page
        header = HeaderPage(BUF::read_page(table_id, 0, pin_id, true));
        header.root_page_number = root;
        BUF::write_page(pin_id, header);
    }

    // set parent page number of child page
    void set_parent_page(int64_t table_id, pagenum_t child, pagenum_t parent)
    {
        int pin_id;
        NodePage child_node;

        // update parent page number of child page
        child_node = load_node_page(table_id, child, pin_id, true);
        child_node.header.parent_page_number = parent;
        save_node_page(table_id, child, child_node, pin_id);
    }

    // set parent page number of child pages of parent page (must have been updated child data in parent)
    void set_parent_pages(int64_t table_id, pagenum_t parent, int start_point) 
    {
        int pin_id;
        pagenum_t child;
        NodePage parent_node, child_node;

        // update parent of child node pages
        parent_node = load_node_page(table_id, parent, pin_id);
        for (int idx = start_point; idx < (int)parent_node.header.number_of_keys; idx++) {
            if (idx == -1) child = parent_node.header.first_child_page_number;
            else child = parent_node.edges[idx].page_number;
            set_parent_page(table_id, child, parent);
        }
    }


    /// Correction functions

    // Correct metadata of node page (header, offset)
    void correct_node(NodePage& node, int start_point)
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
        int pin_id;
        int idx;
        pagenum_t leaf;
        NodePage node;

        // load the given root page
        if (root == 0) return 0;
        node = load_node_page(table_id, root, pin_id);

        // find the leaf page
        leaf = root;
        while (!node.header.is_leaf) {
            // find the candidate index having the key
            idx = find_key_index(node.edges, key);

            // load the page of the index found
            if (idx == -1) leaf = node.header.first_child_page_number;
            else leaf = node.edges[idx].page_number;
            node = load_node_page(table_id, leaf, pin_id);
        }

        return leaf;
    }

    // find the record index having given key
    std::pair<int, int> find_record(int64_t table_id, pagenum_t leaf, int64_t key)
    {
        int pin_id;
        int idx;
        NodePage leaf_node;

        // Load the given leaf page
        if (leaf == 0) return {-1,-1};
        leaf_node = load_node_page(table_id, leaf, pin_id);

        // Find the key in the leaf node
        idx = find_key_index(leaf_node.slots, key);
        if (idx < 0 || idx >= leaf_node.header.number_of_keys || leaf_node.slots[idx].key != key) {
            return {-1,-1};
        }

        // Return the pair of leaf page number and record index
        return {leaf_node.slots[idx].trx_id, idx};
    }

    // Finds and returns the record to which a key refers
    int find(int64_t table_id, pagenum_t root, int64_t key, std::string& value, pagenum_t key_page)
    {
        int pin_id;
        int idx;
        pagenum_t leaf;
        NodePage leaf_node;

        // Initialize the return value to empty string
        value = "";

        // Find the leaf node having the key
        leaf = key_page > 0 ? key_page : find_leaf(table_id, root, key);
        if (leaf == 0) return FLAG::FAILURE;
        leaf_node = load_node_page(table_id, leaf, pin_id);

        // Find the key in the leaf node
        idx = find_key_index(leaf_node.slots, key);
        if (idx < 0 || idx >= leaf_node.header.number_of_keys || leaf_node.slots[idx].key != key) {
            return FLAG::FAILURE;
        }

        // Return the value of the key
        value = leaf_node.slots[idx].value;
        return FLAG::SUCCESS;
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

        // find index to split and check the point is valid
        for (split_point = 0, size = 0; split_point < origin.size(); split_point++) {
            size += origin[split_point].size + SLOT_SIZE;
            if (size >= BODY_SIZE/2) break;
        }
        if (split_point == 0) return FLAG::FAILURE;

        // copy slots after split point to right_slots
        right_slots.clear();
        right_slots = std::deque<Record>(origin.begin()+split_point+1, origin.end());
        origin.erase(origin.begin()+split_point+1, origin.end());

        // Get first key of right node
        prime_key = right_slots[0].key;

        return FLAG::SUCCESS;
    }

    // split origin edge array and return the first key of right node
    int split_internal_edges(NodePage& right_node, std::deque<Edge>& origin, int64_t& prime_key)
    {
        int split_point;

        // find index to split and check the point is valid
        split_point = (origin.size()+1)/2;
        if (split_point == 0) return FLAG::FAILURE;

        // copy slots after split point to right_node.edges
        right_node.edges.clear();
        right_node.edges = std::deque<Edge>(origin.begin()+split_point+1, origin.end());
        
        // Set first child of right node and get first key of right node
        right_node.header.first_child_page_number = origin[split_point].page_number;
        prime_key = origin[split_point].key;
        origin.erase(origin.begin()+split_point, origin.end());

        return FLAG::SUCCESS;
    }

    int insert_into_leaf(int64_t table_id, pagenum_t leaf, int64_t key, std::string value)
    {
        int pin_id;
        int index;
        NodePage leaf_node;
        Record record;

        // load leaf node to be inserted
        leaf_node = load_node_page(table_id, leaf, pin_id, true);
        if (!leaf_node.header.is_leaf) {
            save_node_page(table_id, leaf, leaf_node, pin_id, true);
            return FLAG::FAILURE;
        }

        // make new record and insert the record
        record = Record(key, value);
        index = insert_node_key(leaf_node.slots, record);
        if (index < 0) {
            save_node_page(table_id, leaf, leaf_node, pin_id, true);
            return FLAG::FAILURE;
        }

        // update offsets of records and page header
        correct_node(leaf_node, index);

        // set the node
        save_node_page(table_id, leaf, leaf_node, pin_id);
        return FLAG::SUCCESS;
    }

    // Inserts a new key into a leaf node with split the node into two
    int insert_into_leaf_after_splitting(int64_t table_id, pagenum_t leaf, int64_t key, std::string value)
    {
        int pin_id_l, pin_id_n;
        int index, flag;
        int64_t new_key;
        pagenum_t new_leaf;
        NodePage new_leaf_node, leaf_node;
        Record record;

        // load leaf node
        leaf_node = load_node_page(table_id, leaf, pin_id_l, true);
        if (!leaf_node.header.is_leaf) {
            save_node_page(table_id, leaf, leaf_node, pin_id_l, true);
            return FLAG::FAILURE;
        }

        // make new record and insert the record
        record = Record(key, value);
        index = insert_node_key(leaf_node.slots, record);

        // create new leaf node page
        new_leaf = make_node_page(table_id);
        new_leaf_node = load_node_page(table_id, new_leaf, pin_id_n, true);

        // split array of slots
        flag = split_leaf_slots(new_leaf_node.slots, leaf_node.slots, new_key);
        if (flag) {
            free_node_page(table_id, new_leaf, pin_id_n);
            return FLAG::FAILURE;
        };

        // update sibling and parent page number
        new_leaf_node.header.parent_page_number = leaf_node.header.parent_page_number;
        new_leaf_node.header.right_sibling_page_number = leaf_node.header.right_sibling_page_number;
        leaf_node.header.right_sibling_page_number = new_leaf;

        // update offsets of records and page headers
        correct_node(leaf_node);
        correct_node(new_leaf_node);

        // set the node pages
        save_node_page(table_id, leaf, leaf_node, pin_id_l);
        save_node_page(table_id, new_leaf, new_leaf_node, pin_id_n);

        return insert_into_parent(table_id, leaf, new_key, new_leaf);
    }

    // Inserts a new key into a internal node
    int insert_into_internal(int64_t table_id, pagenum_t internal, pagenum_t left, int64_t key, pagenum_t right)
    {
        int pin_id;
        int right_index;
        NodePage internal_node;
        Edge edge;

        // load internal node to be inserted
        internal_node = load_node_page(table_id, internal, pin_id, true);
        if (internal_node.header.is_leaf) {
            save_node_page(table_id, internal, internal_node, pin_id, true);
            return FLAG::FAILURE;
        }

        // make new edge, and insert the edge
        edge = Edge(key, right);
        right_index = insert_node_key(internal_node.edges, edge);
        if (!is_correct_internal_index(right_index-1, internal_node, left)) {
            save_node_page(table_id, internal, internal_node, pin_id, true);
            return FLAG::FAILURE;
        }
        
        // update page header
        if (internal_node.edges.size() != internal_node.header.number_of_keys + 1) {
            save_node_page(table_id, internal, internal_node, pin_id, true);
            return FLAG::FAILURE;
        }
        correct_node(internal_node);

        // set the node
        save_node_page(table_id, internal, internal_node, pin_id);
        return FLAG::SUCCESS;
    }

    // Inserts a new key into a internal node with split the node into two
    int insert_into_internal_after_splitting(int64_t table_id, pagenum_t internal, pagenum_t left, int64_t key, pagenum_t right) 
    {
        int pin_id_i, pin_id_n;
        int right_index, flag;
        int64_t new_key;
        pagenum_t new_internal, child;
        NodePage new_internal_node, internal_node, child_node;
        Edge edge;

        // load internal node
        internal_node = load_node_page(table_id, internal, pin_id_i, true);
        if (internal_node.header.is_leaf) {
            save_node_page(table_id, internal, internal_node, pin_id_i, true);
            return FLAG::FAILURE;
        }

        // make new edge and insert the edge
        edge = Edge(key, right);
        right_index = insert_node_key(internal_node.edges, edge);
        if (!is_correct_internal_index(right_index-1, internal_node, left)) {
            save_node_page(table_id, internal, internal_node, pin_id_i, true);
            return FLAG::FAILURE;
        }

        // create new internal node page
        new_internal = make_node_page(table_id, false);
        new_internal_node = load_node_page(table_id, new_internal, pin_id_n, true);

        // split array of edges
        flag = split_internal_edges(new_internal_node, internal_node.edges, new_key);
        if (flag) {
            free_node_page(table_id, new_internal, pin_id_n);
            return FLAG::FAILURE;
        };

        // update parent page number
        new_internal_node.header.parent_page_number = internal_node.header.parent_page_number;
        correct_node(internal_node);
        correct_node(new_internal_node);

        // set the node pages
        save_node_page(table_id, internal, internal_node, pin_id_i);
        save_node_page(table_id, new_internal, new_internal_node, pin_id_n);

        // update parent of child node pages
        set_parent_pages(table_id, new_internal);

        // insert a new Edge (new key, new internal node) into its parent node
        return insert_into_parent(table_id, internal, new_key, new_internal);
    }


    // Inserts a new node (leaf or internal node) into the B+ tree. 
    // Returns the root of the tree after insertion.
    int insert_into_parent(int64_t table_id, pagenum_t left, int64_t key, pagenum_t right)
    {
        int pin_id_l, pin_id_p;
        int left_index;
        int64_t left_key;
        pagenum_t parent;
        NodePage parent_node, left_node;

        // load left node
        left_node = load_node_page(table_id, left, pin_id_l);
        parent = left_node.header.parent_page_number;

        // Case: new root
        if (parent == 0) {
            return insert_into_new_root(table_id, left, key, right);
        }

        // Simple case: the new key fits into the node.
        parent_node = load_node_page(table_id, parent, pin_id_p);
        if (parent_node.header.number_of_keys < EDGE_MAX_COUNT) {
            return insert_into_internal(table_id, parent, left, key, right);
        }

        // Harder case:  split a node in order to preserve the B+ tree properties.
        return insert_into_internal_after_splitting(table_id, parent, left, key, right);
    }


    // Creates a new root for two subtrees and inserts the appropriate key into the new root.
    int insert_into_new_root(int64_t table_id, pagenum_t left, int64_t key, pagenum_t right)
    {
        int pin_id;
        pagenum_t new_root;
        NodePage new_root_node, left_node, right_node;

        // create new root node
        new_root = make_node_page(table_id, false);
        new_root_node = load_node_page(table_id, new_root, pin_id, true);
        new_root_node.edges.clear();

        // fill the data of new root page
        new_root_node.header.first_child_page_number = left;
        new_root_node.edges.push_back(Edge(key, right));
        new_root_node.header.number_of_keys = 1;
        save_node_page(table_id, new_root, new_root_node, pin_id);
        
        // update root page number of header page
        set_root_page(table_id, new_root);

        // update parent page number of child pages
        set_parent_page(table_id, left, new_root);
        set_parent_page(table_id, right, new_root);

        return FLAG::SUCCESS;
    }

    // Master insertion function
    int insert(int64_t table_id, pagenum_t root, int64_t key, std::string value) 
    {
        int pin_id;
        pagenum_t leaf;
        NodePage leaf_node;
        std::string temp;

        // if the key is already in the table, return flag 1 (ignore duplicates)
        if (find(table_id, root, key, temp) == 0) {
            return FLAG::FAILURE;
        }

        // Case 0: the tree does not exist yet. start a new tree.
        if (root == 0) {
            leaf = root = make_node_page(table_id);
            set_root_page(table_id, root);
            return insert_into_leaf(table_id, leaf, key, value);
        }

        // find leaf to insert given key
        leaf = find_leaf(table_id, root, key);
        if (leaf == 0) return FLAG::FAILURE;
        leaf_node = load_node_page(table_id, leaf, pin_id);

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
        int pin_id;
        int index;
        NodePage key_node;

        // load key node page to be inserted
        key_node = load_node_page(table_id, key_page, pin_id, true);

        // find key index and delete the record or edge
        if (key_node.header.is_leaf) {
            index = delete_node_key(key_node.slots, key);
            if (index < 0) return FLAG::FAILURE;
            correct_node(key_node, index);
        } else {
            index = delete_node_key(key_node.edges, key);
            correct_node(key_node);
        }

        // set the node
        save_node_page(table_id, key_page, key_node, pin_id);
        return FLAG::SUCCESS;
    }

    int adjust_root(int64_t table_id, pagenum_t root)
    {
        int pin_id;
        pagenum_t new_root;
        NodePage root_node, new_root_node;

        // load root page
        root_node = load_node_page(table_id, root, pin_id, true);
        if (root_node.header.parent_page_number != 0) {
            save_node_page(table_id, root, root_node, pin_id, true);
            return FLAG::FAILURE;
        }

        // Case: nonempty root
        // Key and pointer have already been deleted, so nothing to do.
        if (root_node.header.number_of_keys > 0) {
            save_node_page(table_id, root, root_node, pin_id, true);
            return FLAG::SUCCESS;
        }

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
        free_node_page(table_id, root, pin_id);
        return FLAG::SUCCESS;
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
        int pin_id_l, pin_id_r;
        int insertion_index;
        pagenum_t parent;
        NodePage left_node, right_node;

        // load node pages and get data
        left_node = load_node_page(table_id, left, pin_id_l, true);
        right_node = load_node_page(table_id, right, pin_id_r, true);
        insertion_index = left_node.header.number_of_keys;
        parent = left_node.header.parent_page_number;

        // Merge (Append right keys to left keys)
        if (left_node.header.is_leaf) {
            // Case: leaf node
            append_node_keys(left_node.slots, right_node.slots);
            left_node.header.right_sibling_page_number = right_node.header.right_sibling_page_number;
            correct_node(left_node, insertion_index);
            save_node_page(table_id, left, left_node, pin_id_l);
        } else {
            // Case: internal node
            left_node.edges.push_back(Edge(prime_key, right_node.header.first_child_page_number));
            append_node_keys(left_node.edges, right_node.edges);
            correct_node(left_node);
            save_node_page(table_id, left, left_node, pin_id_l);
            set_parent_pages(table_id, left, insertion_index);
        }

        // Free right node page
        free_node_page(table_id, right, pin_id_r);

        return delete_entry(table_id, root, parent, prime_key);
    }


    // Redistribution
    int redistribute_nodes(int64_t table_id, pagenum_t left, pagenum_t right, int prime_key_index, int64_t prime_key) 
    {
        int pin_id_l, pin_id_r, pin_id_p;
        pagenum_t parent;
        NodePage left_node, right_node, parent_node;

        // Load node pages to be redistributed
        left_node = load_node_page(table_id, left, pin_id_l, true);
        right_node = load_node_page(table_id, right, pin_id_r, true);

        // Load parent page and check the prime key index is valid
        parent = left_node.header.parent_page_number;
        parent_node = load_node_page(table_id, parent, pin_id_p, true);
        if (parent_node.edges[prime_key_index].page_number != right) {
            save_node_page(table_id, left, left_node, pin_id_l, true);
            save_node_page(table_id, right, right_node, pin_id_r, true);
            save_node_page(table_id, parent, parent_node, pin_id_p, true);
            return FLAG::FAILURE;
        }

        // Redistribute
        if (left_node.header.is_leaf) {
            // Case: leaf nodes
            if (left_node.header.amount_of_free_space < right_node.header.amount_of_free_space) {
                // Case: number of left records >= number of right records
                while (right_node.header.amount_of_free_space >= RECORD_THRESHOLD) {
                    right_node.slots.push_front(left_node.slots.back());
                    left_node.slots.pop_back();
                    correct_node(right_node);
                }
                correct_node(left_node, left_node.slots.size());
            } else {
                // Case: number of left records < number of right records
                while (left_node.header.amount_of_free_space >= RECORD_THRESHOLD) {
                    left_node.slots.push_back(right_node.slots.front());
                    right_node.slots.pop_front();
                    correct_node(left_node, left_node.header.number_of_keys);
                }
                correct_node(right_node);
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
            correct_node(left_node);
            correct_node(right_node);
        }

        // Update and save parent node
        parent_node.edges[prime_key_index].key = prime_key;
        save_node_page(table_id, parent, parent_node, pin_id_p);

        // Save redistributed nodes
        save_node_page(table_id, left, left_node, pin_id_l);
        save_node_page(table_id, right, right_node, pin_id_r);

        return FLAG::SUCCESS;
    }


    // Deletes an entry from the B+ tree
    int delete_entry(int64_t table_id, pagenum_t root, pagenum_t key_page, int64_t key) 
    {
        int pin_id_k, pin_id_p, pin_id_n;
        int flag, key_index;
        int64_t prime_key, first_key;
        pagenum_t parent, left, right, neighbor;
        NodePage key_node, parent_node, neighbor_node;

        // Remove a record having the key from given page
        flag = remove_entry_from_node(table_id, key_page, key);
        if (flag) return FLAG::FAILURE;

        // Case 0: deletion from the root (there is no sibling)
        if (key_page == root) {
            return adjust_root(table_id, root);
        }

        // Case 1: amount of free space of key_node is less than RECORD_THRESHOLD (2500 Bytes)
        // Nothing to do (the simple case)
        key_node = load_node_page(table_id, key_page, pin_id_k);
        if (key_node.header.number_of_keys == 0) return FLAG::FAILURE;
        if (key_node.header.is_leaf) {
            if (key_node.header.amount_of_free_space < RECORD_THRESHOLD) return FLAG::SUCCESS;
            first_key = key_node.slots[0].key;
        } else {
            if (key_node.header.number_of_keys >= EDGE_MAX_COUNT/2) return FLAG::SUCCESS;
            first_key = key_node.edges[0].key;
        }

        // Case 2: The other cases (need to be merge or redistribute)
        // Get the index having the key
        parent = key_node.header.parent_page_number;
        parent_node = load_node_page(table_id, parent, pin_id_p);
        key_index = find_key_index(parent_node.edges, first_key);
        if (!is_correct_internal_index(key_index, parent_node, key_page)) return FLAG::FAILURE;

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
        neighbor_node = load_node_page(table_id, neighbor, pin_id_n);
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
        pagenum_t key_leaf;
        NodePage leaf_node;
        std::string temp;

        // if the key is not in the table, return flag 1
        if (find(table_id, root, key, temp) != 0) {
            return FLAG::FAILURE;
        }

        // find leaf page to delete given key
        key_leaf = find_leaf(table_id, root, key);
        if (key_leaf == 0) return FLAG::FAILURE;

        // delete given key from the leaf page
        return delete_entry(table_id, root, key_leaf, key);
    }


    /// Destruction

    void destroy_tree_nodes(int64_t table_id, pagenum_t page_number)
    {
        int pin_id;
        pagenum_t child;
        NodePage node;

        // load given page
        node = load_node_page(table_id, page_number, pin_id);

        // free node pages
        if (node.header.is_leaf) {
            free_node_page(table_id, page_number, pin_id);
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


    /// Update

    // Master update function
    int update(int64_t table_id, pagenum_t leaf, int64_t key, Record& record_old, std::string value_new, int trx_id)
    {
        
        printf("[INFO][%s][trx_id: %d] Begin (table_id: %d, leaf: %d, key: %d, value_new: %s)\n", __func__, trx_id, table_id, leaf, key, value_new.c_str());

        int pin_id, idx;
        NodePage leaf_node;

        // Find the leaf node having the key
        if (leaf == 0) return FLAG::FAILURE;
        leaf_node = load_node_page(table_id, leaf, pin_id, true);

        // Find the key in the leaf node
        idx = find_key_index(leaf_node.slots, key);
        if (idx < 0 || idx >= leaf_node.header.number_of_keys || leaf_node.slots[idx].key != key) {
            save_node_page(table_id, leaf, leaf_node, pin_id, true);
            return FLAG::FAILURE;
        }

        // Get the old value
        record_old = leaf_node.slots[idx];
        if (record_old.value.size() != value_new.size()) {
            save_node_page(table_id, leaf, leaf_node, pin_id, true);
            return FLAG::FAILURE;
        }

        
        printf("[INFO][%s][trx_id: %d] Old record (table_id: %d, leaf: %d, key: %d, value: %s, trx_id: %d)\n", __func__, trx_id, table_id, leaf, key, record_old.value.c_str(), record_old.trx_id);

        // Update the value of the record
        leaf_node.slots[idx].value = value_new;
        leaf_node.slots[idx].trx_id = trx_id;

        
        printf("[INFO][%s][trx_id: %d] Assign (table_id: %d, leaf: %d, key: %d, value_new: %s)\n", __func__, leaf_node.slots[idx].trx_id, table_id, leaf, key, leaf_node.slots[idx].value.c_str());

        save_node_page(table_id, leaf, leaf_node, pin_id);

        return FLAG::SUCCESS;
    }
}
