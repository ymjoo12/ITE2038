#include "log_util.h"

/// Utils for debugging
namespace LogUtil 
{
    // Print Functions
    void PrintSeparator(const std::string message)
    {
        std::cout << "\n================================";
        if (!message.empty()) {
            std::cout << "<< " << message << " >>";
        }
        std::cout << "================================\n";
        std::cout << std::endl;
    }

    void PrintMarker(const std::string title, const std::string message)
    {
        if (!title.empty()) {
            std::cout << "[[ " << title << " ]] " << message;
        }
        std::cout << std::endl;
    }

    template <typename T>
    void PrintVariable(const std::string var_name, const T var)
    {
        std::cout << "- " << var_name << ": " << var << " (" << sizeof(var) << " bytes)" << std::endl;
    }

    void PrintRecord(const Record& record)
    {
        std::cout << "\n[Record Data]" << std::endl;
        std::cout << "- key: " << record.key << " (" << sizeof(record.key) << " bytes)" << std::endl;
        std::cout << "- size: " << record.size << " (" << sizeof(record.size) << " bytes)" << std::endl;
        std::cout << "- offset: " << record.offset << " (" << sizeof(record.offset) << " bytes)" << std::endl;
        std::cout << "- value: \"" << record.value << "\" (" << record.value.size() << " bytes)" << std::endl;
        std::cout << "- size of std::string type: " << sizeof(record.value) << " bytes" << std::endl;
        std::cout << "- size of this record: " << sizeof(record.key) + sizeof(record.size) + sizeof(record.offset) + record.value.size() << " bytes" << std::endl;
        std::cout << "- size of Record Class: " << sizeof(Record) << " bytes" << std::endl;
        std::cout << std::endl;
    }

    void PrintEdge(const Edge& edge)
    {
        std::cout << "\n[Edge Data]" << std::endl;
        std::cout << "- key: " << edge.key << " (" << sizeof(edge.key) << " bytes)" << std::endl;
        std::cout << "- page_number: " << edge.page_number << " (" << sizeof(edge.page_number) << " bytes)" << std::endl;
        std::cout << "- size of this edge (sum): " << sizeof(edge.key) + sizeof(edge.page_number) << " bytes" << std::endl;
        std::cout << "- size of this edge: " << sizeof(edge) << " bytes" << std::endl;
        std::cout << "- size of Edge Class: " << sizeof(Edge) << " bytes" << std::endl;
        std::cout << std::endl;
    }

    void PrintPageBytes(const page_t& page, bool verbose, int start, int end)
    {
        start = std::max(0,start); 
        end = std::min((int)PAGE_SIZE, end);
        const int LINE = 128;
        const int SPACE = 4;
        std::cout << "[A Page Bytes] (start: " << start << ", end: " << end << ")" << std::endl;
        std::cout << "- page size: " << sizeof(page) << " bytes" << std::endl;
        
        if (!verbose) return;
        
        for (int bytes = start; bytes < end; bytes++) {
            printf("%02X ", (uint8_t)page.data[bytes]);
            if ((bytes+1) % SPACE == 0) std::cout << " ";
            if ((bytes+1) % LINE == 0) std::cout << std::endl;
        }
        std::cout << std::endl;
    }

    void PrintPage(const NodePage& page, bool verbose)
    {
        if (page.header.is_leaf) {
            std::cout << "[Leaf Page Data]" << std::endl;
        } else {
            std::cout << "[Internal Page Data]" << std::endl;
        }

        std::cout << "- HEADER" << std::endl;
        std::cout << "  - parent_page_number: " << page.header.parent_page_number << " (" << sizeof(page.header.parent_page_number) << " bytes)" << std::endl;
        std::cout << "  - is_leaf: " << page.header.is_leaf << " (" << sizeof(page.header.is_leaf) << " bytes)" << std::endl;
        std::cout << "  - number_of_keys: " << page.header.number_of_keys << " (" << sizeof(page.header.number_of_keys) << " bytes)" << std::endl;
        
        if (page.header.is_leaf) {
            std::cout << "  - right_sibling_page_number: " << page.header.right_sibling_page_number << " (" << sizeof(page.header.right_sibling_page_number) << " bytes)" << std::endl;
            std::cout << "  - amount_of_free_space: " << page.header.amount_of_free_space << " (" << sizeof(page.header.amount_of_free_space) << " bytes)" << std::endl;
        } else {
            std::cout << "  - first_child_page_number: " << page.header.first_child_page_number << " (" << sizeof(page.header.first_child_page_number) << " bytes)" << std::endl;
        }

        if (!verbose) return;

        std::cout << "- BODY" << std::endl;
        if (page.header.is_leaf) {
            for (int idx = 0; idx < page.header.number_of_keys; idx++) {
                std::cout << "  record " << idx 
                    << " - key: " << page.slots[idx].key 
                    << " / size: " << page.slots[idx].size 
                    << " / offset: " << page.slots[idx].offset 
                    << " / value: \"" << page.slots[idx].value << "\"" << std::endl;
            }
        } else {
            for (int idx = 0; idx < page.header.number_of_keys; idx++) {
                std::cout << "  edge " << idx
                    << " - key: " << page.edges[idx].key 
                    << " / page_number: " << page.edges[idx].page_number << std::endl;
            }
        }
        std::cout << std::endl;
    }

    void PrintPage(int64_t table_id, pagenum_t page_number, bool verbose)
    {
        page_t buf;

        file_read_page(table_id, page_number, &buf);
        NodePage node = buf;

        std::cout << "[ table_id: " << table_id << " / page_number: " << page_number << " ]" << std::endl;
        PrintPage(node, verbose);
    }

    void PrintParent(int64_t table_id, pagenum_t page_number, bool verbose)
    {
        page_t buf;
        pagenum_t parent;

        file_read_page(table_id, page_number, &buf);
        NodePage node = buf;
        parent = node.header.parent_page_number;

        std::cout << "[ Parent node of page number " << page_number << " ]" << std::endl;
        if (parent == 0) std::cout << "- This node is root" << std::endl;
        else PrintPage(table_id, parent, verbose);
    }

    void PrintTree(int64_t table_id)
    {
        page_t buf;
        HeaderPage header;
        NodePage node;
        pagenum_t root;

        std::queue<std::pair<pagenum_t,int>> page_queue;
        int depth = -1;

        file_read_page(table_id, 0, (page_t*)&header);
        root = header.root_page_number;

        std::cout << "\n\n[[ PrintTree START ]]\n" << std::endl;

        std::cout << "<< table id: " << table_id << " >>" << std::endl;
        if (root == 0) {
            std::cout << "<< Tree is empty >>" << std::endl;
            std::cout << "\n[[ PrintTree END ]]\n\n" << std::endl;
            return;
        }

        page_queue.push({root,0});
        while (!empty(page_queue)) {
            std::pair<pagenum_t,int> page = page_queue.front();
            page_queue.pop();

            if (depth < page.second) {
                std::cout << "================================" << std::endl;
                std::cout << "<< depth: " << page.second << " >>" << std::endl;
                depth = page.second;
            }
            std::cout << "<< page number: " << page.first << " >>" << std::endl;

            file_read_page(table_id, page.first, &buf);
            node = NodePage(buf);
            PrintPage(node, true);

            if (!node.header.is_leaf) {
                page_queue.push({node.header.first_child_page_number, page.second+1});
                for (Edge edge : node.edges) {
                    page_queue.push({edge.page_number, page.second+1});
                }
            }
        }

        std::cout << "\n[[ PrintTree END ]]\n\n" << std::endl;
    }
}