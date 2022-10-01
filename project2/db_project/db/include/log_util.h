#ifndef DB_LOG_H__
#define DB_LOG_H__


/// Includes
#include "page.h"
#include "file.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include <string>
#include <iostream>
#include <deque>
#include <algorithm>
#include <queue>


/// Log utils for debugging
namespace LogUtil
{
    // Global variable
    const bool DEBUG_MODE = false;

    // Print normal
    void PrintSeparator(const std::string message = "");
    void PrintMarker(const std::string title = "", const std::string message = "");
    template <typename T>
    void PrintVariable(const std::string var_name, const T var);
    
    // Print Page in memory
    void PrintRecord(const Record& record);
    void PrintEdge(const Edge& edge);
    void PrintPageBytes(const page_t& page, bool verbose = false, int start = 0, int end = PAGE_SIZE);
    void PrintPage(const NodePage& page, bool verbose = false);

    // Read and print node and tree on disk
    void PrintPage(int64_t table_id, pagenum_t page_number, bool verbose = false);
    void PrintParent(int64_t table_id, pagenum_t page_number, bool verbose = false);
    void PrintTree(int64_t table_id);
}


#endif // DB_LOG_H
