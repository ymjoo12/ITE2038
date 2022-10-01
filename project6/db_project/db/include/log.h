#ifndef DB_LOG_H__
#define DB_LOG_H__

/// Includes
#include "page.h"

#include <stdio.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <pthread.h>
#include <vector>

/// Types
struct LogRecord
{
    // Fields for BEGIN, COMMIT, ROLLBACK
    size_t log_size;
    int64_t lsn, prev_lsn;
    int trx_id;
    int log_type;
    
    // + Fields for UPDATE
    int64_t table_id;
    pagenum_t page_num;
    uint16_t offset, length;
    std::string old_img, new_img;

    // + Fields for COMPENSATE
    int64_t next_undo_lsn;

    // Constructor
    LogRecord();
    LogRecord(int trx_id);

    // Member functions
    char* to_bytes();
};

/// Log Manager
class LogManager
{
private:
    // Fields
    FILE *log_message_file;
    int log_fd;
    int64_t lsn_to_flush, lsn_to_assgin;
    std::deque<LogRecord> log_buffer;
    pthread_mutex_t log_buffer_latch;

    // Member functions (private)
    int64_t assign_lsn(size_t size);
    size_t get_log_size(int type, size_t img_length = 0);
    int64_t get_prev_lsn(int trx_id);

public:
    // Constructor
    LogManager();

    // Member functions
    void init(const char* log_path, const char* logmsg_path);
    int recovery(int flag = 0);
    int64_t log(int type, int trx_id, int64_t table_id = 0, pagenum_t page_num = 0, uint16_t offset = 0, uint16_t length = 0, std::string old_img = "", std::string new_img = "", int64_t next_undo_lsn = 0);
    int64_t force();
};

/// APIs for Log manager
namespace LOG
{
    // Types
    constexpr int BEGIN = 0;
    constexpr int UPDATE = 1;
    constexpr int COMMIT = 2;
    constexpr int ROLLBACK = 3;
    constexpr int COMPENSATE = 4;

    // Phases
    constexpr int PHASE_ANALYSIS = 0;
    constexpr int PHASE_REDO = 1;
    constexpr int PHASE_UNDO = 2;

    // GLobal Log Manager
    extern LogManager log_manager;

    // API Forwarding
    void init(const char* log_path, const char* logmsg_path);
    int recovery(int flag = PHASE_ANALYSIS);
    int log(int type, int trx_id, int64_t table_id = 0, pagenum_t page_num = 0, uint16_t offset = 0, uint16_t length = 0, std::string old_img = "", std::string new_img = "", int64_t next_undo_lsn = 0);
    int64_t force();
}

#endif /* DB_LOG_H__ */
