#include "log.h"

/// Log Record
LogRecord::LogRecord()
    : log_size(0), lsn(-1), prev_lsn(-1), trx_id(0), log_type(0),
      table_id(0), page_num(0), offset(0), length(0)
{}

LogRecord::LogRecord(int trx_id)
    : log_size(0), lsn(-1), prev_lsn(-1), trx_id(trx_id), log_type(0),
      table_id(0), page_num(0), offset(0), length(0)
{}

char* LogRecord::to_bytes()
{
    int offset = 0;
    char* bytes = new char[this->log_size+1];
    memset(bytes, 0, this->log_size+1);

    memcpy(bytes, &this->log_size, sizeof(this->log_size));
    offset += sizeof(this->log_size);
    memcpy(bytes + offset, &this->lsn, sizeof(this->lsn));
    offset += sizeof(this->lsn);
    memcpy(bytes + offset, &this->prev_lsn, sizeof(this->prev_lsn));
    offset += sizeof(this->prev_lsn);
    memcpy(bytes + offset, &this->trx_id, sizeof(this->trx_id));
    offset += sizeof(this->trx_id);
    memcpy(bytes + offset, &this->log_type, sizeof(this->log_type));
    offset += sizeof(this->log_type);

    if (this->log_type >= LOG::UPDATE) {
        memcpy(bytes + offset, &this->table_id, sizeof(this->table_id));
        offset += sizeof(this->table_id);
        memcpy(bytes + offset, &this->page_num, sizeof(this->page_num));
        offset += sizeof(this->page_num);
        memcpy(bytes + offset, &this->offset, sizeof(this->offset));
        offset += sizeof(this->offset);
        memcpy(bytes + offset, &this->length, sizeof(this->length));
        offset += sizeof(this->length);
        memcpy(bytes + offset, this->old_img.c_str(), this->old_img.length());
        offset += this->old_img.length();
        memcpy(bytes + offset, this->new_img.c_str(), this->new_img.length());
        offset += this->new_img.length();
    }

    if (this->log_type == LOG::COMPENSATE) {
        memcpy(bytes + offset, &this->next_undo_lsn, sizeof(this->next_undo_lsn));
        offset += sizeof(this->next_undo_lsn);
    }

    return bytes;
}

/// Log Manager
LogManager::LogManager()
    : log_message_file(NULL)
{}

int64_t LogManager::assign_lsn(size_t size)
{
    int64_t lsn = this->lsn_to_assgin;
    this->lsn_to_assgin += size;
    return lsn;
}

size_t LogManager::get_log_size(int type, size_t img_length)
{
    size_t default_size = sizeof(size_t) + sizeof(int64_t)*2 + sizeof(int)*2;
    size_t detail_size = sizeof(int64_t) + sizeof(pagenum_t) + sizeof(uint16_t)*2 + img_length*2;

    // Return size of log record by type
    if (type == LOG::BEGIN || type == LOG::COMMIT || type == LOG::ROLLBACK) {
        return default_size;
    } else if (type == LOG::UPDATE) {
        return default_size + detail_size;
    } else if (type == LOG::COMPENSATE) {
        return default_size + detail_size + sizeof(int64_t);
    } else {
        return 0;
    }
}

int64_t LogManager::get_prev_lsn(int trx_id)
{
    // Find previous lsn of transaction
    for (int i = this->log_buffer.size() - 1; i >= 0; i--) {
        if (this->log_buffer[i].trx_id == trx_id) {
            return this->log_buffer[i].lsn;
        }
    }

    // Return 0 if not found
    return 0;
}

void LogManager::init(const char* log_path, const char* logmsg_path)
{
    int fd;
    const int flags = O_RDWR | O_CREAT | O_SYNC;
    const mode_t permission = 0777;

    // Open log files
    this->log_message_file = fopen(logmsg_path, "w");
    this->log_fd = open(log_path, flags, permission);
    
    // Initialize log buffer
    this->lsn_to_flush = 0;
    this->lsn_to_assgin = 0;
    this->log_buffer.clear();
    pthread_mutex_init(&this->log_buffer_latch, NULL);

    // Check whether the log file is validly opened
    if (this->log_fd < 0 || this->log_message_file == NULL) {
        std::cout << "[LogManager::init] Failed to open a log files" << std::endl;
        exit(1);
    }
}

int LogManager::recovery(int flag)
{

    return 0;
}

int64_t LogManager::log(int type, int trx_id, int64_t table_id, pagenum_t page_num, uint16_t offset, uint16_t length, std::string old_img, std::string new_img, int64_t next_undo_lsn)
{
    int flag;
    LogRecord log_record;
    size_t size;

    // Acquire the log buffer latch
    flag = pthread_mutex_lock(&this->log_buffer_latch);
    if (flag != 0) return -1;

    // Set new log record
    size = this->get_log_size(type, length);
    log_record.lsn = this->assign_lsn(size);
    log_record.prev_lsn = this->get_prev_lsn(trx_id);
    log_record.trx_id = trx_id;
    log_record.log_type = type;

    if (type >= LOG::UPDATE) {
        log_record.table_id = table_id;
        log_record.page_num = page_num;
        log_record.offset = offset;
        log_record.length = length;
        log_record.old_img = old_img;
        log_record.new_img = new_img;
    } 

    if (type == LOG::COMPENSATE) {
        log_record.next_undo_lsn = next_undo_lsn;
    }

    // Append log record to log buffer
    this->log_buffer.push_back(log_record);

    // Release the log buffer latch
    pthread_mutex_unlock(&this->log_buffer_latch);
    return log_record.lsn;
}

int64_t LogManager::force()
{
    int flag;
    size_t size;
    char* bytes;

    // Acquire the log buffer latch
    flag = pthread_mutex_lock(&this->log_buffer_latch);
    if (flag != 0) return -1;

    // Flush log buffer to log file
    while (this->lsn_to_flush < this->lsn_to_assgin) {
        size = this->get_log_size(this->log_buffer[0].log_type, this->log_buffer[0].length);
        bytes = this->log_buffer[0].to_bytes();
        pwrite(this->log_fd, bytes, size, this->lsn_to_flush);
        this->lsn_to_flush += size;
        this->log_buffer.pop_front();
        delete[] bytes;
    }

    // Release the log buffer latch
    pthread_mutex_unlock(&this->log_buffer_latch);
    return this->lsn_to_flush;
}


/// APIs for Log manager
namespace LOG
{
    // GLobal Log Manager
    LogManager log_manager;

    // API Forwarding
    void init(const char* log_path, const char* logmsg_path)
    {
        log_manager.init(log_path, logmsg_path);
    }

    int recovery(int flag)
    {
        return log_manager.recovery(flag);
    }

    int log(int type, int trx_id, int64_t table_id, pagenum_t page_num, uint16_t offset, uint16_t length, std::string old_img, std::string new_img, int64_t next_undo_lsn)
    {
        return log_manager.log(type, trx_id, table_id, page_num, offset, length, old_img, new_img, next_undo_lsn);
    }

    int64_t force()
    {
        return log_manager.force();
    }
}
