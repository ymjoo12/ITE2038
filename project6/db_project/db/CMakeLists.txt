# Sources
set(DB_SOURCE_DIR src)
set(DB_SOURCES
  ${DB_SOURCE_DIR}/page.cc
  ${DB_SOURCE_DIR}/file_util.cc
  ${DB_SOURCE_DIR}/file.cc
  ${DB_SOURCE_DIR}/buffer.cc
  ${DB_SOURCE_DIR}/debug_util.cc
  ${DB_SOURCE_DIR}/bpt.cc
  ${DB_SOURCE_DIR}/index.cc
  ${DB_SOURCE_DIR}/trx.cc
  ${DB_SOURCE_DIR}/trx_type.cc
  ${DB_SOURCE_DIR}/log.cc
)

# Headers
set(DB_HEADER_DIR include)
set(DB_HEADERS
  ${DB_HEADER_DIR}/page.h
  ${DB_HEADER_DIR}/file_util.h
  ${DB_HEADER_DIR}/file.h
  ${DB_HEADER_DIR}/buffer.h
  ${DB_HEADER_DIR}/debug_util.h
  ${DB_HEADER_DIR}/bpt.h
  ${DB_HEADER_DIR}/index.h
  ${DB_HEADER_DIR}/trx.h
  ${DB_HEADER_DIR}/trx_type.h
  ${DB_HEADER_DIR}/log.h
)

add_library(db STATIC ${DB_HEADERS} ${DB_SOURCES})

target_include_directories(db
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/${DB_HEADER_DIR}"
  )

