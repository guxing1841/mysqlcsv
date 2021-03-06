cmake_minimum_required (VERSION 2.6)
project (mysqlcsv)
SET (VERSION 0.15)
SET(CMAKE_C_FLAGS "-Wall -g -O3 ${CMAKE_C_FLAGS}")
SET(CMAKE_INSTALL_PREFIX  CACHE STRING ${BUILDTYPE_DOCSTRING} FORCE)
SET(MYSQL_INCLUDE_DIR "" CACHE STRING  "mysql include directory")
SET(MYSQL_LIB_DIR "" CACHE STRING  "mysql library directory")
SET(PCRE_INCLUDE_DIR "" CACHE PATH "pcre include directory")
SET(PCRE_LIB_DIR "" CACHE PATH "pcre library directory")
SET(ZLIB_INCLUDE_DIR "" CACHE PATH "zlib include directory")
SET(ZLIB_LIB_DIR "" CACHE PATH "zlib library directory")
IF(WANT_LIB64)
  SET(LIB_DIR_NAME "lib64" CACHE TYPE STRING)
ELSE(WANT_LIB64)
  SET(LIB_DIR_NAME "lib" CACHE TYPE STRING)
ENDIF(WANT_LIB64)

SET(INCLUDE_DIRECTORIES ${MYSQL_INCLUDE_DIR} ${MYSQL_INCLUDE_DIR}/mysql ${PCRE_INCLUDE_DIR}${ZLIB_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
INCLUDE_DIRECTORIES(${INCLUDE_DIRECTORIES})
#CONFIGURE_FILE(config.h.cmake   ${CMAKE_BINARY_DIR}/config.h)
SET(CMAKE_REQUIRED_INCLUDES ${INCLUDE_DIRECTORIES})
INCLUDE(CheckIncludeFiles)
CHECK_INCLUDE_FILES (pcre.h HAVE_PCRE_H)
CHECK_INCLUDE_FILES (zlib.h HAVE_ZLIB_H)
CHECK_INCLUDE_FILES (mysql/mysql.h HAVE_MYSQL_H)
 
SET(COMMON_LIST 
	${CMAKE_SOURCE_DIR}/common.c
        )
SET(MYSQLCSV_LIST
	${CMAKE_SOURCE_DIR}/mysqlcsv.c
	)
 
ADD_LIBRARY(common SHARED ${COMMON_LIST})
ADD_LIBRARY(common_static STATIC ${COMMON_LIST})
SET_TARGET_PROPERTIES(common_static PROPERTIES OUTPUT_NAME "common")

SET_TARGET_PROPERTIES(common PROPERTIES VERSION ${VERSION} SOVERSION 1) 

LINK_DIRECTORIES(${MYSQL_LIB_DIR} ${MYSQL_LIB_DIR}/mysql ${PCRE_LIB_DIR} ${ZLIB_LIB_DIR} ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR})
ADD_EXECUTABLE(mysqlcsv ${MYSQLCSV_LIST})
TARGET_LINK_LIBRARIES(mysqlcsv common_static mysqlclient pcre z)
 
INSTALL(TARGETS mysqlcsv DESTINATION ${CMAKE_INSTALL_PREFIX}/bin)
INSTALL(TARGETS common DESTINATION ${CMAKE_INSTALL_PREFIX}/${LIB_DIR_NAME}) 
 
