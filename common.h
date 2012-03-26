/*
 * Copyright (C) Changrong Zhou
 */
#ifndef __COMMON_H
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pcre.h>
#include <stdarg.h>
#include <zlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define MC_TRUE 1
#define MC_FALSE 0

#define MC_OK		0
#define MC_FAILURE	-1
#define MC_IGNORE	1

#define OVECCOUNT 30
#define ADD_SIZE 512
#define BUF_SIZE 4096

typedef struct dynamic_string_s
{
	char *buf;   /* buf 指针 */
	size_t size; /* buf 大小 */
	size_t len;  /* string 长度 */
} DSTRING;

typedef struct dynamic_array_s
{
	void **buf;   /* buf 指针 */
	void **array; /* array 指针 */
	size_t size;  /* buf 大小 */
	size_t ents; /* ents 已用条目数 */
} DARRAY;


#define COEFF_COUNT 128
#define FILLPCT 60
/* From perl x2p hash.h */ 
#ifdef DOINT
const char coeff[] = {
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
};
int ovector[OVECCOUNT];
#else
extern const char coeff[];
extern int ovector[];
#endif /* DOINT */

typedef struct dynamic_hash_entry_s DHENT;
struct dynamic_hash_entry_s {
	size_t  hash;
	DHENT *next;
	char *key;
	void *value;

};

typedef struct dynamic_hash_s
{
	DHENT **array;
	size_t size;
	size_t fill;
	size_t ents;
} DHASH;

int log_error(const char *format, ...);
void *safe_malloc(size_t size);
void *safe_realloc(void *ptr, size_t size);
char *string_ncopy(const char *src, size_t n);
char *string_copy(const char *src);
int dynamic_hash(const char *key);
DHASH *dynamic_hash_new(void);
void dynamic_hash_destroy(DHASH *dht);
//void dynamic_hash_split(DHASH *dht);
void *dynamic_hash_fetch(DHASH *dht, const char *key);
int dynamic_hash_haskey(DHASH *dht, const char *key);
void dynamic_hash_store(DHASH *dht, const char *key, void *value);
void dynamic_hash_delete(DHASH *dht, const char *key);
DSTRING *dynamic_string_new(void);
void dynamic_string_reset(DSTRING *dst);
void dynamic_string_destroy(DSTRING *dst);
void dynamic_string_resize(DSTRING *dst, size_t size);
void dynamic_string_append_char(DSTRING *dst, int c);
void dynamic_string_append_csv_field(DSTRING *dst, const char *str, size_t len, char sep_char);
void dynamic_string_n_append(DSTRING *dst, const char *str, size_t n);
void dynamic_string_append(DSTRING *dst, const char *src);
int dynamic_string_append_nlist(DSTRING *dst, int n, ...);
void dynamic_string_n_insert(DSTRING *dst, size_t index, const char *src, size_t n);
void dynamic_string_insert(DSTRING *dst, size_t index, const char *src);
DSTRING *dynamic_string_readfile(DSTRING *dst, const char *filename);
DSTRING *dynamic_string_gzreadfile(DSTRING *dst, const char *filename);
DARRAY *dynamic_array_new(void);
void dynamic_array_push(DARRAY *dat, void * value);
void *dynamic_array_fetch(DARRAY *dat, ssize_t index);
size_t dynamic_array_count(DARRAY *dat);
void *dynamic_array_shift(DARRAY *dat);
void *dynamic_array_pop(DARRAY *dat);
void dynamic_array_delete(DARRAY *dat, ssize_t index);
void dynamic_array_set(DARRAY *dat, ssize_t index, void * value);
void dynamic_array_destroy(DARRAY *dat);
int f_write(FILE *file, const void *buf, size_t count);
int fd_write(int fd, const void *buf, size_t count);
int gz_write(gzFile *gzfile, const void *buf, size_t count);
pcre *mc_pcre_complie(const char *r);
int mc_pcre_exec(pcre *code, const char *src, size_t length);
#endif /* __COMMON_H */
