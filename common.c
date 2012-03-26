/*
 * Copyright (C) Changrong Zhou
 */
#include "common.h"

int log_error(const char *format, ...)
{
	va_list ap;
	va_start(ap, format);
	int ret;
	ret = vfprintf(stderr, format, ap);
	va_end(ap);
	if (ret == -1)
		return MC_FALSE;
	return MC_TRUE;
}

void *safe_malloc(size_t size)
{
	void *ptr;
	while (1) {
		ptr = malloc(size);
		if (ptr == NULL) {
			log_error("Error: can't malloc: %s\n", strerror(errno));
#ifdef FORCK_EXIT
			exit(1);
		}
#else
			sleep(300);
			continue;
		}
		break;
#endif /* FORCK_EXIT */
	}
	return ptr;
}

void *safe_realloc(void *ptr, size_t size)
{
	void *p;
	while (1) {
		p = realloc(ptr, size);
		if (p == NULL) {
			log_error("Error: can't realloc: %s\n", strerror(errno));
#ifdef FORCK_EXIT
			exit(1);
		}

#else
			sleep(300);
			continue;
		}
		break;
#endif /* FORCK_EXIT */
	}
	return p;
}


char *string_ncopy(const char *src, size_t n)
{
	char *dst;
	if (src == NULL || n == 0) {
		return NULL;
	}
	dst = safe_malloc(n+1);
	memcpy(dst, src, n);
	*(dst+n) = '\0';
	return dst;
}

char *string_copy(const char *src) {
	return string_ncopy(src, strlen(src));
}

int dynamic_hash(const char *key)
{
	register size_t i;
	register const char *s;
	register size_t hash;
	for (s=key, i=0, hash=0; i<COEFF_COUNT && *s; i++, s++, hash*=5) {
		hash += *s * coeff[i];
	} 
	return hash;
}

DHASH * dynamic_hash_new(void)
{
	DHASH *dht = safe_malloc(sizeof(DHASH));
	memset(dht, 0, sizeof(DHASH));
	dht->array = safe_malloc(sizeof(DHENT *) * 8);
	dht->size = 8;
	memset(dht->array, 0, sizeof(DHENT *) * dht->size);
	return dht;
}

void dynamic_hash_destroy(DHASH *dht)
{
	DHENT *entry;
	DHENT *a;
	size_t i;
	for (i=0; i<dht->size; i++) {
		entry = dht->array[i];
		if (entry == NULL)
			continue;
		for (; entry != NULL; entry = a) {
			a = entry->next;
			if (entry->key != NULL)
				free(entry->key);
			free(entry);
		}
	}
	free(dht->array);
	free(dht);
}

void dynamic_hash_split(DHASH *dht)
{
	size_t newsize = dht->size * 2;
	size_t i;
	size_t newindex;
	DHENT **oentry;
	DHENT *entry;
	oentry = (DHENT **)safe_realloc(dht->array, sizeof(DHENT *) * newsize);
	memset(oentry+dht->size, 0, sizeof(DHENT *)*(newsize-dht->size));
	dht->array = oentry;
	for (i=0; i<dht->size; i++) {
		oentry = &(dht->array[i]);
		if (*oentry == NULL)
			continue;
		for (entry = *oentry; entry != NULL; entry = *oentry) {
			newindex = (entry->hash & (newsize-1));
			if (newindex != i) {
				/* New address is NULL */
				if (dht->array[newindex] == NULL)
					dht->fill++;

				*oentry = entry->next;
				entry->next = dht->array[newindex];
				dht->array[newindex] = entry;
			} else {
				oentry = &entry->next;
			}
		}
		/* All is removed */
		if (dht->array[i] == NULL)
			dht->fill--;
	}
	dht->size = newsize;
}


void *dynamic_hash_fetch(DHASH *dht, const char *key)
{
	DHENT *entry;
	int hash = dynamic_hash(key);
	for (entry=dht->array[hash & (dht->size-1)]; entry != NULL; entry=entry->next) {
		if (entry->hash != hash)
			continue;
		if (strcmp(entry->key, key) != 0)
			continue;
		return entry->value;
	}
	return NULL;
}

int dynamic_hash_haskey(DHASH *dht, const char *key)
{
	DHENT *entry;
	int hash = dynamic_hash(key);
	for (entry=dht->array[hash & (dht->size-1)]; entry != NULL; entry=entry->next) {
		if (entry->hash != hash)
			continue;
		if (strcmp(entry->key, key) != 0)
			continue;
		return MC_TRUE;
	}
	return MC_FALSE;
}


void dynamic_hash_store(DHASH *dht, const char *key, void *value)
{
	DHENT *entry;
	int hash = dynamic_hash(key);
	DHENT **oentry;
	oentry = &(dht->array[hash & (dht->size-1)]);
	for (entry=*oentry; entry != NULL; entry=entry->next) {
		if (entry->hash != hash)
			continue;
		if (strcmp(entry->key, key) != 0)
			continue;
		/* Modify the value, No increase in entry*/
		entry->value = value;
		return;
	}
	/* Add an entry */
	entry = (DHENT *)safe_malloc(sizeof(DHENT));
	memset(entry, 0, sizeof(DHENT));
	entry->key = string_copy(key);
	entry->value = value;
	entry->hash = hash;
	entry->next = *oentry;
	if (*oentry == NULL)
		dht->fill++;
	dht->ents++;
	*oentry = entry;
	if (dht->fill * 100 /(dht->size) > FILLPCT) {
		//printf("slipt %lu %lu\n", dht->fill, dht->size);
		dynamic_hash_split(dht);
	}
} 

void dynamic_hash_delete(DHASH *dht, const char *key)
{
	DHENT *entry;
	int hash = dynamic_hash(key);
	DHENT **oentry;
	size_t index = hash & (dht->size-1);
	for (oentry=&(dht->array[index]); *oentry!=NULL; oentry=&((*oentry)->next)) {
		entry = *oentry;
		if (entry->hash != hash)
			continue;
		if (strcmp(entry->key, key) != 0)
			continue;
		*oentry = entry->next;
		dht->ents--;
		if (dht->array[index] == NULL)
			dht->fill--;
		free(entry->key);
		free(entry);
		return;
	}
	return;
}



DSTRING *dynamic_string_new(void)
{
	DSTRING *dst = safe_malloc(sizeof(DSTRING));
	memset(dst, 0, sizeof(DSTRING));
	dst->buf = safe_malloc(sizeof(char *) * ADD_SIZE);
	dst->size = ADD_SIZE;
	memset(dst->buf, 0, sizeof(char *) * dst->size);
	return dst;
}

void dynamic_string_reset(DSTRING *dst)
{
	dst->size = 0;
	dst->len = 0;
}

void dynamic_string_destroy(DSTRING *dst)
{
	if (dst->buf != NULL)
		free(dst->buf);
	free(dst);
}

void dynamic_string_resize(DSTRING *dst, size_t size)
{
	if (dst->len > size) {
		log_error("Error: can't resize dynamic string, size is too small");
		exit(1);
	}
	dst->buf = safe_realloc(dst->buf, size);
	dst->size = size;
}

void dynamic_string_append_char(DSTRING *dst, int c) {
	/* less one char */
	if (dst->len >= dst->size) {
		dynamic_string_resize(dst, dst->size+ADD_SIZE);
	} 
	*(dst->buf+dst->len++) = c;
}

void dynamic_string_append_csv_field(DSTRING *dst, const char *str, size_t len, char sep_char)
{
	const char *p, *e;
	char *n;
	size_t t;
	for (p = str, e = str+len, n = dst->buf+dst->len; p < e; p++, n++) {
		/* less two char */
		if (n - dst->buf + 1 >= dst->size) {
			t = n - dst->buf;
			dynamic_string_resize(dst, dst->size+ADD_SIZE);
			n = dst->buf + t;
		} 
		if (*p == sep_char || *p == '\\' || *p == '\n') {
			*n++ = '\\';
		}
		*n = *p;
	}
	dst->len = n-dst->buf;
}

void dynamic_string_n_append(DSTRING *dst, const char *str, size_t n)
{
	if (dst->len + n > dst->size) {
		dynamic_string_resize(dst, dst->len+n);
	}
	memcpy(dst->buf+dst->len, str, n);
	dst->len += n;
}

void dynamic_string_append(DSTRING *dst, const char *src)
{
	dynamic_string_n_append(dst, src, strlen(src));
}

int dynamic_string_append_nlist(DSTRING *dst, int n, ...)
{
	va_list ap;
	int i;
	va_start(ap, n);
	for (i=0; i<n; i++) {
		dynamic_string_append(dst, va_arg(ap, char *));
	}
	va_end(ap);
	return MC_TRUE;
}

void dynamic_string_n_insert(DSTRING *dst, size_t index, const char *src, size_t n)
{
	if (dst->len+n>dst->size)
		dynamic_string_resize(dst, dst->len+n);
	memmove(dst->buf+index+n, dst->buf+index, dst->len-index);
	memcpy(dst->buf+index, src, n);
	dst->len+=n;
}

void dynamic_string_insert(DSTRING *dst, size_t index, const char *src)
{
	return dynamic_string_n_insert(dst, index, src, strlen(src));
}

DSTRING * dynamic_string_readfile(DSTRING *dst, const char *filename)
{
	int fd;
	ssize_t bytes;
	fd = open(filename, O_RDONLY);
	if (fd == -1)
	{
		log_error("Error: can't open '%s': %s!\n", filename, strerror(errno));
		return NULL;
	}
	while (1) {
		if (dst->len + BUF_SIZE > dst->size) {
			dynamic_string_resize(dst, dst->len+BUF_SIZE);
		}
		bytes = read(fd, dst->buf+dst->len, BUF_SIZE);
		if (bytes == 0)
			break;
		if (bytes == -1) {
			log_error("Error: can't read '%s': %s!", filename, strerror(errno));
			return NULL;
		}
		dst->len += bytes;
	}
	return dst;
}

DSTRING * dynamic_string_gzreadfile(DSTRING *dst, const char *filename)
{
	ssize_t bytes;
	gzFile gzfile;
	int gzerrno;
	gzfile = gzopen(filename, "rb");
	if (gzfile == NULL)
	{
		log_error("Error: can't open '%s': %s!\n", filename, strerror(errno));
		return NULL;
	}
	while (1) {
		if (dst->len + BUF_SIZE > dst->size) {
			dynamic_string_resize(dst, dst->len+BUF_SIZE);
		}
		bytes = gzread(gzfile, dst->buf+dst->len, BUF_SIZE);
		if (bytes == 0)
			break;
		if (bytes == -1) {
			log_error("Error: can't read '%s': %s!", filename, gzerror(gzfile, &gzerrno));
			gzclose(gzfile);
			return NULL;
		}
		dst->len += bytes;
	}
	gzclose(gzfile);
	return dst;
}

DARRAY * dynamic_array_new(void)
{
	DARRAY *dat = safe_malloc(sizeof(DARRAY));
	memset(dat, 0, sizeof(DARRAY));
	dat->ents = 0;
	dat->buf = NULL;
	dat->array = NULL;
	dat->buf = (void **)safe_malloc(sizeof(void *) * 8);
	memset(dat->buf, 0, sizeof(void *) * 8);
	dat->size = 8;
	dat->array = dat->buf;
	return dat;
}


void dynamic_array_push(DARRAY *dat, void * value)
{
	size_t size;
	void **p;
	if (dat->size == (dat->array - dat->buf) + dat->ents)
	{
		size = dat->array-dat->buf;
		if (size > 0) {
			dat->array = (void **)memmove(dat->buf, dat->array, sizeof(void *) * dat->ents);
			memset(dat->buf+dat->ents, 0, size);
		} else {
			size = dat->size * 2;
			p = (void **)safe_realloc(dat->buf, sizeof(void *) * size);
			dat->array = dat->buf = p;
			memset(dat->buf+dat->size, 0, sizeof(void *) * (size-dat->size));
			dat->size = size;
		}
	}
	dat->array[dat->ents++] = value;
}


void * dynamic_array_fetch(DARRAY *dat, ssize_t index)
{
	if (index >= 0) {
		if (index >= dat->ents)
			return NULL;
		return *(dat->array+index);
	}
	else {
		if (-index >= dat->ents)
			return NULL;
		return dat->array[dat->ents+index];
	}
}

void dynamic_array_delete(DARRAY *dat, ssize_t index)
{
	if (index >= 0)	{
		if (index >= dat->ents)
			return;
		dat->array[index] = NULL;
	}
	else {
		if (-index >= dat->ents)
			return;
		dat->array[dat->ents+index] = NULL;
	}
}

void dynamic_array_set(DARRAY *dat, ssize_t index, void *value)
{
	size_t size;
	void **p;
	if (index < 0) {
		if (dat->ents+index < 0) {
			log_error("Modification of non-creatable array value attempted, %zd", index);
			exit(1);
		}
		index = dat->ents+index;
	}
	if (dat->size < (dat->array - dat->buf) + index + 1)
	{
		size = dat->array-dat->buf;
		if (size > 0) {
			dat->array = (void **)memmove(dat->buf, dat->array, sizeof(void *) * dat->ents);
			memset(dat->buf+dat->ents, 0, sizeof(void *) * size);
		}
		size = index+1;
		if (dat->size < size) {
			p = (void **)safe_realloc(dat->buf, sizeof(void *) * size);
			dat->array = dat->buf = p;
			memset(dat->buf+dat->size, 0, sizeof(void *) * (size-dat->size));
			dat->size = size;
		}
	}
	dat->array[index] = value;
	if (index+1 > dat->ents)
		dat->ents = index+1;
}

void *dynamic_array_pop(DARRAY *dat)
{
	void *p;
	if (dat->ents == 0)
		return NULL;
	p = dat->array[dat->ents-1];
	dat->array[dat->ents-1] = NULL;
	dat->ents--;
	return p;
}

void *dynamic_array_shift(DARRAY *dat)
{
	void *p;
	if (dat->ents == 0)
		return NULL;
	p = *dat->array;
	*dat->array = NULL;
	dat->ents--;
	dat->array++;
	return p;
}


size_t dynamic_array_count(DARRAY *dat)
{
	return dat->ents;
}

void dynamic_array_destroy(DARRAY *dat)
{
	if (dat->buf != NULL)
		free(dat->buf);
	free(dat);
}


int f_write(FILE *file, const void *buf, size_t count)
{
	ssize_t bytes = 0;
	const void *p = buf;
	while (p-buf<count) {
		bytes = fwrite(p, count-(p-buf)>BUF_SIZE ? BUF_SIZE : count-(p-buf), 1, file);
		if (bytes == -1) {
			log_error("Error: can't write: %s\n", strerror(errno));
			return MC_FALSE;
		}
		p += bytes;
	}
	return MC_TRUE;
}

int fd_write(int fd, const void *buf, size_t count)
{
	ssize_t bytes = 0;
	const void *p = buf;
	while (p-buf<count) {
		bytes = write(fd, p, count-(p-buf)>BUF_SIZE ? BUF_SIZE : count-(p-buf));
		if (bytes == -1) {
			log_error("Error: can't write: %s\n", strerror(errno));
			return MC_FALSE;
		}
		p += bytes;
	}
	return MC_TRUE;
}

int gz_write(gzFile *gzfile, const void *buf, size_t count)
{
	ssize_t bytes = 0;
	const void *p = buf;
	while (p-buf<count) {
		bytes = gzwrite(gzfile, p, count-(p-buf)>BUF_SIZE ? BUF_SIZE : count-(p-buf));
		if (bytes == 0) {
			log_error("Error: can't write: %s\n", strerror(errno));
			return MC_FALSE;
		}
		p += bytes;
	}
	return MC_TRUE;
} 


pcre *mc_pcre_complie(const char *pattern)
{
	const char *error;
	int erroffset;
	pcre *code;
	code = pcre_compile(
		pattern,
		0,
		&error,
		&erroffset,
		NULL);
	if (code == NULL)
	{
		log_error("Error: Pattern: %s\n", pattern);
		log_error("Error: PCRE compilation failed at offset %d: %s\n", erroffset, error);
		return NULL;
	}
	return code;
}
int mc_pcre_exec(pcre *code, const char *src, size_t length)
{
	int rc;
	rc = pcre_exec(
		code,
		NULL,
		src,
		length,
		0,
		0,
		ovector,
		OVECCOUNT);
	if (rc<0)
	{
		switch (rc)
		{
			case PCRE_ERROR_NOMATCH:
				break;
			default:
				log_error("Error: PCRE match error %d\n", rc);
				break;
		}
	}
	return rc;
}


