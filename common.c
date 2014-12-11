/*
 * Copyright (C) Changrong Zhou
 */
#include "common.h"
#include <assert.h>
/* From perl x2p hash.h */ 
const char coeff[] = {
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
	61,59,53,47,43,41,37,31,29,23,17,13,11,7,3,1,
};
int ovector[OVECCOUNT];

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
	while (1)
	{
		ptr = malloc(size);
		if (ptr == NULL)
		{
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

void *safe_memalign(size_t alignment, size_t size)
{
	void *ptr;
	while (1)
	{
		if (posix_memalign(&ptr, alignment, size) != 0)
		{
			log_error("Error: can't memaligin: %s\n", strerror(errno));
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
	while (1)
	{
		p = realloc(ptr, size);
		if (p == NULL)
		{
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
	if (src == NULL || n == 0)
	{
		return NULL;
	}
	dst = safe_malloc(n+1);
	memcpy(dst, src, n);
	*(dst+n) = '\0';
	return dst;
}

char *string_copy(const char *src)
{
	return string_ncopy(src, strlen(src));
}

int dynamic_hash(const char *key, size_t key_len)
{
	size_t i;
	const char *s;
	size_t hash;
	for (s=key, i=0, hash=0; i<COEFF_COUNT && i<key_len; i++, s++, hash*=5)
	{
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
	for (i=0; i<dht->size; i++)
	{
		entry = dht->array[i];
		if (entry == NULL)
			continue;
		for (; entry != NULL; entry = a)
		{
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
	size_t newindex;
	size_t i;
	DHENT **oentry;
	DHENT *entry;
	oentry = (DHENT **)safe_realloc(dht->array, sizeof(DHENT *) * newsize);
	memset(oentry+dht->size, 0, sizeof(DHENT *)*(newsize-dht->size));
	dht->array = oentry;
	for (i=0; i<dht->size; i++)
	{
		oentry = &(dht->array[i]);
		if (*oentry == NULL)
			continue;
		for (entry = *oentry; entry != NULL; entry = *oentry)
		{
			newindex = (entry->hash & (newsize-1));
			if (newindex != i)
			{
				/* New address is NULL */
				if (dht->array[newindex] == NULL)
					dht->fill++;

				*oentry = entry->next;
				entry->next = dht->array[newindex];
				dht->array[newindex] = entry;
			}
			else
			{
				oentry = &entry->next;
			}
		}
		/* All is removed */
		if (dht->array[i] == NULL)
			dht->fill--;
	}
	dht->size = newsize;
}


void *dynamic_hash_fetch(DHASH *dht, const char *key, size_t key_len)
{
	DHENT *entry;
	int hash = dynamic_hash(key, key_len);
	for (entry=dht->array[hash & (dht->size-1)]; entry != NULL; entry=entry->next)
	{
		if (entry->hash != hash)
			continue;
		if (entry->key_len != key_len)
			continue;
		if (strncmp(entry->key, key, key_len) != 0)
			continue;
		return entry->value;
	}
	return NULL;
}

int dynamic_hash_haskey(DHASH *dht, const char *key, size_t key_len)
{
	DHENT *entry;
	int hash = dynamic_hash(key, key_len);
	for (entry=dht->array[hash & (dht->size-1)]; entry != NULL; entry=entry->next)
	{
		if (entry->hash != hash)
			continue;
		if (entry->key_len != key_len)
			continue;
		if (strncmp(entry->key, key, key_len) != 0)
			continue;
		return MC_TRUE;
	}
	return MC_FALSE;
}


void dynamic_hash_store(DHASH *dht, const char *key, size_t key_len, void *value)
{
	DHENT *entry;
	int hash = dynamic_hash(key, key_len);
	DHENT **oentry = &(dht->array[hash & (dht->size-1)]);
	for (entry=*oentry; entry != NULL; entry=entry->next)
	{
		if (entry->hash != hash)
			continue;
		if (entry->key_len != key_len)
			continue;
		if (strncmp(entry->key, key, key_len) != 0)
			continue;
		/* Modify the value, No increase in entry*/
		entry->value = value;
		return;
	}
	/* Add an entry */
	entry = (DHENT *)safe_malloc(sizeof(DHENT));
	memset(entry, 0, sizeof(DHENT));
	entry->key = string_copy(key);
	entry->key_len = key_len;
	entry->value = value;
	entry->hash = hash;
	entry->next = *oentry;
	if (*oentry == NULL)
		dht->fill++;
	dht->ents++;
	*oentry = entry;
	//if (dht->fill * 100 /(dht->size) > FILLPCT)
	//{
	if (dht->ents > dht->size)
	{
		//printf("slipt %lu %lu\n", dht->fill, dht->size);
		dynamic_hash_split(dht);
	}
} 

void dynamic_hash_delete(DHASH *dht, const char *key, size_t key_len)
{
	DHENT *entry;
	int hash = dynamic_hash(key, key_len);
	DHENT **oentry;
	size_t index = hash & (dht->size-1);
	for (oentry=&(dht->array[index]); *oentry!=NULL; oentry=&((*oentry)->next))
	{
		entry = *oentry;
		if (entry->hash != hash)
			continue;
		if (entry->key_len != key_len)
			continue;
		if (strncmp(entry->key, key, key_len) != 0)
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



DSTRING *dynamic_string_new(size_t size)
{
	DSTRING *dst = safe_malloc(sizeof(DSTRING));
	if (size == 0)
		dst->data = NULL;
	else
		dst->data = (char *)safe_malloc(sizeof(char) * size);
	dst->size = size;
	dst->len = 0;
	//printf("new dst %p data %p size %lu len %lu\n", dst, dst->data, dst->size, dst->len);
	return dst;
}

void dynamic_string_reset(DSTRING *dst)
{
	dst->len = 0;
	//printf("reset dst %p data %p size %lu len %lu\n", dst, dst->data, dst->size, dst->len);
}

void dynamic_string_destroy(DSTRING *dst)
{
	//printf("free dst %p data %p size %lu len %lu\n", dst, dst->data, dst->size, dst->len);
	if (dst->data != NULL)
		free(dst->data);
	free(dst);
}

void dynamic_string_resize(DSTRING *dst, size_t size)
{
	assert(dst->len <= size);
	dst->data = (char *)safe_realloc(dst->data, size);
	dst->size = size;
	//printf("resize dst %p data %p size %lu len %lu\n", dst, dst->data, dst->size, dst->len);
}

void dynamic_string_append_char(DSTRING *dst, int c)
{
	/* less one char */
	if (dst->len == dst->size)
	{
		dynamic_string_resize(dst, dst->size + 1);
	} 
	*(dst->data+dst->len++) = c;
}
void dynamic_string_append_csv_field(DSTRING *dst, const char *str, size_t len)
{
	const char *p, *e;
	char *n;
	size_t t;
	for (p = str, e = str+len, n = dst->data+dst->len; p < e; p++, n++)
	{
		/* less two char */
		if (n - dst->data + 1 >= dst->size)
		{
			t = n - dst->data;
			dynamic_string_resize(dst, dst->size + (e-p)*2);
			n = dst->data + t;
		} 
		if (*p == '\\' || *p == '\t' || *p == '\n')
		{
			*n++ = '\\';
		}
		else if (*p == '\0')
		{
			*n++ = '\\';
			*n = '0';
			continue;
		}
		*n = *p;
	}
	dst->len = n-dst->data;
}

void dynamic_string_n_append(DSTRING *dst, const char *str, size_t n)
{
	if (n == 0)
		return;
	if (dst->len + n > dst->size)
	{
		dynamic_string_resize(dst, dst->len+n);
	}
	//printf("%p %d %d\n", dst->data, dst->size, n);
	memcpy(dst->data+dst->len, str, n);
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
	for (i=0; i<n; i++)
	{
		dynamic_string_append(dst, va_arg(ap, char *));
	}
	va_end(ap);
	return MC_TRUE;
}

void dynamic_string_n_insert(DSTRING *dst, size_t index, const char *src, size_t n)
{
	if (dst->len+n>dst->size)
		dynamic_string_resize(dst, dst->len+n);
	memmove(dst->data+index+n, dst->data+index, dst->len-index);
	memcpy(dst->data+index, src, n);
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
	while (1)
	{
		if (dst->len + BUF_SIZE > dst->size)
		{
			dynamic_string_resize(dst, dst->len+BUF_SIZE);
		}
		bytes = read(fd, dst->data+dst->len, BUF_SIZE);
		if (bytes == 0)
			break;
		if (bytes == -1)
		{
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
	while (1)
	{
		if (dst->len + BUF_SIZE > dst->size)
		{
			dynamic_string_resize(dst, dst->len+BUF_SIZE);
		}
		bytes = gzread(gzfile, dst->data+dst->len, BUF_SIZE);
		if (bytes == 0)
			break;
		if (bytes == -1)
		{
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
	dat->ents = 0;
	dat->buf = (void **)safe_malloc(sizeof(void *) * 8);
	dat->size = 8;
	dat->array = dat->buf;
	return dat;
}

void dynamic_array_reset(DARRAY *dat)
{
	dat->ents = 0;
}


void dynamic_array_push(DARRAY *dat, void * value)
{
	size_t size;
	void **p;
	if (dat->size == (dat->array - dat->buf) + dat->ents)
	{
		size = dat->array - dat->buf;
		if (size > 0)
		{
			dat->array = (void **)memmove(dat->buf, dat->array, sizeof(void *) * dat->ents);
			//memset(dat->buf+dat->ents, 0, size);
		}
		else
		{
			size = dat->size * 2;
			p = (void **)safe_realloc(dat->buf, sizeof(void *) * size);
			dat->array = dat->buf = p;
			//memset(dat->buf+dat->size, 0, sizeof(void *) * (size-dat->size));
			dat->size = size;
		}
	}
	dat->array[dat->ents++] = value;
}


void * dynamic_array_fetch(DARRAY *dat, ssize_t index)
{
	if (index >= 0)
	{
		if (index >= dat->ents)
			return NULL;
		return *(dat->array+index);
	}
	else
	{
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
	else
	{
		if (-index >= dat->ents)
			return;
		dat->array[dat->ents+index] = NULL;
	}
}

void dynamic_array_set(DARRAY *dat, ssize_t index, void *value)
{
	size_t size;
	void **p;
	if (index < 0)
	{
		if (dat->ents+index < 0)
		{
			log_error("Modification of non-creatable array value attempted, %zd", index);
			exit(1);
		}
		index = dat->ents+index;
	}
	if (dat->size < (dat->array - dat->buf) + index + 1)
	{
		size = dat->array-dat->buf;
		if (size > 0)
		{
			dat->array = (void **)memmove(dat->buf, dat->array, sizeof(void *) * dat->ents);
			//memset(dat->buf+dat->ents, 0, sizeof(void *) * size);
		}
		size = index+1;
		if (dat->size < size)
		{
			p = (void **)safe_realloc(dat->buf, sizeof(void *) * size);
			dat->array = dat->buf = p;
			//memset(dat->buf+dat->size, 0, sizeof(void *) * (size-dat->size));
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


void dynamic_array_destroy(DARRAY *dat)
{
	if (dat->buf != NULL)
		free(dat->buf);
	free(dat);
}


int f_write(FILE *file, const char *buf, size_t count)
{
	ssize_t bytes = 0;
	const char *p = buf;
	while (p-buf<count)
	{
		bytes = fwrite(p, count-(p-buf)>BUF_SIZE ? BUF_SIZE : count-(p-buf), 1, file);
		if (bytes == -1)
		{
			log_error("Error: can't write: %s\n", strerror(errno));
			return MC_FALSE;
		}
		p += bytes;
	}
	return MC_TRUE;
}

int fd_write(int fd, const char *buf, size_t count)
{
	ssize_t bytes = 0;
	const char *p = buf;
	while (p-buf<count)
	{
		bytes = write(fd, p, count-(p-buf)>BUF_SIZE ? BUF_SIZE : count-(p-buf));
		if (bytes == -1)
		{
			log_error("Error: can't write: %s\n", strerror(errno));
			return MC_FALSE;
		}
		p += bytes;
	}
	return MC_TRUE;
}

int gz_write(gzFile *gzfile, const char *buf, size_t count)
{
	ssize_t bytes = 0;
	const char *p = buf;
	while (p-buf<count)
	{
		bytes = gzwrite(gzfile, p, count-(p-buf)>BUF_SIZE ? BUF_SIZE : count-(p-buf));
		if (bytes == 0)
		{
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

DFILE *my_file_init(int fd)
{
	DFILE *df = safe_malloc(sizeof(DFILE));
	df->fd = fd;
	df->buf = safe_malloc(BUF_SIZE);
	df->p = NULL;
	df->bufsize = BUF_SIZE;
	df->nb = 0;
	df->eof = 0;	
	return df;
}

void my_file_destroy(DFILE *df)
{
	free(df->buf);
	free(df);
}

DSTRING *my_file_readline(DSTRING *dst, DFILE *df)
{
	int fd = df->fd;
	int len;
	char *b;
	int bufsize = df->bufsize;
	char *p;
	ssize_t bytes;
	if (df->nb > 0)
	{
		b = df->p;
		p = memchr(b, '\n', df->nb);
		if (p != NULL)
		{
			df->p = p+1;
			len = df->p-b;
			df->nb = df->nb-len;
			dynamic_string_n_append(dst, b, len);
			return dst;
		}
		dynamic_string_n_append(dst, b, df->nb);
		df->nb = 0;
		df->p = NULL;
		if (df->eof)
			return dst;
	}
	b = df->buf;
	for (;;)
	{
		//printf("bufsize %d\n", bufsize);
		bytes = read(fd, b, bufsize);
		if (bytes == -1)
		{
			log_error("Error: read file %d: %s\n", errno, strerror(errno)); 
			return NULL;
		}
		if (bytes == 0)
		{
				df->eof = 1;
				break;
		}
		df->eof = 0;
		p = memchr(b, '\n', bytes);
		if (p != NULL)
		{
			df->p = p+1;
			len = df->p-b;
			df->nb = bytes-len;
			dynamic_string_n_append(dst, b, len);
			break;
		}
		dynamic_string_n_append(dst, b, bytes);
	}
	return dst;
}

DSPLIT *my_split_new(int n)
{
	DSPLIT *ds = (DSPLIT *)safe_malloc(sizeof(DSPLIT));
	ds->ents = 0;
	ds->size = n;
	ds->entries = (DSENT *)safe_malloc(sizeof(DSENT)*n);
	return ds;
}

void my_split_destroy(DSPLIT *ds)
{
	free(ds->entries);
	free(ds);
}

DSPLIT *my_split(DSPLIT *ds, const char* s, size_t len, int limit)
{
	char *sp;
	char *p;
	char *ep = (char *)s + len;
	DSENT *dp = ds->entries;
	ds->ents = 0;
	int inword = 0;
	for (sp = p = (char *)s; p<ep; p++)
	{
			if (*p == ' ' || *p == '\t')
			{
				if (inword)
				{
					if (ds->ents > ds->size)
					{
							ds->entries = safe_realloc(ds->entries, ds->size * 2);
							dp = ds->entries + ds->ents -1;
					}
					dp->sp = sp;
					dp->len = p-sp;
					dp++;
					inword = 0;
				}
			}
			else
			{
				if (!inword)
				{
					sp = p;
					inword = 1;
					ds->ents++;
					if (ds->ents == limit)
					{
						break;
					}
				}
			}
	} 
	if (inword)
	{
		if (ds->ents > ds->size)
		{
				ds->entries = safe_realloc(ds->entries, ds->size * 2);
				dp = ds->entries + ds->ents - 1;
		}
		dp->sp = sp;
		dp->len = ep-sp; 
		dp++;
		inword = 0;
	}
	return ds;
}

double my_time()
{
	struct timeval tv;
	if (gettimeofday(&tv, NULL) == -1)
		return 0.0;
	return tv.tv_sec+tv.tv_usec*0.000001;
}
