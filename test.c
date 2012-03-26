#ifndef DOINT
#define DOINT
#include "common.h"
#undef DOINT
#endif /* DOINT */

int main()
{
	printf("malloc\n");
	char *s = safe_malloc(1024*1024*256);
	memset(s, 0, 1024*1024*256);
	sleep(3);
	printf("realloc\n");
	s = safe_realloc(s, 1024*1024);
	memset(s, 1, 1024*1024);
	sleep(3);
	printf("free\n");
	free(s);
	sleep(3);
	
	/* Hash test */
	DHASH *hash;
	hash = dynamic_hash_new();
	printf("size = %zu\n", hash->size);
	printf("fill = %zu\n", hash->fill);
	printf("ents = %zu\n", hash->ents);
	char a[256];
	size_t i;
	for (i = 0; i < 100; i++) {
		sprintf(a, "test_%zu", i);
		dynamic_hash_store(hash, a, (void *)i);
	}
	for (i = 0; i < 100; i++) {
		sprintf(a, "test_%zu", i);
		printf("%s ==>  %zu\n", a, (size_t)dynamic_hash_fetch(hash, a));
	}
	printf("size = %zu\n", hash->size);
	printf("fill = %zu\n", hash->fill);
	printf("ents = %zu\n", hash->ents);


	for (i = 0; i < 100; i++) {
		sprintf(a, "test_i%zu", i);
		dynamic_hash_store(hash, a, (void *)(i*10));
	}
	for (i = 0; i < 100; i++) {
		sprintf(a, "test_i%zu", i);
		printf("%s ==>  %zu\n", a, (size_t)dynamic_hash_fetch(hash, a));
	}
	printf("size = %zu\n", hash->size);
	printf("fill = %zu\n", hash->fill);
	printf("ents = %zu\n", hash->ents);


	for (i = 0; i < 100; i++) {
		sprintf(a, "test_%zu", i);
		dynamic_hash_delete(hash, a);
	}

	for (i = 0; i < 100; i++) {
		sprintf(a, "test_i%zu", i);
		dynamic_hash_delete(hash, a);
	}
	for (i = 0; i < 100; i++) {
		sprintf(a, "test_i%zu", i);
		printf("%s ==>  %zu\n", a, (size_t)dynamic_hash_fetch(hash, a));
	}

	printf("size = %zu\n", hash->size);
	printf("fill = %zu\n", hash->fill);
	printf("ents = %zu\n", hash->ents);

	dynamic_hash_destroy(hash);
	DARRAY *array;
	array = dynamic_array_new();
	for (i = 0; i < 100; i++) {
		dynamic_array_push(array, (void *)i);
	}
	for (i = 0; i < 30; i++) {
		printf("%zu\n", (size_t)dynamic_array_shift(array));
	}
	printf("shift end\n");
	printf("pop %zu\n", (size_t)dynamic_array_pop(array));
	dynamic_array_delete(array, 50);
	dynamic_array_set(array, 2012, (void *)2012);
	dynamic_array_set(array, 2011, (void *)2011);
	for (i = 0; i < dynamic_array_count(array); i++) {
		printf("%zu %zu\n", i, (size_t)dynamic_array_fetch(array, i));
	}

	dynamic_array_destroy(array);
	exit (0);
}
