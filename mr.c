#include "mr.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "hash.h"
#include "kvlist.h"

void map_reduce(mapper_t mapper, size_t num_mapper, reducer_t reducer,
                size_t num_reducer, kvlist_t* input, kvlist_t* output) {
  kvlist_iterator_t* iter = kvlist_iterator_new(input);
  ;
  kvpair_t* pair = kvlist_iterator_next(iter);
  kvlist_t** splits = malloc(num_mapper * sizeof(kvlist_t*));
  // kvlist_t* splits[num_mapper];
  for (size_t i = 0; i < num_mapper; i++) {
    splits[i] = kvlist_new();
  }
  size_t i = 0;

  kvlist_append(splits[i], pair);
  i = (i + 1) % num_mapper;

  // kvlist_print(1 ,pair);

  pthread_t map_threads[num_mapper];
  for (size_t i = 0; i < num_mapper; i++) {
    pthread_create(&map_threads[i], NULL, (void* (*)(void*))mapper, splits[i]);
  }
  for (size_t i = 0; i < num_mapper; i++) {
    pthread_join(map_threads[i], NULL);
  }

  pthread_t map_threadss[num_reducer];
  for (size_t i = 0; i < num_reducer; i++) {
    pthread_create(&map_threadss[i], NULL, (void* (*)(void*))reducer, splits[i]);
  }
  for (size_t i = 0; i < num_reducer; i++) {
    pthread_join(map_threadss[i], NULL);
  }
  
}
