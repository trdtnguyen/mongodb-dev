/*
 *Author: tdnguyen
 Simple map table implementation
 Each element in the map table is a pair of (filename, offset)
 * */
#ifndef __MSSD_H__
#define __MSSD_H__

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <string.h>
#include <stdint.h> //for uint64_t
#define MSSD_MAX_FILE 20
#define MSSD_MAX_FILE_NAME_LENGTH 256
#define MSSD_COLL_INIT_SID 3
#define MSSD_IDX_INIT_SID 5 

/*(file_name, offset) pair used for multi-streamed ssd
 *For multiple collection files and index files, fixed-single boundary is not possible 
 Each collection file or index file need a boundary. 
 Boundaries should get by code (not manually)
 * */
#if defined(SSDM_OP8)
typedef struct __mssd_pair {
	char* fn; //file name
	off_t offset; //last physical offset of a file 
	uint32_t num_w1; //number of write on area 1
	uint32_t num_w2; //number of write on area 2 
	//ranges of writes within the interval time (e.g. checkpoint interval)
	off_t off_min1;
	off_t off_max1;
	off_t off_min2;
	off_t off_max2;
	//stream ids 
	int cur_sid; //current sid
	int sid1;
	int sid2;
} __mssd_pair;
typedef struct __mssd_pair MSSD_PAIR;
#else
typedef struct __mssd_pair {
	char* fn; //file name
	off_t offset; //last physical offset of a file 
} __mssd_pair;
typedef struct __mssd_pair MSSD_PAIR;
#endif

typedef struct __mssd_map {
	__mssd_pair** data;	
	int size; //current number of pairs
} __mssd_map;
typedef __mssd_map MSSD_MAP;

/*
struct __mssd_pair {
	char* fn; //file name
	off_t offset; //last physical offset of a file 
};

struct __mssd_map {
	__mssd_pair** data;	
	int size; //current number of pairs
};
*/

/*
MSSD_MAP* mssdmap_new();
void mssdmap_free(MSSD_MAP* m);
int mssdmap_get_or_append(MSSD_MAP* m, const char* key, const off_t val, off_t* retval);
int mssdmap_set_or_append(MSSD_MAP* m, const char* key, const off_t val);
int mssdmap_find(MSSD_MAP* m, const char* key);
off_t mssdmap_get_offset_by_id(MSSD_MAP* m, int id);
char* mssdmap_get_filename_by_id(MSSD_MAP* m, int id);
int mssdmap_append(MSSD_MAP* m, const char* key, const off_t val);
*/

static inline MSSD_MAP* mssdmap_new();
static inline void mssdmap_free(MSSD_MAP* m);
static inline int mssdmap_find(MSSD_MAP* m, const char* key);
static inline off_t mssdmap_get_offset_by_id(MSSD_MAP* m, int id);
static inline char* mssdmap_get_filename_by_id(MSSD_MAP* m, int id);
#if defined (SSDM_OP8)
static inline int mssdmap_get_or_append(MSSD_MAP* m, const char* key, const off_t val, const int sid, off_t* retval);
static inline int mssdmap_set_or_append(MSSD_MAP* m, const char* key, const off_t val,const int sid);
static inline int mssdmap_append(MSSD_MAP* m, const char* key, const off_t val, const int sid);
#else
static inline int mssdmap_get_or_append(MSSD_MAP* m, const char* key, const off_t val, off_t* retval);
static inline int mssdmap_set_or_append(MSSD_MAP* m, const char* key, const off_t val);
static inline int mssdmap_append(MSSD_MAP* m, const char* key, const off_t val);
#endif //SSDM_OP8

//MSSD_MAP* mssdmap_new() {
static inline MSSD_MAP* mssdmap_new() {
	MSSD_MAP* m = (MSSD_MAP*) malloc(sizeof(MSSD_MAP));
	if(!m) goto err;

	m->data = (MSSD_PAIR**) calloc(MSSD_MAX_FILE, sizeof(MSSD_PAIR*));
	if(!m->data) goto err;
	
	m->size = 0;
	return m;

err:
	if (m)
		mssdmap_free(m);
	return NULL;
}
// void mssdmap_free(MSSD_MAP* m) {
static inline void mssdmap_free(MSSD_MAP* m) {
	int i;
	if (m->size > 0) {
		for (i = 0; i < m->size; i++) {
			free(m->data[i]->fn);
			free(m->data[i]);
		}
	}
	free(m->data);
	free(m);
}
/* Main function use for multi-streamed SSD 
 * input: key, value
 * If key is exist in map table => get the offset, retval will be set to 0; return the id of exist key
 * Else, append (key, value); return the last id 
 * * */
#if defined (SSDM_OP8)
static inline int mssdmap_get_or_append(MSSD_MAP* m, const char* key, const off_t val, const int sid, off_t* retval) {
	int id;
	id = mssdmap_find(m, key);
	if (id >= 0){
		*retval = m->data[id]->offset;
		return id;
	}	
	mssdmap_append(m, key, val, sid);
	*retval = 0;
	return (m->size - 1);
}
/* * input: key, value
 * If key is exist in map table => update its value
 * Else, append (key, value) 
 * * */
//int mssdmap_set_or_append(MSSD_MAP* m, const char* key, const off_t val) {
static inline int mssdmap_set_or_append(MSSD_MAP* m, const char* key, const off_t val, const int sid) {
	int id;
	id = mssdmap_find(m, key);
	if (id >= 0){
		m->data[id]->offset = val;	
		return id;
	}	
	mssdmap_append(m, key, val, sid);
	return (m->size - 1);
}
#else
//int mssdmap_get_or_append(MSSD_MAP* m, const char* key, const off_t val, off_t* retval) {
static inline int mssdmap_get_or_append(MSSD_MAP* m, const char* key, const off_t val, off_t* retval) {
	int id;
	id = mssdmap_find(m, key);
	if (id >= 0){
		*retval = m->data[id]->offset;
		return id;
	}	
	mssdmap_append(m, key, val);
	*retval = 0;
	return (m->size - 1);
}
/* * input: key, value
 * If key is exist in map table => update its value
 * Else, append (key, value) 
 * * */
//int mssdmap_set_or_append(MSSD_MAP* m, const char* key, const off_t val) {
static inline int mssdmap_set_or_append(MSSD_MAP* m, const char* key, const off_t val) {
	int id;
	id = mssdmap_find(m, key);
	if (id >= 0){
		m->data[id]->offset = val;	
		return id;
	}	
	mssdmap_append(m, key, val);
	return (m->size - 1);
}
#endif //SSDM_OP8


/* find key and return index in the array
 * Just simple scan whole items.
 * The number of files are expected small
 *return -1 if key is not exist
 * */
//int mssdmap_find(MSSD_MAP* m, const char* key){
static inline int mssdmap_find(MSSD_MAP* m, const char* key){
	int i;
	for (i = 0; i < m->size; i++){
		if (strcmp(m->data[i]->fn, key) == 0)
			return i;
	}
	return -1;
}
//off_t mssdmap_get_offset_by_id(MSSD_MAP* m, int id) {
static inline off_t mssdmap_get_offset_by_id(MSSD_MAP* m, int id) {
	return (m->data[id]->offset);
}
//char* mssdmap_get_filename_by_id(MSSD_MAP* m, int id){
static inline char* mssdmap_get_filename_by_id(MSSD_MAP* m, int id){
	return (m->data[id]->fn);
}
/*
 *Create new MSSD_PAIR based on input key, val and append on the list
 * */
#if defined(SSDM_OP8)
//int mssdmap_append(MSSD_MAP* m, const char* key, const off_t val) {
static inline int mssdmap_append(MSSD_MAP* m, const char* key, const off_t val, const int sid) {
	if (m->size >= MSSD_MAX_FILE) {
		printf("mssdmap is full!\n");
		return -1;
	}
	MSSD_PAIR* pair = (MSSD_PAIR*) malloc(sizeof(MSSD_PAIR));
	pair->fn = (char*) malloc(MSSD_MAX_FILE_NAME_LENGTH);
	strcpy(pair->fn, key);
	pair->offset = val;
	pair->num_w1 = pair->num_w2 = 0;
	pair->off_min1 = pair->off_min2 = 100 * val;
	pair->off_max1 = pair->off_max2 = 0;
	pair->sid1 = pair->cur_sid = sid;
	pair->sid2 = sid + 1;
	
	m->data[m->size] = pair;
	m->size++;
	return 0;
}
#else //normal (SSDM_OP6, SSDM_OP7)
//int mssdmap_append(MSSD_MAP* m, const char* key, const off_t val) {
static inline int mssdmap_append(MSSD_MAP* m, const char* key, const off_t val) {
	if (m->size >= MSSD_MAX_FILE) {
		printf("mssdmap is full!\n");
		return -1;
	}
	MSSD_PAIR* pair = (MSSD_PAIR*) malloc(sizeof(MSSD_PAIR));
	pair->fn = (char*) malloc(MSSD_MAX_FILE_NAME_LENGTH);
	strcpy(pair->fn, key);
	pair->offset = val;
	
	m->data[m->size] = pair;
	m->size++;
	return 0;
}
#endif //SSDM_OP8
#endif //__MSSD_H__
