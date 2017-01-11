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
#include <sys/time.h> //for struct timeval, gettimeofday()
#include <string.h>
#include <stdint.h> //for uint64_t
#include <math.h> //for log()
#define MSSD_MAX_FILE 20
#define MSSD_MAX_FILE_NAME_LENGTH 256

//streamd id const
#define MSSD_OTHER_SID 1
#define MSSD_JOURNAL_SID 2
#define MSSD_COLL_INIT_SID 4 //collection 3, 4, 5
#define MSSD_IDX_INIT_SID 7 //index 6, 7, 8 

//For hotness compute
#define ALPHA 6
#define THRESHOLD1 0.1

/*(file_name, offset) pair used for multi-streamed ssd
 *For multiple collection files and index files, fixed-single boundary is not possible 
 Each collection file or index file need a boundary. 
 Boundaries should get by code (not manually)
 * */
#if defined(SSDM_OP8)
typedef struct __mssd_pair {
	char* fn; //file name
	off_t offset; //last physical offset of a file 
	size_t num_w1; //number of write on area 1
	size_t num_w2; //number of write on area 2 
	//ranges of writes within the interval time (e.g. checkpoint interval)
	off_t off_min1;
	off_t off_max1;
	off_t off_min2;
	off_t off_max2;
	//support variable for compute hotness
	double gpct1;// global percentage 1
	double gpct2;// global percentage 2
	double ws1; //write speed 1
	double ws2; //write speed 2
	//stream ids 
	int cur_sid; //current sid
	int sid1;
	int sid2;
} __mssd_pair;
typedef struct __mssd_pair MSSD_PAIR;

typedef struct __mssd_map {
	__mssd_pair** data;	
	int size; //current number of pairs
	struct timeval tv; //time when the checkpoint is called
} __mssd_map;
typedef __mssd_map MSSD_MAP;
#else
typedef struct __mssd_pair {
	char* fn; //file name
	off_t offset; //last physical offset of a file 
} __mssd_pair;
typedef struct __mssd_pair MSSD_PAIR;

typedef struct __mssd_map {
	__mssd_pair** data;	
	int size; //current number of pairs
} __mssd_map;
typedef __mssd_map MSSD_MAP;
#endif


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
static inline void mssdmap_flexmap(MSSD_MAP *m, FILE* fp);
#else
static inline int mssdmap_get_or_append(MSSD_MAP* m, const char* key, const off_t val, off_t* retval);
static inline int mssdmap_set_or_append(MSSD_MAP* m, const char* key, const off_t val);
static inline int mssdmap_append(MSSD_MAP* m, const char* key, const off_t val);
#endif //SSDM_OP8

#if defined(SSDM_OP8)
//MSSD_MAP* mssdmap_new() {
static inline MSSD_MAP* mssdmap_new() {
	MSSD_MAP* m = (MSSD_MAP*) malloc(sizeof(MSSD_MAP));
	if(!m) goto err;

	m->data = (MSSD_PAIR**) calloc(MSSD_MAX_FILE, sizeof(MSSD_PAIR*));
	if(!m->data) goto err;
	
	m->size = 0;
	gettimeofday(&m->tv, NULL);
	return m;

err:
	if (m)
		mssdmap_free(m);
	return NULL;
}
#else
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
#endif //SSDM_OP8
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
static inline void mssdmap_flexmap(MSSD_MAP *m, FILE* fp){
//TODO: adjusting the stream id based on our algorithm
//write_density = num_write / (off_max - off_min) 

	int i;
	MSSD_PAIR* obj;
	double den1, den2, local_pct1, local_pct2, global_pct1, global_pct2, wpps1, wpps2;
	struct timeval tv_tem;
	double time_s;
	double coll_min, coll_max, idx_min, idx_max;
	double coll_p1, coll_p2, idx_p1, idx_p2;
	size_t coll_count1, coll_count2, idx_count1, idx_count2; //counts for global cpt computation
	coll_count1 = coll_count2 = idx_count1 = idx_count2 = 0;
	coll_min = idx_min = 10000000;
	coll_max = idx_max = 0;

//compute the ckpt interval time in seconds and update
	gettimeofday(&tv_tem, NULL);
	time_s = (tv_tem.tv_sec - m->tv.tv_sec);
	if(time_s <= 0){
				printf("error! ckpt interval <= 0");	
				return;
	}
	m->tv = tv_tem;
//phase 1: compute the total write in a stream id
		for( i = 0; i < m->size; i++){
			obj = m->data[i];
			if(strstr(obj->fn, "collection") != 0){
				coll_count1 += obj->num_w1;
				coll_count2 += obj->num_w2;

			}
			else if(strstr(obj->fn, "index") != 0) {
				idx_count1 += obj->num_w1;	
				idx_count2 += obj->num_w2;	
			}
		}
//phase 2: compute densities, write per page per second,  and written percentages
		for( i = 0; i < m->size; i++){
			obj = m->data[i];

			den1 = (obj->off_max1 > obj->off_min1) ? ((obj->num_w1 * 4096.0) / (obj->off_max1 - obj->off_min1)) : (-1) ;
			den2 = (obj->off_max2 > obj->off_min2) ? ((obj->num_w2 * 4096.0) / (obj->off_max2 - obj->off_min2)) : (-1) ;
			if(time_s > 0){
				wpps1 = den1 / time_s;
				wpps2 = den2 / time_s;
				//Compute logarit
				obj->ws1 = log(wpps1) / log(10);
				obj->ws2 = log(wpps2) / log(10);
			}


			if(strstr(obj->fn, "collection") != 0){
				obj->gpct1 = global_pct1 = (obj->num_w1 * 1.0) / coll_count1 * 100;
				obj->gpct2 = global_pct2 = (obj->num_w2 * 1.0) / coll_count2 * 100;
				//compute min, max
				if(obj->ws1 < coll_min)
					coll_min = obj->ws1;
				if(obj->ws2 < coll_min)
					coll_min = obj->ws2;
				if(obj->ws1 > coll_max)
					coll_max = obj->ws1;
				if(obj->ws2 > coll_max)
					coll_max = obj->ws2;
			}
			else if(strstr(obj->fn, "index") != 0) {
				obj->gpct1 = global_pct1 = (obj->num_w1 * 1.0) / idx_count1 * 100;
				obj->gpct2 = global_pct2 = (obj->num_w2 * 1.0) / idx_count2 * 100;
				//compute min, max, except index files that has global percentage too small 
				if(global_pct1 > THRESHOLD1 && global_pct2 > THRESHOLD1){
					if(obj->ws1 < idx_min)
						idx_min = obj->ws1;
					if(obj->ws2 < idx_min)
						idx_min = obj->ws2;
					if(obj->ws1 > idx_max)
						idx_max = obj->ws1;
					if(obj->ws2 < idx_max)
						idx_max = obj->ws2;
				}
			}

		}//end for
//phase 3: stream mapping based on hotness
		//compute pivot points
		coll_p1	= coll_min + ((coll_max - coll_min) * (1.0 / ALPHA) );
		coll_p2 = coll_min + ( (coll_max - coll_min) * ((ALPHA-1)*1.0/ALPHA) );
		idx_p1	= idx_min + ((idx_max - idx_min) * (1.0 / ALPHA) );
		idx_p2 = idx_min + ( (idx_max - idx_min) * ((ALPHA-1)*1.0/ALPHA) );
		//mapping
		for( i = 0; i < m->size; i++){
			obj = m->data[i];
			local_pct1 = (obj->num_w1 * 1.0) / (obj->num_w1 + obj->num_w2) * 100;
			local_pct2 = (obj->num_w2 * 1.0) / (obj->num_w1 + obj->num_w2) * 100;

			if(strstr(obj->fn, "collection") != 0){
				if(obj->ws1 <= coll_p1){
					obj->sid1 = MSSD_COLL_INIT_SID - 1;
				}
				else if(coll_p1 < obj->ws1 && obj->ws1 <= coll_p2){
					obj->sid1 = MSSD_COLL_INIT_SID;
				}
				else{
					obj->sid1 = MSSD_COLL_INIT_SID + 1;
				}

				if(obj->ws2 <= coll_p1){
					obj->sid2 = MSSD_COLL_INIT_SID - 1;
				}
				else if(coll_p1 < obj->ws2 && obj->ws2 <= coll_p2){
					obj->sid2 = MSSD_COLL_INIT_SID;
				}
				else{
					obj->sid2 = MSSD_COLL_INIT_SID + 1;
				}
			printf("__ckpt_server name %s offset %jd num_w1 %zu num_w2 %zu cur_sid %d sid1 %d sid2 %d l_pct1 %f l_pct2 %f g_pct1 %f g_pct2 %f duration_s %f wpps1 %f wpps2 %f min %f p1 %f p2 %f max %f \n", obj->fn, obj->offset, obj->num_w1, obj->num_w2, obj->cur_sid, obj->sid1, obj->sid2, local_pct1, local_pct2, obj->gpct1, obj->gpct2, time_s, obj->ws1, obj->ws2, coll_min, coll_p1, coll_p2, coll_max);
			fprintf(fp, "__ckpt_server name %s offset %jd num_w1 %zu num_w2 %zu cur_sid %d sid1 %d sid2 %d l_pct1 %f l_pct2 %f g_pct1 %f g_pct2 %f duration_s %f wpps1 %f wpps2 %f min %f p1 %f p2 %f max %f \n", obj->fn, obj->offset, obj->num_w1, obj->num_w2, obj->cur_sid, obj->sid1, obj->sid2, local_pct1, local_pct2, obj->gpct1, obj->gpct2, time_s, obj->ws1, obj->ws2, coll_min, coll_p1, coll_p2, coll_max);
			}
			else if(strstr(obj->fn, "index") != 0) {
				if(obj->gpct1 < THRESHOLD1 || obj->gpct2 < THRESHOLD1) {
					obj->sid1 = obj->sid2 = MSSD_OTHER_SID;
					continue;
				}

				if(obj->ws1 <= idx_p1){
					obj->sid1 = MSSD_IDX_INIT_SID - 1;
				}
				else if(idx_p1 < obj->ws1 && obj->ws1 <= idx_p2){
					obj->sid1 = MSSD_IDX_INIT_SID;
				}
				else{
					obj->sid1 = MSSD_IDX_INIT_SID + 1;
				}

				if(obj->ws2 <= idx_p1){
					obj->sid2 = MSSD_IDX_INIT_SID - 1;
				}
				else if(idx_p1 < obj->ws2 && obj->ws2 <= idx_p2){
					obj->sid2 = MSSD_IDX_INIT_SID;
				}
				else{
					obj->sid2 = MSSD_IDX_INIT_SID + 1;
				}
			printf("__ckpt_server name %s offset %jd num_w1 %zu num_w2 %zu cur_sid %d sid1 %d sid2 %d l_pct1 %f l_pct2 %f g_pct1 %f g_pct2 %f duration_s %f wpps1 %f wpps2 %f min %f p1 %f p2 %f max %f \n", obj->fn, obj->offset, obj->num_w1, obj->num_w2, obj->cur_sid, obj->sid1, obj->sid2, local_pct1, local_pct2, obj->gpct1, obj->gpct2, time_s, obj->ws1, obj->ws2, idx_min, idx_p1, idx_p2, idx_max);
			fprintf(fp, "__ckpt_server name %s offset %jd num_w1 %zu num_w2 %zu cur_sid %d sid1 %d sid2 %d l_pct1 %f l_pct2 %f g_pct1 %f g_pct2 %f duration_s %f wpps1 %f wpps2 %f min %f p1 %f p2 %f max %f \n", obj->fn, obj->offset, obj->num_w1, obj->num_w2, obj->cur_sid, obj->sid1, obj->sid2, local_pct1, local_pct2, obj->gpct1, obj->gpct2, time_s, obj->ws1, obj->ws2, idx_min, idx_p1, idx_p2, idx_max);
			}

#if defined(SSDM_OP8_DEBUG)
			//printf("__ckpt_server name %s offset %jd num_w1 %zu num_w2 %zu off_min1 %jd off_max1 %jd off_min2 %jd off_max2 %jd cur_sid %d sid1 %d sid2 %d den1 %f den2 %f l_pct1 %f l_pct2 %f g_pct1 %f g_pct2 %f duration_s %f wpps1 %f wpps2 %f \n", obj->fn, obj->offset, obj->num_w1, obj->num_w2, obj->off_min1, obj->off_max1, obj->off_min2, obj->off_max2, obj->cur_sid, obj->sid1, obj->sid2, den1, den2, local_pct1, local_pct2, global_pct1, global_pct2, time_s, wpps1, wpps2);
#endif //SSDM_OP8_DEBUG

			//reset this obj's metadata for next ckpt
			obj->num_w1 = obj->num_w2 = 0;
			obj->off_min1 = obj->off_min2 = 100 * obj->offset;
			obj->off_max1 = obj->off_max2 = 0;
		}//end for

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
