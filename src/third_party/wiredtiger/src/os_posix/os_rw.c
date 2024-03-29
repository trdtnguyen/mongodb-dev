/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"


#ifdef SSDM_OP4
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/ioctl.h> //for ioctl
#include <stdint.h> //for uint64_t
#include <inttypes.h> //for PRI64
#include <sys/types.h>
#include <linux/fs.h> //for FIBMAP
extern FILE* my_fp5;
#ifdef SSDM_OP4_2
extern int my_coll_streamid;
#endif
extern int my_journal_streamid;

#if defined(SSDM_OP4_3) || defined(SSDM_OP4_4)
extern int my_coll_left_streamid;
extern int my_coll_right_streamid;
extern off_t my_b;
extern uint64_t count1;
extern uint64_t count2;
#endif

#endif //SSDM_OP4 

#ifdef SSDM_OP6
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/ioctl.h> //for ioctl
#include <stdint.h> //for uint64_t
#include <inttypes.h> //for PRI64
#include <sys/types.h>
#include <linux/fs.h> //for FIBMAP
//#include "third_party/mssd/mssd.h" //for MSSD_MAP
#include "mssd.h"
extern FILE* my_fp6;
extern MSSD_MAP* mssd_map;
extern off_t* retval;

extern int my_coll_streamid1;
extern int my_coll_streamid2;

extern int my_index_streamid1;
extern int my_index_streamid2;

extern uint64_t count1;
extern uint64_t count2;
//extern int mssdmap_get_or_append(MSSD_MAP* m, const char* key, const off_t val, off_t* retval);
#if defined(SSDM_OP6_DEBUG)
extern struct timeval start;
#endif
#endif //SSDM_OP6 

#ifdef SSDM_OP7
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/ioctl.h> //for ioctl
#include <stdint.h> //for uint64_t
#include <inttypes.h> //for PRI64
#include <sys/types.h>
#include <linux/fs.h> //for FIBMAP
//#include "third_party/mssd/mssd.h" //for MSSD_MAP
#include "mssd.h"
extern FILE* my_fp7;
extern off_t mssd_map[MSSD_MAX_FILE];

extern int my_coll_streamid1;
extern int my_coll_streamid2;

extern int my_index_streamid1;
extern int my_index_streamid2;
#endif //SSDM_OP7 

#if defined (SSDM_OP8) || defined(SSDM_OP8_2) || defined(SSDM_OP9) || defined(SSDM_OP11)
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/ioctl.h> //for ioctl
#include <stdint.h> //for uint64_t
#include <inttypes.h> //for PRI64
#include <sys/types.h>
#include <linux/fs.h> //for FIBMAP
//#include "third_party/mssd/mssd.h" //for MSSD_MAP
#include "mssd.h"

#if defined (SSDM_OP8) || defined(SSDM_OP8_2) || defined(SSDM_OP11)
extern FILE* my_fp8;
#endif

#if defined (SSDM_OP9)
extern FILE* my_fp9;
#endif

extern MSSD_MAP* mssd_map;
extern off_t* retval;

extern int my_coll_streamid1;
extern int my_coll_streamid2;

extern int my_index_streamid1;
extern int my_index_streamid2;

extern uint64_t count1;
extern uint64_t count2;
#if defined (SSDM_OP8_DEBUG) || defined(SSDM_OP9_DEBUG) || defined(SSDM_OP11_DEBUG)
extern struct timeval start;
#endif //SSDM_OP8_DEBUG
//extern int mssdmap_get_or_append(MSSD_MAP* m, const char* key, const off_t val, off_t* retval);
#endif //SSDM_OP8 

/*
 * __wt_read --
 *	Read a chunk.
 */
int
__wt_read(
    WT_SESSION_IMPL *session, WT_FH *fh, wt_off_t offset, size_t len, void *buf)
{
	size_t chunk;
	ssize_t nr;
	uint8_t *addr;

	WT_STAT_FAST_CONN_INCR(session, read_io);

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS,
	    "%s: read %" WT_SIZET_FMT " bytes at offset %" PRIuMAX,
	    fh->name, len, (uintmax_t)offset));

	/* Assert direct I/O is aligned and a multiple of the alignment. */
	WT_ASSERT(session,
	    !fh->direct_io ||
	    S2C(session)->buffer_alignment == 0 ||
	    (!((uintptr_t)buf &
	    (uintptr_t)(S2C(session)->buffer_alignment - 1)) &&
	    len >= S2C(session)->buffer_alignment &&
	    len % S2C(session)->buffer_alignment == 0));

	/* Break reads larger than 1GB into 1GB chunks. */
	for (addr = buf; len > 0; addr += nr, len -= (size_t)nr, offset += nr) {
		chunk = WT_MIN(len, WT_GIGABYTE);
		if ((nr = pread(fh->fd, addr, chunk, offset)) <= 0)
			WT_RET_MSG(session, nr == 0 ? WT_ERROR : __wt_errno(),
			    "%s read error: failed to read %" WT_SIZET_FMT
			    " bytes at offset %" PRIuMAX,
			    fh->name, chunk, (uintmax_t)offset);
	}
	return (0);
}

/*
 * __wt_write --
 *	Write a chunk.
 */
int
__wt_write(WT_SESSION_IMPL *session,
    WT_FH *fh, wt_off_t offset, size_t len, const void *buf)
{
	size_t chunk;
	ssize_t nw;
	const uint8_t *addr;
#if defined(SSDM_OP4_3) || defined(SSDM_OP4_4) || defined(SSDM_OP4_2) || defined(SSDM) || defined(SSDM_OP2)
	int my_ret;
	uint64_t off_tem;
	//int stream_id;
#endif
#if defined(SSDM_OP6) 
	int my_ret;
	off_t dum_off=1024;
	//int stream_id;
#if	defined(SSDM_OP6_DEBUG)
	uint64_t off_tem;
	struct timeval now;
	double time_ms;
#endif

#endif //SSDM_OP6
#if defined(SSDM_OP7)
	off_t ret_off=1024;
#endif //SSDM_OP7

#if defined(SSDM_OP8) || defined(SSDM_OP8_2) || defined(SSDM_OP9) || defined(SSDM_OP11)
	int my_ret, id;
	MSSD_PAIR* obj;
#if defined(SSDM_OP8_DEBUG) || defined(SSDM_OP9) || defined(SSDM_OP11_DEBUG)
	uint64_t off_tem;
	struct timeval now;
	double time_ms;
#endif
#endif //SSDM_OP8

	WT_STAT_FAST_CONN_INCR(session, write_io);

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS,
	    "%s: write %" WT_SIZET_FMT " bytes at offset %" PRIuMAX,
	    fh->name, len, (uintmax_t)offset));

	/* Assert direct I/O is aligned and a multiple of the alignment. */
	WT_ASSERT(session,
	    !fh->direct_io ||
	    S2C(session)->buffer_alignment == 0 ||
	    (!((uintptr_t)buf &
	    (uintptr_t)(S2C(session)->buffer_alignment - 1)) &&
	    len >= S2C(session)->buffer_alignment &&
	    len % S2C(session)->buffer_alignment == 0));
#ifdef SSDM_OP4
/*Naive multi-streamed,
 * stream-id 2: index
 * stream-id 3: journal
 * stream-id 4~7: collection if SSDM_OP4_2 is active
 * stream-id 1: others
 * Except collection, other file types are already assigned
 * stream_id in __wt_open() function
 * */
	//set stream_id depended on data types

#if defined(SSDM_OP4_3) || defined(SSDM_OP4_4)
//	if(strstr(fh->name, "ycsb/collection") != 0){
	if((strstr(fh->name, "collection") != 0) && (strstr(fh->name, "local") == 0)){
		//Convert from file offset to 4096b block offset 
		off_tem = offset / 4096;
		my_ret = ioctl(fh->fd, FIBMAP, &off_tem);
//Comment below codes for reduce overhead, enable them when debug
//		if(my_ret != 0){
//			perror("ioctl");
//		}
//		fprintf(stderr, "offset: %jd, LBA: %"PRIu64" \n", offset, off_tem);
	//	my_coll_streamid = 0; //unused 
		if(off_tem < (uint64_t)my_b){
			posix_fadvise(fh->fd, offset, my_coll_left_streamid, 8); //POSIX_FADV_DONTNEED=8
			//stream_id = my_coll_left_streamid;
			//++count1;
		}
		else {
			posix_fadvise(fh->fd, offset, my_coll_right_streamid, 8); //POSIX_FADV_DONTNEED=8
			//stream_id = my_coll_right_streamid;
			//++count2;
		}	
//		my_ret = posix_fadvise(fh->fd, offset, stream_id, 8); //POSIX_FADV_DONTNEED=8
//		if(my_ret != 0){
//			fprintf(my_fp5, "error call posix_fadvise, my_ret=%d, error is %s\n",my_ret, strerror(errno));		
//			perror("posix_fadvise");	
//		}
	}
#endif  //defined(SSDM_OP4_3) || defined(SSDM_OP4_4)
#if defined(SSDM_OP4_2)
	//Increase streamd id for collecton when ckpt call follow 
	//round-robin fashion
	//if(strstr(fh->name, "ycsb/collection") != 0){
	if((strstr(fh->name, "collection") != 0) && (strstr(fh->name, "local") == 0)){
	//	my_coll_left_streamid = my_coll_right_streamid = 0; //unused 
		stream_id = my_coll_streamid;
		my_ret = posix_fadvise(fh->fd, offset, stream_id, 8); //POSIX_FADV_DONTNEED=8
		if(my_ret != 0){
			fprintf(my_fp5, "error call posix_fadvise, my_ret=%d, error is %s\n",my_ret, strerror(errno));		
			perror("posix_fadvise");	
		}
	}
#endif //defined(SSDM_OP4_2)
//if both SSDM_OP4_2 and SSDM_OP4_3, and SSDM_OP_4 is not defined
//we use the default setting in __wt_open
//
#endif //ifdef SSDM_OP4

#ifdef SSDM_OP6
/*Naive idx multi-streamed,
 * stream-id 1: others 
 * stream-id 2: journal
 * stream-id 4: oplog 
 * stream-id 5~6: collection 
 * stream-id 7~8: index 
 * Except collection, and index  other file types are already assigned
 * stream_id in __wt_open() function
 * */
	//set stream_id depended on data types
	//comment on 2016.11.23
//	if((strstr(fh->name, "collection") != 0) && (strstr(fh->name, "local") == 0)){
	if (strstr(fh->name, "local") == 0) {
		//if(strstr(fh->name, "linkbench/collection") != 0) {
		if(strstr(fh->name, "collection") != 0) {
			//comment on 2016.11.22: use logical offset instead of physical offset
#if defined (SSDM_OP6_DEBUG)
			gettimeofday(&now, NULL);
			time_ms = (now.tv_sec - start.tv_sec)*1000 + (now.tv_usec - start.tv_usec)/1000;
			off_tem = offset;	
			//Convert from file offset to 4096b block offset 
			off_tem = offset / 4096;
			my_ret = ioctl(fh->fd, FIBMAP, &off_tem);
#endif //SSDM_OP6_DEBUG			
			//get offset boundary according to filename
			my_ret = mssdmap_get_or_append(mssd_map, fh->name, dum_off, retval);
			if (!(*retval)){
				fprintf(my_fp6, "====> retval is 0, something is wrong, check again!\n");
				fprintf(my_fp6, "key is %s dum_off: %jd ret_val: %jd, map size: %d \n", fh->name, dum_off, *retval, mssd_map->size);
			}
			if(offset < (*retval)){
				posix_fadvise(fh->fd, offset, my_coll_streamid1, 8); //POSIX_FADV_DONTNEED=8
#if defined (SSDM_OP6_DEBUG)
				fprintf(my_fp6, "os_rw  offset %jd LBA %jd boundary %jd left on %s with streamid %d attime %f\n",
						offset, off_tem, (*retval), fh->name, my_coll_streamid1, time_ms);
#endif
			}
			else {
				posix_fadvise(fh->fd, offset, my_coll_streamid2, 8); //POSIX_FADV_DONTNEED=8
#if defined (SSDM_OP6_DEBUG)
				// fprintf(my_fp6, "os_rw  offset %jd LBA %jd retval %jd right on %s with streamid %d\n",
				//		offset, off_tem, (*retval), fh->name, my_coll_streamid2);
				fprintf(my_fp6, "os_rw  offset %jd LBA %jd boundary %jd right on %s with streamid %d attime %f\n",
						offset, off_tem, (*retval), fh->name, my_coll_streamid2, time_ms);
#endif
			}	
		}
		//else if((strstr(fh->name, "index") != 0) && (strstr(fh->name, "local") == 0)){
		else if(strstr(fh->name, "index") != 0) {
			//comment on 2016.11.22: use logical offset instead of physical offset
#if defined(SSDM_OP6_DEBUG)
			gettimeofday(&now, NULL);
			time_ms = (now.tv_sec - start.tv_sec)*1000 + (now.tv_usec - start.tv_usec)/1000;
			off_tem = offset;	

			//Convert from file offset to 4096b block offset 
			off_tem = offset / 4096;
			my_ret = ioctl(fh->fd, FIBMAP, &off_tem);
#endif //SSDM_OP6_DEBUG

			//get offset boundary according to filename
			my_ret = mssdmap_get_or_append(mssd_map, fh->name, dum_off, retval);
			if (!(*retval)){
				fprintf(my_fp6, "os_rw====> retval is 0, something is wrong, check again!\n");
				fprintf(my_fp6, "key is %s dum_off: %jd ret_val: %jd, map size: %d \n", fh->name, dum_off, *retval, mssd_map->size);
			}
			if(offset < (*retval)){
				posix_fadvise(fh->fd, offset, my_index_streamid1, 8); //POSIX_FADV_DONTNEED=8
#if defined (SSDM_OP6_DEBUG)
				fprintf(my_fp6, "os_rw  offset %jd LBA %jd boundary %jd left on %s with streamid %d attime %f\n",
						offset, off_tem, (*retval), fh->name, my_index_streamid1, time_ms);
#endif
			}
			else {
				posix_fadvise(fh->fd, offset, my_index_streamid2, 8); //POSIX_FADV_DONTNEED=8
#if defined (SSDM_OP6_DEBUG)
				fprintf(my_fp6, "os_rw  offset %jd LBA %jd boundary %jd right on %s with streamid %d attime %f\n",
						offset, off_tem, (*retval), fh->name, my_index_streamid2, time_ms);
				// fprintf(my_fp6, "os_rw  offset %jd LBA %jd retval %jd right on %s with streamid %d\n",
				//		offset, off_tem, (*retval), fh->name, my_index_streamid2);
#endif
			}	
		}
	}//end if (strstr(fh->name, "local") == 0)

#if defined (DEBUG_LOG)
	//use this macro when you want to construct log write plot
	//an anternative for plot form blktrace
#if defined (SSDM_OP6_DEBUG)
	else if(strstr(fh->name, "journal") != 0) {
		gettimeofday(&now, NULL);
		time_ms = (now.tv_sec - start.tv_sec)*1000 + (now.tv_usec - start.tv_usec)/1000;
		off_tem = offset;	
		//Convert from file offset to 4096b block offset 
		off_tem = offset / 4096;
		my_ret = ioctl(fh->fd, FIBMAP, &off_tem);
	    fprintf(my_fp6, "os_rw  offset %jd LBA %jd boundary na na on %s with streamid %d attime %f\n",
					offset, off_tem, fh->name, MSSD_JOURNAL_SID, time_ms);
	}
#endif //defined (SSDM_OP6_DEBUG)
#endif //defined (DEBUG_LOG)

#endif //ifdef SSDM_OP6

#ifdef SSDM_OP7
/*Naive idx multi-streamed,
 * stream-id 1: others 
 * stream-id 2: journal
 * stream-id 3~4: collection 
 * stream-id 5~6: index 
 * Except collection, and index  other file types are already assigned
 * stream_id in __wt_open() function
 * */
	if ( strstr(fh->name, "local") == 0) {
		if(strstr(fh->name, "collection") != 0) {
			//if(strstr(fh->name, "linkbench/collection") != 0) {
			//comment on 2016.11.22: use logical offset instead of physical offset
			//off_tem = offset;	
			//Convert from file offset to 4096b block offset 
			//off_tem = offset / 4096;
			//my_ret = ioctl(fh->fd, FIBMAP, &off_tem);

			//get offset boundary according to filename
			ret_off = mssdmap_get(mssd_map, fh->name, true);
			//debug
			if(offset < ret_off){
				posix_fadvise(fh->fd, offset, my_coll_streamid1, 8); //POSIX_FADV_DONTNEED=8
#if defined (SSDM_OP7_DEBUG)
				fprintf(my_fp7, "os_rw  %jd\t\t%jd left on %s with streamid %d\n",
						offset, ret_off, fh->name, my_coll_streamid1);
#endif
			}
			else {
				posix_fadvise(fh->fd, offset, my_coll_streamid2, 8); //POSIX_FADV_DONTNEED=8
#if defined (SSDM_OP7_DEBUG)
				fprintf(my_fp7, "os_rw  %jd\t\t%jd right on %s with streamid %d\n",
						offset, ret_off, fh->name, my_coll_streamid2);
#endif
			}	
		}
		else if(strstr(fh->name, "index") != 0) {
			//else if(strstr(fh->name, "linkbench/index") != 0) {
			//comment on 2016.11.22: use logical offset instead of physical offset
			//off_tem = offset;	

			//Convert from file offset to 4096b block offset 
			//off_tem = offset / 4096;
			//my_ret = ioctl(fh->fd, FIBMAP, &off_tem);
			//get offset boundary according to filename
			ret_off = mssdmap_get(mssd_map, fh->name, false);
			if(offset < ret_off){
				posix_fadvise(fh->fd, offset, my_index_streamid1, 8); //POSIX_FADV_DONTNEED=8
#if defined (SSDM_OP7_DEBUG)
				fprintf(my_fp7, "os_rw  %jd\t\t%jd left on %s with streamid %d\n",
						offset, ret_off, fh->name, my_index_streamid1);
#endif
			}
			else {
				posix_fadvise(fh->fd, offset, my_index_streamid2, 8); //POSIX_FADV_DONTNEED=8
#if defined (SSDM_OP7_DEBUG)
				fprintf(my_fp7, "os_rw  %jd\t\t%jd right on %s with streamid %d\n",
						offset, ret_off, fh->name, my_index_streamid2);
#endif
			}	
		}
	}//end if ( strstr(fh->name, "local") == 0)
#endif //ifdef SSDM_OP7

#if defined(SSDM_OP8) || defined(SSDM_OP8_2) || defined(SSDM_OP9) || defined(SSDM_OP11)
/*flexible multi-streamed mapping scheme based on write density,
 * stream-id 1: others 
 * stream-id 2: journal
 * stream-id 3~ (3 + k - 1): collection 
 * stream-id 3 + k~ 3 + 2k - 1: index 
 * Except collection, and index  other file types are already assigned
 * stream_id in __wt_open() function
 *
 * */
	//this code will work for both collection files and index files 
	if ( strstr(fh->name, "local") == 0) {
		//if( (strstr(fh->name, "linkbench/collection") != 0) || (strstr(fh->name, "linkbench/index") != 0)) {
		if( (strstr(fh->name, "collection") != 0) || (strstr(fh->name, "index") != 0)) {
			//comment on 2016.11.22: use logical offset instead of physical offset
#if defined (SSDM_OP8_DEBUG) || defined(SSDM_OP9_DEBUG) || defined(SSDM_OP11_DEBUG)
			gettimeofday(&now, NULL);
			time_ms = (now.tv_sec - start.tv_sec)*1000 + (now.tv_usec - start.tv_usec)/1000;
			off_tem = offset;	
			//Convert from file offset to 4096b block offset 
			off_tem = offset / 4096;
			//Get LBA from file offset
			my_ret = ioctl(fh->fd, FIBMAP, &off_tem);
#endif //SSDM_OP8_DEBUG			
			//get offset boundary according to filename
			id = mssdmap_find(mssd_map, fh->name);
			if(id >= 0){
				obj = mssd_map->data[id];
				if(offset < obj->offset){
					//saving posix_fadvise call if the previous sid is same
					if(obj->cur_sid != obj->sid1){
						obj->cur_sid = obj->sid1;
						my_ret = posix_fadvise(fh->fd, offset, obj->cur_sid, 8); //POSIX_FADV_DONTNEED=8
					}
					//update internal metadata
					obj->num_w1++;
					if(offset < obj->off_min1) 
						obj->off_min1 = offset;
					if(offset > obj->off_max1)
						obj->off_max1 = offset;

#if defined (SSDM_OP8_DEBUG) || defined (SSDM_OP11_DEBUG)
					fprintf(my_fp8, "os_rw  offset %jd LBA %jd boundary %jd left on %s with streamid %d attime %f\n",
							offset, off_tem, obj->offset, fh->name, obj->sid1, time_ms);
#endif
#if defined (SSDM_OP9_DEBUG)
					fprintf(my_fp9, "os_rw  offset %jd LBA %jd boundary %jd left on %s with streamid %d attime %f\n",
							offset, off_tem, obj->offset, fh->name, obj->sid1, time_ms);
#endif
				}
				else {
					//saving posix_fadvise call if the previous sid is same
					if(obj->cur_sid != obj->sid2){
						obj->cur_sid = obj->sid2;
						my_ret = posix_fadvise(fh->fd, offset, obj->cur_sid, 8); //POSIX_FADV_DONTNEED=8
					}
					//update internal metadata
					obj->num_w2++;
					if(offset < obj->off_min2) 
						obj->off_min2 = offset;
					if(offset > obj->off_max2)
						obj->off_max2 = offset;
#if defined (SSDM_OP8_DEBUG) || defined (SSDM_OP11_DEBUG)
					fprintf(my_fp8, "os_rw  offset %jd LBA %jd boundary %jd right on %s with streamid %d attime %f\n",
							offset, off_tem, obj->offset, fh->name, obj->sid2, time_ms);
#endif
#if defined (SSDM_OP9_DEBUG)
					fprintf(my_fp9, "os_rw  offset %jd LBA %jd boundary %jd right on %s with streamid %d attime %f\n",
							offset, off_tem, obj->offset, fh->name, obj->sid2, time_ms);
#endif
				}	
			}
			else {
				printf("in os_rw.c, cannot find id for file %s\n", fh->name);	
			}
			//my_ret = mssdmap_get_or_append(mssd_map, fh->name, dum_off, MSSD_COLL_INIT_SID, retval);
		} //end if filter collection or index file
	}//end 	if ( strstr(fh->name, "local") == 0) 
	
#endif //ifdef SSDM_OP8
#ifdef SSDM_OP2 //size range method
	size_t ori_len = len;
	size_t STOP1 = 4096;
	size_t STOP2 = 28672;
	size_t STOP3 = 32768;
	int stream_id = 0;
	//Apply multi-streamed SSD 
	if (STOP3 <= ori_len) {
		stream_id = 4;
		//posix_fadvise(fh->fd, offset, 4, 8); //POSIX_FADV_DONTNEED=8
	}
	else if (STOP2 <= ori_len && ori_len < STOP3){
		stream_id = 3;
		//posix_fadvise(fh->fd, offset, 3, 8); //POSIX_FADV_DONTNEED=8
	}
	else if (STOP1 <= ori_len && ori_len < STOP2){
		stream_id = 2;
		//posix_fadvise(fh->fd, offset, 2, 8); //POSIX_FADV_DONTNEED=8
	}
	else {
		stream_id = 1;
		//posix_fadvise(fh->fd, offset, 1, 8); //POSIX_FADV_DONTNEED=8
	}
	my_ret = posix_fadvise(fh->fd, offset, stream_id, 8); //POSIX_FADV_DONTNEED=8
	if(my_ret != 0){
		perror("posix_fadvise");
	}
#endif //SSDM_OP2 

#if defined(SKIP_WRITE)

	/* Break writes larger than 1GB into 1GB chunks. */
	for (addr = buf; len > 0; addr += nw, len -= (size_t)nw, offset += nw) {
		chunk = WT_MIN(len, WT_GIGABYTE);
		
#if defined (SSDM_OP8)
		//skip write for collection and index files
		fprintf(my_fp8, "pwrite file %s on offset %jd len %zu \n", fh->name, offset, chunk);
#endif
		if ((nw = pwrite(fh->fd, addr, chunk, offset)) < 0)
			WT_RET_MSG(session, __wt_errno(),
					"%s write error: failed to write %" WT_SIZET_FMT
					" bytes at offset %" PRIuMAX,
					fh->name, chunk, (uintmax_t)offset);
	}

	return (0);
#else //original
//This loop write is use for all both optimize version and original
	/* Break writes larger than 1GB into 1GB chunks. */
	for (addr = buf; len > 0; addr += nw, len -= (size_t)nw, offset += nw) {
		chunk = WT_MIN(len, WT_GIGABYTE);
		if ((nw = pwrite(fh->fd, addr, chunk, offset)) < 0)
			WT_RET_MSG(session, __wt_errno(),
			    "%s write error: failed to write %" WT_SIZET_FMT
			    " bytes at offset %" PRIuMAX,
			    fh->name, chunk, (uintmax_t)offset);
	}

	return (0);
#endif //SKIP_WRITE
}
//end __wt_write
