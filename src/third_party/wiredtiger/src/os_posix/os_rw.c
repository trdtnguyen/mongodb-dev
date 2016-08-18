/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#ifdef SSDM
#include <fcntl.h>
extern off_t my_b;
extern long count1;
extern long count2;
extern long count3;
extern int offset_type;
#ifdef TDN_RID
extern FILE* my_fp_ssdm;
extern struct timeval  my_tv2;
extern double my_start_time2;
#endif
#endif

#ifdef SSDM_OP4_2
extern int my_coll_streamid;
#endif
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
 * */
	//set stream_id depended on data types
	int stream_id = 0;
	if(strstr(fh->name, "journal") != 0){
		stream_id = 3;
	}
	else if(strstr(fh->name, "ycsb/collection") != 0){
#ifdef SSDM_OP4_2
		stream_id = my_coll_streamid;
#else
		stream_id = 4;
#endif
	}
	else if(strstr(fh->name, "ycsb/index") != 0){
		stream_id = 2;
	}
	else {
		//Others: metadata, lock, system db
		stream_id = 1;
	}

	for (addr = buf; len > 0; addr += nw, len -= (size_t)nw, offset += nw) {
		chunk = WT_MIN(len, WT_GIGABYTE);

		posix_fadvise(fh->fd, offset, stream_id, 8); //POSIX_FADV_DONTNEED=8

		if ((nw = pwrite(fh->fd, addr, chunk, offset)) < 0)
			WT_RET_MSG(session, __wt_errno(),
					"%s write error: failed to write %" WT_SIZET_FMT
					" bytes at offset %" PRIuMAX,
					fh->name, chunk, (uintmax_t)offset);
	}
#elif SSDM
		if(strstr(fh->name, "ycsb/collection") != 0){
			/*apply multi-streamed SSD for only specific data
			 * left_part: streamid = 1
			 * right_part: streamid = 2
			 * Otherwise streamid = 0
			 * my_b: boundary block offset 
			 *
			 * Below function is modified from original posix_fadvise 
			 * posix_fadvise(fh->fd, offset, streamid, advise)
			 */
			
	/* Break writes larger than 1GB into 1GB chunks. */
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
			for (addr = buf; len > 0; addr += nw, len -= (size_t)nw, offset += nw) {
				chunk = WT_MIN(len, WT_GIGABYTE);
				
				posix_fadvise(fh->fd, offset, stream_id, 8); //POSIX_FADV_DONTNEED=8

				if ((nw = pwrite(fh->fd, addr, chunk, offset)) < 0)
					WT_RET_MSG(session, __wt_errno(),
							"%s write error: failed to write %" WT_SIZET_FMT
							" bytes at offset %" PRIuMAX,
							fh->name, chunk, (uintmax_t)offset);
			}
#else  //boundary method, need to define value my_b
			for (addr = buf; len > 0; addr += nw, len -= (size_t)nw, offset += nw) {
				chunk = WT_MIN(len, WT_GIGABYTE);
				if (offset < my_b){
					posix_fadvise(fh->fd, offset, 1, 8); //POSIX_FADV_DONTNEED=8
					//count1++;
					//offset_type = 1; //left
				}else {
					posix_fadvise(fh->fd, offset, 3, 8); //POSIX_FADV_DONTNEED=8
					//count2++;
					//offset_type = 2; //right
				}
				//end apply multi-streamed SSD
				if ((nw = pwrite(fh->fd, addr, chunk, offset)) < 0)
					WT_RET_MSG(session, __wt_errno(),
							"%s write error: failed to write %" WT_SIZET_FMT
							" bytes at offset %" PRIuMAX,
							fh->name, chunk, (uintmax_t)offset);
			}
#endif //ifdef SSDM_OP2
		}//we only apply multi-streamed for collection 
		else {
				//posix_fadvise(fh->fd, offset, 0, 8); //POSIX_FADV_DONTNEED=8
				//count3++;
				//offset_type = 0; //others
			/* Break writes larger than 1GB into 1GB chunks. */
			for (addr = buf; len > 0; addr += nw, len -= (size_t)nw, offset += nw) {
				chunk = WT_MIN(len, WT_GIGABYTE);
				if ((nw = pwrite(fh->fd, addr, chunk, offset)) < 0)
					WT_RET_MSG(session, __wt_errno(),
							"%s write error: failed to write %" WT_SIZET_FMT
							" bytes at offset %" PRIuMAX,
							fh->name, chunk, (uintmax_t)offset);
			}
		}
#else //original
	/* Break writes larger than 1GB into 1GB chunks. */
	for (addr = buf; len > 0; addr += nw, len -= (size_t)nw, offset += nw) {
		chunk = WT_MIN(len, WT_GIGABYTE);
		if ((nw = pwrite(fh->fd, addr, chunk, offset)) < 0)
			WT_RET_MSG(session, __wt_errno(),
			    "%s write error: failed to write %" WT_SIZET_FMT
			    " bytes at offset %" PRIuMAX,
			    fh->name, chunk, (uintmax_t)offset);
	}
#endif

#ifdef TDN_RID
	gettimeofday(&my_tv2, NULL);
	double time_ms = (my_tv2.tv_sec) * 1000 + (my_tv2.tv_usec) / 1000 ;
	time_ms = time_ms - my_start_time2;
	fprintf(my_fp_ssdm,"%f offset_type %d count1 %ld count2 %ld others %ld \n",
			time_ms, offset_type, count1, count2, count3);	
#endif //TDN_RID
	return (0);
}
