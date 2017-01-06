/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#if defined(SSDM_OP8)
#include "mssd.h"
extern FILE* my_fp8;
extern MSSD_MAP* mssd_map;
#endif //SSDM_OP8

#ifdef TDN_TRIM
	extern FILE* my_fp4;
#endif

#if defined(TDN_TRIM4) || defined(TDN_TRIM4_2) || defined(TDN_TRIM5) || defined (TDN_TRIM5_2)
#include "mytrim.h"

extern TRIM_MAP* trimmap;
extern off_t *my_starts_tem, *my_ends_tem;
extern FILE* my_fp4;
extern size_t my_trim_freq_config; //how often trim will call

extern pthread_t trim_tid;
extern pthread_mutex_t trim_mutex;
extern pthread_cond_t trim_cond;
extern bool my_is_trim_running;
#endif //TDN_TRIM4

#if defined(TDN_TRIM3) || defined(TDN_TRIM3_2) || defined(TDN_TRIM3_3)
#include <sys/ioctl.h> //for ioctl call
#include <linux/fs.h> //for fstrim_range
#include <string.h>
#include <errno.h>
extern FILE* my_fp4;
extern off_t *my_starts, *my_ends;
extern off_t *my_starts_tem, *my_ends_tem;
extern int32_t my_off_size;
extern size_t my_trim_freq_config; //how often trim will call
extern pthread_t trim_tid;
extern pthread_mutex_t trim_mutex;
extern pthread_cond_t trim_cond;
extern bool my_is_trim_running;
extern int my_fd; //fd of collection file
#endif

static int __ckpt_server_start(WT_CONNECTION_IMPL *);

/*
 * __ckpt_server_config --
 *	Parse and setup the checkpoint server options.
 */
static int
__ckpt_server_config(WT_SESSION_IMPL *session, const char **cfg, bool *startp)
{
	WT_CONFIG_ITEM cval;
	WT_CONNECTION_IMPL *conn;
	WT_DECL_ITEM(tmp);
	WT_DECL_RET;
	char *p;

	conn = S2C(session);

	/*
	 * The checkpoint configuration requires a wait time and/or a log
	 * size -- if one is not set, we're not running at all.
	 * Checkpoints based on log size also require logging be enabled.
	 */
	WT_RET(__wt_config_gets(session, cfg, "checkpoint.wait", &cval));
	conn->ckpt_usecs = (uint64_t)cval.val * WT_MILLION;

	WT_RET(__wt_config_gets(session, cfg, "checkpoint.log_size", &cval));
	conn->ckpt_logsize = (wt_off_t)cval.val;

	/* Checkpoints are incompatible with in-memory configuration */
	if (conn->ckpt_usecs != 0 || conn->ckpt_logsize != 0) {
		WT_RET(__wt_config_gets(session, cfg, "in_memory", &cval));
		if (cval.val != 0)
			WT_RET_MSG(session, EINVAL,
			    "In memory configuration incompatible with "
			    "checkpoints");
	}

	__wt_log_written_reset(session);
	if ((conn->ckpt_usecs == 0 && conn->ckpt_logsize == 0) ||
	    (conn->ckpt_logsize && conn->ckpt_usecs == 0 &&
	     !FLD_ISSET(conn->log_flags, WT_CONN_LOG_ENABLED))) {
		*startp = false;
		return (0);
	}
	*startp = true;

	/*
	 * The application can specify a checkpoint name, which we ignore if
	 * it's our default.
	 */
	WT_RET(__wt_config_gets(session, cfg, "checkpoint.name", &cval));
	if (cval.len != 0 &&
	    !WT_STRING_MATCH(WT_CHECKPOINT, cval.str, cval.len)) {
		WT_RET(__wt_checkpoint_name_ok(session, cval.str, cval.len));

		WT_RET(__wt_scr_alloc(session, cval.len + 20, &tmp));
		WT_ERR(__wt_buf_fmt(
		    session, tmp, "name=%.*s", (int)cval.len, cval.str));
		WT_ERR(__wt_strdup(session, tmp->data, &p));

		__wt_free(session, conn->ckpt_config);
		conn->ckpt_config = p;
	}

err:	__wt_scr_free(session, &tmp);
	return (ret);
}

#if defined(TDN_TRIM3) || defined(TDN_TRIM3_2) || defined(TDN_TRIM3_3) || defined(TDN_TRIM4) || defined(TDN_TRIM4_2) || defined(TDN_TRIM5) || defined (TDN_TRIM5_2)
/*
 * quicksort based on x array, move associate element in y arrays
 * x, y have the same length
 * */
static void quicksort(off_t* x, off_t* y,  int32_t first, int32_t last){
	int32_t pivot, i, j;
	off_t temp;

	if(first < last) {
		pivot = first;
		i = first;
		j = last;
		
		while(i < j){
			while(x[i] <= x[pivot] && i < last)
				i++;
			while(x[j] > x[pivot])
				j--;
			if(i < j){
				//swap in x
				temp = x[i];
				x[i] = x[j];
				x[j] = temp;
				//swap in y
				temp = y[i];
				y[i] = y[j];
				y[j] = temp;
			}
		}

		temp = x[pivot];
		x[pivot] = x[j];
		x[j] = temp;

		temp = y[pivot];
		y[pivot] = y[j];
		y[j] = temp;

		quicksort(x, y, first, j - 1);
		quicksort(x, y, j + 1, last);
	}
}
#endif
#if defined(TDN_TRIM4) || defined(TDN_TRIM4_2) || defined(TDN_TRIM5) || defined(TDN_TRIM5_2)

/*
 *1: sort the ranges in order
  2: merge overlap ranges
  3: call TRIM command for merged ranges
 * */
void __trim_sort_merge(TRIM_OBJ* obj, int32_t size){
	off_t cur_start, cur_end;
	struct fstrim_range range;

	int32_t i, myret;

	//copy offsets 
	memcpy(my_starts_tem, obj->starts, size * sizeof(off_t));
	memcpy(my_ends_tem, obj->ends, size * sizeof(off_t));
	//sort
	quicksort(my_starts_tem, my_ends_tem, 0, size - 1);
	//scan through ranges, try join overlap range then call trim
	cur_start = my_starts_tem[0];
	cur_end = my_ends_tem[0];

	//loop call TRIM command for each range
	for(i = 1; i < size; i++){
		if(cur_end < my_starts_tem[i]) {
			//non-overlap, trim the current range
			if ((cur_end - cur_start) <= 0){
				fprintf(my_fp4, "logical error cur_end <= cur_start\n");
				//skip trimming
			}
			else {
				range.len = cur_end - cur_start;
				range.start = cur_start;
				range.minlen = 4096; //at least 4KB
				myret = ioctl(obj->fd, FITRIM, &range);
				if(myret < 0){
					perror("ioctl");
					fprintf(my_fp4, 
							"call trim error ret %d errno %s range.start %llu range.len %llu range.minlen %llu\n",
							myret, strerror(errno), range.start, range.len, range.minlen);
				}	
			}
			cur_start = my_starts_tem[i];
			cur_end = my_ends_tem[i];
		}	
		else {
			//overlap case, join two range, keep the cur_start, 
			//extend the cur_end
			if(cur_end <= my_ends_tem[i]){
				cur_end = my_ends_tem[i]; //extend
			}
			else {
				//kept the same
			}
		}
	} //end for
}
/*
 *Simple trim pros and cons
Pros: eliminate overhead of memcpy, sort, merge ranges
cons: more ioctl() calls 
 * */
static void __trim_simple(TRIM_OBJ* obj, int32_t size) {
	
	struct fstrim_range range;

	int32_t i, myret;
	/*
	 *Since we use the single shared my_starts_tem and my_ends_tem, when the 
	 thread is call too quickly, the previous values may be overwritten by the
	 later call => don't use the tem buffer anymore 
	 * */	
	//memcpy(my_starts_tem, obj->starts, size * sizeof(off_t));
	//memcpy(my_ends_tem, obj->ends, size * sizeof(off_t));
	for(i = 0; i < size; i++){
		range.start = obj->starts[i];
		range.len = (obj->ends[i] - obj->starts[i]);
		range.minlen = 4096;
		myret = ioctl(obj->fd, FITRIM, &range);
		if(myret < 0){
			perror("ioctl");
			fprintf(my_fp4, 
					"call trim error ret %d errno %s range.start %llu range.len %llu range.minlen %llu\n",
					myret, strerror(errno), range.start, range.len, range.minlen);
		}	
	}//end for
}
/* 
 * Call trim for multiple ranges
 * fd: file description that trim will occur on
 * starts: array start offset
 * ends: array end offset
 * size: size of arrays, starts and ends have the same size
 * arg: fd that trim will occur on
 * */
static WT_THREAD_RET 
__trim_ranges(void* arg) {
	
	//off_t cur_start, cur_end;
	//struct fstrim_range range;

	//int32_t i, myret;
	int32_t size;
	TRIM_OBJ* obj;

	while (trimmap->oid == TRIM_INDEX_NOT_SET && my_is_trim_running) {
		//wait for pthread_cond_signal
		pthread_cond_wait(&trim_cond, &trim_mutex);
		// wait ...
	
		//when the process reach this line, trimmap->oid should != TRIM_INDEX_NOT_SET
		printf("TRIM thread is active, trimmap->oid=%d\n ", trimmap->oid);	
		//check again
		if(trimmap->oid < 0) continue;
		
		obj = trimmap->data[trimmap->oid];

		//obj->size may changed during this processs, take a snapshot here
	    size = obj->size;	
		if (obj->size == 0) continue;

		//signaled by other, now handle trim
	
		printf("inside TRIM handle thread, call __trim_ranges, size = %d, oid=%d\n", size, trimmap->oid);
		fprintf(my_fp4, "inside TRIM handle thread, call __trim_ranges, size = %d, oid=%d\n", size, trimmap->oid);
		//Choose between two options: 
		//1: trim with sort and merge 
		//2: simple trim ranges		
		
		//__trim_sort_merge(obj, size);	
		__trim_simple(obj, size);
		//reset

		trimmap->oid = TRIM_INDEX_NOT_SET;
		obj->size = 0; //reset

		//printf("=============> inside TRIM handle thread, reset, size = %d, oid=%d\n", size, trimmap->oid);
		//For large enough time interval, sleep some minutes to avoid unexpected thread bug
		//NOTICE: for multiple files, when the thread is sleeping, there may have another trigger
		//that make the trimmap->oid != TRIM_INDEX_NOT_SET => end WHILE
		//so we don't sleep anymore 
		if(size >= 10000){
			//sleep(500);
			//sleep(10);
		}

	} //end while
	printf("=============> inside TRIM handle thread, end WHILE \n");
	pthread_exit(NULL);
	return (WT_THREAD_RET_VALUE);
}	
#endif // if defined(TDN_TRIM4)

#if defined(TDN_TRIM3) || defined(TDN_TRIM3_2) || defined(TDN_TRIM3_3)
/* 
 * Call trim for multiple ranges
 * fd: file description that trim will occur on
 * starts: array start offset
 * ends: array end offset
 * size: size of arrays, starts and ends have the same size
 * arg: fd that trim will occur on
 * */
static WT_THREAD_RET 
__trim_ranges(void* arg) {
	
	off_t cur_start, cur_end;
	struct fstrim_range range;

	int32_t i, myret;
	int32_t size;
#if defined(TDN_TRIM3) || defined(TDN_TRIM3_2)
	while (my_off_size < (off_t)my_trim_freq_config &&
			my_is_trim_running) {
#elif TDN_TRIM3_3
	while (my_is_trim_running) {
#endif
		//wait for pthread_cond_signal
		pthread_cond_wait(&trim_cond, &trim_mutex);
		// wait ...
		
		//my_off_size may changed during this processs
		size = my_off_size;
		if (size == 0) continue;
		my_off_size = 0;

		//signaled by other, now handle trim
	
		printf("call __trim_ranges, size = %d\n", size);
		fprintf(my_fp4, "call __trim_ranges, size = %d\n", size);
		
		//copy offsets 
		memcpy(my_starts_tem, my_starts, size * sizeof(off_t));
		memcpy(my_ends_tem, my_ends, size * sizeof(off_t));
		//sort
		quicksort(my_starts_tem, my_ends_tem, 0, size - 1);
		//scan through ranges, try join overlap range then call trim
		cur_start = my_starts_tem[0];
		cur_end = my_ends_tem[0];

		for(i = 1; i < size; i++){
			if(cur_end < my_starts_tem[i]) {
				//non-overlap, trim the current range
				if ((cur_end - cur_start) <= 0){
					fprintf(my_fp4, "logical error cur_end <= cur_start\n");
					//skip trimming
				}
				else {
					range.len = cur_end - cur_start;
					range.start = cur_start;
					range.minlen = 4096; //at least 4KB
					myret = ioctl(my_fd, FITRIM, &range);
					if(myret < 0){
						perror("ioctl");
						fprintf(my_fp4, 
								"call trim error ret %d errno %s range.start %llu range.len %llu range.minlen %llu\n",
								myret, strerror(errno), range.start, range.len, range.minlen);
					}	
				}
				cur_start = my_starts_tem[i];
				cur_end = my_ends_tem[i];
			}	
			else {
				//overlap case, join two range, keep the cur_start, 
				//extend the cur_end
				if(cur_end <= my_ends_tem[i]){
					cur_end = my_ends_tem[i]; //extend
				}
				else {
					//kept the same
				}
			}
		}
		//For large enough time interval, sleep some minutes to avoid unexpected thread bug
#if defined(TDN_TRIM3) || defined(TDN_TRIM3_2)
		if(my_trim_freq_config >= 10000){
			//sleep(500);
			sleep(30);
		}
#endif
	}
	pthread_exit(NULL);
	return (WT_THREAD_RET_VALUE);
}	

#endif
/*
 * __ckpt_server --
 *	The checkpoint server thread.
 */
static WT_THREAD_RET
__ckpt_server(void *arg)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION *wt_session;
	WT_SESSION_IMPL *session;

#if defined(SSDM_OP8)
	int i;
	MSSD_PAIR* obj;
	double den1, den2, local_pct1, local_pct2, global_pct1, global_pct2;
	uint32_t coll_count1, coll_count2, idx_count1, idx_count2; //counts for global cpt computation
	coll_count1 = coll_count2 = idx_count1 = idx_count2 = 0;
#endif //SSDM_OP8
	session = arg;
	conn = S2C(session);
	wt_session = (WT_SESSION *)session;

	while (F_ISSET(conn, WT_CONN_SERVER_RUN) &&
	    F_ISSET(conn, WT_CONN_SERVER_CHECKPOINT)) {
		/*
		 * Wait...
		 * NOTE: If the user only configured logsize, then usecs
		 * will be 0 and this wait won't return until signalled.
		 */
		WT_ERR(
		    __wt_cond_wait(session, conn->ckpt_cond, conn->ckpt_usecs));

		printf("call __ckpt_server\n");
#if defined(SSDM_OP8)
//TODO: adjusting the stream id based on our algorithm
//write_density = num_write / (off_max - off_min) 

//compute the total write in a stream id
		for( i = 0; i < mssd_map->size; i++){
			obj = mssd_map->data[i];
			if(strstr(obj->fn, "collection") != 0){
				coll_count1 += obj->num_w1;
				coll_count2 += obj->num_w2;
			}
			else if(strstr(obj->fn, "index") != 0) {
				idx_count1 += obj->num_w1;	
				idx_count2 += obj->num_w2;	
			}
		}
//trace the number of write per 4KB page and range of write for each obj
		for( i = 0; i < mssd_map->size; i++){
			obj = mssd_map->data[i];
			den1 = (obj->off_max1 > obj->off_min1) ? ((obj->num_w1 * 4096.0) / (obj->off_max1 - obj->off_min1)) : (-1) ;
			den2 = (obj->off_max2 > obj->off_min2) ? ((obj->num_w2 * 4096.0) / (obj->off_max2 - obj->off_min2)) : (-1) ;
			local_pct1 = (obj->num_w1 * 1.0) / (obj->num_w1 + obj->num_w2) * 100;
			local_pct2 = (obj->num_w2 * 1.0) / (obj->num_w1 + obj->num_w2) * 100;
			if(strstr(obj->fn, "collection") != 0){
				global_pct1 = (obj->num_w1 * 1.0) / coll_count1 * 100;
				global_pct2 = (obj->num_w2 * 1.0) / coll_count2 * 100;
			}
			else if(strstr(obj->fn, "index") != 0) {
				global_pct1 = (obj->num_w1 * 1.0) / idx_count1 * 100;
				global_pct2 = (obj->num_w2 * 1.0) / idx_count2 * 100;
			}

#if defined(SSDM_OP8_DEBUG)
			printf("__ckpt_server name %s offset %jd num_w1 %"PRIu32" num_w2 %"PRIu32" off_min1 %jd off_max1 %jd off_min2 %jd off_max2 %jd cur_sid %d sid1 %d sid2 %d den1 %f den2 %f l_pct1 %f l_pct2 %f g_pct1 %f g_pct2 %f \n", obj->fn, obj->offset, obj->num_w1, obj->num_w2, obj->off_min1, obj->off_max1, obj->off_min2, obj->off_max2, obj->cur_sid, obj->sid1, obj->sid2, den1, den2, local_pct1, local_pct2, global_pct1, global_pct2);
			fprintf(my_fp8, "__ckpt_server name %s offset %jd num_w1 %"PRIu32" num_w2 %"PRIu32" off_min1 %jd off_max1 %jd off_min2 %jd off_max2 %jd cur_sid %d sid1 %d sid2 %d den1 %f den2 %f l_pct1 %f l_pct2 %f g_pct1 %f g_pct2 %f \n", obj->fn, obj->offset, obj->num_w1, obj->num_w2, obj->off_min1, obj->off_max1, obj->off_min2, obj->off_max2, obj->cur_sid, obj->sid1, obj->sid2, den1, den2, local_pct1, local_pct2, global_pct1, global_pct2);
#endif //SSDM_OP8_DEBUG
			//reset
			obj->num_w1 = obj->num_w2 = 0;
			obj->off_min1 = obj->off_min2 = 100 * obj->offset;
			obj->off_max1 = obj->off_max2 = 0;
		}//end for

#endif //SSDM_OP8

#if defined(TDN_TRIM) || defined(TDN_TRIM3) || defined(TDN_TRIM3_2) || defined(TDN_TRIM3_3)
		fprintf(my_fp4, "__ckpt_server call \n");
#endif
#if defined(TDN_TRIM5) || defined(TDN_TRIM5_2)
		//update obj->max_size for each obj in trimmap and reset obj->count
		printf("call trimmap_update_max_size\n");
		trimmap_update_max_size(trimmap);
		trimmap->oid = TRIM_INDEX_NOT_SET;
#endif
		/* Checkpoint the database. */
		WT_ERR(wt_session->checkpoint(wt_session, conn->ckpt_config));
		/* Reset. */
		if (conn->ckpt_logsize) {
			__wt_log_written_reset(session);
			conn->ckpt_signalled = 0;

			/*
			 * In case we crossed the log limit during the
			 * checkpoint and the condition variable was already
			 * signalled, do a tiny wait to clear it so we don't do
			 * another checkpoint immediately.
			 */
			WT_ERR(__wt_cond_wait(session, conn->ckpt_cond, 1));
		}
	}

	if (0) {
err:		WT_PANIC_MSG(session, ret, "checkpoint server error");
	}
	return (WT_THREAD_RET_VALUE);
}

/*
 * __ckpt_server_start --
 *	Start the checkpoint server thread.
 */
static int
__ckpt_server_start(WT_CONNECTION_IMPL *conn)
{
	WT_SESSION_IMPL *session;
	uint32_t session_flags;

	/* Nothing to do if the server is already running. */
	if (conn->ckpt_session != NULL)
		return (0);

	F_SET(conn, WT_CONN_SERVER_CHECKPOINT);

	/*
	 * The checkpoint server gets its own session.
	 *
	 * Checkpoint does enough I/O it may be called upon to perform slow
	 * operations for the block manager.
	 */
	session_flags = WT_SESSION_CAN_WAIT;
	WT_RET(__wt_open_internal_session(conn,
	    "checkpoint-server", true, session_flags, &conn->ckpt_session));
	session = conn->ckpt_session;

	WT_RET(__wt_cond_alloc(
	    session, "checkpoint server", false, &conn->ckpt_cond));

	/*
	 * Start the thread.
	 */
	WT_RET(__wt_thread_create(
	    session, &conn->ckpt_tid, __ckpt_server, session));
	conn->ckpt_tid_set = true;

#if defined(TDN_TRIM3) || defined(TDN_TRIM3_2) || defined(TDN_TRIM3_3) || defined(TDN_TRIM4) || defined(TDN_TRIM4_2) || defined(TDN_TRIM5) || defined(TDN_TRIM5_2)
	my_is_trim_running = true;
	WT_RET(pthread_create(&trim_tid, NULL, __trim_ranges, NULL));
	printf("========>>||||| create thread for TRIM command, trimmap size=%d \n", trimmap->size);
#endif

	return (0);
}

/*
 * __wt_checkpoint_server_create --
 *	Configure and start the checkpoint server.
 */
int
__wt_checkpoint_server_create(WT_SESSION_IMPL *session, const char *cfg[])
{
	WT_CONNECTION_IMPL *conn;
	bool start;

	conn = S2C(session);
	start = false;

	/* If there is already a server running, shut it down. */
	if (conn->ckpt_session != NULL)
		WT_RET(__wt_checkpoint_server_destroy(session));

	WT_RET(__ckpt_server_config(session, cfg, &start));
	if (start)
		WT_RET(__ckpt_server_start(conn));

	return (0);
}

/*
 * __wt_checkpoint_server_destroy --
 *	Destroy the checkpoint server thread.
 */
int
__wt_checkpoint_server_destroy(WT_SESSION_IMPL *session)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_SESSION *wt_session;

	conn = S2C(session);

	F_CLR(conn, WT_CONN_SERVER_CHECKPOINT);
	if (conn->ckpt_tid_set) {
		WT_TRET(__wt_cond_signal(session, conn->ckpt_cond));
		WT_TRET(__wt_thread_join(session, conn->ckpt_tid));
		conn->ckpt_tid_set = false;
	}
	WT_TRET(__wt_cond_destroy(session, &conn->ckpt_cond));

	__wt_free(session, conn->ckpt_config);

	/* Close the server thread's session. */
	if (conn->ckpt_session != NULL) {
		wt_session = &conn->ckpt_session->iface;
		WT_TRET(wt_session->close(wt_session, NULL));
	}

	/*
	 * Ensure checkpoint settings are cleared - so that reconfigure doesn't
	 * get confused.
	 */
	conn->ckpt_session = NULL;
	conn->ckpt_tid_set = false;
	conn->ckpt_cond = NULL;
	conn->ckpt_config = NULL;
	conn->ckpt_usecs = 0;

	return (ret);
}

/*
 * __wt_checkpoint_signal --
 *	Signal the checkpoint thread if sufficient log has been written.
 *	Return 1 if this signals the checkpoint thread, 0 otherwise.
 */
int
__wt_checkpoint_signal(WT_SESSION_IMPL *session, wt_off_t logsize)
{
	WT_CONNECTION_IMPL *conn;

	conn = S2C(session);
	WT_ASSERT(session, WT_CKPT_LOGSIZE(conn));
	if (logsize >= conn->ckpt_logsize && !conn->ckpt_signalled) {
		WT_RET(__wt_cond_signal(session, conn->ckpt_cond));
		conn->ckpt_signalled = 1;
	}
	return (0);
}
