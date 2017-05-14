/*-
 * Copyright (c) 2014-2015 MongoDB, Inc.
 * Copyright (c) 2008-2014 WiredTiger, Inc.
 *	All rights reserved.
 *
 * See the file LICENSE for redistribution information.
 */

#include "wt_internal.h"

#if defined (SSDM_OP4) || defined (SSDM_OP5) || defined (SSDM_OP6) || defined (SSDM_OP7) || defined(SSDM_OP8) || defined(SSDM_OP8_2) || defined(SSDM_OP9) || defined (SSDM_OP10) || defined(SSDM_OP11)
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/ioctl.h> //for ioctl
#include <linux/fs.h> //for FIBMAP
#endif //SSDM_OP4
//include more map table
#if defined (SSDM_OP6) 
//#include "third_party/mssd/mssd.h"
//#include <third_party/wiredtiger/src/include/mssd.h>
#include "mssd.h"
extern MSSD_MAP* mssd_map;
extern off_t* retval;
extern FILE* my_fp6; 
#endif //SSDM_OP6

#if defined (SSDM_OP7)
#include "mssd.h"
extern off_t mssd_map[MSSD_MAX_FILE];
extern FILE* my_fp7; 
#endif //MSSD_OP7

#if defined (SSDM_OP8) || defined(SSDM_OP8_2) || defined(SSDM_OP11)
#include "mssd.h"
extern MSSD_MAP* mssd_map;
extern off_t* retval;
extern FILE* my_fp8; 
#endif //SSDM_OP8

#if defined (SSDM_OP9) 
#include "mssd.h"
extern MSSD_MAP* mssd_map;
extern off_t* retval;
extern FILE* my_fp9; 
#endif //SSDM_OP9

#if defined (SSDM_OP10)
#include "mssd.h"
extern FILE* my_fp10;
extern MSSD_MAP* mssd_map;
#endif //SSDM_OP10

#if defined (TDN_TRIM4) || defined(TDN_TRIM4_2) || defined(TDN_TRIM5) || defined(TDN_TRIM5_2)
#include "mytrim.h"
extern TRIM_MAP* trimmap;
extern FILE* my_fp4;
extern size_t my_trim_freq_config; //how often trim will call
#endif
/*
 * __open_directory --
 *	Open up a file handle to a directory.
 */
static int
__open_directory(WT_SESSION_IMPL *session, char *path, int *fd)
{
	WT_DECL_RET;

	WT_SYSCALL_RETRY(((*fd =
	    open(path, O_RDONLY, 0444)) == -1 ? 1 : 0), ret);
	if (ret != 0)
		WT_RET_MSG(session, ret, "%s: open_directory", path);
	return (ret);
}
#if defined(SSDM_OP4) || defined(SSDM_OP6) || defined(SSDM_OP7) || defined(SSDM_OP8) || defined(SSDM_OP8_2) || defined(SSDM_OP9) || defined (SSDM_OP10) || defined(SSDM_OP11)
off_t get_last_logical_file_offset(int fd){
	
	struct stat buf;                                                                                                                                                                                              
	int ret;

	ret = fstat(fd, &buf);
	if(ret < 0) {
		perror("fstat");
		return -1; 
	}   
	return buf.st_size;
}
off_t get_physical_file_offset (int fd) {
	struct stat buf;                                                                                                                                                                                              
	int ret;
	off_t offset;

	ret = fstat(fd, &buf);
	if(ret < 0) {
		perror("fstat");
		return -1; 
	}   
	offset = (buf.st_size - 4096) / 4096;
	//map logical offset to physical offset 
	ret = ioctl(fd, FIBMAP, &offset);
	if(ret != 0){ 
		perror("ioctl");
		return -1; 
	}   
	return offset;
}
	
#endif
/*
 * __wt_open --
 *	Open a file handle.
 */
int
__wt_open(WT_SESSION_IMPL *session,
    const char *name, bool ok_create, bool exclusive, int dio_type, WT_FH **fhp)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_FH *fh, *tfh;
	mode_t mode;
	uint64_t bucket, hash;
	int f, fd;
	bool direct_io, matched;
	char *path;
#if defined(SSDM_OP4) || defined(SSDM_OP5) || defined (SSDM_OP6) || defined (SSDM_OP7) || defined(SSDM_OP8) || defined(SSDM_OP8_2) || defined(SSDM_OP9) || defined (SSDM_OP10) || defined (SSDM_OP11)
	int my_ret;
	int stream_id;
#endif
	conn = S2C(session);
	direct_io = false;
	fh = NULL;
	fd = -1;
	path = NULL;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: open", name));

	/* Increment the reference count if we already have the file open. */
	matched = false;
	hash = __wt_hash_city64(name, strlen(name));
	bucket = hash % WT_HASH_ARRAY_SIZE;
	__wt_spin_lock(session, &conn->fh_lock);
	TAILQ_FOREACH(tfh, &conn->fhhash[bucket], hashq) {
		if (strcmp(name, tfh->name) == 0) {
			++tfh->ref;
			*fhp = tfh;
			matched = true;
			break;
		}
	}
	__wt_spin_unlock(session, &conn->fh_lock);
	if (matched)
		return (0);

	WT_RET(__wt_filename(session, name, &path));

	if (dio_type == WT_FILE_TYPE_DIRECTORY) {
		WT_ERR(__open_directory(session, path, &fd));
		goto setupfh;
	}

	f = O_RDWR;
#ifdef O_BINARY
	/* Windows clones: we always want to treat the file as a binary. */
	f |= O_BINARY;
#endif
#ifdef O_CLOEXEC
	/*
	 * Security:
	 * The application may spawn a new process, and we don't want another
	 * process to have access to our file handles.
	 */
	f |= O_CLOEXEC;
#endif
#ifdef O_NOATIME
	/* Avoid updating metadata for read-only workloads. */
	if (dio_type == WT_FILE_TYPE_DATA ||
	    dio_type == WT_FILE_TYPE_CHECKPOINT)
		f |= O_NOATIME;
#endif

	if (ok_create) {
		f |= O_CREAT;
		if (exclusive)
			f |= O_EXCL;
		mode = 0666;
	} else
		mode = 0;

#ifdef O_DIRECT
	if (dio_type && FLD_ISSET(conn->direct_io, dio_type)) {
		f |= O_DIRECT;
		direct_io = true;
	}
#endif
	if (dio_type == WT_FILE_TYPE_LOG &&
	    FLD_ISSET(conn->txn_logsync, WT_LOG_DSYNC))
#ifdef O_DSYNC
		f |= O_DSYNC;
#elif defined(O_SYNC)
		f |= O_SYNC;
#else
		WT_ERR_MSG(session, ENOTSUP,
		    "Unsupported log sync mode requested");
#endif
	WT_SYSCALL_RETRY(((fd = open(path, f, mode)) == -1 ? 1 : 0), ret);
	if (ret != 0)
		WT_ERR_MSG(session, ret,
		    direct_io ?
		    "%s: open failed with direct I/O configured, some "
		    "filesystem types do not support direct I/O" : "%s", path);

setupfh:
#if defined(HAVE_FCNTL) && defined(FD_CLOEXEC) && !defined(O_CLOEXEC)
	/*
	 * Security:
	 * The application may spawn a new process, and we don't want another
	 * process to have access to our file handles.  There's an obvious
	 * race here, so we prefer the flag to open if available.
	 */
	if ((f = fcntl(fd, F_GETFD)) == -1 ||
	    fcntl(fd, F_SETFD, f | FD_CLOEXEC) == -1)
		WT_ERR_MSG(session, __wt_errno(), "%s: fcntl", name);
#endif

#if defined(HAVE_POSIX_FADVISE)
	/* Disable read-ahead on trees: it slows down random read workloads. */
	if (dio_type == WT_FILE_TYPE_DATA ||
	    dio_type == WT_FILE_TYPE_CHECKPOINT)
		WT_ERR(posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM));
#endif

#if defined(SSDM_OP4)
	//Set default stream_id based on file types
	//Other subclass of SSDM_OP4 need to overwrite those value
	//Test print out last phisycal file offset
	
	off_t offs;
	offs = get_physical_file_offset(fd);

//if( strstr(name, "ycsb/collection") != 0){
	if((strstr(name, "collection") != 0) && (strstr(name, "local") == 0)){
		printf("==========> open collection name: %s, physical file offset=%jd\n", name, offs);
		stream_id = 3;
	}
	else if( (strstr(name, "index") != 0) && (strstr(name, "local") == 0)){
		printf("==========> open index name: %s, physical file offset=%jd\n", name, offs);
		stream_id = 5;
	}
	else if( strstr(name, "journal") != 0){
		stream_id = 2;
	}
	else {
		stream_id = 1;
	}
//		fprintf(stderr, "==========> assign file %s stream-id %d\n",name, stream_id);
//Call posix_fadvise to advise stream_id
	my_ret = posix_fadvise(fd, 0, stream_id, 8);	
	if(my_ret != 0){
		perror("posix_fadvise");	
	}
#endif

#if defined(SSDM_OP5)
	//One stream_id for journal file, oen stream id for others
	if( strstr(name, "journal") != 0){
		stream_id = 2;
	}
	else {
		stream_id = 1;
	}
//		fprintf(stderr, "==========> assign file %s stream-id %d\n",name, stream_id);
//Call posix_fadvise to advise stream_id
	my_ret = posix_fadvise(fd, 0, stream_id, 8);	
	if(my_ret != 0){
		perror("posix_fadvise");	
	}
#endif
#if defined(SSDM_OP6) 
	off_t offs;
	// others: 1, journal: 2, collection: 3~4, index: 5~6
	//Exclude collection files and index file in local directory 
	//Comment on 2016.11.23, check local may expensive 

	if ( strstr(name, "local") != 0 ) {
		//only seperate OPLOG collection, the name is always has "2" as prefix
		if( (strstr(name, "local/collection/2") != 0) ) { 
			stream_id = MSSD_OPLOG_SID;
		}
		else //other metadata files 
			stream_id = MSSD_OTHER_SID;
	}
	else if( strstr(name, "journal") != 0){
		stream_id = MSSD_JOURNAL_SID;
	}
	//work for both ycsb and linkbench
	else if( ((strstr(name, "collection") != 0) || (strstr(name, "index") != 0)) ) { 
	//if( ((strstr(name, "linkbench/collection") != 0) || (strstr(name, "linkbench/index") != 0)) ) { 
		if (strstr(name, "collection") != 0)
			stream_id = MSSD_COLL_INIT_SID;
		else
			stream_id = MSSD_IDX_INIT_SID; //index
		//comment on 2016.11.22 use logical offset instead of physical offset
		//offs = get_physical_file_offset(fd);
		offs = get_last_logical_file_offset(fd);

		//Register new (filename, offset) pair. If the pair is existed, no changes
		//achieve offset in retval 
		//my_ret = mssdmap_get_or_append(mssd_map, name, offs, stream_id, retval);
		my_ret = mssdmap_get_or_append(mssd_map, name, offs, retval);
		if (*retval)
			printf("my_ret =  %d retval= %jd, size= %d\n",my_ret, *retval, mssd_map->size);
		else
			printf("append [%s %jd], size=%d\n", name, offs, mssd_map->size);
	}
	else {
			//others
			stream_id = MSSD_OTHER_SID;
	}
//Call posix_fadvise to advise stream_id
	my_ret = posix_fadvise(fd, 0, stream_id, 8);	
//	printf("register file %s with stream-id %d\n", name, stream_id);
	fprintf(my_fp6,"register file %s with stream-id %d\n", name, stream_id);

	if(my_ret != 0){
		perror("posix_fadvise");	
	}
#endif //SSDM_OP6

#if defined(SSDM_OP7)
	off_t offs;
	// others: 1, journal: 2, collection: 3~4, index: 5~6
	//Exclude collection files and index file in local directory 
	//Comment on 2016.11.23, check local may expensive 

	if( ((strstr(name, "linkbench/collection") != 0) || (strstr(name, "linkbench/index") != 0)) ) { 
		//offs = get_physical_file_offset(fd);
		offs = get_last_logical_file_offset(fd);

		if (strstr(name, "collection") != 0){
			stream_id = 3;
			mssdmap_set(mssd_map, name, offs, true);
		}
		else {
			stream_id = 5; //index
			mssdmap_set(mssd_map, name, offs, false);
		}
	}
	else if( strstr(name, "journal") != 0){
		stream_id = MSSD_JOURNAL_SID;
	}
	else { //others
		//only seperate OPLOG collection, the name is always has "2" as prefix
		if( (strstr(name, "local/collection/2") != 0) ) { 
			stream_id = MSSD_OPLOG_SID;
		}
		else //other metadata files 
			stream_id = MSSD_OTHER_SID;
	}
//Call posix_fadvise to advise stream_id
	my_ret = posix_fadvise(fd, 0, stream_id, 8);	
	fprintf(my_fp7,"register file %s with stream-id %d\n", name, stream_id);

	if(my_ret != 0){
		perror("posix_fadvise");	
	}
#endif //SSDM_OP7

#if defined(SSDM_OP8) || defined(SSDM_OP8_2) || defined(SSDM_OP9) || defined (SSDM_OP11)
	off_t offs;
	// others: 1, journal: 2, collection: 3~4, index: 5~6
	//Exclude collection files and index file in local directory 
	//Comment on 2016.11.23, check local may expensive 

//	if( ((strstr(name, "collection") != 0) || (strstr(name, "index") != 0)) && 
//			(strstr(name, "local") == 0)){
	if ( strstr(name, "local") != 0) {
		//only seperate OPLOG collection, the name is always has "2" as prefix
		if( (strstr(name, "local/collection/2") != 0) ) { 
			stream_id = MSSD_OPLOG_SID;
		}
		else //other metadata files 
			stream_id = MSSD_OTHER_SID;

	}
	else if( strstr(name, "journal") != 0){
		stream_id = MSSD_JOURNAL_SID;
	}
	else if( ((strstr(name, "collection") != 0) || (strstr(name, "index") != 0)) ) { 
		if (strstr(name, "collection") != 0)
			stream_id = MSSD_COLL_INIT_SID;
		else
			stream_id = MSSD_IDX_INIT_SID; //index
		//comment on 2016.11.22 use logical offset instead of physical offset
		//offs = get_physical_file_offset(fd);
		offs = get_last_logical_file_offset(fd);

		//Register new (filename, offset) pair. If the pair is existed, no changes
		//achieve offset in retval 
		my_ret = mssdmap_get_or_append(mssd_map, name, offs, stream_id, retval);
		if (*retval)
			printf("my_ret =  %d retval= %jd, size= %d\n",my_ret, *retval, mssd_map->size);
		else
			printf("append [%s %jd], size=%d\n", name, offs, mssd_map->size);

	}
	else { //others
		stream_id = MSSD_OTHER_SID;
	}
//Call posix_fadvise to advise stream_id
	my_ret = posix_fadvise(fd, 0, stream_id, 8);	
	//printf("register file %s with stream-id %d\n", name, stream_id);
#if defined (SSDM_OP8) || defined(SSDM_OP8_2) || defined(SSDM_OP11)
	fprintf(my_fp8,"register file %s with stream-id %d\n", name, stream_id);
#endif
#if defined (SSDM_OP9)
	fprintf(my_fp9,"register file %s with stream-id %d\n", name, stream_id);
#endif

	if(my_ret != 0){
		perror("posix_fadvise");	
	}
#endif //SSDM_OP8

#if defined (SSDM_OP10)
	//ideal stream mapping, require # of stream id equal to # of files
	//Internal fracmentation may occur
	//REQUIRES: MSSD_OPLOG_SID + 9  streams (12)
	stream_id = 1;
	if ( strstr(name, "local") != 0) {
		if( (strstr(name, "local/collection/2") != 0) ) { 
			//OPLOG need to be seperated 
			stream_id = MSSD_OPLOG_SID;
		}
		else //other metadata files 
			stream_id = MSSD_OTHER_SID;
	}
	else if( strstr(name, "journal") != 0){
		stream_id = MSSD_JOURNAL_SID;
	}
	//if( ((strstr(name, "linkbench/collection") != 0) || (strstr(name, "linkbench/index") != 0)) ) { 
	else if( ((strstr(name, "collection") != 0) || (strstr(name, "index") != 0)) ) { 
		int id;
		off_t offs;

		offs = get_last_logical_file_offset(fd);
		id = mssdmap_find(mssd_map, name);
		if( id >= 0) {
			//file is already exist, do nothing
		}
		else {
#if defined(S840_PRO)
		//all colls: MSSD_COLL_INIT_SID, all indexes: MSSD_IDX_INIT_SID
		if (strstr(name, "collection") != 0)
			stream_id = MSSD_COLL_INIT_SID;
		else
			stream_id = MSSD_IDX_INIT_SID; //index
#else //Samsung PM953, use mssd_map
			//add new file and sid to the map
			stream_id = MSSD_OPLOG_SID + mssd_map->size + 1;
			mssdmap_append(mssd_map, name, offs, stream_id);
#endif
		}
	}
	else { //others
		stream_id = MSSD_OTHER_SID;
	}
//Call posix_fadvise to advise stream_id
	my_ret = posix_fadvise(fd, 0, stream_id, 8);	
	if (my_ret != 0){
		fprintf(my_fp10,"register file %s with stream-id %d\n", name, stream_id);
	}

#endif //SSDM_OP10

#if defined(TDN_TRIM4) || defined(TDN_TRIM4_2) || defined(TDN_TRIM5) || defined(TDN_TRIM5_2)
	//simple register object to trimmap
	if( ((strstr(name, "linkbench/collection") != 0) || (strstr(name, "linkbench/index") != 0)) ) { 
		//achieve offset in retval 
		
		printf("==> open %s fd %d\n", name, fd);
		trimmap_add(trimmap, fd, TRIM_INIT_THRESHOLD);
		printf("==> register object %s current size %d\n", name, trimmap->size);

	}
#endif //TDN_TRIM4

	WT_ERR(__wt_calloc_one(session, &fh));
	WT_ERR(__wt_strdup(session, name, &fh->name));
	fh->name_hash = hash;
	fh->fd = fd;
	fh->ref = 1;
	fh->direct_io = direct_io;

	/* Set the file's size. */
	WT_ERR(__wt_filesize(session, fh, &fh->size));

	/* Configure file extension. */
	if (dio_type == WT_FILE_TYPE_DATA ||
	    dio_type == WT_FILE_TYPE_CHECKPOINT)
		fh->extend_len = conn->data_extend_len;

	/* Configure fallocate/posix_fallocate calls. */
	__wt_fallocate_config(session, fh);

	/*
	 * Repeat the check for a match, but then link onto the database's list
	 * of files.
	 */
	matched = false;
	__wt_spin_lock(session, &conn->fh_lock);
	TAILQ_FOREACH(tfh, &conn->fhhash[bucket], hashq) {
		if (strcmp(name, tfh->name) == 0) {
			++tfh->ref;
			*fhp = tfh;
			matched = true;
			break;
		}
	}
	if (!matched) {
		WT_CONN_FILE_INSERT(conn, fh, bucket);
		(void)__wt_atomic_add32(&conn->open_file_count, 1);
		*fhp = fh;
	}
	__wt_spin_unlock(session, &conn->fh_lock);
	if (matched) {
err:		if (fh != NULL) {
			__wt_free(session, fh->name);
			__wt_free(session, fh);
		}
		if (fd != -1)
			(void)close(fd);
	}

	__wt_free(session, path);
	return (ret);
}

/*
 * __wt_close --
 *	Close a file handle.
 */
int
__wt_close(WT_SESSION_IMPL *session, WT_FH **fhp)
{
	WT_CONNECTION_IMPL *conn;
	WT_DECL_RET;
	WT_FH *fh;
	uint64_t bucket;

	conn = S2C(session);

	if (*fhp == NULL)
		return (0);
	fh = *fhp;
	*fhp = NULL;

	WT_RET(__wt_verbose(session, WT_VERB_FILEOPS, "%s: close", fh->name));

	__wt_spin_lock(session, &conn->fh_lock);
	if (fh == NULL || fh->ref == 0 || --fh->ref > 0) {
		__wt_spin_unlock(session, &conn->fh_lock);
		return (0);
	}

	/* Remove from the list. */
	bucket = fh->name_hash % WT_HASH_ARRAY_SIZE;
	WT_CONN_FILE_REMOVE(conn, fh, bucket);
	(void)__wt_atomic_sub32(&conn->open_file_count, 1);

	__wt_spin_unlock(session, &conn->fh_lock);

	/* Discard the memory. */
	if (close(fh->fd) != 0) {
		ret = __wt_errno();
		__wt_err(session, ret, "close: %s", fh->name);
	}

	__wt_free(session, fh->name);
	__wt_free(session, fh);
	return (ret);
}
