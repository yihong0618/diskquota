/* -------------------------------------------------------------------------
 *
 * quotamodel.c
 *
 * This code is responsible for init disk quota model and refresh disk quota
 * model. Disk quota related Shared memory initialization is also implemented
 * in this file.
 *
 * Copyright (c) 2018-2020 Pivotal Software, Inc.
 * Copyright (c) 2020-Present VMware, Inc. or its affiliates
 *
 * IDENTIFICATION
 *		diskquota/quotamodel.c
 *
 * -------------------------------------------------------------------------
 */
#include "diskquota.h"
#include "gp_activetable.h"
#include "relation_cache.h"

#include "postgres.h"

#include "access/xact.h"
#include "catalog/pg_tablespace.h"
#include "commands/dbcommands.h"
#include "commands/tablespace.h"
#include "executor/spi.h"
#include "funcapi.h"
#include "storage/ipc.h"
#include "port/atomics.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/faultinjector.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "libpq-fe.h"

#include "cdb/cdbvars.h"
#include "cdb/cdbdisp_query.h"
#include "cdb/cdbdispatchresult.h"
#include "cdb/cdbutil.h"

#include <math.h>

/* cluster level max size of rejectmap */
#define MAX_DISK_QUOTA_REJECT_ENTRIES (1024 * 1024)
/* cluster level init size of rejectmap */
#define INIT_DISK_QUOTA_REJECT_ENTRIES 8192
/* per database level max size of rejectmap */
#define MAX_LOCAL_DISK_QUOTA_REJECT_ENTRIES 8192
#define MAX_NUM_KEYS_QUOTA_MAP 8
/* Number of attributes in quota configuration records. */
#define NUM_QUOTA_CONFIG_ATTRS 6
/* Number of entries for diskquota.table_size update SQL */
#define SQL_MAX_VALUES_NUMBER 1000000
/* Number of entries for hash table in quota_info */
#define MAX_QUOTA_MAP_ENTRIES (128 * 1024L)

/* TableSizeEntry macro function */
/* Use the top bit of totalsize as a flush flag. If this bit is set, the size should be flushed into
 * diskquota.table_size_table. */
#define TableSizeEntryFlushFlag (1ul << 63)
#define TableSizeEntrySizeMask (TableSizeEntryFlushFlag - 1)
#define TableSizeEntryId(segid) ((segid + 1) / SEGMENT_SIZE_ARRAY_LENGTH)
#define TableSizeEntryIndex(segid) ((segid + 1) % SEGMENT_SIZE_ARRAY_LENGTH)
#define TableSizeEntryGetFlushFlag(entry, segid) \
	(entry->totalsize[TableSizeEntryIndex(segid)] & TableSizeEntryFlushFlag)
#define TableSizeEntrySetFlushFlag(entry, segid) entry->totalsize[TableSizeEntryIndex(segid)] |= TableSizeEntryFlushFlag
#define TableSizeEntryResetFlushFlag(entry, segid) \
	entry->totalsize[TableSizeEntryIndex(segid)] &= TableSizeEntrySizeMask
#define TableSizeEntryGetSize(entry, segid) (entry->totalsize[TableSizeEntryIndex(segid)] & TableSizeEntrySizeMask)
#define TableSizeEntrySetSize(entry, segid, size) entry->totalsize[TableSizeEntryIndex(segid)] = size
#define TableSizeEntrySegidStart(entry) (entry->key.id * SEGMENT_SIZE_ARRAY_LENGTH - 1)
#define TableSizeEntrySegidEnd(entry)                                 \
	(((entry->key.id + 1) * SEGMENT_SIZE_ARRAY_LENGTH - 1) < SEGCOUNT \
	         ? ((entry->key.id + 1) * SEGMENT_SIZE_ARRAY_LENGTH - 1)  \
	         : SEGCOUNT)

typedef struct TableSizeEntry       TableSizeEntry;
typedef struct NamespaceSizeEntry   NamespaceSizeEntry;
typedef struct RoleSizeEntry        RoleSizeEntry;
typedef struct QuotaLimitEntry      QuotaLimitEntry;
typedef struct RejectMapEntry       RejectMapEntry;
typedef struct GlobalRejectMapEntry GlobalRejectMapEntry;
typedef struct LocalRejectMapEntry  LocalRejectMapEntry;

int                      SEGCOUNT = 0;
extern int               diskquota_max_table_segments;
extern pg_atomic_uint32 *diskquota_table_size_entry_num;

/*
 * local cache of table disk size and corresponding schema and owner.
 *
 * When id is 0, this TableSizeEntry stores the table size in the (-1 ~
 * SEGMENT_SIZE_ARRAY_LENGTH - 2)th segment, and so on.
 * |---------|--------------------------------------------------------------------------|
 * |   id    |                                segment index                             |
 * |---------|--------------------------------------------------------------------------|
 * |    0    |  [-1,                                SEGMENT_SIZE_ARRAY_LENGTH - 1)      |
 * |    1    |  [SEGMENT_SIZE_ARRAY_LENGTH - 1,     2 * SEGMENT_SIZE_ARRAY_LENGTH - 1)  |
 * |    2    |  [2 * SEGMENT_SIZE_ARRAY_LENGTH - 1, 3 * SEGMENT_SIZE_ARRAY_LENGTH - 1)  |
 * --------------------------------------------------------------------------------------
 *
 * flag's each bit is used to show the table's status, which is described in TableSizeEntryFlag.
 *
 * totalsize contains tables' size on segments. When id is 0, totalsize[0] is the sum of all segments' table size.
 * table size including fsm, visibility map etc.
 */
typedef struct TableSizeEntryKey
{
	Oid reloid;
	int id;
} TableSizeEntryKey;

struct TableSizeEntry
{
	TableSizeEntryKey key;
	Oid               tablespaceoid;
	Oid               namespaceoid;
	Oid               owneroid;
	uint32            flag;
	int64             totalsize[SEGMENT_SIZE_ARRAY_LENGTH];
};

typedef enum
{
	TABLE_EXIST = (1 << 0), /* whether table is already dropped */
} TableSizeEntryFlag;

/*
 * table disk size and corresponding schema and owner
 */
struct QuotaMapEntryKey
{
	Oid   keys[MAX_NUM_KEYS_QUOTA_MAP];
	int16 segid;
};

struct QuotaMapEntry
{
	Oid   keys[MAX_NUM_KEYS_QUOTA_MAP];
	int16 segid;
	int64 size;
	int64 limit;
};

struct QuotaInfo
{
	char        *map_name;
	unsigned int num_keys;
	Oid         *sys_cache;
	HTAB        *map;
};

struct QuotaInfo quota_info[NUM_QUOTA_TYPES] = {
        [NAMESPACE_QUOTA] = {.map_name  = "Namespace map",
                             .num_keys  = 1,
                             .sys_cache = (Oid[]){NAMESPACEOID},
                             .map       = NULL},
        [ROLE_QUOTA]      = {.map_name = "Role map", .num_keys = 1, .sys_cache = (Oid[]){AUTHOID}, .map = NULL},
        [NAMESPACE_TABLESPACE_QUOTA] = {.map_name  = "Tablespace-namespace map",
                                        .num_keys  = 2,
                                        .sys_cache = (Oid[]){NAMESPACEOID, TABLESPACEOID},
                                        .map       = NULL},
        [ROLE_TABLESPACE_QUOTA]      = {.map_name  = "Tablespace-role map",
                                        .num_keys  = 2,
                                        .sys_cache = (Oid[]){AUTHOID, TABLESPACEOID},
                                        .map       = NULL},
        [TABLESPACE_QUOTA]           = {
                          .map_name = "Tablespace map", .num_keys = 1, .sys_cache = (Oid[]){TABLESPACEOID}, .map = NULL}};

/* global rejectmap for which exceed their quota limit */
struct RejectMapEntry
{
	Oid    targetoid;
	Oid    databaseoid;
	Oid    tablespaceoid;
	uint32 targettype;
	/*
	 * TODO refactor this data structure
	 * QD index the rejectmap by (targetoid, databaseoid, tablespaceoid, targettype).
	 * QE index the rejectmap by (relfilenode).
	 */
	RelFileNode relfilenode;
};

struct GlobalRejectMapEntry
{
	RejectMapEntry keyitem;
	bool           segexceeded;
	/*
	 * When the quota limit is exceeded on segment servers,
	 * we need an extra auxiliary field to preserve the quota
	 * limitation information for error message on segment
	 * servers, e.g., targettype, targetoid. This field is
	 * useful on segment servers.
	 */
	RejectMapEntry auxblockinfo;
};

/* local rejectmap for which exceed their quota limit */
struct LocalRejectMapEntry
{
	RejectMapEntry keyitem;
	bool           isexceeded;
	bool           segexceeded;
};

/* using hash table to support incremental update the table size entry.*/
static HTAB *table_size_map = NULL;

/* rejectmap for database objects which exceed their quota limit */
static HTAB *disk_quota_reject_map       = NULL;
static HTAB *local_disk_quota_reject_map = NULL;

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

/* functions to maintain the quota maps */
static void update_size_for_quota(int64 size, QuotaType type, Oid *keys, int16 segid);
static void update_limit_for_quota(int64 limit, float segratio, QuotaType type, Oid *keys);
static void remove_quota(QuotaType type, Oid *keys, int16 segid);
static void add_quota_to_rejectmap(QuotaType type, Oid targetOid, Oid tablespaceoid, bool segexceeded);
static void check_quota_map(QuotaType type);
static void clear_all_quota_maps(void);
static void transfer_table_for_quota(int64 totalsize, QuotaType type, Oid *old_keys, Oid *new_keys, int16 segid);

/* functions to refresh disk quota model*/
static void refresh_disk_quota_usage(bool is_init);
static void calculate_table_disk_usage(bool is_init, HTAB *local_active_table_stat_map);
static void flush_to_table_size(void);
static bool flush_local_reject_map(void);
static void dispatch_rejectmap(HTAB *local_active_table_stat_map);
static bool load_quotas(void);
static void do_load_quotas(void);

static Size DiskQuotaShmemSize(void);
static void disk_quota_shmem_startup(void);
static void init_lwlocks(void);

static void export_exceeded_error(GlobalRejectMapEntry *entry, bool skip_name);
void        truncateStringInfo(StringInfo str, int nchars);
static void format_name(const char *prefix, uint32 id, StringInfo str);

static bool get_table_size_entry_flag(TableSizeEntry *entry, TableSizeEntryFlag flag);
static void reset_table_size_entry_flag(TableSizeEntry *entry, TableSizeEntryFlag flag);
static void set_table_size_entry_flag(TableSizeEntry *entry, TableSizeEntryFlag flag);

/* add a new entry quota or update the old entry quota */
static void
update_size_for_quota(int64 size, QuotaType type, Oid *keys, int16 segid)
{
	bool                    found;
	struct QuotaMapEntryKey key = {0};
	memcpy(key.keys, keys, quota_info[type].num_keys * sizeof(Oid));
	key.segid                   = segid;
	struct QuotaMapEntry *entry = hash_search(quota_info[type].map, &key, HASH_ENTER, &found);
	if (!found)
	{
		entry->size  = 0;
		entry->limit = -1;
		memcpy(entry->keys, keys, quota_info[type].num_keys * sizeof(Oid));
		entry->segid = key.segid;
	}
	entry->size += size;
}

/* add a new entry quota or update the old entry limit */
static void
update_limit_for_quota(int64 limit, float segratio, QuotaType type, Oid *keys)
{
	bool found;
	for (int i = -1; i < SEGCOUNT; i++)
	{
		struct QuotaMapEntryKey key = {0};
		memcpy(key.keys, keys, quota_info[type].num_keys * sizeof(Oid));
		key.segid                   = i;
		struct QuotaMapEntry *entry = hash_search(quota_info[type].map, &key, HASH_ENTER, &found);
		if (!found)
		{
			entry->size = 0;
			memcpy(entry->keys, keys, quota_info[type].num_keys * sizeof(Oid));
			entry->segid = key.segid;
		}
		if (key.segid == -1)
		{
			entry->limit = limit;
		}
		else
		{
			entry->limit = round((limit / SEGCOUNT) * segratio);
		}
	}
}

/* remove a entry quota from the map */
static void
remove_quota(QuotaType type, Oid *keys, int16 segid)
{
	struct QuotaMapEntryKey key = {0};
	memcpy(key.keys, keys, quota_info[type].num_keys * sizeof(Oid));
	key.segid = segid;
	hash_search(quota_info[type].map, &key, HASH_REMOVE, NULL);
}

/*
 * Compare the disk quota limit and current usage of a database object.
 * Put them into local rejectmap if quota limit is exceeded.
 */
static void
add_quota_to_rejectmap(QuotaType type, Oid targetOid, Oid tablespaceoid, bool segexceeded)
{
	LocalRejectMapEntry *localrejectentry;
	RejectMapEntry       keyitem = {0};

	keyitem.targetoid     = targetOid;
	keyitem.databaseoid   = MyDatabaseId;
	keyitem.tablespaceoid = tablespaceoid;
	keyitem.targettype    = (uint32)type;
	ereport(DEBUG1, (errmsg("[diskquota] Put object %u to rejectmap", targetOid)));
	localrejectentry = (LocalRejectMapEntry *)hash_search(local_disk_quota_reject_map, &keyitem, HASH_ENTER, NULL);
	localrejectentry->isexceeded  = true;
	localrejectentry->segexceeded = segexceeded;
}

/*
 * Check the quota map, if the entry doesn't exist anymore,
 * remove it from the map. Otherwise, check if it has hit
 * the quota limit, if it does, add it to the rejectmap.
 */
static void
check_quota_map(QuotaType type)
{
	HeapTuple             tuple;
	HASH_SEQ_STATUS       iter;
	struct QuotaMapEntry *entry;

	hash_seq_init(&iter, quota_info[type].map);

	while ((entry = hash_seq_search(&iter)) != NULL)
	{
		bool removed = false;
		for (int i = 0; i < quota_info[type].num_keys; ++i)
		{
			tuple = SearchSysCache1(quota_info[type].sys_cache[i], ObjectIdGetDatum(entry->keys[i]));
			if (!HeapTupleIsValid(tuple))
			{
				remove_quota(type, entry->keys, entry->segid);
				removed = true;
				break;
			}
			ReleaseSysCache(tuple);
		}
		if (!removed && entry->limit > 0)
		{
			if (entry->size >= entry->limit)
			{
				Oid targetOid = entry->keys[0];
				/* when quota type is not NAMESPACE_TABLESPACE_QUOTA or ROLE_TABLESPACE_QUOTA, the tablespaceoid
				 * is set to be InvalidOid, so when we get it from map, also set it to be InvalidOid
				 */
				Oid tablespaceoid = (type == NAMESPACE_TABLESPACE_QUOTA) || (type == ROLE_TABLESPACE_QUOTA)
				                            ? entry->keys[1]
				                            : InvalidOid;

				bool segmentExceeded = entry->segid == -1 ? false : true;
				add_quota_to_rejectmap(type, targetOid, tablespaceoid, segmentExceeded);
			}
		}
	}
}

/* transfer one table's size from one quota to another quota */
static void
transfer_table_for_quota(int64 totalsize, QuotaType type, Oid *old_keys, Oid *new_keys, int16 segid)
{
	update_size_for_quota(-totalsize, type, old_keys, segid);
	update_size_for_quota(totalsize, type, new_keys, segid);
}

static void
clear_all_quota_maps(void)
{
	for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
	{
		HASH_SEQ_STATUS iter = {0};
		hash_seq_init(&iter, quota_info[type].map);
		struct QuotaMapEntry *entry = NULL;
		while ((entry = hash_seq_search(&iter)) != NULL)
		{
			entry->limit = -1;
		}
	}
}

/* ---- Functions for disk quota shared memory ---- */
/*
 * DiskQuotaShmemInit
 *		Allocate and initialize diskquota-related shared memory
 *		This function is called in _PG_init().
 */
void
init_disk_quota_shmem(void)
{
	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgss_shmem_startup().
	 */
	RequestAddinShmemSpace(DiskQuotaShmemSize());
	/* locks for diskquota refer to init_lwlocks() for details */
#if GP_VERSION_NUM < 70000
	RequestAddinLWLocks(DiskQuotaLocksItemNumber);
#else
	RequestNamedLWLockTranche("DiskquotaLocks", DiskQuotaLocksItemNumber);
#endif /* GP_VERSION_NUM */

	/* Install startup hook to initialize our shared memory. */
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook      = disk_quota_shmem_startup;
}

/*
 * DiskQuotaShmemInit hooks.
 * Initialize shared memory data and locks.
 */
static void
disk_quota_shmem_startup(void)
{
	bool    found;
	HASHCTL hash_ctl;

	if (prev_shmem_startup_hook) (*prev_shmem_startup_hook)();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	init_lwlocks();

	/*
	 * Four shared memory data. extension_ddl_message is used to handle
	 * diskquota extension create/drop command. disk_quota_reject_map is used
	 * to store out-of-quota rejectmap. active_tables_map is used to store
	 * active tables whose disk usage is changed.
	 */
	extension_ddl_message = ShmemInitStruct("disk_quota_extension_ddl_message", sizeof(ExtensionDDLMessage), &found);
	if (!found) memset((void *)extension_ddl_message, 0, sizeof(ExtensionDDLMessage));

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize   = sizeof(RejectMapEntry);
	hash_ctl.entrysize = sizeof(GlobalRejectMapEntry);
	disk_quota_reject_map =
	        DiskquotaShmemInitHash("rejectmap whose quota limitation is reached", INIT_DISK_QUOTA_REJECT_ENTRIES,
	                               MAX_DISK_QUOTA_REJECT_ENTRIES, &hash_ctl, HASH_ELEM, DISKQUOTA_TAG_HASH);

	init_shm_worker_active_tables();

	init_shm_worker_relation_cache();

	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize   = sizeof(Oid);
	hash_ctl.entrysize = sizeof(struct MonitorDBEntryStruct);

	monitored_dbid_cache = DiskquotaShmemInitHash("table oid cache which shoud tracking", MAX_NUM_MONITORED_DB,
	                                              MAX_NUM_MONITORED_DB, &hash_ctl, HASH_ELEM, DISKQUOTA_OID_HASH);
	init_launcher_shmem();
	LWLockRelease(AddinShmemInitLock);
}

/*
 * Initialize four shared memory locks.
 * active_table_lock is used to access active table map.
 * reject_map_lock is used to access out-of-quota rejectmap.
 * extension_ddl_message_lock is used to access content of
 * extension_ddl_message.
 * extension_ddl_lock is used to avoid concurrent diskquota
 * extension ddl(create/drop) command.
 * monitored_dbid_cache_lock is used to shared `monitored_dbid_cache` on segment process.
 */
static void
init_lwlocks(void)
{
#if GP_VERSION_NUM < 70000
	diskquota_locks.active_table_lock          = LWLockAssign();
	diskquota_locks.reject_map_lock            = LWLockAssign();
	diskquota_locks.extension_ddl_message_lock = LWLockAssign();
	diskquota_locks.extension_ddl_lock         = LWLockAssign();
	diskquota_locks.monitored_dbid_cache_lock  = LWLockAssign();
	diskquota_locks.relation_cache_lock        = LWLockAssign();
	diskquota_locks.dblist_lock                = LWLockAssign();
	diskquota_locks.workerlist_lock            = LWLockAssign();
	diskquota_locks.altered_reloid_cache_lock  = LWLockAssign();
#else
	LWLockPadded *lock_base                    = GetNamedLWLockTranche("DiskquotaLocks");
	diskquota_locks.active_table_lock          = &lock_base[0].lock;
	diskquota_locks.reject_map_lock            = &lock_base[1].lock;
	diskquota_locks.extension_ddl_message_lock = &lock_base[2].lock;
	diskquota_locks.extension_ddl_lock         = &lock_base[3].lock;
	diskquota_locks.monitored_dbid_cache_lock  = &lock_base[4].lock;
	diskquota_locks.relation_cache_lock        = &lock_base[5].lock;
	diskquota_locks.dblist_lock                = &lock_base[6].lock;
	diskquota_locks.workerlist_lock            = &lock_base[7].lock;
	diskquota_locks.altered_reloid_cache_lock  = &lock_base[8].lock;
#endif /* GP_VERSION_NUM */
}

static Size
diskquota_worker_shmem_size()
{
	Size size;
	size = hash_estimate_size(MAX_NUM_TABLE_SIZE_ENTRIES / MAX_NUM_MONITORED_DB + 100, sizeof(TableSizeEntry));
	size = add_size(size, hash_estimate_size(MAX_LOCAL_DISK_QUOTA_REJECT_ENTRIES, sizeof(LocalRejectMapEntry)));
	size = add_size(size, hash_estimate_size(MAX_QUOTA_MAP_ENTRIES * NUM_QUOTA_TYPES, sizeof(struct QuotaMapEntry)));
	return size;
}

/*
 * DiskQuotaShmemSize
 * Compute space needed for diskquota-related shared memory
 */
static Size
DiskQuotaShmemSize(void)
{
	Size size;
	size = sizeof(ExtensionDDLMessage);
	size = add_size(size, hash_estimate_size(MAX_DISK_QUOTA_REJECT_ENTRIES, sizeof(GlobalRejectMapEntry)));
	size = add_size(size, hash_estimate_size(diskquota_max_active_tables, sizeof(DiskQuotaActiveTableEntry)));
	size = add_size(size, hash_estimate_size(diskquota_max_active_tables, sizeof(DiskQuotaRelationCacheEntry)));
	size = add_size(size, hash_estimate_size(diskquota_max_active_tables, sizeof(DiskQuotaRelidCacheEntry)));
	size = add_size(size, hash_estimate_size(diskquota_max_active_tables, sizeof(Oid)));
	size = add_size(size, hash_estimate_size(MAX_NUM_MONITORED_DB,
	                                         sizeof(struct MonitorDBEntryStruct))); // monitored_dbid_cache

	if (IS_QUERY_DISPATCHER())
	{
		size = add_size(size, diskquota_launcher_shmem_size());
		size = add_size(size, sizeof(pg_atomic_uint32));
		size = add_size(size, diskquota_worker_shmem_size() * MAX_NUM_MONITORED_DB);
	}

	return size;
}

/* ---- Functions for disk quota model ---- */
/*
 * Init disk quota model when the worker process firstly started.
 */
void
init_disk_quota_model(uint32 id)
{
	HASHCTL        hash_ctl;
	StringInfoData str;
	initStringInfo(&str);

	format_name("TableSizeEntrymap", id, &str);
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize   = sizeof(TableSizeEntryKey);
	hash_ctl.entrysize = sizeof(TableSizeEntry);
	table_size_map     = DiskquotaShmemInitHash(str.data, INIT_NUM_TABLE_SIZE_ENTRIES, MAX_NUM_TABLE_SIZE_ENTRIES,
	                                            &hash_ctl, HASH_ELEM, DISKQUOTA_TAG_HASH);

	/* for localrejectmap */
	/* WARNNING: The max length of name of the map is 48 */
	format_name("localrejectmap", id, &str);
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize   = sizeof(RejectMapEntry);
	hash_ctl.entrysize = sizeof(LocalRejectMapEntry);
	local_disk_quota_reject_map =
	        DiskquotaShmemInitHash(str.data, MAX_LOCAL_DISK_QUOTA_REJECT_ENTRIES, MAX_LOCAL_DISK_QUOTA_REJECT_ENTRIES,
	                               &hash_ctl, HASH_ELEM, DISKQUOTA_TAG_HASH);

	/* for quota_info */

	for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
	{
		format_name(quota_info[type].map_name, id, &str);
		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.entrysize   = sizeof(struct QuotaMapEntry);
		hash_ctl.keysize     = sizeof(struct QuotaMapEntryKey);
		quota_info[type].map = DiskquotaShmemInitHash(str.data, 1024L, MAX_QUOTA_MAP_ENTRIES, &hash_ctl, HASH_ELEM,
		                                              DISKQUOTA_TAG_HASH);
	}
	pfree(str.data);
}

/*
 * Reset the shared memory of diskquota worker
 *
 * Suppose a user first drops diskquota extension, then recreates it in
 * the same database, as diskquota worker will get the same memory address
 * as before.
 *
 * As the shared memory can not be recycled, so we just clean up the shared
 * memory when dropping the extension.
 * - memset diskquotaDBStatus to 0
 * - clean all items in the maps
 */
void
vacuum_disk_quota_model(uint32 id)
{
	HASH_SEQ_STATUS       iter;
	TableSizeEntry       *tsentry = NULL;
	LocalRejectMapEntry  *localrejectentry;
	struct QuotaMapEntry *qentry;

	HASHCTL        hash_ctl;
	StringInfoData str;
	initStringInfo(&str);

	/* table_size_map */
	format_name("TableSizeEntrymap", id, &str);
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize   = sizeof(TableSizeEntryKey);
	hash_ctl.entrysize = sizeof(TableSizeEntry);
	table_size_map     = DiskquotaShmemInitHash(str.data, INIT_NUM_TABLE_SIZE_ENTRIES, MAX_NUM_TABLE_SIZE_ENTRIES,
	                                            &hash_ctl, HASH_ELEM, DISKQUOTA_TAG_HASH);
	hash_seq_init(&iter, table_size_map);
	while ((tsentry = hash_seq_search(&iter)) != NULL)
	{
		hash_search(table_size_map, &tsentry->key, HASH_REMOVE, NULL);
		pg_atomic_fetch_sub_u32(diskquota_table_size_entry_num, 1);
	}

	/* localrejectmap */
	format_name("localrejectmap", id, &str);
	memset(&hash_ctl, 0, sizeof(hash_ctl));
	hash_ctl.keysize   = sizeof(RejectMapEntry);
	hash_ctl.entrysize = sizeof(LocalRejectMapEntry);
	local_disk_quota_reject_map =
	        DiskquotaShmemInitHash(str.data, MAX_LOCAL_DISK_QUOTA_REJECT_ENTRIES, MAX_LOCAL_DISK_QUOTA_REJECT_ENTRIES,
	                               &hash_ctl, HASH_ELEM, DISKQUOTA_TAG_HASH);
	hash_seq_init(&iter, local_disk_quota_reject_map);
	while ((localrejectentry = hash_seq_search(&iter)) != NULL)
	{
		hash_search(local_disk_quota_reject_map, &localrejectentry->keyitem, HASH_REMOVE, NULL);
	}

	/* quota_info */

	for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
	{
		format_name(quota_info[type].map_name, id, &str);
		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.entrysize   = sizeof(struct QuotaMapEntry);
		hash_ctl.keysize     = sizeof(struct QuotaMapEntryKey);
		quota_info[type].map = DiskquotaShmemInitHash(str.data, 1024L, MAX_QUOTA_MAP_ENTRIES, &hash_ctl, HASH_ELEM,
		                                              DISKQUOTA_TAG_HASH);
		hash_seq_init(&iter, quota_info[type].map);
		while ((qentry = hash_seq_search(&iter)) != NULL)
		{
			hash_search(quota_info[type].map, &qentry->keys, HASH_REMOVE, NULL);
		}
	}
	pfree(str.data);
}

/*
 * Check whether the diskquota state is ready
 */
bool
check_diskquota_state_is_ready()
{
	bool is_ready           = false;
	bool connected          = false;
	bool pushed_active_snap = false;
	bool ret                = true;

	StartTransactionCommand();

	/*
	 * Cache Errors during SPI functions, for example a segment may be down
	 * and current SPI execute will fail. diskquota worker process should
	 * tolerate this kind of errors and continue to check at the next loop.
	 */
	PG_TRY();
	{
		if (SPI_OK_CONNECT != SPI_connect())
		{
			ereport(ERROR,
			        (errcode(ERRCODE_INTERNAL_ERROR), errmsg("[diskquota] unable to connect to execute SPI query")));
		}
		connected = true;
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_active_snap = true;
		is_ready           = do_check_diskquota_state_is_ready();
	}
	PG_CATCH();
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();
		EmitErrorReport();
		FlushErrorState();
		ret = false;
		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();
	if (connected) SPI_finish();
	if (pushed_active_snap) PopActiveSnapshot();
	if (ret)
		CommitTransactionCommand();
	else
		AbortCurrentTransaction();
	return is_ready;
}

/*
 * Check whether the diskquota state is ready.
 * Throw an error or return false if it is not.
 *
 * For empty database, table diskquota.state would be ready after
 * 'CREATE EXTENSION diskquota;'. But for non-empty database,
 * user need to run UDF diskquota.init_table_size_table()
 * manually to get all the table size information and
 * store them into table diskquota.table_size
 */
bool
do_check_diskquota_state_is_ready(void)
{
	int       ret;
	TupleDesc tupdesc;
	ret = SPI_execute("select state from diskquota.state", true, 0);
	ereportif(ret != SPI_OK_SELECT, ERROR,
	          (errcode(ERRCODE_INTERNAL_ERROR),
	           errmsg("[diskquota] check diskquota state SPI_execute failed: error code %d", ret)));

	tupdesc = SPI_tuptable->tupdesc;
#if GP_VERSION_NUM < 70000
	if (SPI_processed != 1 || tupdesc->natts != 1 || ((tupdesc)->attrs[0])->atttypid != INT4OID)
#else
	if (SPI_processed != 1 || tupdesc->natts != 1 || ((tupdesc)->attrs[0]).atttypid != INT4OID)
#endif /* GP_VERSION_NUM */
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
		                errmsg("[diskquota] \"diskquota.state\" is corrupted in database \"%s\","
		                       " please recreate diskquota extension",
		                       get_database_name(MyDatabaseId))));
	}

	HeapTuple tup = SPI_tuptable->vals[0];
	Datum     dat;
	int       state;
	bool      isnull;

	dat           = SPI_getbinval(tup, tupdesc, 1, &isnull);
	state         = isnull ? DISKQUOTA_UNKNOWN_STATE : DatumGetInt32(dat);
	bool is_ready = state == DISKQUOTA_READY_STATE;

	if (!is_ready && !diskquota_is_readiness_logged())
	{
		diskquota_set_readiness_logged();
		ereport(WARNING, (errmsg("[diskquota] diskquota is not ready"),
		                  errhint("please run 'SELECT diskquota.init_table_size_table();' to initialize diskquota")));
	}
	return is_ready;
}

/*
 * Diskquota worker will refresh disk quota model
 * periodically. It will reload quota setting and
 * recalculate the changed disk usage.
 */
void
refresh_disk_quota_model(bool is_init)
{
	SEGCOUNT = getgpsegmentCount();
	if (SEGCOUNT <= 0)
	{
		ereport(ERROR, (errmsg("[diskquota] there is no active segment, SEGCOUNT is %d", SEGCOUNT)));
	}

	if (is_init) ereport(LOG, (errmsg("[diskquota] initialize quota model started")));
	/* skip refresh model when load_quotas failed */
	if (load_quotas())
	{
		refresh_disk_quota_usage(is_init);
	}
	if (is_init) ereport(LOG, (errmsg("[diskquota] initialize quota model finished")));
}

/*
 * Update the disk usage of namespace, role and tablespace.
 * Put the exceeded namespace and role into shared reject map.
 * Parameter 'is_init' is true when it's the first time that worker
 * process is constructing quota model.
 */
static void
refresh_disk_quota_usage(bool is_init)
{
	bool  connected                   = false;
	bool  pushed_active_snap          = false;
	bool  ret                         = true;
	HTAB *local_active_table_stat_map = NULL;

	StartTransactionCommand();

	/*
	 * Cache Errors during SPI functions, for example a segment may be down
	 * and current SPI execute will fail. diskquota worker process should
	 * tolerate this kind of errors and continue to check at the next loop.
	 */
	PG_TRY();
	{
		if (SPI_OK_CONNECT != SPI_connect())
		{
			ereport(ERROR,
			        (errcode(ERRCODE_INTERNAL_ERROR), errmsg("[diskquota] unable to connect to execute SPI query")));
		}
		connected = true;
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_active_snap = true;
		/*
		 * initialization stage all the tables are active. later loop, only the
		 * tables whose disk size changed will be treated as active
		 *
		 * local_active_table_stat_map only contains the active tables which belong
		 * to the current database.
		 */
		local_active_table_stat_map = gp_fetch_active_tables(is_init);
		bool hasActiveTable         = (hash_get_num_entries(local_active_table_stat_map) != 0);
		/* TODO: if we can skip the following steps when there is no active table */
		/* recalculate the disk usage of table, schema and role */
		calculate_table_disk_usage(is_init, local_active_table_stat_map);
		for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
		{
			check_quota_map(type);
		}
		/* flush local table_size_map to user table table_size */
		flush_to_table_size();
		/* copy local reject map back to shared reject map */
		bool reject_map_changed = flush_local_reject_map();
		/*
		 * Dispatch rejectmap entries to segments to perform hard-limit.
		 * If the bgworker is in init mode, the rejectmap should be refreshed anyway.
		 * Otherwise, only when the rejectmap is changed or the active_table_list is
		 * not empty the rejectmap should be dispatched to segments.
		 */
		if (is_init || (diskquota_hardlimit && (reject_map_changed || hasActiveTable)))
			dispatch_rejectmap(local_active_table_stat_map);
		hash_destroy(local_active_table_stat_map);
	}
	PG_CATCH();
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();
		EmitErrorReport();
		FlushErrorState();
		ret = false;
		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();
	if (connected) SPI_finish();
	if (pushed_active_snap) PopActiveSnapshot();
	if (ret)
		CommitTransactionCommand();
	else
		AbortCurrentTransaction();

	return;
}

static List *
merge_uncommitted_table_to_oidlist(List *oidlist)
{
	HASH_SEQ_STATUS              iter;
	DiskQuotaRelationCacheEntry *entry;

	if (relation_cache == NULL)
	{
		return oidlist;
	}

	remove_committed_relation_from_cache();

	LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
	hash_seq_init(&iter, relation_cache);
	while ((entry = hash_seq_search(&iter)) != NULL)
	{
		/* The session of db1 should not see the table inside db2. */
		if (entry->primary_table_relid == entry->relid && entry->rnode.node.dbNode == MyDatabaseId)
		{
			oidlist = lappend_oid(oidlist, entry->relid);
		}
	}
	LWLockRelease(diskquota_locks.relation_cache_lock);

	return oidlist;
}

/*
 *  Incremental way to update the disk quota of every database objects
 *  Recalculate the table's disk usage when it's a new table or active table.
 *  Detect the removed table if it's no longer in pg_class.
 *  If change happens, no matter size change or owner change,
 *  update namespace_size_map and role_size_map correspondingly.
 *  Parameter 'is_init' set to true at initialization stage to fetch tables
 *  size from table table_size
 */

static void
calculate_table_disk_usage(bool is_init, HTAB *local_active_table_stat_map)
{
	bool                       table_size_map_found;
	bool                       active_tbl_found;
	int64                      updated_total_size;
	TableSizeEntry            *tsentry = NULL;
	Oid                        relOid;
	HASH_SEQ_STATUS            iter;
	DiskQuotaActiveTableEntry *active_table_entry;
	TableSizeEntryKey          key;
	TableEntryKey              active_table_key;
	List                      *oidlist;
	ListCell                  *l;

	/*
	 * unset is_exist flag for tsentry in table_size_map this is used to
	 * detect tables which have been dropped.
	 */
	hash_seq_init(&iter, table_size_map);
	while ((tsentry = hash_seq_search(&iter)) != NULL)
	{
		reset_table_size_entry_flag(tsentry, TABLE_EXIST);
	}

	/*
	 * scan pg_class to detect table event: drop, reset schema, reset owner.
	 * calculate the file size for active table and update namespace_size_map
	 * and role_size_map
	 */
	oidlist = get_rel_oid_list();

	oidlist = merge_uncommitted_table_to_oidlist(oidlist);

	foreach (l, oidlist)
	{
		HeapTuple     classTup;
		Form_pg_class classForm     = NULL;
		Oid           relnamespace  = InvalidOid;
		Oid           relowner      = InvalidOid;
		Oid           reltablespace = InvalidOid;
		relOid                      = lfirst_oid(l);

		classTup = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(relOid));
		if (HeapTupleIsValid(classTup))
		{
			classForm     = (Form_pg_class)GETSTRUCT(classTup);
			relnamespace  = classForm->relnamespace;
			relowner      = classForm->relowner;
			reltablespace = classForm->reltablespace;

			if (!OidIsValid(reltablespace))
			{
				reltablespace = MyDatabaseTableSpace;
			}
		}
		else
		{
			LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
			DiskQuotaRelationCacheEntry *relation_entry = hash_search(relation_cache, &relOid, HASH_FIND, NULL);
			if (relation_entry == NULL)
			{
				elog(WARNING, "cache lookup failed for relation %u", relOid);
				LWLockRelease(diskquota_locks.relation_cache_lock);
				continue;
			}
			relnamespace  = relation_entry->namespaceoid;
			relowner      = relation_entry->owneroid;
			reltablespace = relation_entry->rnode.node.spcNode;
			LWLockRelease(diskquota_locks.relation_cache_lock);
		}

		/*
		 * The segid is the same as the content id in gp_segment_configuration
		 * and the content id is continuous, so it's safe to use SEGCOUNT
		 * to get segid.
		 */
		for (int cur_segid = -1; cur_segid < SEGCOUNT; cur_segid++)
		{
			key.reloid = relOid;
			key.id     = TableSizeEntryId(cur_segid);

			uint32 counter = pg_atomic_read_u32(diskquota_table_size_entry_num);
			if (counter > MAX_NUM_TABLE_SIZE_ENTRIES)
			{
				tsentry = (TableSizeEntry *)hash_search(table_size_map, &key, HASH_FIND, &table_size_map_found);
				/* Too many tables have been added to the table_size_map, to avoid diskquota using
				   too much share memory, just quit the loop. The diskquota won't work correctly
				   anymore. */
				if (!table_size_map_found)
				{
					break;
				}
			}
			else
			{
				tsentry = (TableSizeEntry *)hash_search(table_size_map, &key, HASH_ENTER, &table_size_map_found);

				if (!table_size_map_found)
				{
					counter = pg_atomic_add_fetch_u32(diskquota_table_size_entry_num, 1);
					if (counter > MAX_NUM_TABLE_SIZE_ENTRIES)
					{
						ereport(WARNING, (errmsg("[diskquota] the number of tables exceeds the limit, please increase "
						                         "the GUC value for diskquota.max_table_segments. Current "
						                         "diskquota.max_table_segments value: %d",
						                         diskquota_max_table_segments)));
					}
					tsentry->key.reloid = relOid;
					tsentry->key.id     = key.id;
					Assert(TableSizeEntrySegidStart(tsentry) == cur_segid);
					memset(tsentry->totalsize, 0, sizeof(tsentry->totalsize));
					tsentry->owneroid      = InvalidOid;
					tsentry->namespaceoid  = InvalidOid;
					tsentry->tablespaceoid = InvalidOid;
					tsentry->flag          = 0;

					int seg_st = TableSizeEntrySegidStart(tsentry);
					int seg_ed = TableSizeEntrySegidEnd(tsentry);
					for (int j = seg_st; j < seg_ed; j++) TableSizeEntrySetFlushFlag(tsentry, j);
				}
			}

			/* mark tsentry is_exist */
			if (tsentry) set_table_size_entry_flag(tsentry, TABLE_EXIST);
			active_table_key.reloid = relOid;
			active_table_key.segid  = cur_segid;
			active_table_entry      = (DiskQuotaActiveTableEntry *)hash_search(
			             local_active_table_stat_map, &active_table_key, HASH_FIND, &active_tbl_found);

			/* skip to recalculate the tables which are not in active list */
			if (active_tbl_found)
			{
				if (cur_segid == -1)
				{
					/* pretend process as utility mode, and append the table size on master */
					Gp_role = GP_ROLE_UTILITY;

					active_table_entry->tablesize += calculate_table_size(relOid);

					Gp_role = GP_ROLE_DISPATCH;
				}
				/* firstly calculate the updated total size of a table */
				updated_total_size = active_table_entry->tablesize - TableSizeEntryGetSize(tsentry, cur_segid);

				/* update the table_size entry */
				TableSizeEntrySetSize(tsentry, cur_segid, active_table_entry->tablesize);
				TableSizeEntrySetFlushFlag(tsentry, cur_segid);

				/* update the disk usage, there may be entries in the map whose keys are InvlidOid as the tsentry does
				 * not exist in the table_size_map */
				update_size_for_quota(updated_total_size, NAMESPACE_QUOTA, (Oid[]){tsentry->namespaceoid}, cur_segid);
				update_size_for_quota(updated_total_size, ROLE_QUOTA, (Oid[]){tsentry->owneroid}, cur_segid);
				update_size_for_quota(updated_total_size, ROLE_TABLESPACE_QUOTA,
				                      (Oid[]){tsentry->owneroid, tsentry->tablespaceoid}, cur_segid);
				update_size_for_quota(updated_total_size, NAMESPACE_TABLESPACE_QUOTA,
				                      (Oid[]){tsentry->namespaceoid, tsentry->tablespaceoid}, cur_segid);
			}
			/* table size info doesn't need to flush at init quota model stage */
			if (is_init)
			{
				TableSizeEntryResetFlushFlag(tsentry, cur_segid);
			}

			/* if schema change, transfer the file size */
			if (tsentry->namespaceoid != relnamespace)
			{
				transfer_table_for_quota(TableSizeEntryGetSize(tsentry, cur_segid), NAMESPACE_QUOTA,
				                         (Oid[]){tsentry->namespaceoid}, (Oid[]){relnamespace}, cur_segid);
			}
			/* if owner change, transfer the file size */
			if (tsentry->owneroid != relowner)
			{
				transfer_table_for_quota(TableSizeEntryGetSize(tsentry, cur_segid), ROLE_QUOTA,
				                         (Oid[]){tsentry->owneroid}, (Oid[]){relowner}, cur_segid);
			}

			if (tsentry->tablespaceoid != reltablespace || tsentry->namespaceoid != relnamespace)
			{
				transfer_table_for_quota(TableSizeEntryGetSize(tsentry, cur_segid), NAMESPACE_TABLESPACE_QUOTA,
				                         (Oid[]){tsentry->namespaceoid, tsentry->tablespaceoid},
				                         (Oid[]){relnamespace, reltablespace}, cur_segid);
			}
			if (tsentry->tablespaceoid != reltablespace || tsentry->owneroid != relowner)
			{
				transfer_table_for_quota(TableSizeEntryGetSize(tsentry, cur_segid), ROLE_TABLESPACE_QUOTA,
				                         (Oid[]){tsentry->owneroid, tsentry->tablespaceoid},
				                         (Oid[]){relowner, reltablespace}, cur_segid);
			}

			if (cur_segid == (TableSizeEntrySegidEnd(tsentry) - 1))
			{
				tsentry->namespaceoid  = relnamespace;
				tsentry->owneroid      = relowner;
				tsentry->tablespaceoid = reltablespace;
			}
		}
		if (HeapTupleIsValid(classTup))
		{
			heap_freetuple(classTup);
		}
	}

	list_free(oidlist);

	/*
	 * Process removed tables. Reduce schema and role size firstly. Remove
	 * table from table_size_map in flush_to_table_size() function later.
	 */
	hash_seq_init(&iter, table_size_map);
	while ((tsentry = hash_seq_search(&iter)) != NULL)
	{
		if (!get_table_size_entry_flag(tsentry, TABLE_EXIST))
		{
			int seg_st = TableSizeEntrySegidStart(tsentry);
			int seg_ed = TableSizeEntrySegidEnd(tsentry);
			for (int i = seg_st; i < seg_ed; i++)
			{
				update_size_for_quota(-TableSizeEntryGetSize(tsentry, i), NAMESPACE_QUOTA,
				                      (Oid[]){tsentry->namespaceoid}, i);
				update_size_for_quota(-TableSizeEntryGetSize(tsentry, i), ROLE_QUOTA, (Oid[]){tsentry->owneroid}, i);
				update_size_for_quota(-TableSizeEntryGetSize(tsentry, i), ROLE_TABLESPACE_QUOTA,
				                      (Oid[]){tsentry->owneroid, tsentry->tablespaceoid}, i);
				update_size_for_quota(-TableSizeEntryGetSize(tsentry, i), NAMESPACE_TABLESPACE_QUOTA,
				                      (Oid[]){tsentry->namespaceoid, tsentry->tablespaceoid}, i);
			}
		}
	}
}

static void
delete_from_table_size_map(char *str)
{
	StringInfoData delete_statement;
	int            ret;

	initStringInfo(&delete_statement);
	appendStringInfo(&delete_statement,
	                 "WITH deleted_table AS ( VALUES %s ) "
	                 "delete from diskquota.table_size "
	                 "where (tableid, segid) in ( SELECT * FROM deleted_table );",
	                 str);
	ret = SPI_execute(delete_statement.data, false, 0);
	if (ret != SPI_OK_DELETE)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
		                errmsg("[diskquota] delete_from_table_size_map SPI_execute failed: error code %d", ret)));
	pfree(delete_statement.data);
}

static void
insert_into_table_size_map(char *str)
{
	StringInfoData insert_statement;
	int            ret;

	initStringInfo(&insert_statement);
	appendStringInfo(&insert_statement, "insert into diskquota.table_size values %s;", str);
	ret = SPI_execute(insert_statement.data, false, 0);
	if (ret != SPI_OK_INSERT)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
		                errmsg("[diskquota] insert_into_table_size_map SPI_execute failed: error code %d", ret)));
	pfree(insert_statement.data);
}

/*
 * Flush the table_size_map to user table diskquota.table_size
 * To improve update performance, we first delete all the need_to_flush
 * entries in table table_size. And then insert new table size entries into
 * table table_size.
 */
static void
flush_to_table_size(void)
{
	HASH_SEQ_STATUS iter;
	TableSizeEntry *tsentry = NULL;
	StringInfoData  delete_statement;
	StringInfoData  insert_statement;
	int             delete_entries_num = 0;
	int             insert_entries_num = 0;

	/* TODO: Add flush_size_interval to avoid flushing size info in every loop */

	/* Disable ORCA since it does not support non-scalar subqueries. */
	bool old_optimizer = optimizer;
	optimizer          = false;

	initStringInfo(&insert_statement);
	initStringInfo(&delete_statement);

	hash_seq_init(&iter, table_size_map);
	while ((tsentry = hash_seq_search(&iter)) != NULL)
	{
		int seg_st = TableSizeEntrySegidStart(tsentry);
		int seg_ed = TableSizeEntrySegidEnd(tsentry);
		for (int i = seg_st; i < seg_ed; i++)
		{
			/* delete dropped table from both table_size_map and table table_size */
			if (!get_table_size_entry_flag(tsentry, TABLE_EXIST))
			{
				appendStringInfo(&delete_statement, "%s(%u,%d)", (delete_entries_num == 0) ? " " : ", ",
				                 tsentry->key.reloid, i);
				delete_entries_num++;
				if (delete_entries_num > SQL_MAX_VALUES_NUMBER)
				{
					delete_from_table_size_map(delete_statement.data);
					resetStringInfo(&delete_statement);
					delete_entries_num = 0;
				}
			}
			/* update the table size by delete+insert in table table_size */
			else if (TableSizeEntryGetFlushFlag(tsentry, i))
			{
				appendStringInfo(&delete_statement, "%s(%u,%d)", (delete_entries_num == 0) ? " " : ", ",
				                 tsentry->key.reloid, i);
				appendStringInfo(&insert_statement, "%s(%u,%ld,%d)", (insert_entries_num == 0) ? " " : ", ",
				                 tsentry->key.reloid, TableSizeEntryGetSize(tsentry, i), i);
				delete_entries_num++;
				insert_entries_num++;

				if (delete_entries_num > SQL_MAX_VALUES_NUMBER)
				{
					delete_from_table_size_map(delete_statement.data);
					resetStringInfo(&delete_statement);
					delete_entries_num = 0;
				}
				if (insert_entries_num > SQL_MAX_VALUES_NUMBER)
				{
					insert_into_table_size_map(insert_statement.data);
					resetStringInfo(&insert_statement);
					insert_entries_num = 0;
				}

				TableSizeEntryResetFlushFlag(tsentry, i);
			}
		}
		if (!get_table_size_entry_flag(tsentry, TABLE_EXIST))
		{
			hash_search(table_size_map, &tsentry->key, HASH_REMOVE, NULL);
			pg_atomic_fetch_sub_u32(diskquota_table_size_entry_num, 1);
		}
	}

	if (delete_entries_num) delete_from_table_size_map(delete_statement.data);
	if (insert_entries_num) insert_into_table_size_map(insert_statement.data);

	optimizer = old_optimizer;

	pfree(delete_statement.data);
	pfree(insert_statement.data);
}

/*
 * Generate the new shared rejectmap from the local_rejectmap which
 * exceed the quota limit.
 * local_rejectmap is used to reduce the lock contention.
 */
static bool
flush_local_reject_map(void)
{
	bool                  changed = false;
	HASH_SEQ_STATUS       iter;
	LocalRejectMapEntry  *localrejectentry;
	GlobalRejectMapEntry *rejectentry;
	bool                  found;

	LWLockAcquire(diskquota_locks.reject_map_lock, LW_EXCLUSIVE);

	hash_seq_init(&iter, local_disk_quota_reject_map);
	while ((localrejectentry = hash_seq_search(&iter)) != NULL)
	{
		/*
		 * If localrejectentry->isexceeded is true, and it alredy exists in disk_quota_reject_map,
		 * that means the reject entry exists in both last loop and current loop, but its segexceeded
		 * feild may have changed.
		 *
		 * If localrejectentry->isexceeded is true, and it doesn't exist in disk_quota_reject_map,
		 * then it is a new added reject entry in this loop.
		 *
		 * Otherwise, it means the reject entry has gone, we need to delete it.
		 */
		if (localrejectentry->isexceeded)
		{
			rejectentry = (GlobalRejectMapEntry *)hash_search(disk_quota_reject_map, (void *)&localrejectentry->keyitem,
			                                                  HASH_ENTER_NULL, &found);
			if (rejectentry == NULL)
			{
				ereport(WARNING, (errmsg("[diskquota] Shared disk quota reject map size limit reached."
				                         "Some out-of-limit schemas or roles will be lost"
				                         "in rejectmap.")));
				continue;
			}
			/* new db objects which exceed quota limit */
			if (!found)
			{
				rejectentry->keyitem.targetoid     = localrejectentry->keyitem.targetoid;
				rejectentry->keyitem.databaseoid   = MyDatabaseId;
				rejectentry->keyitem.targettype    = localrejectentry->keyitem.targettype;
				rejectentry->keyitem.tablespaceoid = localrejectentry->keyitem.tablespaceoid;
				rejectentry->segexceeded           = localrejectentry->segexceeded;
				changed                            = true;
			}
			if (rejectentry->segexceeded != localrejectentry->segexceeded)
			{
				rejectentry->segexceeded = localrejectentry->segexceeded;
				changed                  = true;
			}
			localrejectentry->isexceeded  = false;
			localrejectentry->segexceeded = false;
		}
		else
		{
			changed = true;
			/* db objects are removed or under quota limit in the new loop */
			(void)hash_search(disk_quota_reject_map, (void *)&localrejectentry->keyitem, HASH_REMOVE, NULL);
			(void)hash_search(local_disk_quota_reject_map, (void *)&localrejectentry->keyitem, HASH_REMOVE, NULL);
		}
	}
	LWLockRelease(diskquota_locks.reject_map_lock);
	return changed;
}

/*
 * Dispatch rejectmap to segment servers.
 */
static void
dispatch_rejectmap(HTAB *local_active_table_stat_map)
{
	HASH_SEQ_STATUS            hash_seq;
	GlobalRejectMapEntry      *rejectmap_entry;
	DiskQuotaActiveTableEntry *active_table_entry;
	int                        num_entries, count = 0;
	CdbPgResults               cdb_pgresults = {NULL, 0};
	StringInfoData             rows;
	StringInfoData             active_oids;
	StringInfoData             sql;

	initStringInfo(&rows);
	initStringInfo(&active_oids);
	initStringInfo(&sql);

	LWLockAcquire(diskquota_locks.reject_map_lock, LW_SHARED);
	num_entries = hash_get_num_entries(disk_quota_reject_map);
	hash_seq_init(&hash_seq, disk_quota_reject_map);
	while ((rejectmap_entry = hash_seq_search(&hash_seq)) != NULL)
	{
		appendStringInfo(&rows, "ROW(%d, %d, %d, %d, %s)", rejectmap_entry->keyitem.targetoid,
		                 rejectmap_entry->keyitem.databaseoid, rejectmap_entry->keyitem.tablespaceoid,
		                 rejectmap_entry->keyitem.targettype, rejectmap_entry->segexceeded ? "true" : "false");

		if (++count != num_entries) appendStringInfo(&rows, ",");
	}
	LWLockRelease(diskquota_locks.reject_map_lock);

	count       = 0;
	num_entries = hash_get_num_entries(local_active_table_stat_map);
	hash_seq_init(&hash_seq, local_active_table_stat_map);
	while ((active_table_entry = hash_seq_search(&hash_seq)) != NULL)
	{
		appendStringInfo(&active_oids, "%d", active_table_entry->reloid);

		if (++count != num_entries) appendStringInfo(&active_oids, ",");
	}

	appendStringInfo(&sql,
	                 "select diskquota.refresh_rejectmap("
	                 "ARRAY[%s]::diskquota.rejectmap_entry[], "
	                 "ARRAY[%s]::oid[])",
	                 rows.data, active_oids.data);
	CdbDispatchCommand(sql.data, DF_NONE, &cdb_pgresults);

	pfree(rows.data);
	pfree(active_oids.data);
	pfree(sql.data);
	cdbdisp_clearCdbPgResults(&cdb_pgresults);
}

/*
 * Make sure a StringInfo's string is no longer than 'nchars' characters.
 */
void
truncateStringInfo(StringInfo str, int nchars)
{
	if (str && str->len > nchars)
	{
		Assert(str->data != NULL && str->len <= str->maxlen);
		str->len          = nchars;
		str->data[nchars] = '\0';
	}
}

/*
 * Interface to load quotas from diskquota configuration table(quota_config).
 */
static bool
load_quotas(void)
{
	bool connected          = false;
	bool pushed_active_snap = false;
	bool ret                = true;

	StartTransactionCommand();

	/*
	 * Cache Errors during SPI functions, for example a segment may be down
	 * and current SPI execute will fail. diskquota worker process should
	 * tolerate this kind of errors and continue to check at the next loop.
	 */
	PG_TRY();
	{
		int ret_code = SPI_connect();
		if (ret_code != SPI_OK_CONNECT)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
			                errmsg("[diskquota] unable to connect to execute SPI query, return code: %d", ret_code)));
		}
		connected = true;
		PushActiveSnapshot(GetTransactionSnapshot());
		pushed_active_snap = true;
		do_load_quotas();
	}
	PG_CATCH();
	{
		/* Prevents interrupts while cleaning up */
		HOLD_INTERRUPTS();
		EmitErrorReport();
		FlushErrorState();
		ret = false;
		/* Now we can allow interrupts again */
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();
	if (connected) SPI_finish();
	if (pushed_active_snap) PopActiveSnapshot();
	if (ret)
		CommitTransactionCommand();
	else
		AbortCurrentTransaction();

	return ret;
}

/*
 * Load quotas from diskquota configuration table(quota_config).
 */
static void
do_load_quotas(void)
{
	int       ret;
	TupleDesc tupdesc;
	int       i;

	/*
	 * TODO: we should skip to reload quota config when there is no change in
	 * quota.config. A flag in shared memory could be used to detect the quota
	 * config change.
	 */
	clear_all_quota_maps();

	/*
	 * read quotas from diskquota.quota_config and target table
	 */
	ret = SPI_execute_with_args(
	        "SELECT c.targetOid, c.quotaType, c.quotalimitMB, COALESCE(c.segratio, 0) AS segratio, "
	        "COALESCE(t.tablespaceoid, 0) AS tablespaceoid, COALESCE(t.primaryOid, 0) AS primaryoid "
	        "FROM diskquota.quota_config AS c LEFT OUTER JOIN diskquota.target AS t "
	        "ON c.targetOid = t.rowId AND c.quotaType IN ($1, $2) AND c.quotaType = t.quotaType",
	        2,
	        (Oid[]){
	                INT4OID,
	                INT4OID,
	        },
	        (Datum[]){
	                Int32GetDatum(NAMESPACE_TABLESPACE_QUOTA),
	                Int32GetDatum(ROLE_TABLESPACE_QUOTA),
	        },
	        NULL, true, 0);
	if (ret != SPI_OK_SELECT)
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
		                errmsg("[diskquota] load_quotas SPI_execute failed: error code %d", ret)));

	tupdesc = SPI_tuptable->tupdesc;
#if GP_VERSION_NUM < 70000
	if (tupdesc->natts != NUM_QUOTA_CONFIG_ATTRS || ((tupdesc)->attrs[0])->atttypid != OIDOID ||
	    ((tupdesc)->attrs[1])->atttypid != INT4OID || ((tupdesc)->attrs[2])->atttypid != INT8OID)
#else
	if (tupdesc->natts != NUM_QUOTA_CONFIG_ATTRS || ((tupdesc)->attrs[0]).atttypid != OIDOID ||
	    ((tupdesc)->attrs[1]).atttypid != INT4OID || ((tupdesc)->attrs[2]).atttypid != INT8OID)
#endif /* GP_VERSION_NUM */
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
		                errmsg("[diskquota] configuration table is corrupted in database \"%s\","
		                       " please recreate diskquota extension",
		                       get_database_name(MyDatabaseId))));
	}

	for (i = 0; i < SPI_processed; i++)
	{
		HeapTuple tup = SPI_tuptable->vals[i];
		Datum     vals[NUM_QUOTA_CONFIG_ATTRS];
		bool      isnull[NUM_QUOTA_CONFIG_ATTRS];

		for (int i = 0; i < NUM_QUOTA_CONFIG_ATTRS; ++i)
		{
			vals[i] = SPI_getbinval(tup, tupdesc, i + 1, &(isnull[i]));
			if (i <= 2 && isnull[i])
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				                errmsg("[diskquota] attibutes in configuration table MUST NOT be NULL")));
			}
		}

		Oid   targetOid      = DatumGetObjectId(vals[0]);
		int   quotaType      = (QuotaType)DatumGetInt32(vals[1]);
		int64 quota_limit_mb = DatumGetInt64(vals[2]);
		float segratio       = DatumGetFloat4(vals[3]);
		Oid   spcOid         = DatumGetObjectId(vals[4]);
		Oid   primaryOid     = DatumGetObjectId(vals[5]);

		if (quotaType == NAMESPACE_TABLESPACE_QUOTA || quotaType == ROLE_TABLESPACE_QUOTA)
		{
			targetOid = primaryOid;
		}

		if (spcOid == InvalidOid)
		{
			if (quota_info[quotaType].num_keys != 1)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
				                errmsg("[diskquota] tablespace Oid MUST NOT be NULL for quota type: %d. num_keys: %d",
				                       quotaType, quota_info[quotaType].num_keys)));
			}
			update_limit_for_quota(quota_limit_mb * (1 << 20), segratio, quotaType, (Oid[]){targetOid});
		}
		else
		{
			update_limit_for_quota(quota_limit_mb * (1 << 20), segratio, quotaType, (Oid[]){targetOid, spcOid});
		}
	}

	return;
}

/*
 * Given table oid, search for namespace and owner.
 */
static bool
get_rel_owner_schema_tablespace(Oid relid, Oid *ownerOid, Oid *nsOid, Oid *tablespaceoid)
{
	HeapTuple tp;

	/*
	 * Since we don't take any lock on relation, check for cache
	 * invalidation messages manually to minimize risk of cache
	 * inconsistency.
	 */
	AcceptInvalidationMessages();
	tp         = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	bool found = HeapTupleIsValid(tp);
	if (HeapTupleIsValid(tp))
	{
		Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);

		*ownerOid      = reltup->relowner;
		*nsOid         = reltup->relnamespace;
		*tablespaceoid = reltup->reltablespace;

		if (!OidIsValid(*tablespaceoid))
		{
			*tablespaceoid = MyDatabaseTableSpace;
		}

		ReleaseSysCache(tp);
	}
	return found;
}

/*
 * Given table oid, search for namespace and name.
 * Memory relname points to should be pre-allocated at least NAMEDATALEN bytes.
 */
bool
get_rel_name_namespace(Oid relid, Oid *nsOid, char *relname)
{
	HeapTuple tp;

	/*
	 * Since we don't take any lock on relation, check for cache
	 * invalidation messages manually to minimize risk of cache
	 * inconsistency.
	 */
	AcceptInvalidationMessages();
	tp         = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
	bool found = HeapTupleIsValid(tp);
	if (found)
	{
		Form_pg_class reltup = (Form_pg_class)GETSTRUCT(tp);

		*nsOid = reltup->relnamespace;
		memcpy(relname, reltup->relname.data, NAMEDATALEN);

		ReleaseSysCache(tp);
	}
	return found;
}

static bool
check_rejectmap_by_relfilenode(RelFileNode relfilenode)
{
	bool                  found;
	RejectMapEntry        keyitem;
	GlobalRejectMapEntry *entry;

	SIMPLE_FAULT_INJECTOR("check_rejectmap_by_relfilenode");

	memset(&keyitem, 0, sizeof(keyitem));
	memcpy(&keyitem.relfilenode, &relfilenode, sizeof(RelFileNode));

	LWLockAcquire(diskquota_locks.reject_map_lock, LW_SHARED);
	entry = hash_search(disk_quota_reject_map, &keyitem, HASH_FIND, &found);

	if (found && entry)
	{
		GlobalRejectMapEntry segrejectentry;
		memcpy(&segrejectentry.keyitem, &entry->auxblockinfo, sizeof(RejectMapEntry));
		segrejectentry.segexceeded = entry->segexceeded;
		LWLockRelease(diskquota_locks.reject_map_lock);

		export_exceeded_error(&segrejectentry, true /*skip_name*/);
		return false;
	}
	LWLockRelease(diskquota_locks.reject_map_lock);
	return true;
}

/*
 * This function takes relowner, relnamespace, reltablespace as arguments,
 * prepares the searching key of the global rejectmap for us.
 */
static void
prepare_rejectmap_search_key(RejectMapEntry *keyitem, QuotaType type, Oid relowner, Oid relnamespace, Oid reltablespace)
{
	Assert(keyitem != NULL);
	memset(keyitem, 0, sizeof(RejectMapEntry));
	if (type == ROLE_QUOTA || type == ROLE_TABLESPACE_QUOTA)
		keyitem->targetoid = relowner;
	else if (type == NAMESPACE_QUOTA || type == NAMESPACE_TABLESPACE_QUOTA)
		keyitem->targetoid = relnamespace;
	else if (type == TABLESPACE_QUOTA)
		keyitem->targetoid = reltablespace;
	else
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("[diskquota] unknown quota type: %d", type)));

	if (type == ROLE_TABLESPACE_QUOTA || type == NAMESPACE_TABLESPACE_QUOTA)
		keyitem->tablespaceoid = reltablespace;
	else
	{
		/* refer to add_quota_to_rejectmap */
		keyitem->tablespaceoid = InvalidOid;
	}
	keyitem->databaseoid = MyDatabaseId;
	keyitem->targettype  = type;
}

/*
 * Given table oid, check whether quota limit
 * of table's schema or table's owner are reached.
 * Do enforcement if quota exceeds.
 */
static bool
check_rejectmap_by_reloid(Oid reloid)
{
	Oid                   ownerOid      = InvalidOid;
	Oid                   nsOid         = InvalidOid;
	Oid                   tablespaceoid = InvalidOid;
	bool                  found;
	RejectMapEntry        keyitem;
	GlobalRejectMapEntry *entry;

	bool found_rel = get_rel_owner_schema_tablespace(reloid, &ownerOid, &nsOid, &tablespaceoid);
	if (!found_rel)
	{
		return true;
	}

	LWLockAcquire(diskquota_locks.reject_map_lock, LW_SHARED);
	for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
	{
		prepare_rejectmap_search_key(&keyitem, type, ownerOid, nsOid, tablespaceoid);
		entry = hash_search(disk_quota_reject_map, &keyitem, HASH_FIND, &found);
		if (found)
		{
			LWLockRelease(diskquota_locks.reject_map_lock);
			export_exceeded_error(entry, false /*skip_name*/);
			return false;
		}
	}
	LWLockRelease(diskquota_locks.reject_map_lock);
	return true;
}

/*
 * Given relation's oid or relfilenode, check whether the
 * quota limits of schema or owner are reached. Do enforcement
 * if the quota exceeds.
 */
bool
quota_check_common(Oid reloid, RelFileNode *relfilenode)
{
	bool enable_hardlimit;

	if (!IsTransactionState()) return true;

	if (diskquota_is_paused()) return true;

	if (OidIsValid(reloid)) return check_rejectmap_by_reloid(reloid);

	enable_hardlimit = diskquota_hardlimit;

#ifdef FAULT_INJECTOR
	if (SIMPLE_FAULT_INJECTOR("enable_check_quota_by_relfilenode") == FaultInjectorTypeSkip) enable_hardlimit = true;
#endif
	if (relfilenode && enable_hardlimit) return check_rejectmap_by_relfilenode(*relfilenode);

	return true;
}

/*
 * invalidate all reject entry with a specific dbid in SHM
 */
void
invalidate_database_rejectmap(Oid dbid)
{
	RejectMapEntry *entry;
	HASH_SEQ_STATUS iter;

	LWLockAcquire(diskquota_locks.reject_map_lock, LW_EXCLUSIVE);
	hash_seq_init(&iter, disk_quota_reject_map);
	while ((entry = hash_seq_search(&iter)) != NULL)
	{
		if (entry->databaseoid == dbid || entry->relfilenode.dbNode == dbid)
		{
			hash_search(disk_quota_reject_map, entry, HASH_REMOVE, NULL);
		}
	}
	LWLockRelease(diskquota_locks.reject_map_lock);
}

static char *
GetNamespaceName(Oid spcid, bool skip_name)
{
	if (skip_name)
	{
		NameData spcstr;
		pg_ltoa(spcid, spcstr.data);
		return pstrdup(spcstr.data);
	}
	return get_namespace_name(spcid);
}

static char *
GetTablespaceName(Oid spcid, bool skip_name)
{
	if (skip_name)
	{
		NameData spcstr;
		pg_ltoa(spcid, spcstr.data);
		return pstrdup(spcstr.data);
	}
	return get_tablespace_name(spcid);
}

static char *
GetUserName(Oid relowner, bool skip_name)
{
	if (skip_name)
	{
		NameData namestr;
		pg_ltoa(relowner, namestr.data);
		return pstrdup(namestr.data);
	}
#if GP_VERSION_NUM < 70000
	return GetUserNameFromId(relowner);
#else
	return GetUserNameFromId(relowner, false);
#endif /* GP_VERSION_NUM */
}

static void
export_exceeded_error(GlobalRejectMapEntry *entry, bool skip_name)
{
	RejectMapEntry *rejectentry = &entry->keyitem;
	switch (rejectentry->targettype)
	{
		case NAMESPACE_QUOTA:
			ereport(ERROR, (errcode(ERRCODE_DISK_FULL), errmsg("schema's disk space quota exceeded with name: %s",
			                                                   GetNamespaceName(rejectentry->targetoid, skip_name))));
			break;
		case ROLE_QUOTA:
			ereport(ERROR, (errcode(ERRCODE_DISK_FULL), errmsg("role's disk space quota exceeded with name: %s",
			                                                   GetUserName(rejectentry->targetoid, skip_name))));
			break;
		case NAMESPACE_TABLESPACE_QUOTA:
			if (entry->segexceeded)
				ereport(ERROR, (errcode(ERRCODE_DISK_FULL),
				                errmsg("tablespace: %s, schema: %s diskquota exceeded per segment quota",
				                       GetTablespaceName(rejectentry->tablespaceoid, skip_name),
				                       GetNamespaceName(rejectentry->targetoid, skip_name))));
			else
				ereport(ERROR,
				        (errcode(ERRCODE_DISK_FULL), errmsg("tablespace: %s, schema: %s diskquota exceeded",
				                                            GetTablespaceName(rejectentry->tablespaceoid, skip_name),
				                                            GetNamespaceName(rejectentry->targetoid, skip_name))));
			break;
		case ROLE_TABLESPACE_QUOTA:
			if (entry->segexceeded)
				ereport(ERROR, (errcode(ERRCODE_DISK_FULL),
				                errmsg("tablespace: %s, role: %s diskquota exceeded per segment quota",
				                       GetTablespaceName(rejectentry->tablespaceoid, skip_name),
				                       GetUserName(rejectentry->targetoid, skip_name))));
			else
				ereport(ERROR,
				        (errcode(ERRCODE_DISK_FULL), errmsg("tablespace: %s, role: %s diskquota exceeded",
				                                            GetTablespaceName(rejectentry->tablespaceoid, skip_name),
				                                            GetUserName(rejectentry->targetoid, skip_name))));
			break;
		default:
			ereport(ERROR, (errcode(ERRCODE_DISK_FULL), errmsg("diskquota exceeded, unknown quota type")));
	}
}

/*
 * refresh_rejectmap() takes two arguments.
 * The first argument is an array of rejectmap entries on QD.
 * The second argument is an array of active relations' oid.
 *
 * The basic idea is that, we iterate over the active relations' oid, check that
 * whether the relation's owner/tablespace/namespace is in one of the rejectmap
 * entries dispatched from diskquota worker from QD. If the relation should be
 * blocked, we then add its relfilenode together with the toast, toast index,
 * appendonly, appendonly index relations' relfilenodes to the global rejectmap.
 * Note that, this UDF is called on segment servers by diskquota worker on QD and
 * the global rejectmap on segment servers is indexed by relfilenode.
 */
PG_FUNCTION_INFO_V1(refresh_rejectmap);
Datum
refresh_rejectmap(PG_FUNCTION_ARGS)
{
	ArrayType            *rejectmap_array_type  = PG_GETARG_ARRAYTYPE_P(0);
	ArrayType            *active_oid_array_type = PG_GETARG_ARRAYTYPE_P(1);
	Oid                   rejectmap_elem_type   = ARR_ELEMTYPE(rejectmap_array_type);
	Oid                   active_oid_elem_type  = ARR_ELEMTYPE(active_oid_array_type);
	Datum                *datums;
	bool                 *nulls;
	int16                 elem_width;
	bool                  elem_type_by_val;
	char                  elem_alignment_code;
	int                   reject_array_count;
	int                   active_array_count;
	HeapTupleHeader       lt;
	bool                  segexceeded;
	GlobalRejectMapEntry *rejectmapentry;
	HASH_SEQ_STATUS       hash_seq;
	HTAB                 *local_rejectmap;
	HASHCTL               hashctl;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE), errmsg("must be superuser to update rejectmap")));
	if (IS_QUERY_DISPATCHER())
		ereport(ERROR,
		        (errcode(ERRCODE_INTERNAL_ERROR), errmsg("\"refresh_rejectmap()\" can only be executed on QE.")));
	if (ARR_NDIM(rejectmap_array_type) > 1 || ARR_NDIM(active_oid_array_type) > 1)
		ereport(ERROR, (errcode(ERRCODE_ARRAY_SUBSCRIPT_ERROR), errmsg("1-dimensional array needed")));

	/*
	 * Iterate over rejectmap entries and add these entries to the local reject map
	 * on segment servers so that we are able to check whether the given relation (by oid)
	 * should be rejected in O(1) time complexity in third step.
	 */
	memset(&hashctl, 0, sizeof(hashctl));
	hashctl.keysize   = sizeof(RejectMapEntry);
	hashctl.entrysize = sizeof(GlobalRejectMapEntry);
	hashctl.hcxt      = CurrentMemoryContext;

	/*
	 * Since uncommitted relations' information and the global rejectmap entries
	 * are cached in shared memory. The memory regions are guarded by lightweight
	 * locks. In order not to hold multiple locks at the same time, We add rejectmap
	 * entries into the local_rejectmap below and then flush the content of the
	 * local_rejectmap to the global rejectmap at the end of this UDF.
	 */
	local_rejectmap =
	        diskquota_hash_create("local_rejectmap", 1024, &hashctl, HASH_ELEM | HASH_CONTEXT, DISKQUOTA_TAG_HASH);
	get_typlenbyvalalign(rejectmap_elem_type, &elem_width, &elem_type_by_val, &elem_alignment_code);
	deconstruct_array(rejectmap_array_type, rejectmap_elem_type, elem_width, elem_type_by_val, elem_alignment_code,
	                  &datums, &nulls, &reject_array_count);
	for (int i = 0; i < reject_array_count; ++i)
	{
		RejectMapEntry keyitem;
		bool           isnull;

		if (nulls[i]) continue;

		memset(&keyitem, 0, sizeof(RejectMapEntry));
		lt                    = DatumGetHeapTupleHeader(datums[i]);
		keyitem.targetoid     = DatumGetObjectId(GetAttributeByNum(lt, 1, &isnull));
		keyitem.databaseoid   = DatumGetObjectId(GetAttributeByNum(lt, 2, &isnull));
		keyitem.tablespaceoid = DatumGetObjectId(GetAttributeByNum(lt, 3, &isnull));
		keyitem.targettype    = DatumGetInt32(GetAttributeByNum(lt, 4, &isnull));
		/* rejectmap entries from QD should have the real tablespace oid */
		if ((keyitem.targettype == NAMESPACE_TABLESPACE_QUOTA || keyitem.targettype == ROLE_TABLESPACE_QUOTA))
		{
			Assert(OidIsValid(keyitem.tablespaceoid));
		}
		segexceeded = DatumGetBool(GetAttributeByNum(lt, 5, &isnull));

		rejectmapentry = hash_search(local_rejectmap, &keyitem, HASH_ENTER_NULL, NULL);
		if (rejectmapentry) rejectmapentry->segexceeded = segexceeded;
	}

	/*
	 * Thirdly, iterate over the active oid list. Check that if the relation should be blocked.
	 * If the relation should be blocked, we insert the toast, toast index, appendonly, appendonly
	 * index relations to the global reject map.
	 */
	get_typlenbyvalalign(active_oid_elem_type, &elem_width, &elem_type_by_val, &elem_alignment_code);
	deconstruct_array(active_oid_array_type, active_oid_elem_type, elem_width, elem_type_by_val, elem_alignment_code,
	                  &datums, &nulls, &active_array_count);
	for (int i = 0; i < active_array_count; ++i)
	{
		Oid       active_oid = InvalidOid;
		HeapTuple tuple;
		if (nulls[i]) continue;

		active_oid = DatumGetObjectId(datums[i]);
		if (!OidIsValid(active_oid)) continue;

		/*
		 * Since we don't take any lock on relation, check for cache
		 * invalidation messages manually to minimize risk of cache
		 * inconsistency.
		 */
		AcceptInvalidationMessages();
		tuple = SearchSysCacheCopy1(RELOID, active_oid);
		if (HeapTupleIsValid(tuple))
		{
			Form_pg_class  form          = (Form_pg_class)GETSTRUCT(tuple);
			Oid            relnamespace  = form->relnamespace;
			Oid            reltablespace = OidIsValid(form->reltablespace) ? form->reltablespace : MyDatabaseTableSpace;
			Oid            relowner      = form->relowner;
			RejectMapEntry keyitem;
			bool           found;

			for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
			{
				/* Check that if the current relation should be blocked. */
				prepare_rejectmap_search_key(&keyitem, type, relowner, relnamespace, reltablespace);
				rejectmapentry = hash_search(local_rejectmap, &keyitem, HASH_FIND, &found);
				if (found && rejectmapentry)
				{
					/*
					 * If the current relation is blocked, we should add the relfilenode
					 * of itself together with the relfilenodes of its toast relation and
					 * appendonly relations to the global reject map.
					 */
					List     *oid_list       = NIL;
					ListCell *cell           = NULL;
					Oid       toastrelid     = form->reltoastrelid;
					Oid       aosegrelid     = InvalidOid;
					Oid       aoblkdirrelid  = InvalidOid;
					Oid       aovisimaprelid = InvalidOid;
					oid_list                 = lappend_oid(oid_list, active_oid);

					/* Append toast relation and toast index to the oid_list if any. */
					if (OidIsValid(toastrelid))
					{
						oid_list = lappend_oid(oid_list, toastrelid);
						oid_list = list_concat(oid_list, diskquota_get_index_list(toastrelid));
					}

					/* Append ao auxiliary relations and their indexes to the oid_list if any. */
					diskquota_get_appendonly_aux_oid_list(active_oid, &aosegrelid, &aoblkdirrelid, &aovisimaprelid);
					if (OidIsValid(aosegrelid))
					{
						oid_list = lappend_oid(oid_list, aosegrelid);
						oid_list = list_concat(oid_list, diskquota_get_index_list(aosegrelid));
					}
					if (OidIsValid(aoblkdirrelid))
					{
						oid_list = lappend_oid(oid_list, aoblkdirrelid);
						oid_list = list_concat(oid_list, diskquota_get_index_list(aoblkdirrelid));
					}
					if (OidIsValid(aovisimaprelid))
					{
						oid_list = lappend_oid(oid_list, aovisimaprelid);
						oid_list = list_concat(oid_list, diskquota_get_index_list(aovisimaprelid));
					}

					/* Iterate over the oid_list and add their relfilenodes to the rejectmap. */
					foreach (cell, oid_list)
					{
						Oid       curr_oid   = lfirst_oid(cell);
						HeapTuple curr_tuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(curr_oid));
						if (HeapTupleIsValid(curr_tuple))
						{
							Form_pg_class curr_form = (Form_pg_class)GETSTRUCT(curr_tuple);
							Oid curr_reltablespace  = OidIsValid(curr_form->reltablespace) ? curr_form->reltablespace
							                                                               : MyDatabaseTableSpace;
							RelFileNode           relfilenode = {.dbNode  = MyDatabaseId,
							                                     .relNode = curr_form->relfilenode,
							                                     .spcNode = curr_reltablespace};
							bool                  found;
							GlobalRejectMapEntry *blocked_filenode_entry;
							RejectMapEntry        blocked_filenode_keyitem;

							memset(&blocked_filenode_keyitem, 0, sizeof(RejectMapEntry));
							memcpy(&blocked_filenode_keyitem.relfilenode, &relfilenode, sizeof(RelFileNode));

							blocked_filenode_entry =
							        hash_search(local_rejectmap, &blocked_filenode_keyitem, HASH_ENTER_NULL, &found);
							if (!found && blocked_filenode_entry)
							{
								memcpy(&blocked_filenode_entry->auxblockinfo, &keyitem, sizeof(RejectMapEntry));
								blocked_filenode_entry->segexceeded = rejectmapentry->segexceeded;
							}
						}
					}
					/*
					 * The current relation may satisfy multiple blocking conditions,
					 * we only add it once.
					 */
					break;
				}
			}
		}
		else
		{
			/*
			 * We cannot fetch the relation from syscache. It may be an uncommitted relation.
			 * Let's try to fetch it from relation_cache.
			 */
			DiskQuotaRelationCacheEntry *relation_cache_entry;
			bool                         found;
			LWLockAcquire(diskquota_locks.relation_cache_lock, LW_SHARED);
			relation_cache_entry = hash_search(relation_cache, &active_oid, HASH_FIND, &found);
			/* The session of db1 should not see the table inside db2. */
			if (found && relation_cache_entry && relation_cache_entry->rnode.node.dbNode == MyDatabaseId)
			{
				Oid            relnamespace  = relation_cache_entry->namespaceoid;
				Oid            reltablespace = relation_cache_entry->rnode.node.spcNode;
				Oid            relowner      = relation_cache_entry->owneroid;
				RejectMapEntry keyitem;
				for (QuotaType type = 0; type < NUM_QUOTA_TYPES; ++type)
				{
					/* Check that if the current relation should be blocked. */
					prepare_rejectmap_search_key(&keyitem, type, relowner, relnamespace, reltablespace);
					rejectmapentry = hash_search(local_rejectmap, &keyitem, HASH_FIND, &found);

					if (found && rejectmapentry)
					{
						List     *oid_list = NIL;
						ListCell *cell     = NULL;

						/* Collect the relation oid together with its auxiliary relations' oid. */
						oid_list = lappend_oid(oid_list, active_oid);
						for (int auxoidcnt = 0; auxoidcnt < relation_cache_entry->auxrel_num; ++auxoidcnt)
							oid_list = lappend_oid(oid_list, relation_cache_entry->auxrel_oid[auxoidcnt]);

						foreach (cell, oid_list)
						{
							bool                  found;
							GlobalRejectMapEntry *blocked_filenode_entry;
							RejectMapEntry        blocked_filenode_keyitem;
							Oid                   curr_oid = lfirst_oid(cell);

							relation_cache_entry = hash_search(relation_cache, &curr_oid, HASH_FIND, &found);
							if (found && relation_cache_entry)
							{
								memset(&blocked_filenode_keyitem, 0, sizeof(RejectMapEntry));
								memcpy(&blocked_filenode_keyitem.relfilenode, &relation_cache_entry->rnode.node,
								       sizeof(RelFileNode));

								blocked_filenode_entry = hash_search(local_rejectmap, &blocked_filenode_keyitem,
								                                     HASH_ENTER_NULL, &found);
								if (!found && blocked_filenode_entry)
								{
									memcpy(&blocked_filenode_entry->auxblockinfo, &keyitem, sizeof(RejectMapEntry));
									blocked_filenode_entry->segexceeded = rejectmapentry->segexceeded;
								}
							}
						}
					}
				}
			}
			LWLockRelease(diskquota_locks.relation_cache_lock);
		}
	}

	LWLockAcquire(diskquota_locks.reject_map_lock, LW_EXCLUSIVE);

	/* Clear rejectmap entries. */
	hash_seq_init(&hash_seq, disk_quota_reject_map);
	while ((rejectmapentry = hash_seq_search(&hash_seq)) != NULL)
	{
		if (rejectmapentry->keyitem.relfilenode.dbNode != MyDatabaseId &&
		    rejectmapentry->keyitem.databaseoid != MyDatabaseId)
			continue;
		hash_search(disk_quota_reject_map, &rejectmapentry->keyitem, HASH_REMOVE, NULL);
	}

	/* Flush the content of local_rejectmap to the global rejectmap. */
	hash_seq_init(&hash_seq, local_rejectmap);
	while ((rejectmapentry = hash_seq_search(&hash_seq)) != NULL)
	{
		bool                  found;
		GlobalRejectMapEntry *new_entry;

		/*
		 * Skip soft limit reject entry. We don't perform soft-limit on segment servers, so we don't flush the
		 * rejectmap entry with a valid targetoid to the global rejectmap on segment servers.
		 */
		if (OidIsValid(rejectmapentry->keyitem.targetoid)) continue;

		new_entry = hash_search(disk_quota_reject_map, &rejectmapentry->keyitem, HASH_ENTER_NULL, &found);
		if (!found && new_entry) memcpy(new_entry, rejectmapentry, sizeof(GlobalRejectMapEntry));
	}
	LWLockRelease(diskquota_locks.reject_map_lock);

	PG_RETURN_VOID();
}

/*
 * show_rejectmap() provides developers or users to dump the rejectmap in shared
 * memory on a single server. If you want to query rejectmap on segment servers,
 * you should dispatch this query to segments.
 */
PG_FUNCTION_INFO_V1(show_rejectmap);
Datum
show_rejectmap(PG_FUNCTION_ARGS)
{
	FuncCallContext      *funcctx;
	GlobalRejectMapEntry *rejectmap_entry;
	struct RejectMapCtx
	{
		HASH_SEQ_STATUS rejectmap_seq;
		HTAB           *rejectmap;
	} * rejectmap_ctx;

	if (SRF_IS_FIRSTCALL())
	{
		TupleDesc       tupdesc;
		MemoryContext   oldcontext;
		HASHCTL         hashctl;
		HASH_SEQ_STATUS hash_seq;

		/* Create a function context for cross-call persistence. */
		funcctx = SRF_FIRSTCALL_INIT();

		/* Switch to memory context appropriate for multiple function calls */
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		tupdesc    = DiskquotaCreateTemplateTupleDesc(9);
		TupleDescInitEntry(tupdesc, (AttrNumber)1, "target_type", TEXTOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)2, "target_oid", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)3, "database_oid", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)4, "tablespace_oid", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)5, "seg_exceeded", BOOLOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)6, "dbnode", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)7, "spcnode", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)8, "relnode", OIDOID, -1 /*typmod*/, 0 /*attdim*/);
		TupleDescInitEntry(tupdesc, (AttrNumber)9, "segid", INT4OID, -1 /*typmod*/, 0 /*attdim*/);

		funcctx->tuple_desc = BlessTupleDesc(tupdesc);

		rejectmap_ctx = (struct RejectMapCtx *)palloc(sizeof(struct RejectMapCtx));

		/* Create a local hash table and fill it with entries from shared memory. */
		memset(&hashctl, 0, sizeof(hashctl));
		hashctl.keysize          = sizeof(RejectMapEntry);
		hashctl.entrysize        = sizeof(GlobalRejectMapEntry);
		hashctl.hcxt             = CurrentMemoryContext;
		rejectmap_ctx->rejectmap = diskquota_hash_create("rejectmap_ctx rejectmap", 1024, &hashctl,
		                                                 HASH_ELEM | HASH_CONTEXT, DISKQUOTA_TAG_HASH);

		LWLockAcquire(diskquota_locks.reject_map_lock, LW_SHARED);
		hash_seq_init(&hash_seq, disk_quota_reject_map);
		while ((rejectmap_entry = hash_seq_search(&hash_seq)) != NULL)
		{
			GlobalRejectMapEntry *local_rejectmap_entry = NULL;
			local_rejectmap_entry =
			        hash_search(rejectmap_ctx->rejectmap, &rejectmap_entry->keyitem, HASH_ENTER_NULL, NULL);
			if (local_rejectmap_entry)
			{
				memcpy(&local_rejectmap_entry->keyitem, &rejectmap_entry->keyitem, sizeof(RejectMapEntry));
				local_rejectmap_entry->segexceeded = rejectmap_entry->segexceeded;
				memcpy(&local_rejectmap_entry->auxblockinfo, &rejectmap_entry->auxblockinfo, sizeof(RejectMapEntry));
			}
		}
		LWLockRelease(diskquota_locks.reject_map_lock);

		/* Setup first calling context. */
		hash_seq_init(&(rejectmap_ctx->rejectmap_seq), rejectmap_ctx->rejectmap);
		funcctx->user_fctx = (void *)rejectmap_ctx;
		MemoryContextSwitchTo(oldcontext);
	}

	funcctx       = SRF_PERCALL_SETUP();
	rejectmap_ctx = (struct RejectMapCtx *)funcctx->user_fctx;

	while ((rejectmap_entry = hash_seq_search(&(rejectmap_ctx->rejectmap_seq))) != NULL)
	{
#define _TARGETTYPE_STR_SIZE 32
		Datum          result;
		Datum          values[9];
		bool           nulls[9];
		HeapTuple      tuple;
		RejectMapEntry keyitem;
		char           targettype_str[_TARGETTYPE_STR_SIZE];
		RelFileNode    blocked_relfilenode;

		memcpy(&blocked_relfilenode, &rejectmap_entry->keyitem.relfilenode, sizeof(RelFileNode));
		/*
		 * If the rejectmap entry is indexed by relfilenode, we dump the blocking
		 * condition from auxblockinfo.
		 */
		if (!OidIsValid(blocked_relfilenode.relNode))
			memcpy(&keyitem, &rejectmap_entry->keyitem, sizeof(keyitem));
		else
			memcpy(&keyitem, &rejectmap_entry->auxblockinfo, sizeof(keyitem));
		memset(targettype_str, 0, sizeof(targettype_str));

		switch ((QuotaType)keyitem.targettype)
		{
			case ROLE_QUOTA:
				StrNCpy(targettype_str, "ROLE_QUOTA", _TARGETTYPE_STR_SIZE);
				break;
			case NAMESPACE_QUOTA:
				StrNCpy(targettype_str, "NAMESPACE_QUOTA", _TARGETTYPE_STR_SIZE);
				break;
			case ROLE_TABLESPACE_QUOTA:
				StrNCpy(targettype_str, "ROLE_TABLESPACE_QUOTA", _TARGETTYPE_STR_SIZE);
				break;
			case NAMESPACE_TABLESPACE_QUOTA:
				StrNCpy(targettype_str, "NAMESPACE_TABLESPACE_QUOTA", _TARGETTYPE_STR_SIZE);
				break;
			default:
				StrNCpy(targettype_str, "UNKNOWN", _TARGETTYPE_STR_SIZE);
				break;
		}

		values[0] = CStringGetTextDatum(targettype_str);
		values[1] = ObjectIdGetDatum(keyitem.targetoid);
		values[2] = ObjectIdGetDatum(keyitem.databaseoid);
		values[3] = ObjectIdGetDatum(keyitem.tablespaceoid);
		values[4] = BoolGetDatum(rejectmap_entry->segexceeded);
		values[5] = ObjectIdGetDatum(blocked_relfilenode.dbNode);
		values[6] = ObjectIdGetDatum(blocked_relfilenode.spcNode);
		values[7] = ObjectIdGetDatum(blocked_relfilenode.relNode);
		values[8] = Int32GetDatum(GpIdentity.segindex);

		memset(nulls, false, sizeof(nulls));
		tuple  = heap_form_tuple(funcctx->tuple_desc, values, nulls);
		result = HeapTupleGetDatum(tuple);

		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

void
update_monitor_db_mpp(Oid dbid, FetchTableStatType action, const char *schema)
{
	StringInfoData sql_command;
	initStringInfo(&sql_command);
	appendStringInfo(&sql_command,
	                 "SELECT %s.diskquota_fetch_table_stat(%d, '{%d}'::oid[]) FROM gp_dist_random('gp_id')", schema,
	                 action, dbid);
	/* Add current database to the monitored db cache on all segments */
	int ret = SPI_execute(sql_command.data, true, 0);
	pfree(sql_command.data);

	ereportif(ret != SPI_OK_SELECT, ERROR,
	          (errcode(ERRCODE_INTERNAL_ERROR),
	           errmsg("[diskquota] check diskquota state SPI_execute failed: error code %d", ret)));

	/* Add current database to the monitored db cache on coordinator */
	update_monitor_db(dbid, action);
}

static void
format_name(const char *prefix, uint32 id, StringInfo str)
{
	resetStringInfo(str);
	appendStringInfo(str, "%s_%u", prefix, id);
	Assert(str->len <= SHMEM_INDEX_KEYSIZE);
}

static bool
get_table_size_entry_flag(TableSizeEntry *entry, TableSizeEntryFlag flag)
{
	return (entry->flag & flag) ? true : false;
}

static void
reset_table_size_entry_flag(TableSizeEntry *entry, TableSizeEntryFlag flag)
{
	entry->flag &= (UINT32_MAX ^ flag);
}

static void
set_table_size_entry_flag(TableSizeEntry *entry, TableSizeEntryFlag flag)
{
	entry->flag |= flag;
}
