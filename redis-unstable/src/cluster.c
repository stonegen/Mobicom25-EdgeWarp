// بِسْمِ ٱللَّٰهِ ٱلرَّحْمَٰنِ ٱلرَّحِيمِ

/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2) or the Server Side Public License v1 (SSPLv1).
 *
 * Portions of this file are available under BSD3 terms; see REDISCONTRIBUTIONS for more information.
 */

/*
 * cluster.c contains the common parts of a clustering
 * implementation, the parts that are shared between
 * any implementation of Redis clustering.
 */

#include "server.h"
#include "cluster.h"

#include <ctype.h>
#include <pthread.h>
#include "tpool.h"
#include <sys/time.h>

/* MIGRATE socket cache implementation.
 *
 * We take a map between host:ip and a TCP socket that we used to connect
 * to this instance in recent time.
 * This sockets are closed when the max number we cache is reached, and also
 * in serverCron() when they are around for more than a few seconds. */
#define MIGRATE_SOCKET_CACHE_ITEMS 64 /* max num of items in the cache. */
#define MIGRATE_SOCKET_CACHE_TTL 10   /* close cached sockets after 10 sec. */

typedef struct migrateCachedSocket
{
    connection *conn;
    long last_dbid;
    time_t last_use_time;
} migrateCachedSocket;

typedef struct migrateArgs
{
    client *c;
    migrateCachedSocket *cs;
    migrateCachedSocket *csReply;
    rio **cmd;
    long timeout;
    int num_keys;
    int select;
    int dbid;
    sds name;
    sds name2;
    int ultraFast;
} migrateArgs;

typedef struct ioCmdStruct
{
    client *c;
    robj **ov;
    robj **kv;
    rio **cmd;
    rio *payload;
    long timeout;
    int num_keys;
    int start_key; // Start index of the keys to process
    int end_key;   // End index of the keys to process
    int copy;
    int select;
    int dbid;
    int argv_rewritten;
    sds name;
    robj **copyArgv;
    int replace;
    int firstKey;
    int mainThread;
    int lastKeyIndex;
    int totalKeys;
    long long lastKeyExp;
    int ultraFast;
} ioCmdStruct;

typedef struct restoreCmdStruct
{
    migrateCachedSocket *cs;
    int metaOption;
    sds keyName;
} restoreCmdStruct;

/* -----------------------------------------------------------------------------
 * Key space handling
 * -------------------------------------------------------------------------- */

/* We have 16384 hash slots. The hash slot of a given key is obtained
 * as the least significant 14 bits of the crc16 of the key.
 *
 * However, if the key contains the {...} pattern, only the part between
 * { and } is hashed. This may be useful in the future to force certain
 * keys to be in the same node (assuming no resharding is in progress). */
unsigned int keyHashSlot(char *key, int keylen)
{
    int s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{')
            break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen)
        return crc16(key, keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s + 1; e < keylen; e++)
        if (key[e] == '}')
            break;

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (e == keylen || e == s + 1)
        return crc16(key, keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
     * what is in the middle between { and }. */
    return crc16(key + s + 1, e - s - 1) & 0x3FFF;
}

/* If it can be inferred that the given glob-style pattern, as implemented in
 * stringmatchlen() in util.c, only can match keys belonging to a single slot,
 * that slot is returned. Otherwise -1 is returned. */
int patternHashSlot(char *pattern, int length)
{
    int s = -1; /* index of the first '{' */

    for (int i = 0; i < length; i++)
    {
        if (pattern[i] == '*' || pattern[i] == '?' || pattern[i] == '[')
        {
            /* Wildcard or character class found. Keys can be in any slot. */
            return -1;
        }
        else if (pattern[i] == '\\')
        {
            /* Escaped character. Computing slot in this case is not
             * implemented. We would need a temp buffer. */
            return -1;
        }
        else if (s == -1 && pattern[i] == '{')
        {
            /* Opening brace '{' found. */
            s = i;
        }
        else if (s >= 0 && pattern[i] == '}' && i == s + 1)
        {
            /* Empty tag '{}' found. The whole key is hashed. Ignore braces. */
            s = -2;
        }
        else if (s >= 0 && pattern[i] == '}')
        {
            /* Non-empty tag '{...}' found. Hash what's between braces. */
            return crc16(pattern + s + 1, i - s - 1) & 0x3FFF;
        }
    }

    /* The pattern matches a single key. Hash the whole pattern. */
    return crc16(pattern, length) & 0x3FFF;
}

ConnectionType *connTypeOfCluster(void)
{
    if (server.tls_cluster)
    {
        return connectionTypeTls();
    }

    return connectionTypeTcp();
}

/* Return a migrateCachedSocket containing a TCP socket connected with the
 * target instance, possibly returning a cached one.
 *
 * This function is responsible of sending errors to the client if a
 * connection can't be established. In this case -1 is returned.
 * Otherwise on success the socket is returned, and the caller should not
 * attempt to free it after usage.
 *
 * If the caller detects an error while using the socket, migrateCloseSocket()
 * should be called so that the connection will be created from scratch
 * the next time. */
migrateCachedSocket *migrateGetSocket(client *c, robj *host, robj *port, long timeout)
{
    connection *conn;
    sds name = sdsempty();
    migrateCachedSocket *cs;

    /* Check if we have an already cached socket for this ip:port pair. */
    name = sdscatlen(name, host->ptr, sdslen(host->ptr));
    name = sdscatlen(name, ":", 1);
    name = sdscatlen(name, port->ptr, sdslen(port->ptr));
    cs = dictFetchValue(server.migrate_cached_sockets, name);
    if (cs)
    {
        sdsfree(name);
        cs->last_use_time = server.unixtime;
        return cs;
    }

    /* No cached socket, create one. */
    if (dictSize(server.migrate_cached_sockets) == MIGRATE_SOCKET_CACHE_ITEMS)
    {
        /* Too many items, drop one at random. */
        dictEntry *de = dictGetRandomKey(server.migrate_cached_sockets);
        cs = dictGetVal(de);
        connClose(cs->conn);
        zfree(cs);
        dictDelete(server.migrate_cached_sockets, dictGetKey(de));
    }

    /* Create the connection */
    conn = connCreate(connTypeOfCluster());
    if (connBlockingConnect(conn, host->ptr, atoi(port->ptr), timeout) != C_OK)
    {
        printf("Error connecting to the socket !");
        addReplyError(c, "-IOERR error or timeout connecting to the client");
        connClose(conn);
        sdsfree(name);
        return NULL;
    }
    connEnableTcpNoDelay(conn);

    /* Add to the cache and return it to the caller. */
    cs = zmalloc(sizeof(*cs));
    cs->conn = conn;

    cs->last_dbid = -1;
    cs->last_use_time = server.unixtime;
    dictAdd(server.migrate_cached_sockets, name, cs);
    return cs;
}

/* -----------------------------------------------------------------------------
 * DUMP, RESTORE and MIGRATE commands
 * -------------------------------------------------------------------------- */

/* Generates a DUMP-format representation of the object 'o', adding it to the
 * io stream pointed by 'rio'. This function can't fail. */
void createDumpPayload(rio *payload, robj *o, robj *key, int dbid)
{
    unsigned char buf[2];
    uint64_t crc;

    /* Serialize the object in an RDB-like format. It consist of an object type
     * byte followed by the serialized object. This is understood by RESTORE. */
    rioInitWithBuffer(payload, sdsempty());
    serverAssert(rdbSaveObjectType(payload, o));
    serverAssert(rdbSaveObject(payload, o, key, dbid));

    /* Write the footer, this is how it looks like:
     * ----------------+---------------------+---------------+
     * ... RDB payload | 2 bytes RDB version | 8 bytes CRC64 |
     * ----------------+---------------------+---------------+
     * RDB version and CRC are both in little endian.
     */

    /* RDB version */
    buf[0] = RDB_VERSION & 0xff;
    buf[1] = (RDB_VERSION >> 8) & 0xff;
    payload->io.buffer.ptr = sdscatlen(payload->io.buffer.ptr, buf, 2);

    /* CRC64 */
    crc = crc64(0, (unsigned char *)payload->io.buffer.ptr,
                sdslen(payload->io.buffer.ptr));
    memrev64ifbe(&crc);
    payload->io.buffer.ptr = sdscatlen(payload->io.buffer.ptr, &crc, 8);
}

/* Verify that the RDB version of the dump payload matches the one of this Redis
 * instance and that the checksum is ok.
 * If the DUMP payload looks valid C_OK is returned, otherwise C_ERR
 * is returned. If rdbver_ptr is not NULL, its populated with the value read
 * from the input buffer. */
int verifyDumpPayload(unsigned char *p, size_t len, uint16_t *rdbver_ptr)
{
    unsigned char *footer;
    uint16_t rdbver;
    uint64_t crc;

    /* At least 2 bytes of RDB version and 8 of CRC64 should be present. */
    if (len < 10)
        return C_ERR;
    footer = p + (len - 10);

    /* Set and verify RDB version. */
    rdbver = (footer[1] << 8) | footer[0];
    if (rdbver_ptr)
    {
        *rdbver_ptr = rdbver;
    }
    if (rdbver > RDB_VERSION)
        return C_ERR;

    if (server.skip_checksum_validation)
        return C_OK;

    /* Verify CRC64 */
    crc = crc64(0, p, len - 8);
    memrev64ifbe(&crc);
    return (memcmp(&crc, footer + 2, 8) == 0) ? C_OK : C_ERR;
}

/* DUMP keyname
 * DUMP is actually not used by Redis Cluster but it is the obvious
 * complement of RESTORE and can be useful for different applications. */
void dumpCommand(client *c)
{
    robj *o;
    rio payload;

    /* Check if the key is here. */
    if ((o = lookupKeyRead(c->db, c->argv[1])) == NULL)
    {
        addReplyNull(c);
        return;
    }

    /* Create the DUMP encoded representation. */
    createDumpPayload(&payload, o, c->argv[1], c->db->id);

    /* Transfer to the client */
    addReplyBulkSds(c, payload.io.buffer.ptr);
    return;
}

void sendRestoreReply(void *args)
{

    restoreCmdStruct *rArgs = (restoreCmdStruct *)args;
    sds reply = NULL;
    if (rArgs->metaOption == 1 || rArgs->metaOption == 2)
    {
        reply = sdsnew("MIGRATION COMPLETE!");
    }
    else if (rArgs->metaOption == 3)
    {
        reply = rArgs->keyName;
    }
    else
    {
        printf("RESTORE-BUG : Error Occured in processing Restore Meta Args !\n");
    }
    if (reply != NULL)
    {
        int errorNumb = 0;
        size_t pos = 0, towrite;
        int nwritten = 0;

        while ((towrite = sdslen(reply) - pos) > 0)
        {
            // Most of the keys and data will be just few 10s of bytes at max
            // for our Edge Server Use Case

            towrite = (towrite > (1 * 1024) ? (1 * 1024) : towrite);
            nwritten = connSyncWrite(rArgs->cs->conn, reply + pos, towrite, 1000);
            if (nwritten != (signed)towrite)
            {
                errno = 1;
                break;
            }
            pos += nwritten;
        }
        if (errorNumb)
        {
            printf("RESTORE-BUG : we failed to send data to the Edge Server ! \n");
        }
        // Freeing the memory now :
        if (rArgs->metaOption == 3)
        {
            sdsfree(rArgs->keyName);
            reply = NULL;
        }
        else
        {
            sdsfree(reply);
            reply = NULL;
        }
    }
    zfree(rArgs);
}

// The following Function will create the struct & launch seperate thread for the message :
//  This function will be called when  :
//  1) We encountered error in restoring
//  2) We are migrating last key

void makeRestoreMessage(migrateCachedSocket *cs, int metaOption, int isError, robj *key)
{

    restoreCmdStruct *rArgs = zmalloc(sizeof(restoreCmdStruct));
    rArgs->cs = cs;
    rArgs->metaOption = metaOption;
    if (isError)
    {
        // Error Occured in the Last Key :
        if (metaOption == 2)
        {
            rArgs->keyName = sdsnew("LAST-KEY_ERROR");
        }
        else
        {
            rArgs->keyName = sdsnew(key->ptr);
        }
    }
    else
    {
        rArgs->keyName = NULL;
    }

    // tpool_add_work(tm , sendRestoreReply , rArgs);
    assign_single_key_task(dt, sendRestoreReply, rArgs);
    // sendRestoreReply(rArgs);
}

/* RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency] */
void restoreCommand(client *c)
{
    long long ttl, lfu_freq = -1, lru_idle = -1, lru_clock = -1;
    rio payload;
    int j, type, replace = 0, absttl = 0, edgeHostIndex = 0, edgePortIndex = 0;
    robj *obj;

    // this will tell that whether the restore cmd is from Async migration or not
    int metaFlag = -1;
    migrateCachedSocket *cs; // this is to send reply to the edge server if needed

    /* Parse additional options */
    for (j = 4; j < c->argc; j++)
    {
        int additional = c->argc - j - 1;
        if (!strcasecmp(c->argv[j]->ptr, "replace"))
        {
            replace = 1;
        }
        else if (!strcasecmp(c->argv[j]->ptr, "absttl"))
        {
            absttl = 1;
        }
        else if (!strcasecmp(c->argv[j]->ptr, "idletime") && additional >= 1 &&
                 lfu_freq == -1)
        {
            if (getLongLongFromObjectOrReply(c, c->argv[j + 1], &lru_idle, NULL) != C_OK)
                return;
            if (lru_idle < 0)
            {
                addReplyError(c, "Invalid IDLETIME value, must be >= 0");
                return;
            }
            lru_clock = LRU_CLOCK();
            j++; /* Consume additional arg. */
        }
        else if (!strcasecmp(c->argv[j]->ptr, "freq") && additional >= 1 &&
                 lru_idle == -1)
        {
            if (getLongLongFromObjectOrReply(c, c->argv[j + 1], &lfu_freq, NULL) != C_OK)
                return;
            if (lfu_freq < 0 || lfu_freq > 255)
            {
                addReplyError(c, "Invalid FREQ value, must be >= 0 and <= 255");
                return;
            }
            j++; /* Consume additional arg. */
        }
        else if (!strcasecmp(c->argv[j]->ptr, "NONE"))
        {
            // Now we will be looking at the meta data & act accordingly :
            if (j == (c->argc - 1))
            {
                addReplyError(c, "Meta data NONE with Restore Arguments not properly send!");
                return;
            }
            else if ((c->argc - j) == 3)
            {
                // This means that we got only host & port so this is NONE type Restore CMD
                metaFlag = 3;
                edgeHostIndex = j + 1;
                edgePortIndex = j + 2;
                j = j + 2;
            }
            else if ((c->argc - j) == 4)
            {
                // This means that we got more option aswell to consider
                if (!strcasecmp(c->argv[j + 3]->ptr, "LAST"))
                {
                    metaFlag = 2;
                    edgeHostIndex = j + 1;
                    edgePortIndex = j + 2;
                    j = j + 3;
                }
                else if (!strcasecmp(c->argv[j + 3]->ptr, "SINGLE"))
                {
                    metaFlag = 1;
                    edgeHostIndex = j + 1;
                    edgePortIndex = j + 2;
                    j = j + 3;
                }
                else
                {
                    addReplyError(c, "Invalid Data shared with the NONE , HOST & PORT");
                    return;
                }
            }
        }

        else
        {
            printf("We encountered a syntax error here ! \n");
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    if (metaFlag != -1)
    {
        cs = migrateGetSocket(c, c->argv[edgeHostIndex], c->argv[edgePortIndex], 1000);
    }

    /* Make sure this key does not already exist here... */
    robj *key = c->argv[1];
    if (!replace && lookupKeyWrite(c->db, key) != NULL)
    {
        if (metaFlag == -1)
        {
            addReplyErrorObject(c, shared.busykeyerr);
        }
        else
        {
            makeRestoreMessage(cs, metaFlag, 1, key);
        }

        return;
    }

    /* Check if the TTL value makes sense */
    if (getLongLongFromObjectOrReply(c, c->argv[2], &ttl, NULL) != C_OK)
    {

        if (metaFlag == -1)
        {
            addReplyErrorObject(c, shared.busykeyerr);
        }
        else
        {
            makeRestoreMessage(cs, metaFlag, 1, key);
        }

        return;
    }
    else if (ttl < 0)
    {

        if (metaFlag == -1)
        {
            addReplyError(c, "Invalid TTL value, must be >= 0");
        }
        else
        {
            makeRestoreMessage(cs, metaFlag, 1, key);
        }
        return;
    }

    /* Verify RDB version and data checksum. */
    if (verifyDumpPayload(c->argv[3]->ptr, sdslen(c->argv[3]->ptr), NULL) == C_ERR)
    {

        if (metaFlag == -1)
        {
            addReplyError(c, "DUMP payload version or checksum are wrong");
        }
        else
        {
            makeRestoreMessage(cs, metaFlag, 1, key);
        }
        return;
    }

    rioInitWithBuffer(&payload, c->argv[3]->ptr);
    if (((type = rdbLoadObjectType(&payload)) == -1) ||
        ((obj = rdbLoadObject(type, &payload, key->ptr, c->db->id, NULL)) == NULL))
    {

        if (metaFlag == -1)
        {
            addReplyError(c, "Bad data format");
        }
        else
        {
            makeRestoreMessage(cs, metaFlag, 1, key);
        }
        return;
    }

    // Now that we have checked all the test cases for restoring the key we will send reply if this last (Single) key
    if (metaFlag == 1 || metaFlag == 2)
    {
        makeRestoreMessage(cs, metaFlag, 0, key);
    }

    /* Remove the old key if needed. */
    int deleted = 0;
    if (replace)
        deleted = dbDelete(c->db, key);

    if (ttl && !absttl)
        ttl += commandTimeSnapshot();
    if (ttl && checkAlreadyExpired(ttl))
    {
        if (deleted)
        {
            robj *aux = server.lazyfree_lazy_server_del ? shared.unlink : shared.del;
            rewriteClientCommandVector(c, 2, aux, key);
            signalModifiedKey(c, c->db, key);
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, c->db->id);
            server.dirty++;
        }
        decrRefCount(obj);
        if (metaFlag == -1)
        {
            addReply(c, shared.ok);
        }
        return;
    }

    /* Create the key and set the TTL if any */
    dictEntry *de = dbAdd(c->db, key, obj);

    /* If minExpiredField was set, then the object is hash with expiration
     * on fields and need to register it in global HFE DS */
    if (obj->type == OBJ_HASH)
    {
        uint64_t minExpiredField = hashTypeGetMinExpire(obj, 1);
        if (minExpiredField != EB_EXPIRE_TIME_INVALID)
            hashTypeAddToExpires(c->db, dictGetKey(de), obj, minExpiredField);
    }

    if (ttl)
    {
        setExpire(c, c->db, key, ttl);
        if (!absttl)
        {
            /* Propagate TTL as absolute timestamp */
            robj *ttl_obj = createStringObjectFromLongLong(ttl);
            rewriteClientCommandArgument(c, 2, ttl_obj);
            decrRefCount(ttl_obj);
            rewriteClientCommandArgument(c, c->argc, shared.absttl);
        }
    }
    objectSetLRUOrLFU(obj, lfu_freq, lru_idle, lru_clock, 1000);
    signalModifiedKey(c, c->db, key);
    notifyKeyspaceEvent(NOTIFY_GENERIC, "restore", key, c->db->id);
    if (metaFlag == -1)
    {
        addReply(c, shared.ok);
    }
    server.dirty++;
}

// Following Function is to add meta information with the Restore Cmd that Destination Server will recieve :

void createMeta(int option, client *c, rio **cmd, int index)
{
    // Option 1 will reflect that there is a single Key that we will be transfering
    // So we will be transfering both Host & Port of the Edge Server

    if (option == 1)
    {
        rioWriteBulkString(cmd[index], "SINGLE", 6);
    }
    else if (option == 2)
    {
        // Option 2 will reflect that this is the last key so we expect that Destination will inform
        // both us and the Edge Server
        rioWriteBulkString(cmd[index], "LAST", 4);
    }
    else if (option == 3)
    {
        // Option 3 will reflect that in the case a key fails, tell directly the edge Server about it since it
        // will be making decision on whether to resubmit it or not. We don't want redis to be wasting resources
        // on re-migrating it since time window will be short
        rioWriteBulkString(cmd[index], "NONE", 4);
        rioWriteBulkString(cmd[index], c->argv[4]->ptr, sdslen(c->argv[4]->ptr));
        rioWriteBulkString(cmd[index], c->argv[5]->ptr, sdslen(c->argv[5]->ptr));
    }
}

/* Following function copies the client's argv & argc and their contents , the copy needs to be deleted by the caller */
void copyCmdContents(robj **argv, int argc, robj ***copyargv, int *copyargc)
{
    *copyargc = argc;
    *copyargv = zmalloc(sizeof(robj *) * (*copyargc));
    for (int i = 0; i < *copyargc; i++)
    {
        // printf("done1 \n");
        sds cmdString = sdsnewlen(argv[i]->ptr, sdslen(argv[i]->ptr));
        // printf("done2 \n");
        (*copyargv)[i] = createObject(OBJ_STRING, cmdString);
        // printf("done!!! \n");
        (*copyargv)[i]->refcount = argv[i]->refcount;
        (*copyargv)[i]->lru = argv[i]->lru;
    }
}
void deleteCmdContents(robj **argv, int argc)
{
    for (int i = 0; i < argc; i++)
    {
        sdsfree(argv[i]->ptr);
        zfree(argv[i]);
    }

    zfree(argv);
}

void freeCmdResources(rio **cmd, int num_keys)
{
    // Free each element in the cmd array
    for (int i = 0; i < num_keys; i++)
    {
        if (cmd[i])
        {
            sdsfree(cmd[i]->io.buffer.ptr); // Free the buffer inside each rio object
            zfree(cmd[i]);                  // Free the rio object itself
        }
    }
    // Free the cmd array itself
    zfree(cmd);
}

/* Free a migrate cached connection. */
void migrateCloseSocket(robj *host, robj *port)
{

    sds name = sdsempty();
    migrateCachedSocket *cs;

    name = sdscatlen(name, host->ptr, sdslen(host->ptr));
    name = sdscatlen(name, ":", 1);
    name = sdscatlen(name, port->ptr, sdslen(port->ptr));
    cs = dictFetchValue(server.migrate_cached_sockets, name);
    if (!cs)
    {
        sdsfree(name);
        return;
    }

    connClose(cs->conn);
    zfree(cs);
    dictDelete(server.migrate_cached_sockets, name);
    sdsfree(name);
}

void migrateCloseTimedoutSockets(void)
{
    dictIterator *di = dictGetSafeIterator(server.migrate_cached_sockets);
    dictEntry *de;

    while ((de = dictNext(di)) != NULL)
    {
        migrateCachedSocket *cs = dictGetVal(de);

        if ((server.unixtime - cs->last_use_time) > MIGRATE_SOCKET_CACHE_TTL)
        {
            connClose(cs->conn);
            zfree(cs);
            dictDelete(server.migrate_cached_sockets, dictGetKey(de));
        }
    }
    dictReleaseIterator(di);
}

// Threaded Migrate Code :
/*
    Following args will be needed :
    - ov = objects to migrate (unnecessary)
    - kv = key names (unnecessary)
    - newargv = for writing a new command DEL to event loop (unnecessary)
    - cmd = will hold the actual command to send to destination replica
    - payload = will hold the cmd's payload
    - cs = migration socket
    - timeout = time for blocking thread
    - password (optional) (unnecessary)
    - select
    - redis * db
    - num_keys
    - may_retry
    - dbid = extracted from the command


*/
void migreateViaThread(client *c, migrateCachedSocket *cs, migrateCachedSocket *csReply, rio **cmd, long timeout, int num_keys, int select, long dbid, sds name, sds name2, int ultrFast)
{
    int j = 0;
    char buf1[1024]; /* Select reply. */
    long long failedKeys = 0;
    int socket_error = 0, write_error = 0, read_error = 0;
    rio *preCmd;
    preCmd = (rio *)zmalloc(sizeof(rio));

    // Sending Select Msg & Waiting for it's reply if needed :

    rioInitWithBuffer(preCmd, sdsempty());
    if (select)
    {
        serverAssertWithInfo(c, NULL, rioWriteBulkCount(preCmd, '*', 2));
        serverAssertWithInfo(c, NULL, rioWriteBulkString(preCmd, "SELECT", 6));
        serverAssertWithInfo(c, NULL, rioWriteBulkLongLong(preCmd, dbid));
    }

    {
        sds buf = preCmd->io.buffer.ptr;
        size_t pos = 0, towrite;
        int nwritten = 0;

        while ((towrite = sdslen(buf) - pos) > 0)
        {
            towrite = (towrite > (64 * 1024) ? (64 * 1024) : towrite);
            nwritten = connSyncWrite(cs->conn, buf + pos, towrite, timeout);
            if (nwritten != (signed)towrite)
            {
                socket_error = 1;
                goto socket_err;
            }
            pos += nwritten;
        }
    }

    // Waiting for the reply :
    if (select && connSyncReadLine(cs->conn, buf1, sizeof(buf1), timeout) <= 0)
    {
        read_error = 1;
        goto socket_err;
    }

    /* Create RESTORE or SET payloads and generate the protocols to call the command. */
    for (j = 0; j < num_keys; j++)
    {
        // printf("HERE3");
        /* Transfering the query to the other node in 64K chunks. */
        errno = 0;
        {
            sds buf = cmd[j]->io.buffer.ptr;
            size_t pos = 0, towrite;
            int nwritten = 0;

            while ((towrite = sdslen(buf) - pos) > 0)
            {
                // printf("HERE4");
                towrite = (towrite > (64 * 1024) ? (64 * 1024) : towrite);
                nwritten = connSyncWrite(cs->conn, buf + pos, towrite, timeout);
                if (nwritten != (signed)towrite)
                {
                    write_error = 1;
                    goto socket_err;
                }
                pos += nwritten;
            }
        }
    }

    /* In case we are sending SET COMMANDS we would listen to their replies for now via Pipelining. In future this will become same as
        RESTORE COMMAND */

    if (ultrFast == 1)
    {
        char buf2[1024];
        for (j = 0; j < num_keys; j++)
        {
            if (connSyncReadLine(cs->conn, buf2, sizeof(buf2), timeout) <= 0)
            {
                socket_error = 1;
                break;
            }
            if (buf2[0] == '-')
            {
                failedKeys += 1;
            }
        }

        // printf("HERE5");
        /*Sending Back the Status of the Migration Now : */
        {
            sds migrationReply;
            if (failedKeys == 0)
            {
                migrationReply = sdsnew("MIGRATION COMPLETE");
            }
            else
            {
                sds errorKeys = sdsfromlonglong(failedKeys);
                migrationReply = sdsnew("MIGRATION FAILED WITH : ");
                migrationReply = sdscatlen(migrationReply, errorKeys, sdslen(errorKeys));
                sdsfree(errorKeys);
                // printf("HERE6");
            }

            size_t pos = 0, towrite;
            int nwritten = 0;
            // printf("HERE6.5");
            while ((towrite = sdslen(migrationReply) - pos) > 0)
            {

                towrite = (towrite > (64 * 1024) ? (64 * 1024) : towrite);
                nwritten = connSyncWrite(csReply->conn, migrationReply + pos, towrite, timeout);
                if (nwritten != (signed)towrite)
                {
                    printf("failed to write to the socket!");
                    break;
                }
                pos += nwritten;
            }
            sdsfree(migrationReply);
        }
    }

    cs->last_dbid = dbid;
    sdsfree(preCmd->io.buffer.ptr);
    freeCmdResources(cmd, num_keys);
    zfree(preCmd);
    sdsfree(name);
    sdsfree(name2);
    return;

socket_err:

    // Freeing the data
    sdsfree(preCmd->io.buffer.ptr);
    freeCmdResources(cmd, num_keys);
    zfree(preCmd);
    closeMigration(name);
    sds clientErrorReply = sdsempty();
    if (read_error)
    {
        clientErrorReply = sdsnew("SOURCE_SERVER_ERROR : We failed to read select's reply !");
    }
    else if (socket_error)
    {
        clientErrorReply = sdsnew("SOURCE_SERVER_ERROR : We failed to write select's cmd !");
    }
    else if (write_error)
    {
        clientErrorReply = sdsnew("SOURCE_SERVER_ERROR : We failed to write migrate cmd !");
    }

    // We will attempt to send error to the client , however if the connection fails we will kill thread.
    // We expect client to ensure proper socket listener for incomming error messages

    {

        size_t pos = 0, towrite;
        int nwritten = 0;
        while ((towrite = sdslen(clientErrorReply) - pos) > 0)
        {

            towrite = (towrite > (64 * 1024) ? (64 * 1024) : towrite);
            nwritten = connSyncWrite(csReply->conn, clientErrorReply + pos, towrite, timeout);
            if (nwritten != (signed)towrite)
            {
                printf("failed to write to the socket!");
                break;
            }
            pos += nwritten;
        }
    }
    sdsfree(clientErrorReply);
    closeMigration(name2);
}

void migrateThreadStart(void *arg)
{
    migrateArgs *args = (migrateArgs *)arg;

    migreateViaThread(args->c, args->cs, args->csReply, args->cmd,
                      args->timeout, args->num_keys, args->select,
                      args->dbid, args->name, args->name2, args->ultraFast);

    // Free the allocated structure
    zfree(args);
}

/* MIGRATE host port key dbid timeout [COPY | REPLACE | AUTH password |
 *         AUTH2 username password]
 *
 * On in the multiple keys form:
 *
 * MIGRATE host port "" dbid timeout [COPY | REPLACE | AUTH password |
 *         AUTH2 username password] KEYS key1 key2 ... keyN */

void migrateActualCommand(client *c)
{
    printf("SYNC MIGRATION ON THE WAY IN SHA ALLAH \n");
    migrateCachedSocket *cs;
    int copy = 0, replace = 0, j;
    char *username = NULL;
    char *password = NULL;
    long timeout;
    long dbid;
    robj **ov = NULL;      /* Objects to migrate. */
    robj **kv = NULL;      /* Key names. */
    robj **newargv = NULL; /* Used to rewrite the command as DEL ... keys ... */
    rio cmd, payload;
    int may_retry = 1;
    int write_error = 0;
    int argv_rewritten = 0;

    /* To support the KEYS option we need the following additional state. */
    int first_key = 4; /* Argument index of the first key. */
    int num_keys = 1;  /* By default only migrate the 'key' argument. */

    /* Parse additional options */
    for (j = 7; j < c->argc; j++)
    {
        int moreargs = (c->argc - 1) - j;
        if (!strcasecmp(c->argv[j]->ptr, "copy"))
        {
            copy = 1;
        }
        else if (!strcasecmp(c->argv[j]->ptr, "replace"))
        {
            replace = 1;
        }
        else if (!strcasecmp(c->argv[j]->ptr, "auth"))
        {
            if (!moreargs)
            {
                addReplyErrorObject(c, shared.syntaxerr);
                return;
            }
            j++;
            password = c->argv[j]->ptr;
            redactClientCommandArgument(c, j);
        }
        else if (!strcasecmp(c->argv[j]->ptr, "auth2"))
        {
            if (moreargs < 2)
            {
                addReplyErrorObject(c, shared.syntaxerr);
                return;
            }
            username = c->argv[++j]->ptr;
            redactClientCommandArgument(c, j);
            password = c->argv[++j]->ptr;
            redactClientCommandArgument(c, j);
        }
        else if (!strcasecmp(c->argv[j]->ptr, "keys"))
        {
            if (sdslen(c->argv[4]->ptr) != 0 && !strcasecmp(c->argv[j]->ptr, "\"\""))
            {
                addReplyError(c,
                              "When using MIGRATE KEYS option, the key argument"
                              " must be set to the empty string");
                return;
            }
            first_key = j + 1;
            num_keys = c->argc - j - 1;
            break; /* All the remaining args are keys. */
        }
        else
        {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    /* Sanity check */
    if (getLongFromObjectOrReply(c, c->argv[6], &timeout, NULL) != C_OK ||
        getLongFromObjectOrReply(c, c->argv[5], &dbid, NULL) != C_OK)
    {
        return;
    }
    if (timeout <= 0)
        timeout = 1000;

    /* Check if the keys are here. If at least one key is to migrate, do it
     * otherwise if all the keys are missing reply with "NOKEY" to signal
     * the caller there was nothing to migrate. We don't return an error in
     * this case, since often this is due to a normal condition like the key
     * expiring in the meantime. */
    ov = zrealloc(ov, sizeof(robj *) * num_keys);
    kv = zrealloc(kv, sizeof(robj *) * num_keys);
    int oi = 0;

    for (j = 0; j < num_keys; j++)
    {
        if ((ov[oi] = lookupKeyRead(c->db, c->argv[first_key + j])) != NULL)
        {
            kv[oi] = c->argv[first_key + j];
            oi++;
        }
    }
    num_keys = oi;
    if (num_keys == 0)
    {
        zfree(ov);
        zfree(kv);
        addReplySds(c, sdsnew("+NOKEY\r\n"));
        return;
    }

try_again:
    write_error = 0;

    /* Connect */
    cs = migrateGetSocket(c, c->argv[2], c->argv[3], timeout);
    if (cs == NULL)
    {
        zfree(ov);
        zfree(kv);
        return; /* error sent to the client by migrateGetSocket() */
    }

    rioInitWithBuffer(&cmd, sdsempty());

    /* Authentication */
    if (password)
    {
        int arity = username ? 3 : 2;
        serverAssertWithInfo(c, NULL, rioWriteBulkCount(&cmd, '*', arity));
        serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, "AUTH", 4));
        if (username)
        {
            serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, username, sdslen(username)));
        }
        serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, password, sdslen(password)));
    }

    /* Send the SELECT command if the current DB is not already selected. */
    int select = cs->last_dbid != dbid; /* Should we emit SELECT? */
    if (select)
    {
        serverAssertWithInfo(c, NULL, rioWriteBulkCount(&cmd, '*', 2));
        serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, "SELECT", 6));
        serverAssertWithInfo(c, NULL, rioWriteBulkLongLong(&cmd, dbid));
    }

    int non_expired = 0; /* Number of keys that we'll find non expired.
                            Note that serializing large keys may take some time
                            so certain keys that were found non expired by the
                            lookupKey() function, may be expired later. */

    /* Create RESTORE payload and generate the protocol to call the command. */
    for (j = 0; j < num_keys; j++)
    {
        printf("Creating the Command !");
        long long ttl = 0;
        long long expireat = getExpire(c->db, kv[j]);

        if (expireat != -1)
        {
            ttl = expireat - commandTimeSnapshot();
            if (ttl < 0)
            {
                continue;
            }
            if (ttl < 1)
                ttl = 1;
        }

        /* Relocate valid (non expired) keys and values into the array in successive
         * positions to remove holes created by the keys that were present
         * in the first lookup but are now expired after the second lookup. */
        ov[non_expired] = ov[j];
        kv[non_expired++] = kv[j];

        serverAssertWithInfo(c, NULL,
                             rioWriteBulkCount(&cmd, '*', replace ? 5 : 4));

        if (server.cluster_enabled)
            serverAssertWithInfo(c, NULL,
                                 rioWriteBulkString(&cmd, "RESTORE-ASKING", 14));
        else
            serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, "RESTORE", 7));
        serverAssertWithInfo(c, NULL, sdsEncodedObject(kv[j]));
        serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, kv[j]->ptr, sdslen(kv[j]->ptr)));
        serverAssertWithInfo(c, NULL, rioWriteBulkLongLong(&cmd, ttl));

        /* Emit the payload argument, that is the serialized object using
         * the DUMP format. */
        createDumpPayload(&payload, ov[j], kv[j], dbid);
        serverAssertWithInfo(c, NULL,
                             rioWriteBulkString(&cmd, payload.io.buffer.ptr,
                                                sdslen(payload.io.buffer.ptr)));
        sdsfree(payload.io.buffer.ptr);

        /* Add the REPLACE option to the RESTORE command if it was specified
         * as a MIGRATE option. */
        if (replace)
            serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, "REPLACE", 7));
    }

    /* Fix the actual number of keys we are migrating. */
    num_keys = non_expired;

    /* Transfer the query to the other node in 64K chunks. */
    errno = 0;
    {
        sds buf = cmd.io.buffer.ptr;
        size_t pos = 0, towrite;
        int nwritten = 0;

        while ((towrite = sdslen(buf) - pos) > 0)
        {
            towrite = (towrite > (64 * 1024) ? (64 * 1024) : towrite);
            nwritten = connSyncWrite(cs->conn, buf + pos, towrite, timeout);
            if (nwritten != (signed)towrite)
            {
                write_error = 1;
                goto socket_err;
            }
            pos += nwritten;
        }
    }

    char buf0[1024]; /* Auth reply. */
    char buf1[1024]; /* Select reply. */
    char buf2[1024]; /* Restore reply. */

    /* Read the AUTH reply if needed. */
    if (password && connSyncReadLine(cs->conn, buf0, sizeof(buf0), timeout) <= 0)
        goto socket_err;

    /* Read the SELECT reply if needed. */
    if (select && connSyncReadLine(cs->conn, buf1, sizeof(buf1), timeout) <= 0)
        goto socket_err;

    /* Read the RESTORE replies. */
    int error_from_target = 0;
    int socket_error = 0;
    int del_idx = 1; /* Index of the key argument for the replicated DEL op. */

    /* Allocate the new argument vector that will replace the current command,
     * to propagate the MIGRATE as a DEL command (if no COPY option was given).
     * We allocate num_keys+1 because the additional argument is for "DEL"
     * command name itself. */
    if (!copy)
        newargv = zmalloc(sizeof(robj *) * (num_keys + 1));

    for (j = 0; j < num_keys; j++)
    {
        if (connSyncReadLine(cs->conn, buf2, sizeof(buf2), timeout) <= 0)
        {
            socket_error = 1;
            break;
        }
        if ((password && buf0[0] == '-') ||
            (select && buf1[0] == '-') ||
            buf2[0] == '-')
        {
            /* On error assume that last_dbid is no longer valid. */
            if (!error_from_target)
            {
                cs->last_dbid = -1;
                char *errbuf;
                if (password && buf0[0] == '-')
                    errbuf = buf0;
                else if (select && buf1[0] == '-')
                    errbuf = buf1;
                else
                    errbuf = buf2;

                error_from_target = 1;
                addReplyErrorFormat(c, "Target instance replied with error: %s",
                                    errbuf + 1);
            }
        }
        else
        {
            if (!copy)
            {
                /* No COPY option: remove the local key, signal the change. */
                dbDelete(c->db, kv[j]);
                signalModifiedKey(c, c->db, kv[j]);
                notifyKeyspaceEvent(NOTIFY_GENERIC, "del", kv[j], c->db->id);
                server.dirty++;

                /* Populate the argument vector to replace the old one. */
                newargv[del_idx++] = kv[j];
                incrRefCount(kv[j]);
            }
        }
    }

    /* On socket error, if we want to retry, do it now before rewriting the
     * command vector. We only retry if we are sure nothing was processed
     * and we failed to read the first reply (j == 0 test). */
    if (!error_from_target && socket_error && j == 0 && may_retry &&
        errno != ETIMEDOUT)
    {
        goto socket_err; /* A retry is guaranteed because of tested conditions.*/
    }

    /* On socket errors, close the migration socket now that we still have
     * the original host/port in the ARGV. Later the original command may be
     * rewritten to DEL and will be too later. */
    if (socket_error)
        migrateCloseSocket(c->argv[2], c->argv[3]);

    if (!copy)
    {
        /* Translate MIGRATE as DEL for replication/AOF. Note that we do
         * this only for the keys for which we received an acknowledgement
         * from the receiving Redis server, by using the del_idx index. */
        if (del_idx > 1)
        {
            newargv[0] = createStringObject("DEL", 3);
            /* Note that the following call takes ownership of newargv. */
            replaceClientCommandVector(c, del_idx, newargv);
            argv_rewritten = 1;
        }
        else
        {
            /* No key transfer acknowledged, no need to rewrite as DEL. */
            zfree(newargv);
        }
        newargv = NULL; /* Make it safe to call zfree() on it in the future. */
    }

    /* If we are here and a socket error happened, we don't want to retry.
     * Just signal the problem to the client, but only do it if we did not
     * already queue a different error reported by the destination server. */
    if (!error_from_target && socket_error)
    {
        may_retry = 0;
        goto socket_err;
    }

    if (!error_from_target)
    {
        /* Success! Update the last_dbid in migrateCachedSocket, so that we can
         * avoid SELECT the next time if the target DB is the same. Reply +OK.
         *
         * Note: If we reached this point, even if socket_error is true
         * still the SELECT command succeeded (otherwise the code jumps to
         * socket_err label. */
        cs->last_dbid = dbid;
        addReply(c, shared.ok);
    }
    else
    {
        /* On error we already sent it in the for loop above, and set
         * the currently selected socket to -1 to force SELECT the next time. */
    }

    sdsfree(cmd.io.buffer.ptr);
    zfree(ov);
    zfree(kv);
    zfree(newargv);
    return;

    /* On socket errors we try to close the cached socket and try again.
     * It is very common for the cached socket to get closed, if just reopening
     * it works it's a shame to notify the error to the caller. */
socket_err:
    /* Cleanup we want to perform in both the retry and no retry case.
     * Note: Closing the migrate socket will also force SELECT next time. */
    sdsfree(cmd.io.buffer.ptr);

    /* If the command was rewritten as DEL and there was a socket error,
     * we already closed the socket earlier. While migrateCloseSocket()
     * is idempotent, the host/port arguments are now gone, so don't do it
     * again. */
    if (!argv_rewritten)
        migrateCloseSocket(c->argv[2], c->argv[3]);
    zfree(newargv);
    newargv = NULL; /* This will get reallocated on retry. */

    /* Retry only if it's not a timeout and we never attempted a retry
     * (or the code jumping here did not set may_retry to zero). */
    if (errno != ETIMEDOUT && may_retry)
    {
        may_retry = 0;
        goto try_again;
    }

    /* Cleanup we want to do if no retry is attempted. */
    zfree(ov);
    zfree(kv);
    addReplyErrorSds(c, sdscatprintf(sdsempty(),
                                     "-IOERR error or timeout %s to target instance",
                                     write_error ? "writing" : "reading"));
    return;
}

void processKeys(void *args)
{
    ioCmdStruct *mArgs = (ioCmdStruct *)args;
    int oi = 0;
    int startIndex = mArgs->start_key + mArgs->firstKey;
    int endIndex = mArgs->end_key + mArgs->firstKey;
    int totalBulkCount = 0;

    for (int j = startIndex; j < endIndex; j++)
    {

        totalBulkCount = 0;
        if ((mArgs->ov[oi] = lookupKeyRead(mArgs->c->db, mArgs->c->argv[j])) != NULL)
        {

            mArgs->kv[oi] = mArgs->c->argv[j];

            // Get Expiry details
            long long ttl = 0;
            long long expireat = getExpire(mArgs->c->db, mArgs->kv[oi]);
            if (expireat != -1)
            {
                ttl = expireat - commandTimeSnapshot();
                if (ttl < 0)
                    continue;
                if (ttl < 1)
                    ttl = 1;
            }

            // Creating a stream object to send
            mArgs->cmd[oi] = (rio *)zmalloc(sizeof(rio));
            if (!mArgs->cmd[oi])
            {
                // Handle allocation failure
                break;
            }
            rioInitWithBuffer(mArgs->cmd[oi], sdsempty());

            if (mArgs->ultraFast == 1)
            {
                // Here we will create instead a SET Command :
                totalBulkCount = 3;
                serverAssertWithInfo(mArgs->c, NULL,
                                     rioWriteBulkCount(mArgs->cmd[oi], '*', totalBulkCount));

                serverAssertWithInfo(mArgs->c, NULL, sdsEncodedObject(mArgs->kv[oi]));
                serverAssertWithInfo(mArgs->c, NULL, sdsEncodedObject(mArgs->ov[oi]));
                serverAssertWithInfo(mArgs->c, NULL, rioWriteBulkString(mArgs->cmd[oi], "SET", 3));

                serverAssertWithInfo(mArgs->c, NULL, rioWriteBulkString(mArgs->cmd[oi], mArgs->kv[oi]->ptr, sdslen(mArgs->kv[oi]->ptr)));
                serverAssertWithInfo(mArgs->c, NULL, rioWriteBulkString(mArgs->cmd[oi], mArgs->ov[oi]->ptr, sdslen(mArgs->ov[oi]->ptr)));
            }
            else
            {

                if (mArgs->mainThread)
                {
                    if (j == (endIndex - 1))
                    {
                        totalBulkCount = 8;
                    }
                    else
                    {
                        totalBulkCount = 7;
                    }
                }
                else
                {
                    totalBulkCount = 7;
                }

                serverAssertWithInfo(mArgs->c, NULL,
                                     rioWriteBulkCount(mArgs->cmd[oi], '*', mArgs->replace ? (totalBulkCount + 1) : totalBulkCount));

                if (server.cluster_enabled)
                    serverAssertWithInfo(mArgs->c, NULL,
                                         rioWriteBulkString(mArgs->cmd[oi], "RESTORE-ASKING", 14));
                else
                    serverAssertWithInfo(mArgs->c, NULL, rioWriteBulkString(mArgs->cmd[oi], "RESTORE", 7));

                serverAssertWithInfo(mArgs->c, NULL, sdsEncodedObject(mArgs->kv[oi]));
                serverAssertWithInfo(mArgs->c, NULL, rioWriteBulkString(mArgs->cmd[oi], mArgs->kv[oi]->ptr, sdslen(mArgs->kv[oi]->ptr)));
                serverAssertWithInfo(mArgs->c, NULL, rioWriteBulkLongLong(mArgs->cmd[oi], ttl));

                // Emit the payload argument, that is the serialized object using the DUMP format
                createDumpPayload(mArgs->payload, mArgs->ov[oi], mArgs->kv[oi], mArgs->dbid);
                serverAssertWithInfo(mArgs->c, NULL,
                                     rioWriteBulkString(mArgs->cmd[oi], mArgs->payload->io.buffer.ptr,
                                                        sdslen(mArgs->payload->io.buffer.ptr)));
                sdsfree(mArgs->payload->io.buffer.ptr);

                if (mArgs->replace)
                    serverAssertWithInfo(mArgs->c, NULL, rioWriteBulkString(mArgs->cmd[oi], "REPLACE", 7));

                createMeta(3, mArgs->c, mArgs->cmd, oi);

                // In the end we will be passing our meta data :
                if (mArgs->totalKeys == 1 && mArgs->mainThread)
                {
                    createMeta(1, mArgs->c, mArgs->cmd, oi);
                }
                else if (mArgs->mainThread && (j == (endIndex - 1)))
                {
                    createMeta(2, mArgs->c, mArgs->cmd, oi);
                }
            }

            oi++;
        }
        else
        {
            printf("this key with the start index %d wasnt there = %s \n", startIndex, sdsnew(mArgs->c->argv[startIndex + j]->ptr));
        }
    }
    // after the loop ends --> we would need to make rest of the cmds pointers NULL

    // printf("the number of the keys are for start index :%d =  %d \n", startIndex, oi);
    mArgs->num_keys = oi;
    if (mArgs->mainThread == 1)
    {
        return;
    }
    // pthread_exit(NULL);
}

void migrateCommand(client *c)
{

    /*
        We would check in the start that whether this Command is on Weak or Strong Migration
        Remember :
        - Strong Migration means strong consistency hence atomicity is maintained
        - Weak Migration means weak consistency
            - there are two types of weak migrations I will be implementing In Sha Allah :
                1) That Supports Complex Redis Data Structures
                2) That Supports Only String
    */

    int ultraFast = 0;

    if (strcasecmp(c->argv[1]->ptr, "sync") == 0)
    {
        migrateActualCommand(c);
        return;
    }
    else if (strcasecmp(c->argv[1]->ptr, "close") == 0)
    {
        // For running multiple experiments on the same ports we want to get rid of old socket cached connection
        migrateCloseSocket(c->argv[2], c->argv[3]);
        addReplySds(c, sdsnew("+MIGRATE CONNECTION CLOSED\r\n"));
        return;
    }
    else if (strcasecmp(c->argv[1]->ptr, "async-x") == 0)
    {
        ultraFast = 1;
    }

    migrateCachedSocket *cs;
    migrateCachedSocket *csReply;
    int copy = 0, replace = 0, j;
    long timeout;
    long dbid;
    rio **cmd = NULL;
    timeout = 5000000;

    /* To support the KEYS option we need the following additional state. */
    int first_key = 6; /* Argument index of the first key. */
    int num_keys = 1;  /* By default only migrate the 'key' argument. */
    replace = 1;
    /* Parse additional options for keys */
    for (j = 10; j < c->argc; j++)
    {

        if (!strcasecmp(c->argv[j]->ptr, "keys"))
        {
            if (sdslen(c->argv[6]->ptr) != 0 && !strcasecmp(c->argv[j]->ptr, "\"\""))
            {

                addReplyError(c,
                              "When using MIGRATE KEYS option, the key argument"
                              " must be set to the empty string");
                return;
            }
            first_key = j + 1;
            num_keys = c->argc - j - 1;
            break; /* All the remaining args are keys. */
        }
        else
        {
            addReplyErrorObject(c, shared.syntaxerr);
            return;
        }
    }

    /* Sanity check */
    if (getLongFromObjectOrReply(c, c->argv[8], &timeout, NULL) != C_OK ||
        getLongFromObjectOrReply(c, c->argv[7], &dbid, NULL) != C_OK)
    {
        return;
    }

    // We assumed that these keys are present , if not then from processKeys function we will figure it out.

    cmd = zrealloc(cmd, sizeof(rio *) * num_keys);
    if (cmd == NULL)
    {
        printf("We couldn't provide CMD with the proper space ! \n");
        return;
    }

    // SETTING UP THE ENVIRONMENT FOR THREADS FOR SERLIAZING THE KEYS :
    int num_threads = 7;
    if (num_keys < (num_threads + 1))
        num_threads = num_keys - 1;

    ioCmdStruct ioargs[num_threads + 1];

    // Make the number of threads equal to the number of keys if number of keys < 5
    int keys_per_thread = num_keys / (num_threads + 1);
    int final_cmd_index = 0;

    // Initialize and start threads
    for (int i = 0; i < (num_threads + 1); i++)
    {
        int myNumKeys = ((i == (num_threads + 1) - 1) ? num_keys : (i + 1) * keys_per_thread) - (i * keys_per_thread);
        ioargs[i].c = c;
        ioargs[i].payload = (rio *)zmalloc(sizeof(rio));
        ioargs[i].start_key = i * keys_per_thread;
        ioargs[i].end_key = (i == (num_threads + 1) - 1) ? num_keys : (i + 1) * keys_per_thread;
        ioargs[i].num_keys = ((i == (num_threads + 1) - 1) ? num_keys : (i + 1) * keys_per_thread) - (i * keys_per_thread);
        ioargs[i].ov = zrealloc(NULL, sizeof(robj *) * myNumKeys);
        ioargs[i].kv = zrealloc(NULL, sizeof(robj *) * myNumKeys);
        ioargs[i].cmd = zcalloc(sizeof(rio *) * myNumKeys);
        ioargs[i].replace = replace;
        ioargs[i].firstKey = first_key;
        ioargs[i].copy = copy;
        ioargs[i].totalKeys = num_keys;
        ioargs[i].lastKeyIndex = 0;
        ioargs[i].lastKeyExp = -2;
        ioargs[i].ultraFast = ultraFast;

        if (i > 0)
        {
            ioargs[i].mainThread = 0;
            tpool_add_work(tm2, processKeys, (void *)&ioargs[i]);
        }
        else
        {
            ioargs[i].mainThread = 1;
        }
    }

    processKeys((void *)&ioargs[0]);

    // Wait for threads to complete
    if (num_threads > 0)
    {
        tpool_wait(tm2);
        for (int i = 1; i < (num_threads + 1); i++)
        {
            // --> also need to cater the case where the pthread doesn't joins properly
            // pthread_join(threads[i], NULL);
            // Combine results from each thread
            for (int j = 0; j < ioargs[i].num_keys; j++)
            {
                // --> ensure that processKeys make it Null
                if (ioargs[i].cmd[j])
                {
                    cmd[final_cmd_index++] = ioargs[i].cmd[j];
                }
            }

            // Free thread-specific resources
            // freeCmdResources(args[i].cmd, args[i].num_keys);
            // zfree(ioargs[i].cmd);
            zfree(ioargs[i].ov);
            zfree(ioargs[i].kv);
            zfree(ioargs[i].payload);
        }
    }

    // Making the main thread to add the results of the cmd in the end :
    for (int j = 0; j < ioargs[0].num_keys; j++)
    {
        if (ioargs[0].cmd[j])
        {
            cmd[final_cmd_index++] = ioargs[0].cmd[j];
        }
    }

    num_keys = final_cmd_index;
    if (num_keys == 0)
    {
        freeCmdResources(cmd, num_keys);
        zfree(ioargs[0].cmd);
        zfree(ioargs[0].ov);
        zfree(ioargs[0].kv);
        zfree(ioargs[0].payload);
        addReplySds(c, sdsnew("+NOKEY\r\n"));
        return;
    }

    /* Connect */

    // if (handOverException == 0)
    // {
    //     printf("here 1");
    //     cs = migrateGetSocket(c, c->argv[2], c->argv[3], timeout);
    // }
    // else
    // {
    //     connection *conn;
    //     conn = connCreate(connTypeOfCluster());
    //     if (connBlockingConnect(conn, c->argv[2]->ptr, atoi(c->argv[3]->ptr), timeout) != C_OK)
    //     {
    //         printf("Error connecting to the socket !");
    //         addReplyError(c, "-IOERR error or timeout connecting to the client");
    //         connClose(conn);
    //         cs = NULL;
    //     }
    //     if (cs != NULL)
    //     {
    //         connEnableTcpNoDelay(conn);
    //         cs = zmalloc(sizeof(*cs));
    //         cs->last_dbid = -1;
    //     }
    // }
    cs = migrateGetSocket(c, c->argv[2], c->argv[3], timeout);
    int select = cs->last_dbid != dbid;

    // csReply = migrateGetSocket(c, c->argv[4] , c->argv[5], timeout);

    if (ultraFast)
    {
        csReply = migrateGetSocket(c, c->argv[4], c->argv[5], timeout);
    }
    else
    {
        csReply = NULL;
    }

    if (cs == NULL)
    {
        // deleteCmdContents(copyArgv, copyArgc);
        printf("We have reached in here !");
        freeCmdResources(cmd, num_keys);
        zfree(ioargs[0].cmd);
        zfree(ioargs[0].ov);
        zfree(ioargs[0].kv);
        zfree(ioargs[0].payload);
        addReplySds(c, sdsnew("+COULDN'T CREATE SOCKET TO DESTINATION SOCKETS\r\n"));
        migrateCloseSocket(c->argv[2], c->argv[3]);
        migrateCloseSocket(c->argv[4], c->argv[5]);
        return; /* error sent to the client by migrateGetSocket() */
    }

    // --> we may wanna check the expiration of these keys after 5 threads have created the serliased versions
    /* Creating a Migrate Struct for the thread : */

    migrateArgs *args = zmalloc(sizeof(migrateArgs));
    args->c = c;
    args->csReply = csReply;
    args->cs = cs;
    args->cmd = cmd;
    args->timeout = timeout;
    args->num_keys = num_keys;
    args->select = select;
    args->dbid = dbid;
    args->ultraFast = ultraFast;

    // Creating host + port from the client c:
    args->name = sdsempty();
    args->name = sdscatlen(args->name, c->argv[2]->ptr, sdslen(c->argv[2]->ptr));
    args->name = sdscatlen(args->name, ":", 1);
    args->name = sdscatlen(args->name, c->argv[3]->ptr, sdslen(c->argv[3]->ptr));

    args->name2 = sdsempty();
    args->name2 = sdscatlen(args->name2, c->argv[4]->ptr, sdslen(c->argv[4]->ptr));
    args->name2 = sdscatlen(args->name2, ":", 1);
    args->name2 = sdscatlen(args->name2, c->argv[5]->ptr, sdslen(c->argv[5]->ptr));

    // tpool_add_work(tm , migrateThreadStart , args);
    assign_single_key_task(dt, migrateThreadStart, args);

    zfree(ioargs[0].ov);
    zfree(ioargs[0].kv);
    zfree(ioargs[0].payload);

    addReplySds(c, sdsnew("+MIGRATION_THREAD_CREATED\r\n"));
    return;
}

// This function is almost like a wrapper function for the close migration but it is client object independent
void closeMigration(sds name)
{
    migrateCachedSocket *cs;
    cs = dictFetchValue(server.migrate_cached_sockets, name);
    if (!cs)
    {
        sdsfree(name);
        return;
    }

    connClose(cs->conn);
    zfree(cs);
    dictDelete(server.migrate_cached_sockets, name);
    sdsfree(name);
}

/* Cluster node sanity check. Returns C_OK if the node id
 * is valid an C_ERR otherwise. */
int verifyClusterNodeId(const char *name, int length)
{
    if (length != CLUSTER_NAMELEN)
        return C_ERR;
    for (int i = 0; i < length; i++)
    {
        if (name[i] >= 'a' && name[i] <= 'z')
            continue;
        if (name[i] >= '0' && name[i] <= '9')
            continue;
        return C_ERR;
    }
    return C_OK;
}

int isValidAuxChar(int c)
{
    return isalnum(c) || (strchr("!#$%&()*+:;<>?@[]^{|}~", c) == NULL);
}

int isValidAuxString(char *s, unsigned int length)
{
    for (unsigned i = 0; i < length; i++)
    {
        if (!isValidAuxChar(s[i]))
            return 0;
    }
    return 1;
}

void clusterCommandMyId(client *c)
{
    char *name = clusterNodeGetName(getMyClusterNode());
    if (name)
    {
        addReplyBulkCBuffer(c, name, CLUSTER_NAMELEN);
    }
    else
    {
        addReplyError(c, "No ID yet");
    }
}

char *getMyClusterId(void)
{
    return clusterNodeGetName(getMyClusterNode());
}

void clusterCommandMyShardId(client *c)
{
    char *sid = clusterNodeGetShardId(getMyClusterNode());
    if (sid)
    {
        addReplyBulkCBuffer(c, sid, CLUSTER_NAMELEN);
    }
    else
    {
        addReplyError(c, "No shard ID yet");
    }
}

/* When a cluster command is called, we need to decide whether to return TLS info or
 * non-TLS info by the client's connection type. However if the command is called by
 * a Lua script or RM_call, there is no connection in the fake client, so we use
 * server.current_client here to get the real client if available. And if it is not
 * available (modules may call commands without a real client), we return the default
 * info, which is determined by server.tls_cluster. */
static int shouldReturnTlsInfo(void)
{
    if (server.current_client && server.current_client->conn)
    {
        return connIsTLS(server.current_client->conn);
    }
    else
    {
        return server.tls_cluster;
    }
}

unsigned int countKeysInSlot(unsigned int slot)
{
    return kvstoreDictSize(server.db->keys, slot);
}

void clusterCommandHelp(client *c)
{
    const char *help[] = {
        "COUNTKEYSINSLOT <slot>",
        "    Return the number of keys in <slot>.",
        "GETKEYSINSLOT <slot> <count>",
        "    Return key names stored by current node in a slot.",
        "INFO",
        "    Return information about the cluster.",
        "KEYSLOT <key>",
        "    Return the hash slot for <key>.",
        "MYID",
        "    Return the node id.",
        "MYSHARDID",
        "    Return the node's shard id.",
        "NODES",
        "    Return cluster configuration seen by node. Output format:",
        "    <id> <ip:port@bus-port[,hostname]> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ...",
        "REPLICAS <node-id>",
        "    Return <node-id> replicas.",
        "SLOTS",
        "    Return information about slots range mappings. Each range is made of:",
        "    start, end, master and replicas IP addresses, ports and ids",
        "SHARDS",
        "    Return information about slot range mappings and the nodes associated with them.",
        NULL};

    addExtendedReplyHelp(c, help, clusterCommandExtendedHelp());
}

void clusterCommand(client *c)
{
    if (server.cluster_enabled == 0)
    {
        addReplyError(c, "This instance has cluster support disabled");
        return;
    }

    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr, "help"))
    {
        clusterCommandHelp(c);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "nodes") && c->argc == 2)
    {
        /* CLUSTER NODES */
        /* Report TLS ports to TLS client, and report non-TLS port to non-TLS client. */
        sds nodes = clusterGenNodesDescription(c, 0, shouldReturnTlsInfo());
        addReplyVerbatim(c, nodes, sdslen(nodes), "txt");
        sdsfree(nodes);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "myid") && c->argc == 2)
    {
        /* CLUSTER MYID */
        clusterCommandMyId(c);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "myshardid") && c->argc == 2)
    {
        /* CLUSTER MYSHARDID */
        clusterCommandMyShardId(c);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "slots") && c->argc == 2)
    {
        /* CLUSTER SLOTS */
        clusterCommandSlots(c);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "shards") && c->argc == 2)
    {
        /* CLUSTER SHARDS */
        clusterCommandShards(c);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "info") && c->argc == 2)
    {
        /* CLUSTER INFO */

        sds info = genClusterInfoString();

        /* Produce the reply protocol. */
        addReplyVerbatim(c, info, sdslen(info), "txt");
        sdsfree(info);
    }
    else if (!strcasecmp(c->argv[1]->ptr, "keyslot") && c->argc == 3)
    {
        /* CLUSTER KEYSLOT <key> */
        sds key = c->argv[2]->ptr;

        addReplyLongLong(c, keyHashSlot(key, sdslen(key)));
    }
    else if (!strcasecmp(c->argv[1]->ptr, "countkeysinslot") && c->argc == 3)
    {
        /* CLUSTER COUNTKEYSINSLOT <slot> */
        long long slot;

        if (getLongLongFromObjectOrReply(c, c->argv[2], &slot, NULL) != C_OK)
            return;
        if (slot < 0 || slot >= CLUSTER_SLOTS)
        {
            addReplyError(c, "Invalid slot");
            return;
        }
        addReplyLongLong(c, countKeysInSlot(slot));
    }
    else if (!strcasecmp(c->argv[1]->ptr, "getkeysinslot") && c->argc == 4)
    {
        /* CLUSTER GETKEYSINSLOT <slot> <count> */
        long long maxkeys, slot;

        if (getLongLongFromObjectOrReply(c, c->argv[2], &slot, NULL) != C_OK)
            return;
        if (getLongLongFromObjectOrReply(c, c->argv[3], &maxkeys, NULL) != C_OK)
            return;
        if (slot < 0 || slot >= CLUSTER_SLOTS || maxkeys < 0)
        {
            addReplyError(c, "Invalid slot or number of keys");
            return;
        }

        unsigned int keys_in_slot = countKeysInSlot(slot);
        unsigned int numkeys = maxkeys > keys_in_slot ? keys_in_slot : maxkeys;
        addReplyArrayLen(c, numkeys);
        kvstoreDictIterator *kvs_di = NULL;
        dictEntry *de = NULL;
        kvs_di = kvstoreGetDictIterator(server.db->keys, slot);
        for (unsigned int i = 0; i < numkeys; i++)
        {
            de = kvstoreDictIteratorNext(kvs_di);
            serverAssert(de != NULL);
            sds sdskey = dictGetKey(de);
            addReplyBulkCBuffer(c, sdskey, sdslen(sdskey));
        }
        kvstoreReleaseDictIterator(kvs_di);
    }
    else if ((!strcasecmp(c->argv[1]->ptr, "slaves") ||
              !strcasecmp(c->argv[1]->ptr, "replicas")) &&
             c->argc == 3)
    {
        /* CLUSTER SLAVES <NODE ID> */
        /* CLUSTER REPLICAS <NODE ID> */
        clusterNode *n = clusterLookupNode(c->argv[2]->ptr, sdslen(c->argv[2]->ptr));
        int j;

        /* Lookup the specified node in our table. */
        if (!n)
        {
            addReplyErrorFormat(c, "Unknown node %s", (char *)c->argv[2]->ptr);
            return;
        }

        if (clusterNodeIsSlave(n))
        {
            addReplyError(c, "The specified node is not a master");
            return;
        }

        /* Report TLS ports to TLS client, and report non-TLS port to non-TLS client. */
        addReplyArrayLen(c, clusterNodeNumSlaves(n));
        for (j = 0; j < clusterNodeNumSlaves(n); j++)
        {
            sds ni = clusterGenNodeDescription(c, clusterNodeGetSlave(n, j), shouldReturnTlsInfo());
            addReplyBulkCString(c, ni);
            sdsfree(ni);
        }
    }
    else if (!clusterCommandSpecial(c))
    {
        addReplySubcommandSyntaxError(c);
        return;
    }
}

/* Return the pointer to the cluster node that is able to serve the command.
 * For the function to succeed the command should only target either:
 *
 * 1) A single key (even multiple times like RPOPLPUSH mylist mylist).
 * 2) Multiple keys in the same hash slot, while the slot is stable (no
 *    resharding in progress).
 *
 * On success the function returns the node that is able to serve the request.
 * If the node is not 'myself' a redirection must be performed. The kind of
 * redirection is specified setting the integer passed by reference
 * 'error_code', which will be set to CLUSTER_REDIR_ASK or
 * CLUSTER_REDIR_MOVED.
 *
 * When the node is 'myself' 'error_code' is set to CLUSTER_REDIR_NONE.
 *
 * If the command fails NULL is returned, and the reason of the failure is
 * provided via 'error_code', which will be set to:
 *
 * CLUSTER_REDIR_CROSS_SLOT if the request contains multiple keys that
 * don't belong to the same hash slot.
 *
 * CLUSTER_REDIR_UNSTABLE if the request contains multiple keys
 * belonging to the same slot, but the slot is not stable (in migration or
 * importing state, likely because a resharding is in progress).
 *
 * CLUSTER_REDIR_DOWN_UNBOUND if the request addresses a slot which is
 * not bound to any node. In this case the cluster global state should be
 * already "down" but it is fragile to rely on the update of the global state,
 * so we also handle it here.
 *
 * CLUSTER_REDIR_DOWN_STATE and CLUSTER_REDIR_DOWN_RO_STATE if the cluster is
 * down but the user attempts to execute a command that addresses one or more keys. */
clusterNode *getNodeByQuery(client *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, int *error_code)
{
    clusterNode *myself = getMyClusterNode();
    clusterNode *n = NULL;
    robj *firstkey = NULL;
    int multiple_keys = 0;
    multiState *ms, _ms;
    multiCmd mc;
    int i, slot = 0, migrating_slot = 0, importing_slot = 0, missing_keys = 0,
           existing_keys = 0;
    int pubsubshard_included = 0; /* Flag to indicate if a pubsub shard cmd is included. */

    /* Allow any key to be set if a module disabled cluster redirections. */
    if (server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_REDIRECTION)
        return myself;

    /* Set error code optimistically for the base case. */
    if (error_code)
        *error_code = CLUSTER_REDIR_NONE;

    /* Modules can turn off Redis Cluster redirection: this is useful
     * when writing a module that implements a completely different
     * distributed system. */

    /* We handle all the cases as if they were EXEC commands, so we have
     * a common code path for everything */
    if (cmd->proc == execCommand)
    {
        /* If CLIENT_MULTI flag is not set EXEC is just going to return an
         * error. */
        if (!(c->flags & CLIENT_MULTI))
            return myself;
        ms = &c->mstate;
    }
    else
    {
        /* In order to have a single codepath create a fake Multi State
         * structure if the client is not in MULTI/EXEC state, this way
         * we have a single codepath below. */
        ms = &_ms;
        _ms.commands = &mc;
        _ms.count = 1;
        mc.argv = argv;
        mc.argc = argc;
        mc.cmd = cmd;
    }

    /* Check that all the keys are in the same hash slot, and obtain this
     * slot and the node associated. */
    for (i = 0; i < ms->count; i++)
    {
        struct redisCommand *mcmd;
        robj **margv;
        int margc, numkeys, j;
        keyReference *keyindex;

        mcmd = ms->commands[i].cmd;
        margc = ms->commands[i].argc;
        margv = ms->commands[i].argv;

        /* Only valid for sharded pubsub as regular pubsub can operate on any node and bypasses this layer. */
        if (!pubsubshard_included &&
            doesCommandHaveChannelsWithFlags(mcmd, CMD_CHANNEL_PUBLISH | CMD_CHANNEL_SUBSCRIBE))
        {
            pubsubshard_included = 1;
        }

        getKeysResult result = GETKEYS_RESULT_INIT;
        numkeys = getKeysFromCommand(mcmd, margv, margc, &result);
        keyindex = result.keys;

        for (j = 0; j < numkeys; j++)
        {
            robj *thiskey = margv[keyindex[j].pos];
            int thisslot = keyHashSlot((char *)thiskey->ptr,
                                       sdslen(thiskey->ptr));

            if (firstkey == NULL)
            {
                /* This is the first key we see. Check what is the slot
                 * and node. */
                firstkey = thiskey;
                slot = thisslot;
                n = getNodeBySlot(slot);

                /* Error: If a slot is not served, we are in "cluster down"
                 * state. However the state is yet to be updated, so this was
                 * not trapped earlier in processCommand(). Report the same
                 * error to the client. */
                if (n == NULL)
                {
                    getKeysFreeResult(&result);
                    if (error_code)
                        *error_code = CLUSTER_REDIR_DOWN_UNBOUND;
                    return NULL;
                }

                /* If we are migrating or importing this slot, we need to check
                 * if we have all the keys in the request (the only way we
                 * can safely serve the request, otherwise we return a TRYAGAIN
                 * error). To do so we set the importing/migrating state and
                 * increment a counter for every missing key. */
                if (n == myself &&
                    getMigratingSlotDest(slot) != NULL)
                {
                    migrating_slot = 1;
                }
                else if (getImportingSlotSource(slot) != NULL)
                {
                    importing_slot = 1;
                }
            }
            else
            {
                /* If it is not the first key/channel, make sure it is exactly
                 * the same key/channel as the first we saw. */
                if (slot != thisslot)
                {
                    /* Error: multiple keys from different slots. */
                    getKeysFreeResult(&result);
                    if (error_code)
                        *error_code = CLUSTER_REDIR_CROSS_SLOT;
                    return NULL;
                }
                if (importing_slot && !multiple_keys && !equalStringObjects(firstkey, thiskey))
                {
                    /* Flag this request as one with multiple different
                     * keys/channels when the slot is in importing state. */
                    multiple_keys = 1;
                }
            }

            /* Migrating / Importing slot? Count keys we don't have.
             * If it is pubsubshard command, it isn't required to check
             * the channel being present or not in the node during the
             * slot migration, the channel will be served from the source
             * node until the migration completes with CLUSTER SETSLOT <slot>
             * NODE <node-id>. */
            int flags = LOOKUP_NOTOUCH | LOOKUP_NOSTATS | LOOKUP_NONOTIFY | LOOKUP_NOEXPIRE;
            if ((migrating_slot || importing_slot) && !pubsubshard_included)
            {
                if (lookupKeyReadWithFlags(&server.db[0], thiskey, flags) == NULL)
                    missing_keys++;
                else
                    existing_keys++;
            }
        }
        getKeysFreeResult(&result);
    }

    /* No key at all in command? then we can serve the request
     * without redirections or errors in all the cases. */
    if (n == NULL)
        return myself;

    uint64_t cmd_flags = getCommandFlags(c);
    /* Cluster is globally down but we got keys? We only serve the request
     * if it is a read command and when allow_reads_when_down is enabled. */
    if (!isClusterHealthy())
    {
        if (pubsubshard_included)
        {
            if (!server.cluster_allow_pubsubshard_when_down)
            {
                if (error_code)
                    *error_code = CLUSTER_REDIR_DOWN_STATE;
                return NULL;
            }
        }
        else if (!server.cluster_allow_reads_when_down)
        {
            /* The cluster is configured to block commands when the
             * cluster is down. */
            if (error_code)
                *error_code = CLUSTER_REDIR_DOWN_STATE;
            return NULL;
        }
        else if (cmd_flags & CMD_WRITE)
        {
            /* The cluster is configured to allow read only commands */
            if (error_code)
                *error_code = CLUSTER_REDIR_DOWN_RO_STATE;
            return NULL;
        }
        else
        {
            /* Fall through and allow the command to be executed:
             * this happens when server.cluster_allow_reads_when_down is
             * true and the command is not a write command */
        }
    }

    /* Return the hashslot by reference. */
    if (hashslot)
        *hashslot = slot;

    /* MIGRATE always works in the context of the local node if the slot
     * is open (migrating or importing state). We need to be able to freely
     * move keys among instances in this case. */
    if ((migrating_slot || importing_slot) && cmd->proc == migrateCommand)
        return myself;

    /* If we don't have all the keys and we are migrating the slot, send
     * an ASK redirection or TRYAGAIN. */
    if (migrating_slot && missing_keys)
    {
        /* If we have keys but we don't have all keys, we return TRYAGAIN */
        if (existing_keys)
        {
            if (error_code)
                *error_code = CLUSTER_REDIR_UNSTABLE;
            return NULL;
        }
        else
        {
            if (error_code)
                *error_code = CLUSTER_REDIR_ASK;
            return getMigratingSlotDest(slot);
        }
    }

    /* If we are receiving the slot, and the client correctly flagged the
     * request as "ASKING", we can serve the request. However if the request
     * involves multiple keys and we don't have them all, the only option is
     * to send a TRYAGAIN error. */
    if (importing_slot &&
        (c->flags & CLIENT_ASKING || cmd_flags & CMD_ASKING))
    {
        if (multiple_keys && missing_keys)
        {
            if (error_code)
                *error_code = CLUSTER_REDIR_UNSTABLE;
            return NULL;
        }
        else
        {
            return myself;
        }
    }

    /* Handle the read-only client case reading from a slave: if this
     * node is a slave and the request is about a hash slot our master
     * is serving, we can reply without redirection. */
    int is_write_command = (cmd_flags & CMD_WRITE) ||
                           (c->cmd->proc == execCommand && (c->mstate.cmd_flags & CMD_WRITE));
    if (((c->flags & CLIENT_READONLY) || pubsubshard_included) &&
        !is_write_command &&
        clusterNodeIsSlave(myself) &&
        clusterNodeGetSlaveof(myself) == n)
    {
        return myself;
    }

    /* Base case: just return the right node. However, if this node is not
     * myself, set error_code to MOVED since we need to issue a redirection. */
    if (n != myself && error_code)
        *error_code = CLUSTER_REDIR_MOVED;
    return n;
}

/* Send the client the right redirection code, according to error_code
 * that should be set to one of CLUSTER_REDIR_* macros.
 *
 * If CLUSTER_REDIR_ASK or CLUSTER_REDIR_MOVED error codes
 * are used, then the node 'n' should not be NULL, but should be the
 * node we want to mention in the redirection. Moreover hashslot should
 * be set to the hash slot that caused the redirection. */
void clusterRedirectClient(client *c, clusterNode *n, int hashslot, int error_code)
{
    if (error_code == CLUSTER_REDIR_CROSS_SLOT)
    {
        addReplyError(c, "-CROSSSLOT Keys in request don't hash to the same slot");
    }
    else if (error_code == CLUSTER_REDIR_UNSTABLE)
    {
        /* The request spawns multiple keys in the same slot,
         * but the slot is not "stable" currently as there is
         * a migration or import in progress. */
        addReplyError(c, "-TRYAGAIN Multiple keys request during rehashing of slot");
    }
    else if (error_code == CLUSTER_REDIR_DOWN_STATE)
    {
        addReplyError(c, "-CLUSTERDOWN The cluster is down");
    }
    else if (error_code == CLUSTER_REDIR_DOWN_RO_STATE)
    {
        addReplyError(c, "-CLUSTERDOWN The cluster is down and only accepts read commands");
    }
    else if (error_code == CLUSTER_REDIR_DOWN_UNBOUND)
    {
        addReplyError(c, "-CLUSTERDOWN Hash slot not served");
    }
    else if (error_code == CLUSTER_REDIR_MOVED ||
             error_code == CLUSTER_REDIR_ASK)
    {
        /* Report TLS ports to TLS client, and report non-TLS port to non-TLS client. */
        int port = clusterNodeClientPort(n, shouldReturnTlsInfo());
        addReplyErrorSds(c, sdscatprintf(sdsempty(),
                                         "-%s %d %s:%d",
                                         (error_code == CLUSTER_REDIR_ASK) ? "ASK" : "MOVED",
                                         hashslot, clusterNodePreferredEndpoint(n), port));
    }
    else
    {
        serverPanic("getNodeByQuery() unknown error.");
    }
}

/* This function is called by the function processing clients incrementally
 * to detect timeouts, in order to handle the following case:
 *
 * 1) A client blocks with BLPOP or similar blocking operation.
 * 2) The master migrates the hash slot elsewhere or turns into a slave.
 * 3) The client may remain blocked forever (or up to the max timeout time)
 *    waiting for a key change that will never happen.
 *
 * If the client is found to be blocked into a hash slot this node no
 * longer handles, the client is sent a redirection error, and the function
 * returns 1. Otherwise 0 is returned and no operation is performed. */
int clusterRedirectBlockedClientIfNeeded(client *c)
{
    clusterNode *myself = getMyClusterNode();
    if (c->flags & CLIENT_BLOCKED &&
        (c->bstate.btype == BLOCKED_LIST ||
         c->bstate.btype == BLOCKED_ZSET ||
         c->bstate.btype == BLOCKED_STREAM ||
         c->bstate.btype == BLOCKED_MODULE))
    {
        dictEntry *de;
        dictIterator *di;

        /* If the cluster is down, unblock the client with the right error.
         * If the cluster is configured to allow reads on cluster down, we
         * still want to emit this error since a write will be required
         * to unblock them which may never come.  */
        if (!isClusterHealthy())
        {
            clusterRedirectClient(c, NULL, 0, CLUSTER_REDIR_DOWN_STATE);
            return 1;
        }

        /* If the client is blocked on module, but not on a specific key,
         * don't unblock it (except for the CLUSTER_FAIL case above). */
        if (c->bstate.btype == BLOCKED_MODULE && !moduleClientIsBlockedOnKeys(c))
            return 0;

        /* All keys must belong to the same slot, so check first key only. */
        di = dictGetIterator(c->bstate.keys);
        if ((de = dictNext(di)) != NULL)
        {
            robj *key = dictGetKey(de);
            int slot = keyHashSlot((char *)key->ptr, sdslen(key->ptr));
            clusterNode *node = getNodeBySlot(slot);

            /* if the client is read-only and attempting to access key that our
             * replica can handle, allow it. */
            if ((c->flags & CLIENT_READONLY) &&
                !(c->lastcmd->flags & CMD_WRITE) &&
                clusterNodeIsSlave(myself) && clusterNodeGetSlaveof(myself) == node)
            {
                node = myself;
            }

            /* We send an error and unblock the client if:
             * 1) The slot is unassigned, emitting a cluster down error.
             * 2) The slot is not handled by this node, nor being imported. */
            if (node != myself && getImportingSlotSource(slot) == NULL)
            {
                if (node == NULL)
                {
                    clusterRedirectClient(c, NULL, 0,
                                          CLUSTER_REDIR_DOWN_UNBOUND);
                }
                else
                {
                    clusterRedirectClient(c, node, slot,
                                          CLUSTER_REDIR_MOVED);
                }
                dictReleaseIterator(di);
                return 1;
            }
        }
        dictReleaseIterator(di);
    }
    return 0;
}

/* Returns an indication if the replica node is fully available
 * and should be listed in CLUSTER SLOTS response.
 * Returns 1 for available nodes, 0 for nodes that have
 * not finished their initial sync, in failed state, or are
 * otherwise considered not available to serve read commands. */
static int isReplicaAvailable(clusterNode *node)
{
    if (clusterNodeIsFailing(node))
    {
        return 0;
    }
    long long repl_offset = clusterNodeReplOffset(node);
    if (clusterNodeIsMyself(node))
    {
        /* Nodes do not update their own information
         * in the cluster node list. */
        repl_offset = replicationGetSlaveOffset();
    }
    return (repl_offset != 0);
}

void addNodeToNodeReply(client *c, clusterNode *node)
{
    char *hostname = clusterNodeHostname(node);
    addReplyArrayLen(c, 4);
    if (server.cluster_preferred_endpoint_type == CLUSTER_ENDPOINT_TYPE_IP)
    {
        addReplyBulkCString(c, clusterNodeIp(node));
    }
    else if (server.cluster_preferred_endpoint_type == CLUSTER_ENDPOINT_TYPE_HOSTNAME)
    {
        if (hostname != NULL && hostname[0] != '\0')
        {
            addReplyBulkCString(c, hostname);
        }
        else
        {
            addReplyBulkCString(c, "?");
        }
    }
    else if (server.cluster_preferred_endpoint_type == CLUSTER_ENDPOINT_TYPE_UNKNOWN_ENDPOINT)
    {
        addReplyNull(c);
    }
    else
    {
        serverPanic("Unrecognized preferred endpoint type");
    }

    /* Report TLS ports to TLS client, and report non-TLS port to non-TLS client. */
    addReplyLongLong(c, clusterNodeClientPort(node, shouldReturnTlsInfo()));
    addReplyBulkCBuffer(c, clusterNodeGetName(node), CLUSTER_NAMELEN);

    /* Add the additional endpoint information, this is all the known networking information
     * that is not the preferred endpoint. Note the logic is evaluated twice so we can
     * correctly report the number of additional network arguments without using a deferred
     * map, an assertion is made at the end to check we set the right length. */
    int length = 0;
    if (server.cluster_preferred_endpoint_type != CLUSTER_ENDPOINT_TYPE_IP)
    {
        length++;
    }
    if (server.cluster_preferred_endpoint_type != CLUSTER_ENDPOINT_TYPE_HOSTNAME && hostname != NULL && hostname[0] != '\0')
    {
        length++;
    }
    addReplyMapLen(c, length);

    if (server.cluster_preferred_endpoint_type != CLUSTER_ENDPOINT_TYPE_IP)
    {
        addReplyBulkCString(c, "ip");
        addReplyBulkCString(c, clusterNodeIp(node));
        length--;
    }
    if (server.cluster_preferred_endpoint_type != CLUSTER_ENDPOINT_TYPE_HOSTNAME && hostname != NULL && hostname[0] != '\0')
    {
        addReplyBulkCString(c, "hostname");
        addReplyBulkCString(c, hostname);
        length--;
    }
    serverAssert(length == 0);
}

void addNodeReplyForClusterSlot(client *c, clusterNode *node, int start_slot, int end_slot)
{
    int i, nested_elements = 3; /* slots (2) + master addr (1) */
    for (i = 0; i < clusterNodeNumSlaves(node); i++)
    {
        if (!isReplicaAvailable(clusterNodeGetSlave(node, i)))
            continue;
        nested_elements++;
    }
    addReplyArrayLen(c, nested_elements);
    addReplyLongLong(c, start_slot);
    addReplyLongLong(c, end_slot);
    addNodeToNodeReply(c, node);

    /* Remaining nodes in reply are replicas for slot range */
    for (i = 0; i < clusterNodeNumSlaves(node); i++)
    {
        /* This loop is copy/pasted from clusterGenNodeDescription()
         * with modifications for per-slot node aggregation. */
        if (!isReplicaAvailable(clusterNodeGetSlave(node, i)))
            continue;
        addNodeToNodeReply(c, clusterNodeGetSlave(node, i));
        nested_elements--;
    }
    serverAssert(nested_elements == 3); /* Original 3 elements */
}

void clusterCommandSlots(client *c)
{
    /* Format: 1) 1) start slot
     *            2) end slot
     *            3) 1) master IP
     *               2) master port
     *               3) node ID
     *            4) 1) replica IP
     *               2) replica port
     *               3) node ID
     *           ... continued until done
     */
    clusterNode *n = NULL;
    int num_masters = 0, start = -1;
    void *slot_replylen = addReplyDeferredLen(c);

    for (int i = 0; i <= CLUSTER_SLOTS; i++)
    {
        /* Find start node and slot id. */
        if (n == NULL)
        {
            if (i == CLUSTER_SLOTS)
                break;
            n = getNodeBySlot(i);
            start = i;
            continue;
        }

        /* Add cluster slots info when occur different node with start
         * or end of slot. */
        if (i == CLUSTER_SLOTS || n != getNodeBySlot(i))
        {
            addNodeReplyForClusterSlot(c, n, start, i - 1);
            num_masters++;
            if (i == CLUSTER_SLOTS)
                break;
            n = getNodeBySlot(i);
            start = i;
        }
    }
    setDeferredArrayLen(c, slot_replylen, num_masters);
}

/* -----------------------------------------------------------------------------
 * Cluster functions related to serving / redirecting clients
 * -------------------------------------------------------------------------- */

/* The ASKING command is required after a -ASK redirection.
 * The client should issue ASKING before to actually send the command to
 * the target instance. See the Redis Cluster specification for more
 * information. */
void askingCommand(client *c)
{
    if (server.cluster_enabled == 0)
    {
        addReplyError(c, "This instance has cluster support disabled");
        return;
    }
    c->flags |= CLIENT_ASKING;
    addReply(c, shared.ok);
}

/* The READONLY command is used by clients to enter the read-only mode.
 * In this mode slaves will not redirect clients as long as clients access
 * with read-only commands to keys that are served by the slave's master. */
void readonlyCommand(client *c)
{
    if (server.cluster_enabled == 0)
    {
        addReplyError(c, "This instance has cluster support disabled");
        return;
    }
    c->flags |= CLIENT_READONLY;
    addReply(c, shared.ok);
}

/* The READWRITE command just clears the READONLY command state. */
void readwriteCommand(client *c)
{
    if (server.cluster_enabled == 0)
    {
        addReplyError(c, "This instance has cluster support disabled");
        return;
    }
    c->flags &= ~CLIENT_READONLY;
    addReply(c, shared.ok);
}
