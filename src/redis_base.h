/** @file
 * @brief RedisBase2 : redis base interface
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#ifndef _LANGTAOJIN_LIBREDIS_REDIS_BASE_H_
#define _LANGTAOJIN_LIBREDIS_REDIS_BASE_H_

#include "redis_cmd.h"

LIBREDIS_NAMESPACE_BEGIN

struct ZRangebyscoreLimit
{
  int64_t offset;
  int64_t count;
};

enum kZUnionstoreAggregate
{
  kSum,
  kMin,
  kMax
};

/************************************************************************/
/**
 *                       API interface design
 * All commands
 * return true for success(a non-erroneous reply),
 * return false for failure(network error, an erroneous reply, or an erroneous status).
 *
 * for Integer reply:
 *   'int64_t * _return' is the output
 * for Multi-bulk reply:
 *   'mbulk_t * _return' is the output
 *   NOTICE: 'mbulk_t * _return' must be 'clear_mbulks' or 'delete_mbulks'
 * for Special Multi-bulk reply:
 *   No interface supported, use exec_command instead. Deal with the output yourself.
 * for Bulk reply:
 *   1.reply may a nil object: 'std::string * _return, bool * is_nil' is the output
 *   2.reply may not a nil object: 'std::string * _return' is the output
 *   3.reply contains a float number in the bulk: 'double * _return' is the output
 * for Status reply:
 *   1.status string will be convert to the return value 'true or false'
 *   2.'std::string * _return' is the output
 * for Error reply:
 *   error string will be stored, and invoke last_error() to get it
 */
/************************************************************************/
class RedisBase2
{
  public:
    virtual ~RedisBase2();

    /************************************************************************/
    /*old style API(to be deprecated)*/
    // return 1 ok
    // return 0 if nil is returned
    // return -1 if failed
    int get(const std::string& key, std::string * value);
    // return 1 ok
    // return 0 if 'key' does not exists
    // return -1 failed
    int expire(const std::string& key, int64_t seconds);
    int expireat(const std::string& key, int64_t abs_seconds);
    bool keys(const std::string& pattern, string_vector_t * keys);
    // return 1 ok
    // return 0 if nil is returned
    // return -1 failed
    int hget(const std::string& key, const std::string& field, std::string * value);
    bool hset(const std::string& key, const std::string& field,
        const std::string& value);
    // return 1 ok
    // return 0 if 'key'/'field' does not exists
    // return -1 failed
    int hdel(const std::string& key, const std::string& field);
    // hgetall does not return nil
    bool hgetall(const std::string& key, string_pair_vector_t * fields_and_values);
    bool hgetall(const std::string& key, string_map_t * fields_and_values);
    bool hmset(const std::string& key, const string_pair_vector_t& fields_and_values);
    bool hmset(const std::string& key, const string_map_t& fields_and_values);
    /*old style API(to be deprecated)*/
    /************************************************************************/

    // NOTICE: all successful operations shall not clear previous error,
    // but any failures will set error.
    // Retrieving error on failures is reasonable.
    virtual void last_error(const std::string& err) = 0;
    virtual std::string last_error()const = 0;
    virtual const char * last_c_error()const = 0;

    /************************************************************************/
    /*KEYS command*/
    /************************************************************************/
    // _return the number of keys that were removed
    virtual bool del(const std::string& key, int64_t * _return) = 0;
    virtual bool del(const string_vector_t& keys, int64_t * _return) = 0;
    // >= 2.6
    // _return the serialized value of 'key'
    virtual bool dump(const std::string& key, std::string * _return, bool * is_nil) = 0;
    // _return 1, 'key' exists
    // _return 0, 'key' does not exist
    virtual bool exists(const std::string& key, int64_t * _return) = 0;
    // _return 1, the timeout was set
    // _return 0, 'key' does not exist or the timeout could not be set
    virtual bool expire(const std::string& key, int64_t seconds, int64_t * _return) = 0;
    virtual bool expireat(const std::string& key, int64_t abs_seconds, int64_t * _return) = 0;
    virtual bool keys(const std::string& pattern, mbulk_t * _return) = 0;
    // _return 1, 'key' was moved
    // _return 0, 'key' was not moved
    virtual bool move(const std::string& key, int db, int64_t * _return) = 0;
    // _return 1, the timeout was removed
    // _return 0, 'key' does not exist or does not have an associated timeout
    virtual bool persist(const std::string& key, int64_t * _return) = 0;

    // >= 2.6
    // _return 1, the timeout was set
    // _return 0, 'key' does not exist or the timeout could not be set
    virtual bool pexpire(const std::string& key, int64_t milliseconds, int64_t * _return) = 0;
    // >= 2.6
    virtual bool pexpireat(const std::string& key, int64_t abs_milliseconds,
        int64_t * _return) = 0;
    // >= 2.6
    // _return time to live in milliseconds
    // _return -1, 'key' does not exist or does not have a timeout
    virtual bool pttl(const std::string& key, int64_t * _return) = 0;

    virtual bool randomkey(std::string * _return, bool * is_nil) = 0;
    // >= 2.6
    virtual bool restore(const std::string& key, int64_t ttl, const std::string& value) = 0;
    virtual bool sort(const std::string& key, const string_vector_t * phrases,
        mbulk_t * _return) = 0;
    // _return time to live in seconds
    // _return -1, 'key' does not exist or does not have a timeout
    virtual bool ttl(const std::string& key, int64_t * _return) = 0;
    virtual bool type(const std::string& key, std::string * _return) = 0;

    /************************************************************************/
    /*String command*/
    /************************************************************************/
    // _return the length of the string after the append operation
    virtual bool append(const std::string& key, const std::string& value, int64_t * _return) = 0;
    // _return the value of 'key' after the decrement
    virtual bool decr(const std::string& key, int64_t * _return) = 0;
    virtual bool decrby(const std::string& key, int64_t dec, int64_t * _return) = 0;
    virtual bool get(const std::string& key, std::string * _return, bool * is_nil) = 0;
    virtual bool getbit(const std::string& key, int64_t offset, int64_t * _return) = 0;
    virtual bool getrange(const std::string& key, int64_t start, int64_t end,
        std::string * _return, bool * is_nil) = 0;
    virtual bool getset(const std::string& key, const std::string& value,
        std::string * _return, bool * is_nil) = 0;
    // _return the value of 'key' after the increment
    virtual bool incr(const std::string& key, int64_t * _return) = 0;
    virtual bool incrby(const std::string& key, int64_t inc, int64_t * _return) = 0;
    // >= 2.6
    virtual bool incrbyfloat(const std::string& key, double inc, double * _return) = 0;
    // NOTICE: if return true, keys.size()==values->size()
    virtual bool mget(const string_vector_t& keys, mbulk_t * _return) = 0;
    virtual bool mset(const string_vector_t& keys, const string_vector_t& values) = 0;
    // >= 2.6
    virtual bool psetex(const std::string& key, int64_t milliseconds,
        const std::string& value) = 0;
    virtual bool set(const std::string& key, const std::string& value) = 0;
    // _return the original bit value stored at offset
    virtual bool setbit(const std::string& key, int64_t offset, int64_t value,
        int64_t * _return) = 0;
    virtual bool setex(const std::string& key, int64_t seconds,
        const std::string& value) = 0;
    // _return 1, 'key' was set
    // _return 0, 'key' was not set
    virtual bool setnx(const std::string& key, const std::string& value,
        int64_t * _return) = 0;
    // _return the length of the string after it was modified by the command
    virtual bool setrange(const std::string& key, int64_t offset,
        const std::string& value, int64_t * _return) = 0;
    // _return the length of the string at 'key'
    // _return 0, when 'key' does not exist
    virtual bool strlen(const std::string& key, int64_t * _return) = 0;

    /************************************************************************/
    /*Hashes command*/
    /************************************************************************/
    // _return the number of fields that were removed from the hash
    // _return 0, 'key'/'field' does not exists
    virtual bool hdel(const std::string& key, const std::string& field,
        int64_t * _return) = 0;
    // >= 2.4
    virtual bool hdel(const std::string& key, const string_vector_t& fields,
        int64_t * _return) = 0;
    // _return 1, the hash contains 'field'
    // _return 0, the hash does not contain 'field', or 'key' does not exist
    virtual bool hexists(const std::string& key, const std::string& field,
        int64_t * _return) = 0;
    virtual bool hget(const std::string& key, const std::string& field,
        std::string * _return, bool * is_nil) = 0;
    virtual bool hgetall(const std::string& key, mbulk_t * _return) = 0;
    // _return the value of 'key'/'field' after the increment
    virtual bool hincr(const std::string& key, const std::string& field,
        int64_t * _return) = 0;
    virtual bool hincrby(const std::string& key, const std::string& field,
        int64_t inc, int64_t * _return) = 0;
    // >= 2.6
    virtual bool hincrbyfloat(const std::string& key, const std::string& field,
        double inc, double * _return) = 0;
    virtual bool hkeys(const std::string& key, mbulk_t * _return) = 0;
    // _return number of fields in the hash
    // _return 0, 'key' does not exist
    virtual bool hlen(const std::string& key, int64_t * _return) = 0;
    // NOTICE: if return true, 'fields.size()==values->size()' is true
    virtual bool hmget(const std::string& key, const string_vector_t& fields,
        mbulk_t * _return) = 0;
    virtual bool hmset(const std::string& key,
        const string_vector_t& fields, const string_vector_t& values) = 0;
    virtual bool hset(const std::string& key, const std::string& field,
        const std::string& value, int64_t * _return) = 0;
    // _return 1, 'field' is a new field in the hash and 'value' was set
    // _return 0, 'field' already exists in the hash and no operation was performed
    virtual bool hsetnx(const std::string& key, const std::string& field,
        const std::string& value, int64_t * _return) = 0;
    virtual bool hvals(const std::string& key, mbulk_t * _return) = 0;

    /************************************************************************/
    /*Lists command*/
    /************************************************************************/
    virtual bool lindex(const std::string& key, int64_t index,
        std::string * _return, bool * is_nil) = 0;
    // _return the length of the list after the insert operation
    // _return -1, when the value pivot was not found
    virtual bool linsert(const std::string& key, bool before,
        const std::string& pivot, const std::string& value, int64_t * _return) = 0;
    // _return the length of the list at 'key'
    virtual bool llen(const std::string& key, int64_t * _return) = 0;
    virtual bool lpop(const std::string& key, std::string * _return, bool * is_nil) = 0;
    // _return the length of the list after the push operations
    virtual bool lpush(const std::string& key, const std::string& value,
        int64_t * _return) = 0;
    // >= 2.4
    virtual bool lpush(const std::string& key, const string_vector_t& values,
        int64_t * _return) = 0;
    // _return the length of the list after the push operations
    virtual bool lpushx(const std::string& key, const std::string& value,
        int64_t * _return) = 0;
    virtual bool lrange(const std::string& key, int64_t start, int64_t stop,
        mbulk_t * _return) = 0;
    // count>0: remove elements equal to value moving from head to tail
    // count<0: remove elements equal to value moving from tail to head
    // count=0: remove all elements equal to value
    // _return the number of removed elements
    virtual bool lrem(const std::string& key, int64_t count, const std::string& value,
        int64_t * _return) = 0;
    virtual bool lset(const std::string& key, int64_t index,
        const std::string& value) = 0;
    virtual bool ltrim(const std::string& key, int64_t start, int64_t stop) = 0;
    virtual bool rpop(const std::string& key, std::string * _return, bool * is_nil) = 0;
    // _return the length of the list after the push operations
    virtual bool rpush(const std::string& key, const std::string& value,
        int64_t * _return) = 0;
    // >= 2.4
    virtual bool rpush(const std::string& key, const string_vector_t& values,
        int64_t * _return) = 0;
    // _return the length of the list after the push operations
    virtual bool rpushx(const std::string& key, const std::string& value,
        int64_t * _return) = 0;

    /************************************************************************/
    /*Sets command*/
    /************************************************************************/
    // _return the number of elements that were added to the set
    virtual bool sadd(const std::string& key, const std::string& member,
        int64_t * _return) = 0;
    // >= 2.4
    virtual bool sadd(const std::string& key, const string_vector_t& members,
        int64_t * _return) = 0;
    // _return the cardinality (number of elements) of the set
    // _return 0, 'key' does not exist
    virtual bool scard(const std::string& key, int64_t * _return) = 0;
    // _return 1, the element is a member of the set
    // _return 0, the element is not a member of the set, or 'key' does not exist
    virtual bool sismember(const std::string& key, const std::string& member,
        int64_t * _return) = 0;
    virtual bool smembers(const std::string& key, mbulk_t * _return) = 0;
    virtual bool spop(const std::string& key, std::string * _return, bool * is_nil) = 0;
    virtual bool srandmember(const std::string& key,
        std::string * _return, bool * is_nil) = 0;
    // _return the number of members that were removed from the set
    virtual bool srem(const std::string& key, const std::string& member,
        int64_t * _return) = 0;
    // >= 2.4
    virtual bool srem(const std::string& key, const string_vector_t& members,
        int64_t * _return) = 0;

    /************************************************************************/
    /*Sorted Sets command*/
    /************************************************************************/
    // _return the number of elements added to the sorted sets
    virtual bool zadd(const std::string& key, double score,
        const std::string& member, int64_t * _return) = 0;
    // >=2.4
    virtual bool zadd(const std::string& key, std::vector<double>& scores,
        const string_vector_t& members, int64_t * _return) = 0;
    // _return the cardinality (number of elements) of the sorted set
    // _return 0, 'key' does not exist
    virtual bool zcard(const std::string& key, int64_t * _return) = 0;
    // _return the number of elements in the specified score range
    virtual bool zcount(const std::string& key,
        const std::string& _min, const std::string& _max, int64_t * _return) = 0;
    // _return the new score of member
    virtual bool zincrby(const std::string& key, double increment,
        const std::string& member, double * _return) = 0;
    virtual bool zrange(const std::string& key, int64_t start, int64_t stop,
        bool withscores, mbulk_t * _return) = 0;
    virtual bool zrevrange(const std::string& key, int64_t start, int64_t stop,
        bool withscores, mbulk_t * _return) = 0;
    virtual bool zrangebyscore(const std::string& key,
        const std::string& _min, const std::string& _max,
        bool withscores, const ZRangebyscoreLimit * limit,
        mbulk_t * _return) = 0;
    virtual bool zrevrangebyscore(const std::string& key,
        const std::string& _max, const std::string& _min,
        bool withscores, const ZRangebyscoreLimit * limit,
        mbulk_t * _return) = 0;
    // _return the rank of member, if member exists in the sorted set
    // or
    // is_nil is set, if member does not exist in the sorted set or 'key' does not exist
    virtual bool zrank(const std::string& key, const std::string& member,
        int64_t * _return, bool * not_exists) = 0;
    virtual bool zrevrank(const std::string& key, const std::string& member,
        int64_t * _return, bool * not_exists) = 0;
    // _return the number of members removed from the sorted set
    virtual bool zrem(const std::string& key, const std::string& member,
        int64_t * _return) = 0;
    // >=2.4
    virtual bool zrem(const std::string& key, const string_vector_t& members,
        int64_t * _return) = 0;
    // _return the number of elements removed
    virtual bool zremrangebyrank(const std::string& key,
        int64_t start, int64_t stop, int64_t * _return) = 0;
    // _return the number of elements removed
    virtual bool zremrangebyscore(const std::string& key,
        const std::string& _min, const std::string& _max, int64_t * _return) = 0;
    // _return the score of member
    virtual bool zscore(const std::string& key, const std::string& member,
        double * _return, bool * is_nil) = 0;

    /************************************************************************/
    /*Connection command*/
    /************************************************************************/
    virtual bool select(int index) = 0;

    /************************************************************************/
    /*Server command*/
    /************************************************************************/
    virtual bool flushall() = 0;
    virtual bool flushdb() = 0;
};


class RedisBase2Single : public RedisBase2
{
  public:
    virtual ~RedisBase2Single();

    /************************************************************************/
    /*KEYS command*/
    /************************************************************************/
    virtual bool rename(const std::string& key, const std::string& newkey) = 0;
    // _return 1, 'key' was renamed to 'newkey'
    // _return 0, 'newkey' already exists
    virtual bool renamenx(const std::string& key, const std::string& newkey,
        int64_t * _return) = 0;

    // the following commands are not implemented in interface:
    // MIGRATE
    // OBJECT

    /************************************************************************/
    /*String command*/
    /************************************************************************/
    // the following commands are not implemented in interface:
    // BITCOUNT
    // BITOP
    // MSETNX

    /************************************************************************/
    /*Lists command*/
    /************************************************************************/
    virtual bool blpop(const string_vector_t& keys, int64_t timeout,
        std::string * key, std::string * member, bool * expired) = 0;
    virtual bool brpop(const string_vector_t& keys, int64_t timeout,
        std::string * key, std::string * member, bool * expired) = 0;
    virtual bool brpoplpush(const std::string& source, const std::string& destination,
        int64_t timeout, std::string * member, bool * expired) = 0;
    virtual bool rpoplpush(const std::string& source, const std::string& destination,
        std::string * _return, bool * is_nil) = 0;

    /************************************************************************/
    /*Sets command*/
    /************************************************************************/
    virtual bool sdiff(const string_vector_t& keys, mbulk_t * _return) = 0;
    // _return the number of elements in the resulting set
    virtual bool sdiffstore(const std::string& destination,
        const string_vector_t& keys, int64_t * _return) = 0;
    virtual bool sinter(const string_vector_t& keys, mbulk_t * _return) = 0;
    // _return the number of elements in the resulting set
    virtual bool sinterstore(const std::string& destination,
        const string_vector_t& keys, int64_t * _return) = 0;
    // _return 1, the element is moved
    // _return 0, the element is not a member of source and no operation was performed
    virtual bool smove(const std::string& source,
        const std::string& destination, const std::string& member,
        int64_t * _return) = 0;
    virtual bool sunion(const string_vector_t& keys, mbulk_t * _return) = 0;
    virtual bool sunionstore(const std::string& destination,
        const string_vector_t& keys, int64_t * _return) = 0;

    /************************************************************************/
    /*Sorted Sets command*/
    /************************************************************************/
    // _return the number of elements in the resulting sorted set at destination
    virtual bool zinterstore(const std::string& destination,
        const string_vector_t& keys, const std::vector<double> * weights,
        kZUnionstoreAggregate agg, int64_t * _return) = 0;
    virtual bool zunionstore(const std::string& destination,
        const string_vector_t& keys, const std::vector<double> * weights,
        kZUnionstoreAggregate agg, int64_t * _return) = 0;

    /************************************************************************/
    /*pipeline*/
    /************************************************************************/
    // independently and immediately execute command and pipeline commands
    // through inner socket ignoring the pipeline status
    virtual bool exec_command(RedisCommand * command) = 0;

    // NOTICE:
    // printf like(but it is text only and words are split by space,
    // binary strings are not supported):
    // RedisCommand cmd;
    // r.exec_command(&cmd, "SET %s %s", "foo", "helloworld");
    virtual bool exec_command(RedisCommand * command, const char * format, ...) = 0;

    // execute a set of commands in pipeline mode
    virtual bool exec_pipeline(redis_command_vector_t * commands) = 0;

    /************************************************************************/
    /*Pub/Sub command*/
    /************************************************************************/
    // _return the number of clients that received the message
    virtual bool publish(const std::string& channel, const std::string& message,
        int64_t * _return) = 0;

    // the following commands are not implemented in interface
    // for mode problem(commands may block or not, it is difficult to design a common interface):
    // PSUBSCRIBE
    // PUNSUBSCRIBE
    // SUBSCRIBE
    // UNSUBSCRIBE

    /************************************************************************/
    /*Transactions command*/
    /************************************************************************/
    virtual bool multi() = 0;
    virtual bool unwatch() = 0;
    virtual bool watch(const string_vector_t& keys) = 0;

    virtual bool add_command(RedisCommand * command) = 0;
    virtual bool add_command(RedisCommand * command, const char * format, ...) = 0;

    virtual bool exec(redis_command_vector_t * commands) = 0;
    virtual bool discard() = 0;

    /************************************************************************/
    /*Scripting command*/
    /************************************************************************/
    // the following commands are not implemented in interface
    // for special multi-bulk problem(it is difficult to design a common interface):
    // EVAL>=2.6
    // EVALSHA>=2.6
    // SCRIPT>=2.6

    /************************************************************************/
    /*Connection command*/
    /************************************************************************/
    virtual bool ping() = 0;

    // the following commands are not implemented in interface
    // because they are rarely used:
    // AUTH
    // ECHO
    // QUIT

    /************************************************************************/
    /*Server command*/
    /************************************************************************/
    virtual bool bgrewriteaof() = 0;
    virtual bool bgsave() = 0;
    virtual bool dbsize(int64_t * _return) = 0;
    virtual bool info(std::string * _return) = 0;
    // >= 2.6
    // type may be:
    //  server
    //  clients
    //  memory
    //  persistence
    //  stats
    //  replication
    //  cpu
    //  keyspace
    //  commandstats
    virtual bool info(const std::string& type, std::string * _return) = 0;
    virtual bool lastsave(int64_t * _return) = 0;
    virtual bool slaveof(const std::string& host, const std::string& port) = 0;

    // the following commands are not implemented in interface:
    // CONFIG
    // DEBUG
    // MONITOR, mode problem
    // SAVE
    // SHUTDOWN
    // SLOWLOG
    // SYNC
    // TIME>=2.6
};


class RedisBase2Multi : public RedisBase2
{
  private:
    bool __inner_init();

  protected:
    const std::string host_list_;
    const std::string port_list_;
    const int db_index_;
    const int timeout_ms_;

    // hosts_.size()==ports_.size()
    string_vector_t hosts_;
    string_vector_t ports_;

    std::string error_;
  public:
    RedisBase2Multi(
        const std::string& host_list,// a host list
        const std::string& port_list,// a port list matching host_list or only one port
        int db_index,
        int timeout_ms);
    virtual ~RedisBase2Multi();

    virtual void last_error(const std::string& err);
    virtual std::string last_error()const;
    virtual const char * last_c_error()const;
};

class RedisException : public std::exception
{
  public:
    RedisException(const char * exception);
    RedisException(const std::string& exception);
    virtual ~RedisException() throw();
    virtual const char * what() const throw();
  private:
    std::string exception_;
};

LIBREDIS_NAMESPACE_END

#endif// _LANGTAOJIN_LIBREDIS_REDIS_BASE_H_
