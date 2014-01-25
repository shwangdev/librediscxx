/** @file
 * @brief Redis2 : a normal redis client for one server
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#ifndef _LANGTAOJIN_LIBREDIS_REDIS_H_
#define _LANGTAOJIN_LIBREDIS_REDIS_H_

#include "redis_base.h"

LIBREDIS_NAMESPACE_BEGIN

class RedisProtocol;

class Redis2 : public RedisBase2Single
{
  private:
    int db_index_;
    bool db_index_select_failure_;

    redis_command_vector_t transaction_cmds_;

    RedisProtocol * proto_;

    void on_reset();
    void on_reply_type_error(const RedisCommand * command);

    bool bxpop(
        bool is_blpop, const string_vector_t& keys, int64_t timeout,
        std::string * key, std::string * member, bool * expired);
    bool zxxxxxstore(
        bool is_zinterstore,
        const std::string& destination,
        const string_vector_t& keys,
        const std::vector<double> * weights,
        kZUnionstoreAggregate agg, int64_t * _return);
    bool zxxxrange(
        bool rev, const std::string& key, int64_t start, int64_t stop,
        bool withscores, mbulk_t * _return);
    bool zxxxrangebyscore(
        bool rev, const std::string& key,
        const std::string& _min, const std::string& _max,
        bool withscores, const ZRangebyscoreLimit * limit,
        mbulk_t * _return);
    bool zxxxrank(
        bool rev, const std::string& key, const std::string& member,
        int64_t * _return, bool * not_exists);

  public:
    bool assure_connect();
    bool connect();
    void close();
    bool available()const;
    bool is_open()const;
    bool check_connect();
    std::string get_host()const;
    std::string get_port()const;
    bool get_blocking_mode()const;
    void set_blocking_mode(bool blocking_mode);
    bool get_transaction_mode()const;

    Redis2(const std::string& host, const std::string& port,
        int db_index = 0, int timeout_ms = 50);
    virtual ~Redis2();

    virtual void last_error(const std::string& err);
    virtual std::string last_error()const;
    virtual const char * last_c_error()const;

    /************************************************************************/
    /*KEYS command*/
    /************************************************************************/
    virtual bool del(const std::string& key, int64_t * _return);
    virtual bool del(const string_vector_t& keys, int64_t * _return);
    virtual bool dump(const std::string& key, std::string * _return, bool * is_nil);
    virtual bool exists(const std::string& key, int64_t * _return);
    virtual bool expire(const std::string& key, int64_t seconds, int64_t * _return);
    virtual bool expireat(const std::string& key, int64_t abs_seconds, int64_t * _return);
    virtual bool keys(const std::string& pattern, mbulk_t * _return);
    virtual bool move(const std::string& key, int db, int64_t * _return);
    virtual bool persist(const std::string& key, int64_t * _return);
    virtual bool pexpire(const std::string& key, int64_t milliseconds, int64_t * _return);
    virtual bool pexpireat(const std::string& key, int64_t abs_milliseconds, int64_t * _return);
    virtual bool pttl(const std::string& key, int64_t * _return);
    virtual bool randomkey(std::string * _return, bool * is_nil);
    virtual bool restore(const std::string& key, int64_t ttl, const std::string& value);
    virtual bool sort(const std::string& key, const string_vector_t * phrases, mbulk_t * _return);
    virtual bool ttl(const std::string& key, int64_t * _return);
    virtual bool type(const std::string& key, std::string * _return);

    /************************************************************************/
    /*String command*/
    /************************************************************************/
    virtual bool append(const std::string& key, const std::string& value, int64_t * _return);
    virtual bool decr(const std::string& key, int64_t * _return);
    virtual bool decrby(const std::string& key, int64_t dec, int64_t * _return);
    virtual bool get(const std::string& key, std::string * _return, bool * is_nil);
    virtual bool getbit(const std::string& key, int64_t offset, int64_t * _return);
    virtual bool getrange(const std::string& key, int64_t start, int64_t end,
        std::string * _return, bool * is_nil);
    virtual bool getset(const std::string& key, const std::string& value,
        std::string * _return, bool * is_nil);
    virtual bool incr(const std::string& key, int64_t * _return);
    virtual bool incrby(const std::string& key, int64_t inc, int64_t * _return);
    virtual bool incrbyfloat(const std::string& key, double inc, double * _return);
    virtual bool mget(const string_vector_t& keys, mbulk_t * _return);
    virtual bool mset(const string_vector_t& keys, const string_vector_t& values);
    virtual bool psetex(const std::string& key, int64_t milliseconds, const std::string& value);
    virtual bool set(const std::string& key, const std::string& value);
    virtual bool setbit(const std::string& key, int64_t offset, int64_t value, int64_t * _return);
    virtual bool setex(const std::string& key, int64_t seconds, const std::string& value);
    virtual bool setnx(const std::string& key, const std::string& value, int64_t * _return);
    virtual bool setrange(const std::string& key, int64_t offset,
        const std::string& value, int64_t * _return);
    virtual bool strlen(const std::string& key, int64_t * _return);

    /************************************************************************/
    /*Hashes command*/
    /************************************************************************/
    virtual bool hdel(const std::string& key, const std::string& field, int64_t * _return);
    virtual bool hdel(const std::string& key, const string_vector_t& fields, int64_t * _return);
    virtual bool hexists(const std::string& key, const std::string& field, int64_t * _return);
    virtual bool hget(const std::string& key, const std::string& field,
        std::string * _return, bool * is_nil);
    virtual bool hgetall(const std::string& key, mbulk_t * _return);
    virtual bool hincr(const std::string& key, const std::string& field, int64_t * _return);
    virtual bool hincrby(const std::string& key, const std::string& field,
        int64_t inc, int64_t * _return);
    virtual bool hincrbyfloat(const std::string& key, const std::string& field,
        double inc, double * _return);
    virtual bool hkeys(const std::string& key, mbulk_t * _return);
    virtual bool hlen(const std::string& key, int64_t * _return);
    virtual bool hmget(const std::string& key, const string_vector_t& fields, mbulk_t * _return);
    virtual bool hmset(const std::string& key,
        const string_vector_t& fields, const string_vector_t& values);
    virtual bool hset(const std::string& key, const std::string& field,
        const std::string& value, int64_t * _return);
    virtual bool hsetnx(const std::string& key, const std::string& field,
        const std::string& value, int64_t * _return);
    virtual bool hvals(const std::string& key, mbulk_t * _return);

    /************************************************************************/
    /*Lists command*/
    /************************************************************************/
    virtual bool lindex(const std::string& key, int64_t index,
        std::string * _return, bool * is_nil);
    virtual bool linsert(const std::string& key, bool before,
        const std::string& pivot, const std::string& value, int64_t * _return);
    virtual bool llen(const std::string& key, int64_t * _return);
    virtual bool lpop(const std::string& key, std::string * _return, bool * is_nil);
    virtual bool lpush(const std::string& key, const std::string& value, int64_t * _return);
    virtual bool lpush(const std::string& key, const string_vector_t& values, int64_t * _return);
    virtual bool lpushx(const std::string& key, const std::string& value, int64_t * _return);
    virtual bool lrange(const std::string& key, int64_t start, int64_t stop, mbulk_t * _return);
    virtual bool lrem(const std::string& key, int64_t count, const std::string& value,
        int64_t * _return);
    virtual bool lset(const std::string& key, int64_t index, const std::string& value);
    virtual bool ltrim(const std::string& key, int64_t start, int64_t stop);
    virtual bool rpop(const std::string& key, std::string * _return, bool * is_nil);
    virtual bool rpush(const std::string& key, const std::string& value, int64_t * _return);
    virtual bool rpush(const std::string& key, const string_vector_t& values, int64_t * _return);
    virtual bool rpushx(const std::string& key, const std::string& value, int64_t * _return);

    /************************************************************************/
    /*Sets command*/
    /************************************************************************/
    virtual bool sadd(const std::string& key, const std::string& member, int64_t * _return);
    virtual bool sadd(const std::string& key, const string_vector_t& members, int64_t * _return);
    virtual bool scard(const std::string& key, int64_t * _return);
    virtual bool sismember(const std::string& key, const std::string& member, int64_t * _return);
    virtual bool smembers(const std::string& key, mbulk_t * _return);
    virtual bool spop(const std::string& key, std::string * member, bool * is_nil);
    virtual bool srandmember(const std::string& key, std::string * member, bool * is_nil);
    virtual bool srem(const std::string& key, const std::string& member, int64_t * _return);
    virtual bool srem(const std::string& key, const string_vector_t& members, int64_t * _return);

    /************************************************************************/
    /*Sorted Sets command*/
    /************************************************************************/
    virtual bool zadd(const std::string& key, double score,
        const std::string& member, int64_t * _return);
    virtual bool zadd(const std::string& key, std::vector<double>& scores,
        const string_vector_t& members, int64_t * _return);
    virtual bool zcard(const std::string& key, int64_t * _return);
    virtual bool zcount(const std::string& key,
        const std::string& _min, const std::string& _max, int64_t * _return);
    virtual bool zincrby(const std::string& key, double increment,
        const std::string& member, double * _return);
    virtual bool zrange(const std::string& key, int64_t start, int64_t stop,
        bool withscores, mbulk_t * _return);
    virtual bool zrevrange(const std::string& key, int64_t start, int64_t stop,
        bool withscores, mbulk_t * _return);
    virtual bool zrangebyscore(const std::string& key,
        const std::string& _min, const std::string& _max,
        bool withscores, const ZRangebyscoreLimit * limit,
        mbulk_t * _return);
    virtual bool zrevrangebyscore(const std::string& key,
        const std::string& _max, const std::string& _min,
        bool withscores, const ZRangebyscoreLimit * limit,
        mbulk_t * _return);
    virtual bool zrank(const std::string& key, const std::string& member,
        int64_t * _return, bool * not_exists);
    virtual bool zrevrank(const std::string& key, const std::string& member,
        int64_t * _return, bool * not_exists);
    virtual bool zrem(const std::string& key, const std::string& member, int64_t * _return);
    virtual bool zrem(const std::string& key, const string_vector_t& members, int64_t * _return);
    virtual bool zremrangebyrank(const std::string& key,
        int64_t start, int64_t stop, int64_t * _return);
    virtual bool zremrangebyscore(const std::string& key,
        const std::string& _min, const std::string& _max, int64_t * _return);
    virtual bool zscore(const std::string& key, const std::string& member,
        double * _return, bool * is_nil);

    /************************************************************************/
    /*Connection command*/
    /************************************************************************/
    virtual bool select(int index);

    /************************************************************************/
    /*Server command*/
    /************************************************************************/
    virtual bool flushall();
    virtual bool flushdb();




    /************************************************************************/
    /*KEYS command*/
    /************************************************************************/
    virtual bool rename(const std::string& key, const std::string& newkey);
    virtual bool renamenx(const std::string& key, const std::string& newkey, int64_t * _return);

    /************************************************************************/
    /*Lists command*/
    /************************************************************************/
    virtual bool blpop(const string_vector_t& keys, int64_t timeout,
        std::string * key, std::string * member, bool * expired);
    virtual bool brpop(const string_vector_t& keys, int64_t timeout,
        std::string * key, std::string * member, bool * expired);
    virtual bool brpoplpush(const std::string& source, const std::string& destination,
        int64_t timeout, std::string * member, bool * expired);
    virtual bool rpoplpush(const std::string& source, const std::string& destination,
        std::string * _return, bool * is_nil);

    /************************************************************************/
    /*Sets command*/
    /************************************************************************/
    virtual bool sdiff(const string_vector_t& keys, mbulk_t * _return);
    virtual bool sdiffstore(const std::string& destination,
        const string_vector_t& keys, int64_t * _return);
    virtual bool sinter(const string_vector_t& keys, mbulk_t * _return);
    virtual bool sinterstore(const std::string& destination,
        const string_vector_t& keys, int64_t * _return);
    virtual bool smove(const std::string& source,
        const std::string& destination, const std::string& member,
        int64_t * _return);
    virtual bool sunion(const string_vector_t& keys, mbulk_t * _return);
    virtual bool sunionstore(const std::string& destination,
        const string_vector_t& keys, int64_t * _return);

    /************************************************************************/
    /*Sorted Sets command*/
    /************************************************************************/
    virtual bool zinterstore(const std::string& destination,
        const string_vector_t& keys, const std::vector<double> * weights,
        kZUnionstoreAggregate agg, int64_t * _return);
    virtual bool zunionstore(const std::string& destination,
        const string_vector_t& keys, const std::vector<double> * weights,
        kZUnionstoreAggregate agg, int64_t * _return);

    /************************************************************************/
    /*pipeline*/
    /************************************************************************/
    virtual bool exec_command(RedisCommand * command);
    virtual bool exec_command(RedisCommand * command, const char * format, ...);
    virtual bool exec_pipeline(redis_command_vector_t * commands);

    /************************************************************************/
    /*Pub/Sub command*/
    /************************************************************************/
    virtual bool publish(const std::string& channel, const std::string& message,
        int64_t * _return);

    /************************************************************************/
    /*Transactions command*/
    /************************************************************************/
    virtual bool multi();
    virtual bool unwatch();
    virtual bool watch(const string_vector_t& keys);

    virtual bool add_command(RedisCommand * command);
    virtual bool add_command(RedisCommand * command, const char * format, ...);

    virtual bool exec(redis_command_vector_t * commands);
    virtual bool discard();

    /************************************************************************/
    /*Connection command*/
    /************************************************************************/
    virtual bool ping();

    /************************************************************************/
    /*Server command*/
    /************************************************************************/
    virtual bool bgrewriteaof();
    virtual bool bgsave();
    virtual bool dbsize(int64_t * _return);
    virtual bool info(std::string * _return);
    virtual bool info(const std::string& type, std::string * _return);
    virtual bool lastsave(int64_t * _return);
    virtual bool slaveof(const std::string& host, const std::string& port);
};

typedef boost::shared_ptr<Redis2> redis2_sp_t;
typedef std::vector<redis2_sp_t> redis2_sp_vector_t;

LIBREDIS_NAMESPACE_END

#endif// _LANGTAOJIN_LIBREDIS_REDIS_H_
