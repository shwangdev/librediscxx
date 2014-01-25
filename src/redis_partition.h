/** @file
 * @brief Redis2P : a redis client for fixed-size partitions of servers,
 *                  where there is no consistent hash
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#ifndef _LANGTAOJIN_LIBREDIS_REDIS_PARTITION_H_
#define _LANGTAOJIN_LIBREDIS_REDIS_PARTITION_H_

#include "redis.h"
#include <set>

LIBREDIS_NAMESPACE_BEGIN

class Redis2P : public RedisBase2Multi
{
  private:
    bool inner_init();
    // bool is_invalid(size_t index)const;

    bool __get_key_client(const std::string& key,
        size_t_vector_t * host_indexes, bool write = true);
    bool __get_keys_client(const string_vector_t& keys,
        size_t_vector_vector_t * host_indexes, bool write = true);
    bool __get_group_client(size_t_vector_t * host_indexes);

    void __set_index_error(size_t host_index);
    void __set_host_error(const Redis2& host);

    size_t __get_key_host_index(const std::string& key)const;


    const size_t partitions_;
    const key_hasher hash_fn_;
    size_t groups_;

    redis2_sp_vector_t redis2_sp_vector_;
    // std::set<size_t> invalid_redis_;

  public:
    Redis2P(
        const std::string& host_list,// a host list
        const std::string& port_list,// a port list matching host_list or only one port
        int db_index = 0,
        int timeout_ms = 50,
        int partitions = 1,
        key_hasher fn = time33_hash_32);
    virtual ~Redis2P();

    // retrieve the inner Redis2 client, whose ownership is still in Redis2P by key
    bool get_key_client(const std::string& key, redis2_sp_vector_t * redis_clients);
    // retrieve the inner Redis2 client, whose ownership is still in Redis2P by index
    bool get_index_client(size_t host_index, redis2_sp_vector_t * redis_clients);
    // retrieve all of them ignoring the availability
    bool get_all_client(redis2_sp_vector_t * redis_clients);

    //// usually, this is called in class RedisTss to detect and shield unavailable clients
    // void set_invalid_redis(const std::set<size_t>& invalid_redis);

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
    virtual bool lindex(const std::string& key, int64_t index, std::string * _return,
        bool * is_nil);
    virtual bool linsert(const std::string& key, bool before, const std::string& pivot,
        const std::string& value, int64_t * _return);
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
};

LIBREDIS_NAMESPACE_END

#endif// _LANGTAOJIN_LIBREDIS_REDIS_PARTITION_H_
