/** @file
 * @brief RedisTss: thread specific storage support,
 *                  also can be used as a connection pool
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#ifndef _LANGTAOJIN_LIBREDIS_REDIS_TSS_H_
#define _LANGTAOJIN_LIBREDIS_REDIS_TSS_H_

#include "redis_base.h"

LIBREDIS_NAMESPACE_BEGIN

enum kRedisClientType
{
  kNormal = 0,  // Redis2
  kPartition    // Redis2P
};

enum kTssFlag
{
  kThreadSpecific = 0,
  kNotThreadSpecific
};

class RedisTss
{
  private:
    class Impl;
    Impl * impl_;

  public:
    // 'check_interval' is not used now
    RedisTss(const std::string& host, int port,
        int db_index = 0, int partitions = 1,
        int timeout_ms = 50, kRedisClientType type = kPartition,
        size_t pool_size = 100, size_t check_interval = 120);

    RedisTss(const std::string& host, const std::string& port,
        int db_index = 0, int partitions = 1,
        int timeout_ms = 50, kRedisClientType type = kPartition,
        size_t pool_size = 100, size_t check_interval = 120);

    ~RedisTss();

    // There are three usages:
    // 1. get(kThreadSpecific)
    //    do not put it back, until the thread exits
    // 2. get(kThreadSpecific)
    //    put(redis, kThreadSpecific), it is useful to reclaim redis connections
    // 3. get(kNotThreadSpecific)
    //    put(redis, kNotThreadSpecific), it is essential, or redis connections leak
    RedisBase2 * get(kTssFlag flag = kThreadSpecific);
    void put(RedisBase2 * redis, kTssFlag flag = kThreadSpecific);
};

class RedisScopedPtr
{
  private:
    RedisTss * const tss_;
    const kTssFlag flag_;
    RedisBase2 * ptr_;

  public:
    RedisScopedPtr(RedisTss * tss, kTssFlag flag)
      : tss_(tss), flag_(flag), ptr_(0)
    {
      ptr_ = tss_->get(flag_);
    }

    ~RedisScopedPtr()
    {
      if (ptr_)
      {
        tss_->put(ptr_, flag_);
        ptr_ = NULL;
      }
    }

    RedisBase2 * get()
    {
      return ptr_;
    }
};

LIBREDIS_NAMESPACE_END

#endif// _LANGTAOJIN_LIBREDIS_REDIS_TSS_H_
