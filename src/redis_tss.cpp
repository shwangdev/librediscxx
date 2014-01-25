/** @file
 * @brief RedisTss: thread specific storage support,
 *                  also can be used as a connection pool
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#include "redis_tss.h"
#include "redis.h"
#include "redis_protocol.h"
#include "redis_partition.h"
#include "tss.h"
#include <set>
#include <boost/bind.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>

LIBREDIS_NAMESPACE_BEGIN

class RedisTss::Impl
{
  public:
    Impl(const std::string& host, int port,
        int db_index, int partitions,
        int timeout_ms, kRedisClientType type,
        size_t pool_size, size_t /* check_interval */);

    Impl(const std::string& host, const std::string& port,
        int db_index, int partitions,
        int timeout_ms, kRedisClientType type,
        size_t pool_size, size_t /* check_interval */);

    ~Impl();

    RedisBase2 * get(kTssFlag flag);
    void put(RedisBase2 * redis, kTssFlag flag);

  private:
    const std::string host_;
    const std::string port_;
    const int db_index_;
    const int timeout_ms_;
    const int partitions_;
    const kRedisClientType type_;
    const size_t pool_size_;

    std::vector<std::string> redis_hosts_;

    boost::mutex free_redis_lock_;
    std::vector<RedisBase2 *> free_redis_;

    // NOTICE:
    // 'client_' must be the last one, constructed last and destroyed first.
    // Because its cleanup handler may use 'free_redis_lock_', 'free_redis_'
    // or some others members.
    thread_specific_ptr<RedisBase2> client_;

    void inner_init();

    void clear_free_redis()
    {
      boost::mutex::scoped_lock guard(free_redis_lock_);
      for (size_t i=0; i<free_redis_.size(); i++)
      {
        delete free_redis_[i];
      }
      free_redis_.clear();
    }

    RedisBase2 * get_free_redis()
    {
      boost::mutex::scoped_lock guard(free_redis_lock_);
      if (free_redis_.empty())
        return NULL;

      RedisBase2 * redis = free_redis_.back();
      free_redis_.pop_back();
      return redis;
    }

    RedisBase2 * get_free_or_create_redis()
    {
      RedisBase2 * redis_ptr = get_free_redis();
      if (redis_ptr==NULL)
      {
        switch (type_)
        {
          case kNormal:
            redis_ptr = new Redis2(host_, port_, db_index_, timeout_ms_);
            break;
          case kPartition:
            redis_ptr = new Redis2P(host_, port_, db_index_, timeout_ms_, partitions_);
            break;
        }
      }
      return redis_ptr;
    }

    void put_free_redis(RedisBase2 * redis)
    {
      if (redis==NULL)
        return;

      boost::mutex::scoped_lock guard(free_redis_lock_);
      if (free_redis_.size()>=pool_size_)
      {
        guard.unlock();
        delete redis;
      }
      else
      {
        free_redis_.push_back(redis);
      }
    }
};

RedisTss::Impl::Impl(const std::string& host, int port,
    int db_index, int partitions,
    int timeout_ms, kRedisClientType type,
    size_t pool_size, size_t /* check_interval */)
: host_(host), port_(boost::lexical_cast<std::string>(port)),
  db_index_(db_index), timeout_ms_(timeout_ms),
  partitions_(partitions), type_(type),
  pool_size_(pool_size),
  client_(boost::bind(&RedisTss::Impl::put_free_redis, this, _1))
{
  inner_init();
}

RedisTss::Impl::Impl(const std::string& host, const std::string& port,
    int db_index, int partitions,
    int timeout_ms, kRedisClientType type,
    size_t pool_size, size_t /* check_interval */)
: host_(host), port_(port),
  db_index_(db_index), timeout_ms_(timeout_ms),
  partitions_(partitions), type_(type),
  pool_size_(pool_size),
  client_(boost::bind(&RedisTss::Impl::put_free_redis, this, _1))
{
  inner_init();
}

RedisTss::Impl::~Impl()
{
  clear_free_redis();
}

RedisBase2 * RedisTss::Impl::get(kTssFlag flag)
{
  RedisBase2 * redis_ptr;
  if (flag==kThreadSpecific)
  {
    redis_ptr = client_.get();
    if (redis_ptr==NULL)
    {
      redis_ptr = get_free_or_create_redis();
      client_.reset(redis_ptr);
    }
  }
  else
  {
    redis_ptr = get_free_or_create_redis();
  }

  return redis_ptr;
}

void RedisTss::Impl::put(RedisBase2 * redis, kTssFlag flag)
{
  if (redis==NULL)
    return;

  if (flag==kThreadSpecific)
  {
    if (client_.get()==redis)
      (void)client_.release();
  }

  put_free_redis(redis);
}

void RedisTss::Impl::inner_init()
{
  (void)boost::split(redis_hosts_, host_, boost::is_any_of(","));
}

/************************************************************************/
/*RedisTss*/
/************************************************************************/
RedisTss::RedisTss(const std::string& host, int port,
    int db_index, int partitions,
    int timeout_ms, kRedisClientType type,
    size_t pool_size, size_t check_interval)
{
  impl_ = new Impl(host, port, db_index, partitions, timeout_ms, type,
      pool_size, check_interval);
}

RedisTss::RedisTss(const std::string& host, const std::string& port,
    int db_index, int partitions,
    int timeout_ms, kRedisClientType type,
    size_t pool_size, size_t check_interval)
{
  impl_ = new Impl(host, port, db_index, partitions, timeout_ms, type,
      pool_size, check_interval);
}

RedisTss::~RedisTss()
{
  delete impl_;
}

RedisBase2 * RedisTss::get(kTssFlag flag)
{
  return impl_->get(flag);
}

void RedisTss::put(RedisBase2 * redis, kTssFlag flag)
{
  impl_->put(redis, flag);
}

LIBREDIS_NAMESPACE_END
