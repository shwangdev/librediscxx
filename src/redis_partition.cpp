/** @file
 * @brief Redis2P : a redis client for fixed-size partitions of servers,
 *                  where there is no consistent hash
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#include "redis_partition.h"
#include "os.h"
#include <assert.h>
#include <boost/foreach.hpp>
#include <boost/format.hpp>

#define CHECK_PTR_PARAM(ptr) \
  if (ptr==NULL) {last_error("EINVAL");return false;}

#define CHECK_EXPR(exp) \
  if (!(exp)) {last_error("EINVAL");return false;}

#define FOR_EACH_GROUP_WRITE(func, key, ...) \
  do { \
    size_t_vector_t index_v; \
    if (!__get_key_client(key, &index_v)) \
    return false; \
    bool ret; \
    BOOST_FOREACH(size_t host_index, index_v) \
    { \
      ret = redis2_sp_vector_[host_index]->func(__VA_ARGS__); \
      if (!ret) \
      { \
        __set_index_error(host_index); \
        return false; \
      } \
    } \
    return true; \
  } while (0)

#define FOR_EACH_GROUP_READ(func, key, ...) \
  do { \
    size_t_vector_t index_v; \
    if (!__get_key_client(key, &index_v)) \
    return false; \
    bool ret; \
    BOOST_FOREACH(size_t host_index, index_v) \
    { \
      ret = redis2_sp_vector_[host_index]->func(__VA_ARGS__); \
      if (!ret) __set_index_error(host_index); \
      else return true; \
    } \
    return false;\
  } while (0)

LIBREDIS_NAMESPACE_BEGIN

static inline size_t __get_seed()
{
  return static_cast<size_t>(get_thread_id());
}

bool Redis2P::inner_init()
{
  if (partitions_==0)
  {
    error_ = "the number of partition must be not zero";
    return false;
  }

  groups_ = hosts_.size() / partitions_;
  if (groups_==0)
  {
    error_ = "the number of group must be not zero";
    return false;
  }

  if (hosts_.size() % groups_!=0)
  {
    error_ = str(boost::format(
          "invalid group number, hosts: %lu, groups: %lu, partitions: %lu")
        % hosts_.size() % groups_ % partitions_);
    return false;
  }

  for (size_t i=0 ; i<hosts_.size(); i++)
  {
    redis2_sp_vector_.push_back(redis2_sp_t(
          new Redis2(hosts_[i], ports_[i], db_index_, timeout_ms_)));
  }

  return true;
}

bool Redis2P::__get_key_client(const std::string& key, size_t_vector_t * index_v, bool write)
{
  size_t host_num = redis2_sp_vector_.size() / groups_;
  size_t host_index = __get_key_host_index(key);
  size_t index;
  size_t seed = __get_seed();

  index_v->clear();

  if (!write)
  {
    // get one client in one group
    index = host_index + host_num * (seed % groups_);
    // if (is_invalid(index))
    index_v->push_back(index);
  }
  else
  {
    // get clients in all groups
    for (size_t i=0; i<groups_; i++)
    {
      index = host_index + host_num * ((seed + i) % groups_);
      // if (is_invalid(index))
      index_v->push_back(index);
    }
  }

  if (index_v->empty())
  {
    error_ = "no available redis server";
    return false;
  }

  return true;
}

bool Redis2P::__get_keys_client(const string_vector_t& keys,
    size_t_vector_vector_t * index_v, bool write)
{
  size_t host_num = redis2_sp_vector_.size() / groups_;
  size_t host_index;
  size_t index;

  index_v->clear();
  index_v->reserve(keys.size());

  if (!write)
  {
    // get one client in one group
    size_t seed = __get_seed();
    for (size_t i=0; i<keys.size(); i++)
    {
      host_index = __get_key_host_index(keys[i]);
      index = host_index + host_num * (seed % groups_);
      // if (is_invalid(index))
      (*index_v)[i].push_back(index);
    }
  }
  else
  {
    // get clients in all groups
    index_v->resize(keys.size());
    for (size_t i=0; i<keys.size(); i++)
    {
      host_index = __get_key_host_index(keys[i]);
      (*index_v)[i].reserve(groups_);
      for (size_t j=0; j<groups_; j++)
      {
        index = host_index + host_num * j;
        // if (is_invalid(index))
        (*index_v)[i].push_back(index);
      }
    }
  }

  if (index_v->empty())
  {
    error_ = "no available redis server";
    return false;
  }

  return true;
}

bool Redis2P::__get_group_client(size_t_vector_t * index_v)
{
  size_t host_num = redis2_sp_vector_.size() / groups_;
  size_t host_index = 0;
  size_t index;

  if (groups_>1)
    host_index = (__get_seed() % groups_) * host_num;

  index_v->clear();
  index_v->reserve(host_num);

  for (size_t i=0; i<host_num; i++)
  {
    index = host_index + i;
    // if (is_invalid(index))
    index_v->push_back(index);
  }

  if (index_v->empty())
  {
    error_ = "no available redis server";
    return false;
  }

  return true;
}

void Redis2P::__set_index_error(size_t host_index)
{
  assert(host_index<redis2_sp_vector_.size());
  error_ = str(boost::format("[%s:%s] %s")
      % hosts_[host_index] % ports_[host_index] % redis2_sp_vector_[host_index]->last_error());
}

void Redis2P::__set_host_error(const Redis2& host)
{
  error_ = str(boost::format("[%s:%s] %s")
      % host.get_host() % host.get_port() % host.last_error());
}

size_t Redis2P::__get_key_host_index(const std::string& key)const
{
  return (static_cast<size_t>(hash_fn_(key)) % (redis2_sp_vector_.size() / groups_));
}

Redis2P::Redis2P(const std::string& host_list,
    const std::string& port_list,
    int db_index,
    int timeout_ms,
    int partitions,
    key_hasher fn)
: RedisBase2Multi(host_list, port_list, db_index, timeout_ms),
  partitions_(static_cast<size_t>(partitions)),
  hash_fn_(fn),
  groups_(0)
{
  if (!inner_init())
  {
    throw RedisException(error_);
  }
}

Redis2P::~Redis2P()
{
}

bool Redis2P::get_key_client(const std::string& key, redis2_sp_vector_t * redis_clients)
{
  CHECK_PTR_PARAM(redis_clients);

  size_t host_index = __get_key_host_index(key);
  return get_index_client(host_index, redis_clients);
}

bool Redis2P::get_index_client(size_t host_index, redis2_sp_vector_t * redis_clients)
{
  CHECK_PTR_PARAM(redis_clients);
  redis_clients->clear();

  size_t host_num = redis2_sp_vector_.size() / groups_;
  size_t index;
  for (size_t i=0; i<groups_; i++)
  {
    index = host_index + host_num * i;
    // if (is_invalid(index))
    redis_clients->push_back(redis2_sp_vector_[index]);
  }

  if (redis_clients->empty())
  {
    error_ = "no available redis server";
    return false;
  }

  return true;
}

bool Redis2P::get_all_client(redis2_sp_vector_t * redis_clients)
{
  CHECK_PTR_PARAM(redis_clients);
  redis_clients->assign(redis2_sp_vector_.begin(), redis2_sp_vector_.end());
  return true;
}

//void Redis2P::set_invalid_redis(const std::set<size_t>& invalid_redis)
//{
//  invalid_redis_ = invalid_redis;
//}

bool Redis2P::del(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(del, key, key, _return);
}

bool Redis2P::del(const string_vector_t& keys, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  size_t_vector_vector_t index_vv;
  if (!__get_keys_client(keys, &index_vv))
    return false;

  int64_t deleted = 0;
  int64_t total_deleted = 0;
  bool ret = true;

  // convert multi-del to del to multi redis instance
  // for each key
  for (size_t i=0; i<index_vv.size(); i++)
  {
    const size_t_vector_t& index_v = index_vv[i];
    // for each group
    for (size_t j=0; j<index_v.size(); j++)
    {
      ret = redis2_sp_vector_[index_v[j]]->del(keys[i], &deleted);
      if (!ret)
      {
        __set_index_error(index_v[j]);
        return false;
      }
    }
    total_deleted += deleted;
  }

  if (ret)
    *_return = total_deleted;
  return ret;
}

bool Redis2P::dump(const std::string& key, std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);
  FOR_EACH_GROUP_READ(dump, key, key, _return, is_nil);
}

bool Redis2P::exists(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(exists, key, key, _return);
}

bool Redis2P::expire(const std::string& key, int64_t seconds, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(expire, key, key, seconds, _return);
}

bool Redis2P::expireat(const std::string& key, int64_t abs_seconds, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(expireat, key, key, abs_seconds, _return);
}

bool Redis2P::keys(const std::string& pattern, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  clear_mbulks(_return);
  mbulk_t mb;

  size_t_vector_t index_v;
  if (!__get_group_client(&index_v))
    return false;

  // for each host
  bool ret;
  BOOST_FOREACH(size_t host_index, index_v)
  {
    clear_mbulks(&mb);
    ret = redis2_sp_vector_[host_index]->keys(pattern, &mb);
    if (!ret)
    {
      clear_mbulks(_return);
      __set_index_error(host_index);
      return false;
    }

    append_mbulks(_return, &mb);
  }
  return true;
}

bool Redis2P::move(const std::string& key, int db, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(move, key, key, db, _return);
}

bool Redis2P::persist(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(persist, key, key, _return);
}

bool Redis2P::pexpire(const std::string& key, int64_t milliseconds, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(pexpire, key, key, milliseconds, _return);
}

bool Redis2P::pexpireat(const std::string& key, int64_t abs_milliseconds, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(pexpireat, key, key, abs_milliseconds, _return);
}

bool Redis2P::pttl(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(pttl, key, key, _return);
}

bool Redis2P::restore(const std::string& key, int64_t ttl, const std::string& value)
{
  FOR_EACH_GROUP_WRITE(restore, key, key, ttl, value);
}

bool Redis2P::randomkey(std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);

  size_t_vector_t index_v;
  if (!__get_group_client(&index_v))
    return false;

  // for each host
  bool ret;
  BOOST_FOREACH(size_t host_index, index_v)
  {
    ret = redis2_sp_vector_[host_index]->randomkey(_return, is_nil);
    if (!ret)
      __set_index_error(host_index);
    else
      return true;
  }
  return false;
}

bool Redis2P::sort(const std::string& key, const string_vector_t * phrases, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(sort, key, key, phrases, _return);
}

bool Redis2P::ttl(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(ttl, key, key, _return);
}

bool Redis2P::type(const std::string& key, std::string * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(type, key, key, _return);
}

bool Redis2P::append(const std::string& key, const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(append, key, key, value, _return);
}

bool Redis2P::decr(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(decr, key, key, _return);
}

bool Redis2P::decrby(const std::string& key, int64_t dec, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(decrby, key, key, dec, _return);
}

bool Redis2P::get(const std::string& key, std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);
  FOR_EACH_GROUP_READ(get, key, key, _return, is_nil);
}

bool Redis2P::getbit(const std::string& key, int64_t offset, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(getbit, key, key, offset, _return);
}

bool Redis2P::getrange(const std::string& key, int64_t start, int64_t end,
    std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);
  FOR_EACH_GROUP_READ(getrange, key, key, start, end, _return, is_nil);
}

bool Redis2P::getset(const std::string& key, const std::string& value,
    std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);
  FOR_EACH_GROUP_WRITE(getset, key, key, value, _return, is_nil);
}

bool Redis2P::incr(const std::string& key, int64_t * _return)
{
  FOR_EACH_GROUP_WRITE(incr, key, key, _return);
}

bool Redis2P::incrby(const std::string& key, int64_t inc, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(incrby, key, key, inc, _return);
}

bool Redis2P::incrbyfloat(const std::string& key, double inc, double * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(incrbyfloat, key, key, inc, _return);
}

bool Redis2P::mget(const string_vector_t& keys, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  clear_mbulks(_return);
  _return->reserve(keys.size());

  bool ret;
  std::string value;
  std::string * tmp_value;
  bool is_nil;

  // convert mget to get to multi redis instance
  BOOST_FOREACH(const std::string& key, keys)
  {
    ret = get(key, &value, &is_nil);
    if (!ret)
    {
      clear_mbulks(_return);
      return false;
    }
    else if (is_nil)
    {
      _return->push_back(NULL);
    }
    else
    {
      tmp_value = new std::string;
      tmp_value->swap(value);
      _return->push_back(tmp_value);
    }
  }
  assert(keys.size()==_return->size());
  return true;
}

bool Redis2P::mset(const string_vector_t& keys, const string_vector_t& values)
{
  CHECK_EXPR(keys.size()==values.size());

  bool ret;

  // convert mset to set to multi redis instance
  for (size_t i=0; i<keys.size(); i++)
  {
    ret = set(keys[i], values[i]);
    if (!ret)
    {
      return false;
    }
  }
  return true;
}

bool Redis2P::psetex(const std::string& key, int64_t milliseconds, const std::string& value)
{
  FOR_EACH_GROUP_WRITE(psetex, key, key, milliseconds, value);
}

bool Redis2P::set(const std::string& key, const std::string& value)
{
  FOR_EACH_GROUP_WRITE(set, key, key, value);
}

bool Redis2P::setbit(const std::string& key, int64_t offset, int64_t value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(setbit, key, key, offset, value, _return);
}

bool Redis2P::setex(const std::string& key, int64_t seconds, const std::string& value)
{
  FOR_EACH_GROUP_WRITE(setex, key, key, seconds, value);
}

bool Redis2P::setnx(const std::string& key, const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(setnx, key, key, value, _return);
}

bool Redis2P::setrange(const std::string& key, int64_t offset,
    const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(setrange, key, key, offset, value, _return);
}

bool Redis2P::strlen(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(strlen, key, key, _return);
}

bool Redis2P::hdel(const std::string& key, const std::string& field, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(hdel, key, key, field, _return);
}

bool Redis2P::hdel(const std::string& key, const string_vector_t& fields, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(hdel, key, key, fields, _return);
}

bool Redis2P::hexists(const std::string& key, const std::string& field, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(hexists, key, key, field, _return);
}

bool Redis2P::hget(const std::string& key, const std::string& field,
    std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);
  FOR_EACH_GROUP_READ(hget, key, key, field, _return, is_nil);
}

bool Redis2P::hgetall(const std::string& key, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(hgetall, key, key, _return);
}

bool Redis2P::hincr(const std::string& key, const std::string& field, int64_t * _return)
{
  FOR_EACH_GROUP_WRITE(hincr, key, key, field, _return);
}

bool Redis2P::hincrby(const std::string& key, const std::string& field,
    int64_t inc, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(hincrby, key, key, field, inc, _return);
}

bool Redis2P::hincrbyfloat(const std::string& key, const std::string& field,
    double inc, double * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(hincrbyfloat, key, key, field, inc, _return);
}

bool Redis2P::hkeys(const std::string& key, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(hkeys, key, key, _return);
}

bool Redis2P::hlen(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(hlen, key, key, _return);
}

bool Redis2P::hmget(const std::string& key, const string_vector_t& fields, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(hmget, key, key, fields, _return);
}

bool Redis2P::hmset(const std::string& key,
    const string_vector_t& fields, const string_vector_t& values)
{
  CHECK_EXPR(fields.size()==values.size());
  FOR_EACH_GROUP_WRITE(hmset, key, key, fields, values);
}

bool Redis2P::hset(const std::string& key, const std::string& field,
    const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(hset, key, key, field, value, _return);
}

bool Redis2P::hsetnx(const std::string& key, const std::string& field,
    const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(hset, key, key, field, value, _return);
}

bool Redis2P::hvals(const std::string& key, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(hvals, key, key, _return);
}

bool Redis2P::lindex(const std::string& key, int64_t index, std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);
  FOR_EACH_GROUP_READ(lindex, key, key, index, _return, is_nil);
}

bool Redis2P::linsert(const std::string& key, bool before,
    const std::string& pivot, const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(linsert, key, key, before, pivot, value, _return);
}

bool Redis2P::llen(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(llen, key, key, _return);
}

bool Redis2P::lpop(const std::string& key, std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);
  FOR_EACH_GROUP_WRITE(lpop, key, key, _return, is_nil);
}

bool Redis2P::lpush(const std::string& key, const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(lpush, key, key, value, _return);
}

bool Redis2P::lpush(const std::string& key, const string_vector_t& values, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(lpush, key, key, values, _return);
}

bool Redis2P::lpushx(const std::string& key, const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(lpushx, key, key, value, _return);
}

bool Redis2P::lrange(const std::string& key, int64_t start, int64_t stop, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(lrange, key, key, start, stop, _return);
}

bool Redis2P::lrem(const std::string& key, int64_t count, const std::string& value,
    int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(lrem, key, key, count, value, _return);
}

bool Redis2P::lset(const std::string& key, int64_t index, const std::string& value)
{
  FOR_EACH_GROUP_WRITE(lset, key, key, index, value);
}

bool Redis2P::ltrim(const std::string& key, int64_t start, int64_t stop)
{
  FOR_EACH_GROUP_WRITE(ltrim, key, key, start, stop);
}

bool Redis2P::rpop(const std::string& key, std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);
  FOR_EACH_GROUP_WRITE(rpop, key, key, _return, is_nil);
}

bool Redis2P::rpush(const std::string& key, const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(rpush, key, key, value, _return);
}

bool Redis2P::rpush(const std::string& key, const string_vector_t& values, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(rpush, key, key, values, _return);
}

bool Redis2P::rpushx(const std::string& key, const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(rpushx, key, key, value, _return);
}

bool Redis2P::sadd(const std::string& key, const std::string& member, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(sadd, key, key, member, _return);
}

bool Redis2P::sadd(const std::string& key, const string_vector_t& members, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(sadd, key, key, members, _return);
}

bool Redis2P::scard(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(scard, key, key, _return);
}

bool Redis2P::sismember(const std::string& key, const std::string& member, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(sismember, key, key, member, _return);
}

bool Redis2P::smembers(const std::string& key, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(smembers, key, key, _return);
}

bool Redis2P::spop(const std::string& key, std::string * member, bool * is_nil)
{
  CHECK_PTR_PARAM(member);
  CHECK_PTR_PARAM(is_nil);
  FOR_EACH_GROUP_WRITE(spop, key, key, member, is_nil);
}

bool Redis2P::srandmember(const std::string& key, std::string * member, bool * is_nil)
{
  CHECK_PTR_PARAM(member);
  CHECK_PTR_PARAM(is_nil);
  FOR_EACH_GROUP_READ(srandmember, key, key, member, is_nil);
}

bool Redis2P::srem(const std::string& key, const std::string& member, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(srem, key, key, member, _return);
}

bool Redis2P::srem(const std::string& key, const string_vector_t& members, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(srem, key, key, members, _return);
}

bool Redis2P::zadd(const std::string& key, double score,
    const std::string& member, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(zadd, key, key, score, member, _return);
}

bool Redis2P::zadd(const std::string& key, std::vector<double>& scores,
    const string_vector_t& members, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  CHECK_EXPR(scores.size()==members.size());
  FOR_EACH_GROUP_WRITE(zadd, key, key, scores, members, _return);
}

bool Redis2P::zcard(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(zcard, key, key, _return);
}

bool Redis2P::zcount(const std::string& key,
    const std::string& _min, const std::string& _max, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(zcount, key, key, _min, _max, _return);
}

bool Redis2P::zincrby(const std::string& key, double increment,
    const std::string& member, double * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(zincrby, key, key, increment, member, _return);
}

bool Redis2P::zrange(const std::string& key, int64_t start, int64_t stop,
    bool withscores, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(zrange, key, key, start, stop, withscores, _return);
}

bool Redis2P::zrevrange(const std::string& key, int64_t start, int64_t stop,
    bool withscores, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(zrevrange, key, key, start, stop, withscores, _return);
}

bool Redis2P::zrangebyscore(const std::string& key,
    const std::string& _min, const std::string& _max,
    bool withscores, const ZRangebyscoreLimit * limit,
    mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(zrangebyscore, key, key, _min, _max, withscores, limit, _return);
}

bool Redis2P::zrevrangebyscore(const std::string& key,
    const std::string& _max, const std::string& _min,
    bool withscores, const ZRangebyscoreLimit * limit,
    mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(zrevrangebyscore, key, key, _max, _min, withscores, limit, _return);
}

bool Redis2P::zrank(const std::string& key, const std::string& member,
    int64_t * _return, bool * not_exists)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(not_exists);
  FOR_EACH_GROUP_READ(zrank, key, key, member, _return, not_exists);
}

bool Redis2P::zrevrank(const std::string& key, const std::string& member,
    int64_t * _return, bool * not_exists)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(not_exists);
  FOR_EACH_GROUP_READ(zrevrank, key, key, member, _return, not_exists);
}

bool Redis2P::zrem(const std::string& key, const std::string& member, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(zrem, key, key, member, _return);
}

bool Redis2P::zrem(const std::string& key, const string_vector_t& members, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(zrem, key, key, members, _return);
}

bool Redis2P::zremrangebyrank(const std::string& key,
    int64_t start, int64_t stop, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(zremrangebyrank, key, key, start, stop, _return);
}

bool Redis2P::zremrangebyscore(const std::string& key,
    const std::string& _min, const std::string& _max, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_WRITE(zremrangebyscore, key, key, _min, _max, _return);
}

bool Redis2P::zscore(const std::string& key, const std::string& member,
    double * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  FOR_EACH_GROUP_READ(zscore, key, key, member, _return, is_nil);
}

bool Redis2P::select(int index)
{
  bool ret = true;
  BOOST_FOREACH(redis2_sp_t& sp, redis2_sp_vector_)
  {
    if (!sp->select(index))
    {
      __set_host_error(*sp);
      ret = false;
    }
  }
  return ret;
}

bool Redis2P::flushall()
{
  bool ret = true;
  BOOST_FOREACH(redis2_sp_t& sp, redis2_sp_vector_)
  {
    if (!sp->flushall())
    {
      __set_host_error(*sp);
      ret = false;
    }
  }
  return ret;
}

bool Redis2P::flushdb()
{
  bool ret = true;
  BOOST_FOREACH(redis2_sp_t& sp, redis2_sp_vector_)
  {
    if (!sp->flushdb())
    {
      __set_host_error(*sp);
      ret = false;
    }
  }
  return ret;
}

LIBREDIS_NAMESPACE_END
