/** @file
 * @brief RedisBase2 : redis base interface
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#include "redis_base.h"
#include <assert.h>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>

LIBREDIS_NAMESPACE_BEGIN

/************************************************************************/
/*RedisBase2*/
/************************************************************************/
RedisBase2::~RedisBase2() {}

int RedisBase2::get(const std::string& key, std::string * value)
{
  bool is_nil;

  if (get(key, value, &is_nil))
  {
    if (is_nil)
      return 0;
    else
      return 1;
  }

  return -1;
}

int RedisBase2::expire(const std::string& key, int64_t seconds)
{
  int64_t _return;
  if (expire(key, seconds, &_return))
    return static_cast<int>(_return);

  return -1;
}

int RedisBase2::expireat(const std::string& key, int64_t abs_seconds)
{
  int64_t _return;
  if (expireat(key, abs_seconds, &_return))
    return static_cast<int>(_return);

  return -1;
}

bool RedisBase2::keys(const std::string& pattern, string_vector_t * _keys)
{
  if (_keys==NULL)
  {
    last_error("EINVAL");
    return false;
  }

  mbulk_t _return;

  if (keys(pattern, &_return))
    convert(&_return, _keys);
  return true;
}

int RedisBase2::hget(const std::string& key, const std::string& field, std::string * value)
{
  bool is_nil;

  if (hget(key, field, value, &is_nil))
  {
    if (is_nil)
      return 0;
    else
      return 1;
  }

  return -1;
}

bool RedisBase2::hset(const std::string& key, const std::string& field,
    const std::string& value)
{
  int64_t _return;
  return hset(key, field, value, &_return);
}

int RedisBase2::hdel(const std::string& key, const std::string& field)
{
  int64_t _return;

  if (hdel(key, field, &_return))
  {
    return static_cast<int>(_return);
  }

  return -1;
}

bool RedisBase2::hgetall(const std::string& key, string_pair_vector_t * fields_and_values)
{
  if (fields_and_values==NULL)
  {
    last_error("EINVAL");
    return false;
  }

  mbulk_t _return;

  if (hgetall(key, &_return))
  {
    size_t size = _return.size()/2;
    fields_and_values->reserve(size);

    for (size_t i=0; i<size; i++)
    {
      assert(_return[i*2] && _return[i*2+1]);

      fields_and_values->push_back(string_pair_t());
      fields_and_values->back().first.swap(*_return[i*2]);
      fields_and_values->back().second.swap(*_return[i*2+1]);
    }

    clear_mbulks(&_return);
    return true;
  }
  return false;
}

bool RedisBase2::hgetall(const std::string& key, string_map_t * fields_and_values)
{
  if (fields_and_values==NULL)
  {
    last_error("EINVAL");
    return false;
  }

  mbulk_t _return;

  if (hgetall(key, &_return))
  {
    size_t size = _return.size()/2;

    for (size_t i=0; i<size; i++)
    {
      assert(_return[i*2] && _return[i*2+1]);
      (void)fields_and_values->insert(std::make_pair(*_return[i*2], *_return[i*2+1]));
    }

    clear_mbulks(&_return);
    return true;
  }
  return false;
}

bool RedisBase2::hmset(const std::string& key, const string_pair_vector_t& fields_and_values)
{
  string_vector_t fields, values;

  size_t s = fields_and_values.size();
  for (size_t i=0; i<s; i++)
  {
    const string_pair_t& fv = fields_and_values[i];
    fields.push_back(fv.first);
    values.push_back(fv.second);
  }

  return hmset(key, fields, values);
}

bool RedisBase2::hmset(const std::string& key, const string_map_t& fields_and_values)
{
  string_vector_t fields, values;

  string_map_t::const_iterator first = fields_and_values.begin();
  string_map_t::const_iterator last = fields_and_values.end();
  for (; first!=last; ++first)
  {
    const string_pair_t& fv = (*first);
    fields.push_back(fv.first);
    values.push_back(fv.second);
  }

  return hmset(key, fields, values);
}

/************************************************************************/
/*RedisBase2Single*/
/************************************************************************/
RedisBase2Single::~RedisBase2Single() {}

/************************************************************************/
/*RedisBase2Multi*/
/************************************************************************/
bool RedisBase2Multi::__inner_init()
{
  // parse host and port
  (void)boost::split(hosts_, host_list_, boost::is_any_of(","));
  (void)boost::split(ports_, port_list_, boost::is_any_of(","));

  if (hosts_.size()!=ports_.size() && ports_.size()!=1)
  {
    error_ = (boost::format("the number of hosts and ports do not match: %lu vs %lu")
        % hosts_.size() % ports_.size()).str();
    return false;
  }

  // make host and port match
  if (ports_.size()==1 && hosts_.size()!=1)
    ports_.insert(ports_.end(), hosts_.size() - 1, ports_[0]);

  assert(hosts_.size()==ports_.size());

  return true;
}

RedisBase2Multi::RedisBase2Multi(const std::string& host_list,
    const std::string& port_list,
    int db_index,
    int timeout_ms)
: host_list_(host_list),
  port_list_(port_list),
  db_index_(db_index),
  timeout_ms_(timeout_ms)
{
  if (!__inner_init())
  {
    throw RedisException(error_);
  }
}

RedisBase2Multi::~RedisBase2Multi() {}

void RedisBase2Multi::last_error(const std::string& err)
{
  error_ = err;
}

std::string RedisBase2Multi::last_error()const
{
  return error_;
}

const char * RedisBase2Multi::last_c_error()const
{
  return error_.c_str();
}

/************************************************************************/
/*RedisException*/
/************************************************************************/
RedisException::RedisException(const char * exception)
{
  if (exception)
    exception_ = exception;
  else
    exception_ = "redis exception";
}

RedisException::RedisException(const std::string& exception)
  : exception_(exception) {}

  RedisException::~RedisException() throw() {}

const char * RedisException::what() const throw()
{
  return exception_.c_str();
}

LIBREDIS_NAMESPACE_END
