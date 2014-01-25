/** @file
 * @brief Redis2 : a normal redis client for one server
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#include "redis.h"
#include "redis_protocol.h"
#include <boost/lexical_cast.hpp>
#include <boost/format.hpp>

#define CHECK_PTR_PARAM(ptr) \
  do {if (ptr==NULL) {last_error("EINVAL");return false;}} while(0)

#define CHECK_EXPR(exp) \
  do {if (!(exp)) {last_error("EINVAL");return false;}} while(0)

#define CHECK_STATUS_OK() \
  do {if (c.out.is_status_ok()) return true;else {on_reply_type_error(&c);return false;}} while(0)

#define CHECK_STATUS_PONG() \
  do {if (c.out.is_status_pong()) return true;else {on_reply_type_error(&c);return false;}} while(0)

#define GET_INTEGER_REPLY() \
  do {if (c.out.get_i(_return)) return true;else {on_reply_type_error(&c);return false;}} while(0)

#define GET_BULK_REPLY() \
  do { \
    if (c.out.get_bulk(_return)) {*is_nil = false;return true;} \
    else if (c.out.is_nil_bulk()) {*is_nil = true;return true;} \
    else {on_reply_type_error(&c);return false;} \
  } while(0)

#define GET_BULK_REPLY2() \
  do {if (c.out.get_bulk(_return)) return true;else {on_reply_type_error(&c);return false;}} while(0)

#define GET_MBULKS_REPLY() \
  do {if (c.out.get_mbulks(_return)) return true;else {on_reply_type_error(&c);return false;}} while(0)

LIBREDIS_NAMESPACE_BEGIN

void Redis2::on_reset()
{
  if (db_index_)
    db_index_select_failure_ = true;
  clear_commands(&transaction_cmds_);
}

void Redis2::on_reply_type_error(const RedisCommand * command)
{
  last_error(str(boost::format("expect %s, but got %s")
        % to_string(command->in.command_info().reply_type)
        % to_string(command->out.reply_type)));
  // no need to disconnect
}

bool Redis2::assure_connect()
{
  int status;
  if (!proto_->assure_connect(&status))
    return false;

  if (status==1)
    on_reset();

  if (db_index_ && db_index_select_failure_)
  {
    if (!Redis2::select(db_index_))
      return false;
  }

  return true;
}

bool Redis2::connect()
{
  return proto_->connect();
}

void Redis2::close()
{
  proto_->close();
}

bool Redis2::available()const
{
  return proto_->available();
}

bool Redis2::is_open()const
{
  return proto_->is_open();
}

bool Redis2::check_connect()
{
  return proto_->check_connect();
}

std::string Redis2::get_host()const
{
  return proto_->get_host();
}

std::string Redis2::get_port()const
{
  return proto_->get_port();
}

bool Redis2::get_blocking_mode()const
{
  return proto_->get_blocking_mode();
}

void Redis2::set_blocking_mode(bool blocking_mode)
{
  proto_->set_blocking_mode(blocking_mode);
}

bool Redis2::get_transaction_mode()const
{
  return proto_->get_transaction_mode();
}


Redis2::Redis2(const std::string& host, const std::string& port, int db_index,
    int timeout_ms)
: db_index_(db_index), db_index_select_failure_(true)
{
  proto_ = new RedisProtocol(host, port, timeout_ms);
}

Redis2::~Redis2()
{
  delete proto_;
  on_reset();
}

void Redis2::last_error(const std::string& err)
{
  proto_->last_error(err);
}

std::string Redis2::last_error()const
{
  return proto_->last_error();
}

const char * Redis2::last_c_error()const
{
  return proto_->last_c_error();
}

bool Redis2::del(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(DEL);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::del(const string_vector_t& keys, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(DEL);
  c.push_arg(keys);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::dump(const std::string& key, std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(DUMP);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY();
}

bool Redis2::exists(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(EXISTS);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::expire(const std::string& key, int64_t seconds,
    int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(EXPIRE);
  c.push_arg(key);
  c.push_arg(seconds);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::expireat(const std::string& key, int64_t abs_seconds,
    int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(EXPIREAT);
  c.push_arg(key);
  c.push_arg(abs_seconds);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::keys(const std::string& pattern, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(KEYS);
  c.push_arg(pattern);
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::move(const std::string& key, int db, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(MOVE);
  c.push_arg(key);
  c.push_arg(db);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::persist(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(PERSIST);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::pexpire(const std::string& key, int64_t milliseconds, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(PEXPIRE);
  c.push_arg(key);
  c.push_arg(milliseconds);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::pexpireat(const std::string& key, int64_t abs_milliseconds, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(PEXPIREAT);
  c.push_arg(key);
  c.push_arg(abs_milliseconds);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::pttl(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(PTTL);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::restore(const std::string& key, int64_t ttl, const std::string& value)
{
  if (!assure_connect())
    return false;

  RedisCommand c(RESTORE);
  c.push_arg(key);
  c.push_arg(ttl);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::randomkey(std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);

  if (!assure_connect())
    return false;

  RedisCommand c(RANDOMKEY);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY();
}

bool Redis2::sort(const std::string& key, const string_vector_t * phrases, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SORT);
  c.push_arg(key);
  if (phrases)
    c.push_arg(*phrases);
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::ttl(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(TTL);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::type(const std::string& key, std::string * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(TYPE);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  if (c.out.get_status(_return))
    return true;
  else
  {
    on_reply_type_error(&c);
    return false;
  }
}

bool Redis2::append(const std::string& key, const std::string& value,
    int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(APPEND);
  c.push_arg(key);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::decr(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(DECR);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::decrby(const std::string& key, int64_t dec, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(DECRBY);
  c.push_arg(key);
  c.push_arg(dec);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::get(const std::string& key, std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);

  if (!assure_connect())
    return false;

  RedisCommand c(GET);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY();
}

bool Redis2::getbit(const std::string& key, int64_t offset, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(GETBIT);
  c.push_arg(key);
  c.push_arg(offset);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::getrange(const std::string& key, int64_t start, int64_t end,
    std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);

  if (!assure_connect())
    return false;

  RedisCommand c(GETRANGE);
  c.push_arg(key);
  c.push_arg(start);
  c.push_arg(end);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY();
}

bool Redis2::getset(const std::string& key, const std::string& value,
    std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);

  if (!assure_connect())
    return false;

  RedisCommand c(GETSET);
  c.push_arg(key);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY();
}

bool Redis2::incr(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(INCR);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::incrby(const std::string& key, int64_t inc, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(INCRBY);
  c.push_arg(key);
  c.push_arg(inc);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::incrbyfloat(const std::string& key, double inc, double * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(INCRBYFLOAT);
  c.push_arg(key);
  c.push_arg(inc);
  if (!proto_->exec_command(&c))
    return false;

  std::string _return_string;
  if (c.out.get_bulk(&_return_string))
  {
    *_return = boost::lexical_cast<double>(_return_string);
    return true;
  }
  else
  {
    on_reply_type_error(&c);
    return false;
  }
}

bool Redis2::mget(const string_vector_t& keys, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(MGET);
  c.push_arg(keys);
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::mset(const string_vector_t& keys, const string_vector_t& values)
{
  CHECK_EXPR(keys.size()==values.size());

  if (!assure_connect())
    return false;

  RedisCommand c(MSET);
  for (size_t i=0; i<keys.size(); i++)
  {
    c.push_arg(keys[i]);
    c.push_arg(values[i]);
  }
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::psetex(const std::string& key, int64_t milliseconds, const std::string& value)
{
  if (!assure_connect())
    return false;

  RedisCommand c(PSETEX);
  c.push_arg(key);
  c.push_arg(milliseconds);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::set(const std::string& key, const std::string& value)
{
  if (!assure_connect())
    return false;

  RedisCommand c(SET);
  c.push_arg(key);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::setbit(const std::string& key, int64_t offset, int64_t value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SETBIT);
  c.push_arg(key);
  c.push_arg(offset);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::setex(const std::string& key, int64_t seconds, const std::string& value)
{
  if (!assure_connect())
    return false;

  RedisCommand c(SETEX);
  c.push_arg(key);
  c.push_arg(seconds);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::setnx(const std::string& key, const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SETNX);
  c.push_arg(key);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::setrange(const std::string& key, int64_t offset,
    const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SETRANGE);
  c.push_arg(key);
  c.push_arg(offset);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::strlen(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(STRLEN);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::hdel(const std::string& key, const std::string& field, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(HDEL);
  c.push_arg(key);
  c.push_arg(field);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::hdel(const std::string& key, const string_vector_t& fields, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(HDEL);
  c.push_arg(key);
  c.push_arg(fields);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::hexists(const std::string& key, const std::string& field, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(HEXISTS);
  c.push_arg(key);
  c.push_arg(field);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::hget(const std::string& key, const std::string& field,
    std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);

  if (!assure_connect())
    return false;

  RedisCommand c(HGET);
  c.push_arg(key);
  c.push_arg(field);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY();
}

bool Redis2::hgetall(const std::string& key, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(HGETALL);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::hincr(const std::string& key, const std::string& field, int64_t * _return)
{
  return hincrby(key, field, 1, _return);
}

bool Redis2::hincrby(const std::string& key, const std::string& field,
    int64_t inc, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(HINCRBY);
  c.push_arg(key);
  c.push_arg(field);
  c.push_arg(inc);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::hincrbyfloat(const std::string& key, const std::string& field,
    double inc, double * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(HINCRBYFLOAT);
  c.push_arg(key);
  c.push_arg(field);
  c.push_arg(inc);
  if (!proto_->exec_command(&c))
    return false;

  std::string _return_string;
  if (c.out.get_bulk(&_return_string))
  {
    *_return = boost::lexical_cast<double>(_return_string);
    return true;
  }
  else
  {
    on_reply_type_error(&c);
    return false;
  }
}

bool Redis2::hkeys(const std::string& key, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(HKEYS);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::hlen(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(HLEN);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::hmget(const std::string& key, const string_vector_t& fields,
    mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(HMGET);
  c.push_arg(key);
  c.push_arg(fields);
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::hmset(const std::string& key,
    const string_vector_t& fields, const string_vector_t& values)
{
  CHECK_EXPR(fields.size()==values.size());

  if (!assure_connect())
    return false;

  RedisCommand c(HMSET);
  c.push_arg(key);
  for (size_t i=0; i<fields.size(); i++)
  {
    c.push_arg(fields[i]);
    c.push_arg(values[i]);
  }
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::hset(const std::string& key, const std::string& field,
    const std::string& value, int64_t * _return)
{
  if (!assure_connect())
    return false;

  RedisCommand c(HSET);
  c.push_arg(key);
  c.push_arg(field);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::hsetnx(const std::string& key, const std::string& field,
    const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(HSETNX);
  c.push_arg(key);
  c.push_arg(field);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::hvals(const std::string& key, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(HVALS);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::bxpop(bool is_blpop, const string_vector_t& keys, int64_t timeout,
    std::string * key, std::string * member, bool * expired)
{
  CHECK_PTR_PARAM(key);
  CHECK_PTR_PARAM(member);
  CHECK_PTR_PARAM(expired);

  if (!assure_connect())
    return false;

  RedisCommand c(is_blpop?BLPOP:BRPOP);
  c.push_arg(keys);
  c.push_arg(timeout);

  bool blocked = get_blocking_mode();
  set_blocking_mode(true);
  bool exec = proto_->exec_command(&c);
  set_blocking_mode(blocked);
  if (!exec)
    return false;

  mbulk_t mb;
  if (c.out.get_mbulks(&mb)
      && mb.size()==2 && mb[0] && mb[1])
  {
    key->swap(*mb[0]);
    member->swap(*mb[1]);
    *expired = false;
    clear_mbulks(&mb);
    return true;
  }
  else if (c.out.is_nil_mbulks())
  {
    *expired = true;
    clear_mbulks(&mb);
    return true;
  }
  else
  {
    on_reply_type_error(&c);
    return false;
  }
}

bool Redis2::lindex(const std::string& key, int64_t index, std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);

  if (!assure_connect())
    return false;

  RedisCommand c(LINDEX);
  c.push_arg(key);
  c.push_arg(index);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY();
}

bool Redis2::linsert(const std::string& key, bool before,
    const std::string& pivot, const std::string& value,
    int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(LINSERT);
  c.push_arg(key);
  c.push_arg(before?"BEFORE":"AFTER");
  c.push_arg(pivot);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::llen(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(LLEN);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::lpop(const std::string& key, std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);

  if (!assure_connect())
    return false;

  RedisCommand c(LPOP);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY();
}

bool Redis2::lpush(const std::string& key, const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(LPUSH);
  c.push_arg(key);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::lpush(const std::string& key, const string_vector_t& values, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(LPUSH);
  c.push_arg(key);
  c.push_arg(values);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::lpushx(const std::string& key, const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(LPUSHX);
  c.push_arg(key);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::lrange(const std::string& key, int64_t start, int64_t stop, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(LRANGE);
  c.push_arg(key);
  c.push_arg(start);
  c.push_arg(stop);
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::lrem(const std::string& key, int64_t count, const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(LREM);
  c.push_arg(key);
  c.push_arg(count);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::lset(const std::string& key, int64_t index, const std::string& value)
{
  if (!assure_connect())
    return false;

  RedisCommand c(LSET);
  c.push_arg(key);
  c.push_arg(index);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::ltrim(const std::string& key, int64_t start, int64_t stop)
{
  if (!assure_connect())
    return false;

  RedisCommand c(LTRIM);
  c.push_arg(key);
  c.push_arg(start);
  c.push_arg(stop);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::rpop(const std::string& key, std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);

  if (!assure_connect())
    return false;

  RedisCommand c(RPOP);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY();
}

bool Redis2::rpush(const std::string& key, const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(RPUSH);
  c.push_arg(key);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::rpush(const std::string& key, const string_vector_t& values, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(RPUSH);
  c.push_arg(key);
  c.push_arg(values);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}


bool Redis2::rpushx(const std::string& key, const std::string& value, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(RPUSHX);
  c.push_arg(key);
  c.push_arg(value);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::sadd(const std::string& key, const std::string& member, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SADD);
  c.push_arg(key);
  c.push_arg(member);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::sadd(const std::string& key, const string_vector_t& members, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SADD);
  c.push_arg(key);
  c.push_arg(members);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::scard(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SCARD);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::sismember(const std::string& key, const std::string& member, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SISMEMBER);
  c.push_arg(key);
  c.push_arg(member);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::smembers(const std::string& key, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SMEMBERS);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::spop(const std::string& key, std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);

  if (!assure_connect())
    return false;

  RedisCommand c(SPOP);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY();
}

bool Redis2::srandmember(const std::string& key, std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);

  if (!assure_connect())
    return false;

  RedisCommand c(SRANDMEMBER);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY();
}

bool Redis2::srem(const std::string& key, const std::string& member, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SREM);
  c.push_arg(key);
  c.push_arg(member);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::srem(const std::string& key, const string_vector_t& members, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SREM);
  c.push_arg(key);
  c.push_arg(members);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::zadd(const std::string& key, double score,
    const std::string& member, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(ZADD);
  c.push_arg(key);
  c.push_arg(score);
  c.push_arg(member);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::zadd(const std::string& key, std::vector<double>& scores,
    const string_vector_t& members, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);
  CHECK_EXPR(scores.size()==members.size());

  if (!assure_connect())
    return false;

  RedisCommand c(ZADD);
  c.push_arg(key);
  for (size_t i=0; i<scores.size(); i++)
  {
    c.push_arg(scores[i]);
    c.push_arg(members[i]);
  }
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::zcard(const std::string& key, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(ZCARD);
  c.push_arg(key);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::zcount(const std::string& key,
    const std::string& _min, const std::string& _max,
    int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(ZCOUNT);
  c.push_arg(key);
  c.push_arg(_min);
  c.push_arg(_max);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::zincrby(const std::string& key, double increment,
    const std::string& member, double * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(ZINCRBY);
  c.push_arg(key);
  c.push_arg(increment);
  c.push_arg(member);
  if (!proto_->exec_command(&c))
    return false;

  std::string _return_string;
  if (c.out.get_bulk(&_return_string))
  {
    *_return = boost::lexical_cast<double>(_return_string);
    return true;
  }
  else
  {
    on_reply_type_error(&c);
    return false;
  }
}

bool Redis2::zxxxrange(bool rev,
    const std::string& key, int64_t start, int64_t stop,
    bool withscores, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(rev?ZREVRANGE:ZRANGE);
  c.push_arg(key);
  c.push_arg(start);
  c.push_arg(stop);
  if (withscores)
    c.push_arg("WITHSCORES");
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::zrange(const std::string& key, int64_t start, int64_t stop,
    bool withscores, mbulk_t * _return)
{
  return zxxxrange(false, key, start, stop, withscores, _return);
}

bool Redis2::zrevrange(const std::string& key, int64_t start, int64_t stop,
    bool withscores, mbulk_t * _return)
{
  return zxxxrange(true, key, start, stop, withscores, _return);
}

bool Redis2::zxxxrangebyscore(bool rev, const std::string& key,
    const std::string& _min, const std::string& _max,
    bool withscores, const ZRangebyscoreLimit * limit,
    mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(rev?ZREVRANGEBYSCORE:ZRANGEBYSCORE);
  c.push_arg(key);
  c.push_arg(_min);
  c.push_arg(_max);
  if (withscores)
    c.push_arg("WITHSCORES");
  if (limit)
  {
    c.push_arg("LIMIT");
    c.push_arg(limit->offset);
    c.push_arg(limit->count);
  }
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::zrangebyscore(const std::string& key,
    const std::string& _min, const std::string& _max,
    bool withscores, const ZRangebyscoreLimit * limit,
    mbulk_t * _return)
{
  return zxxxrangebyscore(false, key, _min, _max, withscores, limit, _return);
}

bool Redis2::zrevrangebyscore(const std::string& key,
    const std::string& _max, const std::string& _min,
    bool withscores, const ZRangebyscoreLimit * limit,
    mbulk_t * _return)
{
  return zxxxrangebyscore(true, key, _max, _min, withscores, limit, _return);
}

bool Redis2::zxxxrank(bool rev, const std::string& key, const std::string& member,
    int64_t * _return, bool * not_exists)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(not_exists);

  if (!assure_connect())
    return false;

  RedisCommand c(rev?ZREVRANK:ZRANK);
  c.push_arg(key);
  c.push_arg(member);
  if (!proto_->exec_command(&c))
    return false;

  if (c.out.get_i(_return))
  {
    *not_exists = false;
    return true;
  }
  else if (c.out.is_nil_bulk())
  {
    *not_exists = true;
    return true;
  }
  else
  {
    on_reply_type_error(&c);
    return false;
  }
}

bool Redis2::zrank(const std::string& key, const std::string& member,
    int64_t * _return, bool * not_exists)
{
  return zxxxrank(false, key, member, _return, not_exists);
}

bool Redis2::zrevrank(const std::string& key, const std::string& member,
    int64_t * _return, bool * not_exists)
{
  return zxxxrank(true, key, member, _return, not_exists);
}

bool Redis2::zrem(const std::string& key, const std::string& member,
    int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(ZREM);
  c.push_arg(key);
  c.push_arg(member);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::zrem(const std::string& key, const string_vector_t& members,
    int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(ZREM);
  c.push_arg(key);
  c.push_arg(members);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();

}

bool Redis2::zremrangebyrank(const std::string& key,
    int64_t start, int64_t stop, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(ZREMRANGEBYRANK);
  c.push_arg(key);
  c.push_arg(start);
  c.push_arg(stop);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::zremrangebyscore(const std::string& key,
    const std::string& _min, const std::string& _max,
    int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(ZREMRANGEBYSCORE);
  c.push_arg(key);
  c.push_arg(_min);
  c.push_arg(_max);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::zscore(const std::string& key, const std::string& member,
    double * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);

  if (!assure_connect())
    return false;

  RedisCommand c(ZSCORE);
  c.push_arg(key);
  c.push_arg(member);
  if (!proto_->exec_command(&c))
    return false;

  std::string _return_string;
  if (c.out.get_bulk(&_return_string))
  {
    *_return = boost::lexical_cast<double>(_return_string);
    *is_nil = false;
    return true;
  }
  else if (c.out.is_nil_bulk())
  {
    *is_nil = true;
    return true;
  }
  else
  {
    on_reply_type_error(&c);
    return false;
  }
}

bool Redis2::select(int index)
{
  if (!proto_->assure_connect(NULL))
    return false;

  RedisCommand c(SELECT);
  c.push_arg(index);
  if (!proto_->exec_command(&c))
    return false;

  db_index_ = index;

  if (c.out.is_status_ok())
  {
    db_index_select_failure_ = false;
    return true;
  }
  else
  {
    db_index_select_failure_ = true;
    on_reply_type_error(&c);
    return false;
  }
}

bool Redis2::flushall()
{
  if (!assure_connect())
    return false;

  RedisCommand c(FLUSHALL);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::flushdb()
{
  if (!assure_connect())
    return false;

  RedisCommand c(FLUSHDB);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::rename(const std::string& key, const std::string& newkey)
{
  if (!assure_connect())
    return false;

  RedisCommand c(RENAME);
  c.push_arg(key);
  c.push_arg(newkey);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::renamenx(const std::string& key, const std::string& newkey, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(RENAMENX);
  c.push_arg(key);
  c.push_arg(newkey);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::blpop(const string_vector_t& keys, int64_t timeout,
    std::string * key, std::string * member, bool * expired)
{
  return bxpop(true, keys, timeout, key, member, expired);
}

bool Redis2::brpop(const string_vector_t& keys, int64_t timeout,
    std::string * key, std::string * member, bool * expired)
{
  return bxpop(false, keys, timeout, key, member, expired);
}

bool Redis2::brpoplpush(const std::string& source, const std::string& destination,
    int64_t timeout, std::string * member, bool * expired)
{
  CHECK_PTR_PARAM(member);
  CHECK_PTR_PARAM(expired);

  if (!assure_connect())
    return false;

  RedisCommand c(BRPOPLPUSH);
  c.push_arg(source);
  c.push_arg(destination);
  c.push_arg(timeout);

  bool blocked = get_blocking_mode();
  set_blocking_mode(true);
  bool exec = proto_->exec_command(&c);
  set_blocking_mode(blocked);
  if (!exec)
    return false;

  if (c.out.get_bulk(member))
  {
    *expired = false;
    return true;
  }
  else if (c.out.is_nil_smbulks())
  {
    *expired = true;
    return true;
  }
  else
  {
    on_reply_type_error(&c);
    return false;
  }
}

bool Redis2::rpoplpush(const std::string& source, const std::string& destination,
    std::string * _return, bool * is_nil)
{
  CHECK_PTR_PARAM(_return);
  CHECK_PTR_PARAM(is_nil);

  if (!assure_connect())
    return false;

  RedisCommand c(RPOPLPUSH);
  c.push_arg(source);
  c.push_arg(destination);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY();
}

bool Redis2::sdiff(const string_vector_t& keys, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SDIFF);
  c.push_arg(keys);
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::sdiffstore(const std::string& destination,
    const string_vector_t& keys, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SDIFFSTORE);
  c.push_arg(destination);
  c.push_arg(keys);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::sinter(const string_vector_t& keys, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SINTER);
  c.push_arg(keys);
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::sinterstore(const std::string& destination,
    const string_vector_t& keys, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SINTERSTORE);
  c.push_arg(destination);
  c.push_arg(keys);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::smove(const std::string& source,
    const std::string& destination, const std::string& member,
    int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SMOVE);
  c.push_arg(source);
  c.push_arg(destination);
  c.push_arg(member);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::sunion(const string_vector_t& keys, mbulk_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SUNION);
  c.push_arg(keys);
  if (!proto_->exec_command(&c))
    return false;

  GET_MBULKS_REPLY();
}

bool Redis2::sunionstore(const std::string& destination,
    const string_vector_t& keys, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(SUNIONSTORE);
  c.push_arg(destination);
  c.push_arg(keys);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::zxxxxxstore(bool is_zinterstore,
    const std::string& destination,
    const string_vector_t& keys,
    const std::vector<double> * weights,
    kZUnionstoreAggregate agg, int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(is_zinterstore?ZINTERSTORE:ZUNIONSTORE);
  c.push_arg(destination);
  c.push_arg(keys.size());
  c.push_arg(keys);
  if (weights)
  {
    c.push_arg("WEIGHTS");
    c.push_arg(*weights);
  }
  if (agg==kMin)
  {
    c.push_arg("AGGREGATE");
    c.push_arg("MIN");
  }
  else if (agg==kMax)
  {
    c.push_arg("AGGREGATE");
    c.push_arg("MAX");
  }
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::zinterstore(const std::string& destination,
    const string_vector_t& keys,
    const std::vector<double> * weights,
    kZUnionstoreAggregate agg, int64_t * _return)
{
  return zxxxxxstore(true, destination, keys, weights, agg, _return);
}

bool Redis2::zunionstore(const std::string& destination,
    const string_vector_t& keys,
    const std::vector<double> * weights,
    kZUnionstoreAggregate agg, int64_t * _return)
{
  return zxxxxxstore(false, destination, keys, weights, agg, _return);
}

bool Redis2::exec_command(RedisCommand * command)
{
  CHECK_PTR_PARAM(command);

  if (!assure_connect())
    return false;

  return proto_->exec_command(command);
}

bool Redis2::exec_command(RedisCommand * command, const char * format, ...)
{
  CHECK_PTR_PARAM(command);

  if (!assure_connect())
    return false;

  va_list ap;
  va_start(ap, format);
  bool ret = proto_->exec_commandv(command, format, ap);
  va_end(ap);
  return ret;
}

bool Redis2::exec_pipeline(redis_command_vector_t * commands)
{
  CHECK_PTR_PARAM(commands);

  if (!assure_connect())
    return false;

  return proto_->exec_pipeline(commands);
}

bool Redis2::publish(const std::string& channel, const std::string& message,
    int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(PUBLISH);
  c.push_arg(channel);
  c.push_arg(message);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::multi()
{
  if (!assure_connect())
    return false;

  RedisCommand c(MULTI);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::unwatch()
{
  if (!assure_connect())
    return false;

  RedisCommand c(UNWATCH);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::watch(const string_vector_t& keys)
{
  if (!assure_connect())
    return false;

  RedisCommand c(WATCH);
  c.push_arg(keys);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::add_command(RedisCommand * command)
{
  CHECK_PTR_PARAM(command);

  if (!check_connect())
    return false;

  RedisCommand * inner_command = new RedisCommand;
  inner_command->swap(*command);

  if (!proto_->exec_command(inner_command))
    return false;

  transaction_cmds_.push_back(inner_command);
  return false;
}

bool Redis2::add_command(RedisCommand * command, const char * format, ...)
{
  CHECK_PTR_PARAM(command);

  if (!check_connect())
    return false;

  RedisCommand * inner_command = new RedisCommand;
  inner_command->swap(*command);

  va_list ap;
  va_start(ap, format);
  bool ret = proto_->exec_commandv(inner_command, format, ap);
  va_end(ap);
  if (!ret)
    return false;

  transaction_cmds_.push_back(inner_command);
  return true;
}

bool Redis2::exec(redis_command_vector_t * commands)
{
  CHECK_PTR_PARAM(commands);

  if (!check_connect())
    return false;

  RedisCommand c(EXEC);
  if (!proto_->exec_command(&c))
    return false;

  smbulk_t smb;
  if (c.out.get_smbulks(&smb)
      && smb.size()==transaction_cmds_.size())
  {
    for (size_t i=0; i<smb.size(); i++)
    {
      smb[i]->swap(transaction_cmds_[i]->out);

      RedisOutput& out = transaction_cmds_[i]->out;
      if (out.reply_type==kSpecialMultiBulk
          && out.ptr.smbulks
          && convertible_2_mbulks(*out.ptr.smbulks))
      {
        mbulk_t mb;
        (void)convert(out.ptr.smbulks, &mb);
        out.set_mbulks(&mb);
      }
    }
    commands->swap(transaction_cmds_);
    clear_smbulks(&smb);
    clear_commands(&transaction_cmds_);
    return true;
  }
  else
  {
    on_reply_type_error(&c);
    return false;
  }
}

bool Redis2::discard()
{
  if (!check_connect())
    return false;

  RedisCommand c(DISCARD);
  if (!proto_->exec_command(&c))
    return false;

  if (c.out.is_status_ok())
  {
    clear_commands(&transaction_cmds_);
    return true;
  }
  else
  {
    on_reply_type_error(&c);
    return false;
  }
}

bool Redis2::ping()
{
  if (!assure_connect())
    return false;

  RedisCommand c(PING);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_PONG();
}

bool Redis2::bgrewriteaof()
{
  if (!assure_connect())
    return false;

  RedisCommand c(BGREWRITEAOF);
  if (!proto_->exec_command(&c))
    return false;

  return c.out.is_status();
}

bool Redis2::bgsave()
{
  if (!assure_connect())
    return false;

  RedisCommand c(BGSAVE);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

bool Redis2::dbsize(int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(DBSIZE);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::info(std::string * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(INFO);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY2();
}

bool Redis2::info(const std::string& type, std::string * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(INFO);
  c.push_arg(type);
  if (!proto_->exec_command(&c))
    return false;

  GET_BULK_REPLY2();
}

bool Redis2::lastsave(int64_t * _return)
{
  CHECK_PTR_PARAM(_return);

  if (!assure_connect())
    return false;

  RedisCommand c(LASTSAVE);
  if (!proto_->exec_command(&c))
    return false;

  GET_INTEGER_REPLY();
}

bool Redis2::slaveof(const std::string& host, const std::string& port)
{
  if (!assure_connect())
    return false;

  RedisCommand c(SLAVEOF);
  c.push_arg(host);
  c.push_arg(port);
  if (!proto_->exec_command(&c))
    return false;

  CHECK_STATUS_OK();
}

LIBREDIS_NAMESPACE_END
