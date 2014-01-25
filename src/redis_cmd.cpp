/** @file
 * @brief redis command definition
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#include "redis_cmd.h"
#include <ctype.h>// toupper
#include <algorithm>
#include <boost/assign/list_of.hpp>
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>

LIBREDIS_NAMESPACE_BEGIN

static const CommandInfo s_command_map[] =
{
  {NOOP, "NOOP", ARGC_NO_CHECKING, kNone},// place holder
  {APPEND, "APPEND", 2, kInteger},//
  {AUTH, "AUTH", 1, kStatus},//
  {BGREWRITEAOF, "BGREWRITEAOF", 0, kStatus},//
  {BGSAVE, "BGSAVE", 0, kStatus},//
  {BITCOUNT, "BITCOUNT", -1, kInteger},//
  {BITOP, "BITOP", -3, kInteger},//
  // BLPOP may block
  {BLPOP, "BLPOP", -2, kMultiBulk},//
  // BRPOP may block
  {BRPOP, "BRPOP", -2, kMultiBulk},//
  // BRPOPLPUSH may block
  {BRPOPLPUSH, "BRPOPLPUSH", 3, kDepends},//
  {CONFIG, "CONFIG", -1, kDepends},//
  {DBSIZE, "DBSIZE", 0, kInteger},//
  {DEBUG, "DEBUG", -1, kDepends},//
  {DECR, "DECR", 1, kInteger},//
  {DECRBY, "DECRBY", 2, kInteger},//
  {DEL, "DEL", -1, kInteger},//
  {DISCARD, "DISCARD", 0, kStatus},//
  {DUMP, "DUMP", 1, kBulk},//
  {ECHO, "ECHO", 1, kBulk},//
  {EVAL, "EVAL", -2, kDepends},//
  {EVALSHA, "EVALSHA", -2, kDepends},//
  // EXEC may return a special multi-bulk
  {EXEC, "EXEC", 0, kSpecialMultiBulk},//
  {EXISTS, "EXISTS", 1, kInteger},//
  {EXPIRE, "EXPIRE", 2, kInteger},//
  {EXPIREAT, "EXPIREAT", 2, kInteger},//
  {FLUSHALL, "FLUSHALL", 0, kStatus},//
  {FLUSHDB, "FLUSHDB", 0, kStatus},//
  {GET, "GET", 1, kBulk},//
  {GETBIT, "GETBIT", 2, kInteger},//
  {GETRANGE, "GETRANGE", 3, kBulk},//
  {GETSET, "GETSET", 2, kBulk},//
  {HDEL, "HDEL", -2, kInteger},//
  {HEXISTS, "HEXISTS", 2, kInteger},//
  {HGET, "HGET", 2, kBulk},//
  {HGETALL, "HGETALL", 1, kMultiBulk},//
  {HINCRBY, "HINCRBY", 3, kInteger},//
  {HINCRBYFLOAT, "HINCRBYFLOAT", 3, kBulk},//
  {HKEYS, "HKEYS", 1, kMultiBulk},//
  {HLEN, "HLEN", 1, kInteger},//
  {HMGET, "HMGET", -2, kMultiBulk},//
  {HMSET, "HMSET", -3, kStatus},//
  {HSET, "HSET", 3, kInteger},//
  {HSETNX, "HSETNX", 3, kInteger},//
  {HVALS, "HVALS", 1, kMultiBulk},//
  {INCR, "INCR", 1, kInteger},//
  {INCRBY, "INCRBY", 2, kInteger},//
  {INCRBYFLOAT, "INCRBYFLOAT", 2, kBulk},//
  {INFO, "INFO", ARGC_NO_CHECKING, kBulk},//
  {KEYS, "KEYS", 1, kMultiBulk},//
  {LASTSAVE, "LASTSAVE", 0, kInteger},//
  {LINDEX, "LINDEX", 2, kBulk},//
  {LINSERT, "LINSERT", 4, kInteger},//
  {LLEN, "LLEN", 1, kInteger},//
  {LPOP, "LPOP", 1, kBulk},//
  {LPUSH, "LPUSH", -2, kInteger},//
  {LPUSHX, "LPUSHX", 2, kInteger},//
  {LRANGE, "LRANGE", 3, kMultiBulk},//
  {LREM, "LREM", 3, kInteger},//
  {LSET, "LSET", 3, kStatus},//
  {LTRIM, "LTRIM", 3, kStatus},//
  {MGET, "MGET", -1, kMultiBulk},//
  {MIGRATE, "MIGRATE", 5, kStatus},//
  {MONITOR, "MONITOR", 0, kDepends},//
  {MOVE, "MOVE", 2, kInteger},//
  {MSET, "MSET", -2, kStatus},//
  {MSETNX, "MSETNX", -2, kInteger},//
  {MULTI, "MULTI", 0, kStatus},//
  {OBJECT, "OBJECT", -1, kDepends},//
  {PERSIST, "PERSIST", 1, kInteger},//
  {PEXPIRE, "PEXPIRE", 2, kInteger},//
  {PEXPIREAT, "PEXPIREAT", 2, kInteger},//
  {PING, "PING", 0, kStatus},//
  {PSETEX, "PSETEX", 3, kStatus},//
  // PSUBSCRIBE ... will return a special multi-bulk,
  {PSUBSCRIBE, "PSUBSCRIBE", -1, kSpecialMultiBulk},//
  {PTTL, "PTTL", 1, kInteger},//
  {PUBLISH, "PUBLISH", 2, kInteger},//
  // PUNSUBSCRIBE will block
  // PUNSUBSCRIBE ... will return a special multi-bulk
  {PUNSUBSCRIBE, "PUNSUBSCRIBE", ARGC_NO_CHECKING, kSpecialMultiBulk},//
  {QUIT, "QUIT", 0, kStatus},//
  {RANDOMKEY, "RANDOMKEY", 0, kBulk},//
  {RENAME, "RENAME", 2, kStatus},//
  {RENAMENX, "RENAMENX", 2, kInteger},//
  {RESTORE, "RESTORE", 3, kStatus},//
  {RPOP, "RPOP", 1, kBulk},//
  {RPOPLPUSH, "RPOPLPUSH", 2, kBulk},//
  {RPUSH, "RPUSH", -2, kInteger},//
  {RPUSHX, "RPUSHX", 2, kInteger},//
  {SADD, "SADD", -2, kInteger},//
  {SAVE, "SAVE", 0, kDepends},//
  {SCARD, "SCARD", 1, kInteger},//
  {SCRIPT, "SCRIPT", -1, kDepends},//
  {SDIFF, "SDIFF", -1, kMultiBulk},//
  {SDIFFSTORE, "SDIFFSTORE", -2, kInteger},//
  {SELECT, "SELECT", 1, kStatus},//
  {SET, "SET", 2, kStatus},//
  {SETBIT, "SETBIT", 3, kInteger},//
  {SETEX, "SETEX", 3, kStatus},//
  {SETNX, "SETNX", 2, kInteger},//
  {SETRANGE, "SETRANGE", 3, kInteger},//
  {SHUTDOWN, "SHUTDOWN", ARGC_NO_CHECKING, kStatus},//
  {SINTER, "SINTER", -1, kMultiBulk},//
  {SINTERSTORE, "SINTERSTORE", -2, kInteger},//
  {SISMEMBER, "SISMEMBER", 2, kInteger},//
  {SLAVEOF, "SLAVEOF", 2, kStatus},//
  {SLOWLOG, "SLOWLOG", 1, kDepends},//
  {SMEMBERS, "SMEMBERS", 1, kMultiBulk},//
  {SMOVE, "SMOVE", 3, kInteger},//
  {SORT, "SORT", -1, kMultiBulk},//
  {SPOP, "SPOP", 1, kBulk},//
  {SRANDMEMBER, "SRANDMEMBER", 1, kBulk},//
  {SREM, "SREM", -2, kInteger},//
  {STRLEN, "STRLEN", 1, kInteger},//
  // SUBSCRIBE ... will return a special multi-bulk,
  {SUBSCRIBE, "SUBSCRIBE", -1, kSpecialMultiBulk},//
  {SUNION, "SUNION", -1, kMultiBulk},//
  {SUNIONSTORE, "SUNIONSTORE", -2, kInteger},//
  {SYNC, "SYNC", ARGC_NO_CHECKING, kDepends},//
  {TIME, "TIME", 0, kMultiBulk},//
  {TTL, "TTL", 1, kInteger},//
  {TYPE, "TYPE", 1, kStatus},//
  // UNSUBSCRIBE will block
  // UNSUBSCRIBE ... will return a special multi-bulk
  {UNSUBSCRIBE, "UNSUBSCRIBE", ARGC_NO_CHECKING, kSpecialMultiBulk},//
  {UNWATCH, "UNWATCH", 0, kStatus},//
  {WATCH, "WATCH", -1, kStatus},//
  {ZADD, "ZADD", -3, kInteger},//
  {ZCARD, "ZCARD", 1, kInteger},//
  {ZCOUNT, "ZCOUNT", 3, kInteger},//
  {ZINCRBY, "ZINCRBY", 3, kBulk},//
  {ZINTERSTORE, "ZINTERSTORE", -3, kInteger},//
  {ZRANGE, "ZRANGE", -3, kMultiBulk},//
  {ZRANGEBYSCORE, "ZRANGEBYSCORE", -3, kMultiBulk},//
  {ZRANK, "ZRANK", 2, kDepends},//
  {ZREM, "ZREM", -2, kInteger},//
  {ZREMRANGEBYRANK, "ZREMRANGEBYRANK", 3, kInteger},//
  {ZREMRANGEBYSCORE, "ZREMRANGEBYSCORE", 3, kInteger},//
  {ZREVRANGE, "ZREVRANGE", -3, kMultiBulk},//
  {ZREVRANGEBYSCORE, "ZREVRANGEBYSCORE", -3, kMultiBulk},//
  {ZREVRANK, "ZREVRANK", 2, kDepends},//
  {ZSCORE, "ZSCORE", 2, kBulk},//
  {ZUNIONSTORE, "ZUNIONSTORE", -3, kInteger},//
  {COMMAND_MAX, "COMMAND_MAX", ARGC_NO_CHECKING, kNone},// place holder
};

typedef std::map<std::string, kCommand> command_rev_map_t;
command_rev_map_t s_command_rev_map = /*lint --e(64) */boost::assign::map_list_of
("APPEND",APPEND)
("AUTH",AUTH)
("BGREWRITEAOF",BGREWRITEAOF)
("BGSAVE",BGSAVE)
("BITCOUNT",BITCOUNT)
("BITOP",BITOP)
("BLPOP",BLPOP)
("BRPOP",BRPOP)
("BRPOPLPUSH",BRPOPLPUSH)
("CONFIG",CONFIG)
("DBSIZE",DBSIZE)
("DEBUG",DEBUG)
("DECR",DECR)
("DECRBY",DECRBY)
("DEL",DEL)
("DISCARD",DISCARD)
("DUMP",DUMP)
("ECHO",ECHO)
("EVAL",EVAL)
("EVALSHA",EVALSHA)
("EXEC",EXEC)
("EXISTS",EXISTS)
("EXPIRE",EXPIRE)
("EXPIREAT",EXPIREAT)
("FLUSHALL",FLUSHALL)
("FLUSHDB",FLUSHDB)
("GET",GET)
("GETBIT",GETBIT)
("GETRANGE",GETRANGE)
("GETSET",GETSET)
("HDEL",HDEL)
("HEXISTS",HEXISTS)
("HGET",HGET)
("HGETALL",HGETALL)
("HINCRBY",HINCRBY)
("HINCRBYFLOAT",HINCRBYFLOAT)
("HKEYS",HKEYS)
("HLEN",HLEN)
("HMGET",HMGET)
("HMSET",HMSET)
("HSET",HSET)
("HSETNX",HSETNX)
("HVALS",HVALS)
("INCR",INCR)
("INCRBY",INCRBY)
("INCRBYFLOAT",INCRBYFLOAT)
("INFO",INFO)
("KEYS",KEYS)
("LASTSAVE",LASTSAVE)
("LINDEX",LINDEX)
("LINSERT",LINSERT)
("LLEN",LLEN)
("LPOP",LPOP)
("LPUSH",LPUSH)
("LPUSHX",LPUSHX)
("LRANGE",LRANGE)
("LREM",LREM)
("LSET",LSET)
("LTRIM",LTRIM)
("MGET",MGET)
("MIGRATE",MIGRATE)
("MONITOR",MONITOR)
("MOVE",MOVE)
("MSET",MSET)
("MSETNX",MSETNX)
("MULTI",MULTI)
("OBJECT",OBJECT)
("PERSIST",PERSIST)
("PEXPIRE",PEXPIRE)
("PEXPIREAT",PEXPIREAT)
("PING",PING)
("PSETEX",PSETEX)
("PSUBSCRIBE",PSUBSCRIBE)
("PTTL",PTTL)
("PUBLISH",PUBLISH)
("PUNSUBSCRIBE",PUNSUBSCRIBE)
("QUIT",QUIT)
("RANDOMKEY",RANDOMKEY)
("RENAME",RENAME)
("RENAMENX",RENAMENX)
("RESTORE",RESTORE)
("RPOP",RPOP)
("RPOPLPUSH",RPOPLPUSH)
("RPUSH",RPUSH)
("RPUSHX",RPUSHX)
("SADD",SADD)
("SAVE",SAVE)
("SCARD",SCARD)
("SCRIPT",SCRIPT)
("SDIFF",SDIFF)
("SDIFFSTORE",SDIFFSTORE)
("SELECT",SELECT)
("SET",SET)
("SETBIT",SETBIT)
("SETEX",SETEX)
("SETNX",SETNX)
("SETRANGE",SETRANGE)
("SHUTDOWN",SHUTDOWN)
("SINTER",SINTER)
("SINTERSTORE",SINTERSTORE)
("SISMEMBER",SISMEMBER)
("SLAVEOF",SLAVEOF)
("SLOWLOG",SLOWLOG)
("SMEMBERS",SMEMBERS)
("SMOVE",SMOVE)
("SORT",SORT)
("SPOP",SPOP)
("SRANDMEMBER",SRANDMEMBER)
("SREM",SREM)
("STRLEN",STRLEN)
("SUBSCRIBE",SUBSCRIBE)
("SUNION",SUNION)
("SUNIONSTORE",SUNIONSTORE)
("SYNC",SYNC)
("TTL",TTL)
("TIME",TIME)
("TYPE",TYPE)
("UNSUBSCRIBE",UNSUBSCRIBE)
("UNWATCH",UNWATCH)
("WATCH",WATCH)
("ZADD",ZADD)
("ZCARD",ZCARD)
("ZCOUNT",ZCOUNT)
("ZINCRBY",ZINCRBY)
("ZINTERSTORE",ZINTERSTORE)
("ZRANGE",ZRANGE)
("ZRANGEBYSCORE",ZRANGEBYSCORE)
("ZRANK",ZRANK)
("ZREM",ZREM)
("ZREMRANGEBYRANK",ZREMRANGEBYRANK)
("ZREMRANGEBYSCORE",ZREMRANGEBYSCORE)
("ZREVRANGE",ZREVRANGE)
("ZREVRANGEBYSCORE",ZREVRANGEBYSCORE)
("ZREVRANK",ZREVRANK)
("ZSCORE",ZSCORE)
("ZUNIONSTORE",ZUNIONSTORE);


/************************************************************************/
/*global helper functions*/
/************************************************************************/
const char * to_string(kReplyType reply_type)
{
  switch (reply_type)
  {
    case kStatus:
      return "status reply";
    case kError:
      return "error reply";
    case kInteger:
      return "integer reply";
    case kBulk:
      return "bulk reply";
    case kMultiBulk:
      return "multi-bulk reply";
    case kSpecialMultiBulk:
      return "special multi-bulk reply";
    case kDepends:
      return "non-defined reply";
    case kNone:
    default:
      return "none type reply";
  }
}

void clear_mbulks(mbulk_t * mbulks)
{
  if (mbulks==NULL)
    return;

  BOOST_FOREACH(std::string * str, (*mbulks))
  {
    delete str;
  }
  mbulks->clear();
}

void delete_mbulks(mbulk_t * mbulks)
{
  if (mbulks==NULL)
    return;

  clear_mbulks(mbulks);
  delete mbulks;
}

void append_mbulks(mbulk_t * to, mbulk_t * from)
{
  if (to==NULL || from==NULL)
    return;

  mbulk_t::const_iterator first = from->begin();
  mbulk_t::const_iterator last = from->end();
  to->insert(to->end(), first, last);
  from->clear();
}

void convert(mbulk_t * from, string_vector_t * to)
{
  if (from==NULL || to==NULL)
    return;

  to->clear();
  to->reserve(from->size());

  BOOST_FOREACH(std::string * s, (*from))
  {
    if (s)
    {
      to->push_back(std::string());
      to->back().swap(*s);
    }
  }

  clear_mbulks(from);
}

void clear_smbulks(smbulk_t * smbulks)
{
  if (smbulks==NULL)
    return;

  BOOST_FOREACH(RedisOutput * output, (*smbulks))
  {
    delete output;
  }
  smbulks->clear();
}

void delete_smbulks(smbulk_t * smbulks)
{
  if (smbulks==NULL)
    return;

  clear_smbulks(smbulks);
  delete smbulks;
}

void append_smbulks(smbulk_t * to, smbulk_t * from)
{
  if (to==NULL || from==NULL)
    return;

  smbulk_t::const_iterator first = from->begin();
  smbulk_t::const_iterator last = from->end();
  to->insert(to->end(), first, last);
  from->clear();
}

bool convertible_2_mbulks(const smbulk_t& from)
{
  BOOST_FOREACH(RedisOutput * output, from)
  {
    if (output->reply_type!=kBulk)
      return false;
  }
  return true;
}

bool convert(smbulk_t * from, mbulk_t * to)
{
  if (from==NULL || to==NULL)
    return false;

  if (!convertible_2_mbulks(*from))
    return false;

  clear_mbulks(to);
  to->reserve(from->size());

  BOOST_FOREACH(RedisOutput * output, (*from))
  {
    if (output->ptr.bulk)
    {
      to->push_back(output->ptr.bulk);
      output->ptr.bulk = NULL;
    }
    else
    {
      to->push_back(NULL);
    }
  }

  clear_smbulks(from);
  return true;
}

void clear_commands(redis_command_vector_t * commands)
{
  if (commands==NULL)
    return;

  BOOST_FOREACH(RedisCommand * command, (*commands))
  {
    delete command;
  }
  commands->clear();
}

void delete_commands(redis_command_vector_t * commands)
{
  if (commands==NULL)
    return;

  clear_commands(commands);
  delete commands;
}

/**
 * default hash function (a popular one from Bernstein)
 * inspired by redis
 */
uint32_t time33_hash_32(const void * key, size_t length)
{
  uint32_t hash = 5381;// magic number
  const char * str = (const char *)key;

  if (NULL==key || 0==length)
    return 0;

  /*lint -save -e737 */
  for (;length>=8;length -= 8)
  {
    // expand loop
    hash = ((hash << 5) + hash) + *str++;
    hash = ((hash << 5) + hash) + *str++;
    hash = ((hash << 5) + hash) + *str++;
    hash = ((hash << 5) + hash) + *str++;
    hash = ((hash << 5) + hash) + *str++;
    hash = ((hash << 5) + hash) + *str++;
    hash = ((hash << 5) + hash) + *str++;
    hash = ((hash << 5) + hash) + *str++;
  }

  switch (length)
  {
    case 7: hash = ((hash << 5) + hash) + *str++; /*lint -fallthrough */
    case 6: hash = ((hash << 5) + hash) + *str++; /*lint -fallthrough */
    case 5: hash = ((hash << 5) + hash) + *str++; /*lint -fallthrough */
    case 4: hash = ((hash << 5) + hash) + *str++; /*lint -fallthrough */
    case 3: hash = ((hash << 5) + hash) + *str++; /*lint -fallthrough */
    case 2: hash = ((hash << 5) + hash) + *str++; /*lint -fallthrough */
    case 1: hash = ((hash << 5) + hash) + *str++; break;
    case 0: break;
    default: break;
  }
  /*lint -restore */

  return hash;
}

uint32_t time33_hash_32(const std::string& key)
{
  return time33_hash_32(key.c_str(), key.size());
}

/************************************************************************/
/*RedisInput*/
/************************************************************************/
RedisInput::RedisInput()
  : command_(NOOP), command_info_(&s_command_map[NOOP]) {}

RedisInput::RedisInput(kCommand cmd)
  : command_(cmd), command_info_(&s_command_map[cmd]) {}

RedisInput::RedisInput(const std::string& cmd)
  : command_(NOOP), command_info_(&s_command_map[NOOP])
{
  set_command(cmd);
}

void RedisInput::set_command(kCommand cmd)
{
  command_ = cmd;
  command_info_ = &s_command_map[cmd];
}

void RedisInput::set_command(const std::string& cmd)
{
  std::string upper_cmd = cmd;
  for (size_t i=0; i<upper_cmd.size(); i++)
  {
    char& c = upper_cmd[i];
    c = static_cast<char>(::toupper(c));
  }

  command_rev_map_t::const_iterator iter
    = s_command_rev_map.find(upper_cmd);

  if (iter!=s_command_rev_map.end())
    set_command(iter->second);
  else
    set_command(NOOP);
}

void RedisInput::swap(RedisInput& other)
{
  std::swap(command_, other.command_);
  std::swap(command_info_, other.command_info_);
  args_.swap(other.args_);
}

void RedisInput::clear_arg()
{
  args_.clear();
}

void RedisInput::push_arg(const std::string& s)
{
  args_.push_back(s);
}

void RedisInput::push_arg(const char * s)
{
  if (s)
    args_.push_back(s);
}

void RedisInput::push_arg(const string_vector_t& sv)
{
  BOOST_FOREACH(const std::string& s, sv)
  {
    args_.push_back(s);
  }
}

void RedisInput::push_arg(int64_t i)
{
  args_.push_back(boost::lexical_cast<std::string>(i));
}

void RedisInput::push_arg(size_t i)
{
  args_.push_back(boost::lexical_cast<std::string>(i));
}

void RedisInput::push_arg(int i)
{
  args_.push_back(boost::lexical_cast<std::string>(i));
}

void RedisInput::push_arg(const std::vector<int64_t>& iv)
{
  BOOST_FOREACH(int64_t i, iv)
  {
    args_.push_back(boost::lexical_cast<std::string>(i));
  }
}

void RedisInput::push_arg(double d)
{
  args_.push_back(boost::lexical_cast<std::string>(d));
}

void RedisInput::push_arg(const std::vector<double>& dv)
{
  BOOST_FOREACH(double d, dv)
  {
    args_.push_back(boost::lexical_cast<std::string>(d));
  }
}

/************************************************************************/
/*RedisOutput*/
/************************************************************************/
  RedisOutput::RedisOutput()
: reply_type(kNone)
{
  ptr.status = NULL;
}

RedisOutput::~RedisOutput()
{
  clear();
}

void RedisOutput::clear()
{
  switch (reply_type)
  {
    case kStatus:
      if (ptr.status)
      {
        delete ptr.status;
        ptr.status = NULL;
      }
      break;
    case kError:
      if (ptr.error)
      {
        delete ptr.error;
        ptr.error = NULL;
      }
      break;
    case kInteger:
      if (ptr.i)
      {
        delete ptr.i;
        ptr.i = NULL;
      }
      break;
    case kBulk:
      if (ptr.bulk)
      {
        delete ptr.bulk;
        ptr.bulk = NULL;
      }
      break;
    case kMultiBulk:
      if (ptr.mbulks)
      {
        delete_mbulks(ptr.mbulks);
        ptr.mbulks = NULL;
      }
      break;
    case kSpecialMultiBulk:
      if (ptr.smbulks)
      {
        delete_smbulks(ptr.smbulks);
        ptr.smbulks = NULL;
      }
      break;
    default:
      ptr.status = NULL;
  }
}

bool RedisOutput::get_mbulks(string_vector_t * mb)
{
  if (mb==NULL)
    return false;

  if (is_mbulks())
  {
    convert(ptr.mbulks, mb);
    return true;
  }

  return false;
}

void RedisOutput::swap(RedisOutput& other)
{
  std::swap(ptr, other.ptr);
  std::swap(reply_type, other.reply_type);
}

/************************************************************************/
/*RedisCommand*/
/************************************************************************/
RedisCommand::RedisCommand()
  : in() {}

RedisCommand::RedisCommand(kCommand cmd)
  : in(cmd) {}

RedisCommand::RedisCommand(const std::string& cmd)
  : in(cmd) {}

void RedisCommand::swap(RedisCommand& other)
{
  in.swap(other.in);
  out.swap(other.out);
}

LIBREDIS_NAMESPACE_END
