/** @file
 * @brief redis command definition
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 *
 */
#ifndef _LANGTAOJIN_LIBREDIS_REDIS_CMD_H_
#define _LANGTAOJIN_LIBREDIS_REDIS_CMD_H_

#include "redis_common.h"

// an 'ECHO' macro may be disturbing
#ifdef ECHO
#undef ECHO
#endif

// a 'setbit' macro may be disturbing
#ifdef setbit
#undef setbit
#endif

//lint -esym(148,*push_arg)

LIBREDIS_NAMESPACE_BEGIN

enum kCommand
{
  NOOP = 0,// place holder
  APPEND,
  AUTH,
  BGREWRITEAOF,
  BGSAVE,
  BITCOUNT,
  BITOP,
  BLPOP,
  BRPOP,
  BRPOPLPUSH,
  CONFIG,
  DBSIZE,
  DEBUG,
  DECR,
  DECRBY,
  DEL,
  DISCARD,
  DUMP,
  ECHO,
  EVAL,
  EVALSHA,
  EXEC,
  EXISTS,
  EXPIRE,
  EXPIREAT,
  FLUSHALL,
  FLUSHDB,
  GET,
  GETBIT,
  GETRANGE,
  GETSET,
  HDEL,
  HEXISTS,
  HGET,
  HGETALL,
  HINCRBY,
  HINCRBYFLOAT,
  HKEYS,
  HLEN,
  HMGET,
  HMSET,
  HSET,
  HSETNX,
  HVALS,
  INCR,
  INCRBY,
  INCRBYFLOAT,
  INFO,
  KEYS,
  LASTSAVE,
  LINDEX,
  LINSERT,
  LLEN,
  LPOP,
  LPUSH,
  LPUSHX,
  LRANGE,
  LREM,
  LSET,
  LTRIM,
  MGET,
  MIGRATE,
  MONITOR,
  MOVE,
  MSET,
  MSETNX,
  MULTI,
  OBJECT,
  PERSIST,
  PEXPIRE,
  PEXPIREAT,
  PING,
  PSETEX,
  PSUBSCRIBE,
  PTTL,
  PUBLISH,
  PUNSUBSCRIBE,
  QUIT,
  RANDOMKEY,
  RENAME,
  RENAMENX,
  RESTORE,
  RPOP,
  RPOPLPUSH,
  RPUSH,
  RPUSHX,
  SADD,
  SAVE,
  SCARD,
  SCRIPT,
  SDIFF,
  SDIFFSTORE,
  SELECT,
  SET,
  SETBIT,
  SETEX,
  SETNX,
  SETRANGE,
  SHUTDOWN,
  SINTER,
  SINTERSTORE,
  SISMEMBER,
  SLAVEOF,
  SLOWLOG,
  SMEMBERS,
  SMOVE,
  SORT,
  SPOP,
  SRANDMEMBER,
  SREM,
  STRLEN,
  SUBSCRIBE,
  SUNION,
  SUNIONSTORE,
  SYNC,
  TIME,
  TTL,
  TYPE,
  UNSUBSCRIBE,
  UNWATCH,
  WATCH,
  ZADD,
  ZCARD,
  ZCOUNT,
  ZINCRBY,
  ZINTERSTORE,
  ZRANGE,
  ZRANGEBYSCORE,
  ZRANK,
  ZREM,
  ZREMRANGEBYRANK,
  ZREMRANGEBYSCORE,
  ZREVRANGE,
  ZREVRANGEBYSCORE,
  ZREVRANK,
  ZSCORE,
  ZUNIONSTORE,
  COMMAND_MAX// place holder
};

enum kReplyType
{
  kNone = 0,// place holder

  // +xxx\r\n
  kStatus,

  // -xxx\r\n
  kError,

  // :0\r\n
  // :1000\r\n
  kInteger,

  // $6\r\nfoobar\r\n
  // $0\r\n    ---   an empty bulk
  // $-1\r\n   ---   a nil bulk
  kBulk,

  /**
   * *4\r\n
   * $3\r\n
   * foo\r\n
   * $3\r\n
   * bar\r\n
   * $5\r\n
   * Hello\r\n
   * $5\r\n
   * World\r\n
   */
  /**
   * *4\r\n
   * $3\r\n
   * foo\r\n
   * $3\r\n
   * bar\r\n
   * $-1\r\n   ---   a nil bulk
   * $5\r\n
   * World\r\n
   */
  // *0\r\n    ---   an empty multi-bulk
  // *-1\r\n   ---   a nil multi-bulk
  kMultiBulk,

  // in a multi-bulk, there may be a an integer or another multi-bulk recursively
  kSpecialMultiBulk,

  // return type is not certain
  kDepends
};

struct CommandInfo
{
  kCommand command;
  std::string command_str;

  // 'argc' is the argument number for input checking:
  //  zero and positive value: the exact argument number
  //  negative value: varidic args, real argument number must be more than or equal to abs('argc')
  //  -65535 means 'argc' will not be checked
#define ARGC_NO_CHECKING (-65535)
  int argc;

  kReplyType reply_type;
};

/************************************************************************/
/*typedefs*/
/************************************************************************/
typedef std::vector<std::string> string_vector_t;
typedef std::vector<std::string *> mbulk_t;
struct RedisOutput;
typedef std::vector<RedisOutput *> smbulk_t;
struct RedisCommand;
typedef std::vector<RedisCommand *> redis_command_vector_t;
typedef std::pair<std::string, std::string> string_pair_t;
typedef std::vector<string_pair_t> string_pair_vector_t;
typedef std::map<std::string, std::string> string_map_t;
typedef std::vector<size_t> size_t_vector_t;
typedef std::vector<size_t_vector_t> size_t_vector_vector_t;

/************************************************************************/
/*global helper functions*/
/************************************************************************/
const char * to_string(kReplyType reply_type);

void clear_mbulks(mbulk_t * mbulks);
void delete_mbulks(mbulk_t * mbulks);
void append_mbulks(mbulk_t * to, mbulk_t * from);

void convert(mbulk_t * from, string_vector_t * to);

void clear_smbulks(smbulk_t * smbulks);
void delete_smbulks(smbulk_t * smbulks);
void append_smbulks(smbulk_t * to, smbulk_t * from);

bool convertible_2_mbulks(const smbulk_t& from);
bool convert(smbulk_t * from, mbulk_t * to);

void clear_commands(redis_command_vector_t * commands);
void delete_commands(redis_command_vector_t * commands);

uint32_t time33_hash_32(const void * key, size_t length);
uint32_t time33_hash_32(const std::string& key);

typedef uint32_t (*key_hasher) (const std::string& key);

/************************************************************************/
/*smart pointers for mbulk_t,smbulk_t and redis_command_vector_t*/
/************************************************************************/
inline void clear(mbulk_t * mbulks)
{
  clear_mbulks(mbulks);
}

// delete is a keyword in C++
inline void _delete(mbulk_t * mbulks)
{
  delete_mbulks(mbulks);
}

inline void clear(smbulk_t * smbulks)
{
  clear_smbulks(smbulks);
}

// delete is a keyword in C++
inline void _delete(smbulk_t * smbulks)
{
  delete_smbulks(smbulks);
}

inline void clear(redis_command_vector_t * commands)
{
  clear_commands(commands);
}

inline void _delete(redis_command_vector_t * commands)
{
  delete_commands(commands);
}

template <class T>
class ClearGuard
{
  private:
    T * t_;

  public:
    ClearGuard(T * t) : t_(t) {}

    ~ClearGuard()
    {
      if (t_)
        clear(t_);
    }
};

template <class T>
class DeleteGuard
{
  private:
    T * t_;

  public:
    DeleteGuard(T * t)
      :t_(t)
    {
    }

    ~DeleteGuard()
    {
      if (t_)
        _delete(t_);
    }
};

/************************************************************************/
/*RedisInput*/
/************************************************************************/
class RedisInput
{
  private:
    kCommand command_;
    const CommandInfo * command_info_;
    string_vector_t args_;

  public:
    RedisInput();
    explicit RedisInput(kCommand cmd);
    explicit RedisInput(const std::string& cmd);

    void set_command(kCommand cmd);
    void set_command(const std::string& cmd);
    void swap(RedisInput& other);

    const kCommand& command()const
    {
      return command_;
    }

    const CommandInfo& command_info()const
    {
      return *command_info_;
    }

    string_vector_t& args()
    {
      return /*lint -e(1536) */args_;
    }

    const string_vector_t& args()const
    {
      return args_;
    }

    void clear_arg();
    void push_arg(const std::string& s);
    void push_arg(const char * s);
    void push_arg(const string_vector_t& sv);
    void push_arg(int64_t i);
    void push_arg(size_t i);
    void push_arg(int i);
    void push_arg(const std::vector<int64_t>& iv);
    void push_arg(double d);
    void push_arg(const std::vector<double>& dv);
};

/************************************************************************/
/*RedisOutput*/
/************************************************************************/
struct RedisOutput
{
  // NOTICE:
  // All outputs variables are public for efficient access(READING only).
  // All writing operations must use setters.
  union _ptr
  {
    // 'reply_type'==kStatus
    std::string * status;

    // 'reply_type'==kError
    std::string * error;

    // 'reply_type'==kInteger
    int64_t * i;

    // 'reply_type'==kBulk
    // NULL means nil bulk
    std::string * bulk;

    // 'reply_type'==kMultiBulk
    // 1.NULL means a nil multi-bulk
    // 2.'mbulks' being not NULL and 'mbulks->empty()' means an empty multi-bulk
    // 3.NULL elements in 'mbulks' mean nil objects
    mbulk_t * mbulks;

    // 'reply_type'==kSpecialMultiBulk
    smbulk_t * smbulks;
  } ptr;

  kReplyType reply_type;

  public:
  RedisOutput();
  ~RedisOutput();

  void clear();

  // setters
  void set_status(const std::string& s)
  {
    clear();
    ptr.status = new std::string(s);
    reply_type = kStatus;
  }

  void set_error(const std::string& e)
  {
    clear();
    ptr.error = new std::string(e);
    reply_type = kError;
  }

  void set_i(int64_t _i)
  {
    clear();
    ptr.i = new int64_t;
    *ptr.i = _i;
    reply_type = kInteger;
  }

  void set_bulk(const std::string& b)
  {
    clear();
    ptr.bulk = new std::string(b);
    reply_type = kBulk;
  }

  void set_nil_bulk()
  {
    clear();
    reply_type = kBulk;
  }

  void set_mbulks(mbulk_t * mb)
  {
    clear();
    reply_type = kMultiBulk;

    if (mb)
    {
      ptr.mbulks = new mbulk_t();
      ptr.mbulks->swap(*mb);
    }
  }

  void set_nil_mbulks()
  {
    clear();
    reply_type = kMultiBulk;
  }

  void set_smbulks(smbulk_t * smbulks)
  {
    clear();
    reply_type = kSpecialMultiBulk;

    if (smbulks)
    {
      ptr.smbulks = new smbulk_t();
      ptr.smbulks->swap(*smbulks);
    }
  }

  void set_nil_smbulks()
  {
    clear();
    reply_type = kSpecialMultiBulk;
  }

  // is...
  bool is_status()const
  {
    return reply_type==kStatus && ptr.status;
  }

  bool is_status_ok()const
  {
    return reply_type==kStatus && ptr.status && *ptr.status=="OK";
  }

  bool is_status_pong()const
  {
    return reply_type==kStatus && ptr.status && *ptr.status=="PONG";
  }

  bool is_error()const
  {
    return reply_type==kError && ptr.error;
  }

  bool is_i()const
  {
    return reply_type==kInteger && ptr.i;
  }

  bool is_bulk()const
  {
    return reply_type==kBulk && ptr.bulk;
  }

  bool is_nil_bulk()const
  {
    return reply_type==kBulk && ptr.bulk==NULL;
  }

  bool is_mbulks()const
  {
    return reply_type==kMultiBulk && ptr.mbulks;
  }

  bool is_nil_mbulks()const
  {
    return reply_type==kMultiBulk && ptr.mbulks==NULL;
  }

  bool is_empty_mbulks()const
  {
    return reply_type==kMultiBulk && ptr.mbulks && ptr.mbulks->empty();
  }

  bool is_smbulks()const
  {
    return reply_type==kSpecialMultiBulk && ptr.smbulks;
  }

  bool is_nil_smbulks()const
  {
    return reply_type==kSpecialMultiBulk && ptr.smbulks==NULL;
  }

  bool is_empty_smbulks()const
  {
    return reply_type==kSpecialMultiBulk && ptr.smbulks && ptr.smbulks->empty();
  }

  // getters
  kReplyType get_reply_type()const
  {
    return reply_type;
  }

  bool get_status(std::string * _status)const
  {
    if (_status==NULL)
      return false;

    if (is_status())
    {
      *_status = *ptr.status;
      return true;
    }

    return false;
  }

  bool get_error(std::string * _error)const
  {
    if (_error==NULL)
      return false;

    if (is_error())
    {
      *_error = *ptr.error;
      return true;
    }

    return false;
  }

  bool get_i(int64_t * _i)const
  {
    if (_i==NULL)
      return false;

    if (is_i())
    {
      *_i = *ptr.i;
      return true;
    }

    return false;
  }

  bool get_bulk(std::string * b)
  {
    if (b==NULL)
      return false;

    if (is_bulk())
    {
      b->swap(*ptr.bulk);
      return true;
    }

    return false;
  }

  bool get_mbulks(mbulk_t * mb)
  {
    if (mb==NULL)
      return false;

    clear_mbulks(mb);
    if (is_mbulks())
    {
      mb->swap(*ptr.mbulks);
      return true;
    }

    return false;
  }

  bool get_mbulks(string_vector_t * mb);

  bool get_smbulks(smbulk_t * smb)
  {
    if (smb==NULL)
      return false;

    clear_smbulks(smb);
    if (is_smbulks())
    {
      smb->swap(*ptr.smbulks);
      return true;
    }

    return false;
  }

  void swap(RedisOutput& other);
};

/************************************************************************/
/*RedisCommand*/
/************************************************************************/
struct RedisCommand
{
  RedisInput in;
  RedisOutput out;

  RedisCommand();
  explicit RedisCommand(kCommand cmd);
  explicit RedisCommand(const std::string& cmd);
  void swap(RedisCommand& other);

  const kCommand& command()const
  {
    return in.command();
  }

  const CommandInfo& command_info()const
  {
    return in.command_info();
  }

  string_vector_t& args()
  {
    return in.args();
  }

  const string_vector_t& args()const
  {
    return in.args();
  }

  void clear_arg()
  {
    in.clear_arg();
  }

  void push_arg(const std::string& s)
  {
    in.push_arg(s);
  }

  void push_arg(const char * s)
  {
    in.push_arg(s);
  }

  void push_arg(const string_vector_t& sv)
  {
    in.push_arg(sv);
  }

  void push_arg(int64_t i)
  {
    in.push_arg(i);
  }

  void push_arg(size_t i)
  {
    in.push_arg(i);
  }

  void push_arg(int i)
  {
    in.push_arg(i);
  }

  void push_arg(const std::vector<int64_t>& iv)
  {
    in.push_arg(iv);
  }

  void push_arg(double d)
  {
    in.push_arg(d);
  }

  void push_arg(const std::vector<double>& dv)
  {
    in.push_arg(dv);
  }
};

LIBREDIS_NAMESPACE_END

#endif// _LANGTAOJIN_LIBREDIS_REDIS_CMD_H_
