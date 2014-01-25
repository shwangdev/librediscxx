/** @file
 * @brief thread specific storage with boost::function as the cleanup function
 * @author yafei.zhang@langtaojin.com
 * @date
 * @version
 * inner header
 */
#ifndef _LANGTAOJIN_LIBREDIS_TSS_H_
#define _LANGTAOJIN_LIBREDIS_TSS_H_

#include "redis_common.h"
#include <boost/function.hpp>
#include <boost/thread/tss.hpp>

LIBREDIS_NAMESPACE_BEGIN

/* an example:
 * #include <iostream>
 *
 * using namespace std;
 * using namespace com::langtaojin::adgaga;
 *
 * void free_int(int * i)
 * {
 *   cout << __FUNCTION__ << ":" << *i << endl;
 *   delete i;
 * }
 *
 *
 * int main()
 * {
 *   boost::function<void (int *)> cleanup = free_int;
 *   thread_specific_ptr<int> tss(cleanup);
 *
 *   tss.reset(new int(10));
 *   tss.reset(new int(20));
 *   tss.reset(new int(30));
 *
 *   return 0;
 * }
 */

template <typename T>
class thread_specific_ptr
{
  private:
    thread_specific_ptr(const thread_specific_ptr&);
    thread_specific_ptr& operator=(const thread_specific_ptr&);

  public:
    typedef T element_type;
    typedef void (*cleanup_function_ptr_type)(T *);
    typedef boost::function<void (T *)> cleanup_function_obj_type;

  private:
    struct delete_data
      : public boost::detail::tss_cleanup_function
    {
      void operator()(void * data)
      {
        delete static_cast<T *>(data);
      }
    };

    struct custom_cleanup_function_ptr
      : public boost::detail::tss_cleanup_function
    {
      private:
        cleanup_function_ptr_type cleanup_function_;

      public:
        explicit custom_cleanup_function_ptr(cleanup_function_ptr_type cleanup_function)
          : cleanup_function_(cleanup_function) {}

        void operator()(void * data)
        {
          cleanup_function_(static_cast<T *>(data));
        }
    };

    struct custom_cleanup_function_obj
      : public boost::detail::tss_cleanup_function
    {
      private:
        cleanup_function_obj_type cleanup_function_;

      public:
        explicit custom_cleanup_function_obj(cleanup_function_obj_type cleanup_function)
          : cleanup_function_(cleanup_function) {}

        void operator()(void * data)
        {
          cleanup_function_(static_cast<T *>(data));
        }
    };

    boost::shared_ptr<boost::detail::tss_cleanup_function> cleanup_;

  public:
    thread_specific_ptr()
    {
      cleanup_.reset(boost::detail::heap_new<delete_data>(),
          boost::detail::do_heap_delete<delete_data>());
    }

    explicit thread_specific_ptr(cleanup_function_ptr_type func)
    {
      if (func)
      {
        cleanup_.reset(boost::detail::heap_new<custom_cleanup_function_ptr>(func),
            boost::detail::do_heap_delete<custom_cleanup_function_ptr>());
      }
      else
      {
        cleanup_.reset(boost::detail::heap_new<delete_data>(),
            boost::detail::do_heap_delete<delete_data>());
      }
    }

    explicit thread_specific_ptr(cleanup_function_obj_type func)
    {
      cleanup_.reset(boost::detail::heap_new<custom_cleanup_function_obj>(func),
          boost::detail::do_heap_delete<custom_cleanup_function_obj>());
    }

    ~thread_specific_ptr()
    {
      boost::detail::set_tss_data(this,
          boost::shared_ptr<boost::detail::tss_cleanup_function>(), NULL, true);
    }

    T * get() const
    {
      return static_cast<T*>(boost::detail::get_tss_data(this));
    }

    T * operator->() const
    {
      return get();
    }

    T& operator*() const
    {
      return *get();
    }

    T * release()const
    {
      T * const temp = get();
      boost::detail::set_tss_data(this,
          boost::shared_ptr<boost::detail::tss_cleanup_function>(), NULL, false);
      return temp;
    }

    void reset(T * new_value = NULL)const
    {
      T * const current_value = get();
      if (current_value!=new_value)
      {
        boost::detail::set_tss_data(this, cleanup_, new_value, true);
      }
    }
};

LIBREDIS_NAMESPACE_END

#endif// _LANGTAOJIN_LIBREDIS_TSS_H_
