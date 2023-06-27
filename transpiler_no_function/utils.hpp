#ifndef UTILS
#define UTILS
#include <iostream>
#include <utility>
#include <vector>
#include <cassert>

namespace utils
{
    template <typename ReturnType, typename... Args>
    struct function_traits_defs
    {
        static constexpr size_t arity = sizeof...(Args);

        using result_type = ReturnType;

        template <size_t i>
        struct arg
        {
            using type = typename std::tuple_element<i, std::tuple<Args...>>::type;
        };
    };

    template <typename T>
    struct function_traits_impl;

    template <typename ReturnType, typename... Args>
    struct function_traits_impl<ReturnType(Args...)>
        : function_traits_defs<ReturnType, Args...>
    {
    };

    template <typename ReturnType, typename... Args>
    struct function_traits_impl<ReturnType (*)(Args...)>
        : function_traits_defs<ReturnType, Args...>
    {
    };

    template <typename ClassType, typename ReturnType, typename... Args>
    struct function_traits_impl<ReturnType (ClassType::*)(Args...)>
        : function_traits_defs<ReturnType, Args...>
    {
    };

    template <typename ClassType, typename ReturnType, typename... Args>
    struct function_traits_impl<ReturnType (ClassType::*)(Args...) const>
        : function_traits_defs<ReturnType, Args...>
    {
    };

    template <typename ClassType, typename ReturnType, typename... Args>
    struct function_traits_impl<ReturnType (ClassType::*)(Args...) const &>
        : function_traits_defs<ReturnType, Args...>
    {
    };

    template <typename ClassType, typename ReturnType, typename... Args>
    struct function_traits_impl<ReturnType (ClassType::*)(Args...) const &&>
        : function_traits_defs<ReturnType, Args...>
    {
    };

    template <typename ClassType, typename ReturnType, typename... Args>
    struct function_traits_impl<ReturnType (ClassType::*)(Args...) volatile>
        : function_traits_defs<ReturnType, Args...>
    {
    };

    template <typename ClassType, typename ReturnType, typename... Args>
    struct function_traits_impl<ReturnType (ClassType::*)(Args...) volatile &>
        : function_traits_defs<ReturnType, Args...>
    {
    };

    template <typename ClassType, typename ReturnType, typename... Args>
    struct function_traits_impl<ReturnType (ClassType::*)(Args...) volatile &&>
        : function_traits_defs<ReturnType, Args...>
    {
    };

    template <typename ClassType, typename ReturnType, typename... Args>
    struct function_traits_impl<ReturnType (ClassType::*)(Args...) const volatile>
        : function_traits_defs<ReturnType, Args...>
    {
    };

    template <typename ClassType, typename ReturnType, typename... Args>
    struct function_traits_impl<ReturnType (ClassType::*)(Args...) const volatile &>
        : function_traits_defs<ReturnType, Args...>
    {
    };

    template <typename ClassType, typename ReturnType, typename... Args>
    struct function_traits_impl<ReturnType (ClassType::*)(Args...) const volatile &&>
        : function_traits_defs<ReturnType, Args...>
    {
    };

    template <typename T, typename V = void>
    struct function_traits
        : function_traits_impl<T>
    {
    };

    template <typename T>
    struct function_traits<T, decltype((void)&T::operator())>
        : function_traits_impl<decltype(&T::operator())>
    {
    };

    template <size_t... Indices>
    struct indices
    {
        using next = indices<Indices..., sizeof...(Indices)>;
    };
    template <size_t N>
    struct build_indices
    {
        using type = typename build_indices<N - 1>::type::next;
    };
    template <>
    struct build_indices<0>
    {
        using type = indices<>;
    };
    template <size_t N>
    using BuildIndices = typename build_indices<N>::type;

    namespace details
    {
        template <typename FuncType,
                  typename VecType,
                  size_t... I,
                  typename Traits = function_traits<FuncType>,
                  typename ReturnT = typename Traits::result_type>
        ReturnT do_call(FuncType &func,
                        VecType &args,
                        indices<I...>)
        {
            assert(args.size() >= Traits::arity);
            return func(args[I]...);
        }
    } // namespace details

    template <typename FuncType,
              typename VecType,
              typename Traits = function_traits<FuncType>,
              typename ReturnT = typename Traits::result_type>
    ReturnT unpack_caller(FuncType &func,
                          VecType &args)
    {
        return details::do_call(func, args, BuildIndices<Traits::arity>());
    }
} // namespace util

template <size_t num_args>
struct unpack_caller
{
private:
    template <typename FuncType, size_t... I>
    auto call(FuncType &f, std::vector<int> &args, std::index_sequence<I...>){
        return f(args[I]...);
    }

public:
    template <typename FuncType>
    auto operator()(FuncType &f, std::vector<int> &args){
        assert(args.size() == num_args); // just to be sure
        return call(f, args, std::make_index_sequence<num_args>{});
    }
};

template <size_t num_args>
struct format_caller
{
private:
    template <size_t... I>
    std::string call(std::string &f, std::vector<int> &args, std::string &temp, std::index_sequence<I...>){
        return f(temp, args[I]...);
    }

public:
    // template <typename FuncType>
    std::string operator()(std::string &f, std::string temp, std::vector<int> &args){
        assert(args.size() == num_args); // just to be sure
        return call(f, args, temp, std::make_index_sequence<num_args>{});
    }
};

#endif