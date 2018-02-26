// "THE BEER-WARE LICENSE" (Revision 42):
// <msmk@cern.ch> wrote this file.  As long as you retain this notice you
// can do whatever you want with this stuff. If we meet some day, and you think
// this stuff is worth it, you can buy me a beer in return.   Moritz Kiehn

/// \file
/// \brief   Command dispatcher that takes function names and string arguments
/// \author  Moritz Kiehn <msmk@cern.ch>
/// \date    2018-02-20

#pragma once

#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

namespace dfe {

/// A simple command dispatcher.
///
/// You can register commands and call them by name using string arguments.
class Dispatcher {
public:
  using NativeInterface = std::function<void(const std::vector<std::string>&)>;

  /// Register a command that implements the native dispatcher call interface.
  void add(std::string name, NativeInterface func, std::size_t nargs);
  /// Register a command with arbitrary arguments.
  ///
  /// All argument types must be constructible via the
  /// `std::istream& operator>>(...)` formatted input operator.
  template<typename... Args>
  void add(std::string name, void (*free_func)(Args...));
  template<typename T, typename... Args>
  void add(std::string name, void (T::*member_func)(Args...), T& t);
  template<typename... Args>
  void add(std::string name, std::function<void(Args...)> func);
  /// Call the command with some arguments.
  void call(const std::string& name, const std::vector<std::string>& args);

  /// Return a list of all registered commands and required number of arguments.
  std::vector<std::pair<std::string, std::size_t>> commands() const;

private:
  struct Command {
    NativeInterface func;
    std::size_t nargs;
  };
  std::unordered_map<std::string, Command> m_commands;
};

// implementations

inline void
Dispatcher::add(
  std::string name, Dispatcher::NativeInterface func, std::size_t nargs)
{
  if (m_commands.count(name)) {
    throw std::invalid_argument(
      "Can not register command '" + name + "' more than once");
  }
  m_commands[std::move(name)] = Command{func, nargs};
}

template<typename... Args>
inline void
Dispatcher::add(std::string name, void (*free_func)(Args...))
{
  add(std::move(name), std::function<void(Args...)>(free_func));
}

template<typename T, typename... Args>
inline void
Dispatcher::add(std::string name, void (T::*member_func)(Args...), T& t)
{
  add(
    std::move(name),
    std::function<void(Args...)>(std::bind(member_func, std::forward<T>(t))));
}

namespace dispatcher_impl {
namespace {

// compile time index sequence
// see e.g. http://loungecpp.net/cpp/indices-trick/
template<std::size_t...>
struct Sequence {
};
template<std::size_t N, std::size_t... INDICES>
struct SequenceGenerator : SequenceGenerator<N - 1, N - 1, INDICES...> {
};
template<std::size_t... INDICES>
struct SequenceGenerator<0, INDICES...> : Sequence<INDICES...> {
};

template<typename... Args>
struct WithArgumentDecoder {
  std::function<void(Args...)> func;

  void operator()(const std::vector<std::string>& args)
  {
    decode_and_call(args, SequenceGenerator<sizeof...(Args)>());
  }
  template<typename T>
  static T decode(const std::string& str)
  {
    T tmp;
    try {
      std::istringstream istr(str);
      // always throw on any error
      istr.exceptions(std::istringstream::badbit | std::istringstream::failbit);
      istr >> tmp;
    } catch (...) {
      throw std::invalid_argument(
        "Could not convert value '" + str + "' to type '" + typeid(T).name() +
        "'");
    }
    return tmp;
  }
  template<std::size_t... I>
  void decode_and_call(const std::vector<std::string>& args, Sequence<I...>)
  {
    func(decode<typename std::decay<Args>::type>(args.at(I))...);
  }
};

template<typename... Args>
inline Dispatcher::NativeInterface
make_native_interface(std::function<void(Args...)>&& function)
{
  return WithArgumentDecoder<Args...>{std::move(function)};
}

} // namespace
} // namespace dispatcher_impl

template<typename... Args>
inline void
Dispatcher::add(std::string name, std::function<void(Args...)> func)
{
  m_commands[std::move(name)] = Command{
    dispatcher_impl::make_native_interface(std::move(func)), sizeof...(Args)};
}

inline void
Dispatcher::call(const std::string& name, const std::vector<std::string>& args)
{
  auto cmd = m_commands.find(name);
  if (cmd == m_commands.end()) {
    throw std::invalid_argument("Unknown command '" + name + "'");
  }
  if (args.size() != cmd->second.nargs) {
    std::string msg;
    throw std::invalid_argument(
      "Command '" + name + "' expects " + std::to_string(cmd->second.nargs) +
      " arguments but " + std::to_string(args.size()) + " given");
  }
  cmd->second.func(args);
}

inline std::vector<std::pair<std::string, std::size_t>>
Dispatcher::commands() const
{
  std::vector<std::pair<std::string, std::size_t>> cmds;

  for (const auto& cmd : m_commands) {
    cmds.emplace_back(cmd.first, cmd.second.nargs);
  }
  return cmds;
}

} // namespace dfe
