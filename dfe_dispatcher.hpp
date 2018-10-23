// Copyright 2018 Moritz Kiehn
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

/// \file
/// \brief   Command dispatcher that takes function names and string arguments
/// \author  Moritz Kiehn <msmk@cern.ch>
/// \date    2018-02-20

#pragma once

#include <cassert>
#include <cstdint>
#include <functional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

namespace dfe {

/// Variable-type value object a.k.a. a poor mans std::variant.
class Variable final {
public:
  /// Supported value types.
  enum class Type { Empty, Boolean, Integer, Float, String };

  Variable()
    : m_type(Type::Empty)
  {
  }
  Variable(Variable&& v) { *this = std::move(v); }
  Variable(const Variable& v) { *this = v; }
  explicit Variable(std::string&& s)
    : m_string(std::move(s))
    , m_type(Type::String)
  {
  }
  explicit Variable(const std::string& s)
    : Variable(std::string(s))
  {
  }
  explicit Variable(const char* s)
    : Variable(std::string(s))
  {
  }
  // suppport all possible integer types
  template<typename I, typename = std::enable_if_t<std::is_integral<I>::value>>
  explicit Variable(I integer)
    : m_integer(static_cast<int64_t>(integer))
    , m_type(Type::Integer)
  {
  }
  explicit Variable(double d)
    : m_float(d)
    , m_type(Type::Float)
  {
  }
  explicit Variable(float f)
    : Variable(static_cast<double>(f))
  {
  }
  explicit Variable(bool b)
    : m_boolean(b)
    , m_type(Type::Boolean)
  {
  }
  ~Variable() = default;

  Variable& operator=(Variable&& v);
  Variable& operator=(const Variable& v);

  /// Parse a string into a value of the requested type.
  static Variable parse_as(const std::string& str, Type type);

  /// In a boolean context a variable is false if it does not contains a value.
  ///
  /// \warning This is not the value of the stored boolean.
  constexpr bool operator!() const { return m_type == Type::Empty; }
  /// \see operator!()
  constexpr explicit operator bool() const { return !!(*this); }
  /// The type of the currently stored value.
  constexpr Type type() const { return m_type; }
  /// Get value of the variable as a specific type.
  ///
  /// \exception std::invalid_argument if the requested type is incompatible
  template<typename T>
  auto as() const;

private:
  template<typename T>
  struct Converter;
  template<typename I>
  struct IntegerConverter;

  union {
    int64_t m_integer;
    double m_float;
    bool m_boolean;
  };
  // std::string has non-trivial constructor; cannot store by value in union
  // TODO 2018-11-29 msmk: more space-efficient string storage
  std::string m_string;
  Type m_type;

  friend std::ostream& operator<<(std::ostream& os, const Variable& v);
};

/// A simple command dispatcher.
///
/// You can register commands and call them by name using string arguments.
class Dispatcher {
public:
  /// Internally functions take variable string arguments and return a string.
  using NativeInterface =
    std::function<std::string(const std::vector<std::string>&)>;

  /// Register a command that implements the native dispatcher call interface.
  void add(std::string name, NativeInterface func, std::size_t nargs);
  /// Register a command with arbitrary arguments.
  ///
  /// All argument types must be constructible via the
  /// `std::istream& operator>>(...)` formatted input operator and the return
  /// type must be support the `std::ostream& operator<<(...)` formatted output
  /// operator.
  template<typename R, typename... Args>
  void add(std::string name, std::function<R(Args...)> func);
  template<typename R, typename... Args>
  void add(std::string name, R (*func)(Args...));
  template<typename T, typename R, typename... Args>
  void add(std::string name, R (T::*member_func)(Args...), T* t);

  /// Call a command with some arguments.
  std::string call(
    const std::string& name, const std::vector<std::string>& args);

  /// Return a list of registered commands and required number of arguments.
  std::vector<std::pair<std::string, std::size_t>> commands() const;

private:
  struct Command {
    NativeInterface func;
    std::size_t nargs;
  };
  std::unordered_map<std::string, Command> m_commands;
};

// implementation Variable

inline Variable
Variable::parse_as(const std::string& str, Type type)
{
  if (type == Type::Boolean) {
    return Variable((str == "true"));
  } else if (type == Type::Integer) {
    return Variable(std::stoll(str));
  } else if (type == Type::Float) {
    return Variable(std::stod(str));
  } else if (type == Type::String) {
    return Variable(str);
  } else {
    return Variable();
  }
}

inline std::ostream&
operator<<(std::ostream& os, const Variable& v)
{
  if (v.type() == Variable::Type::Boolean) {
    os << (v.m_boolean ? "true" : "false");
  } else if (v.m_type == Variable::Type::Integer) {
    os << v.m_integer;
  } else if (v.m_type == Variable::Type::Float) {
    os << v.m_float;
  } else if (v.m_type == Variable::Type::String) {
    os << v.m_string;
  }
  return os;
}

inline Variable&
Variable::operator=(Variable&& v)
{
  // handle `x = std::move(x)`
  if (this == &v) { return *this; }
  if (v.m_type == Type::Boolean) {
    m_boolean = v.m_boolean;
  } else if (v.m_type == Type::Integer) {
    m_integer = v.m_integer;
  } else if (v.m_type == Type::Float) {
    m_float = v.m_float;
  } else if (v.m_type == Type::String) {
    m_string = std::move(v.m_string);
  }
  m_type = v.m_type;
  return *this;
}

inline Variable&
Variable::operator=(const Variable& v)
{
  if (v.m_type == Type::Boolean) {
    m_boolean = v.m_boolean;
  } else if (v.m_type == Type::Integer) {
    m_integer = v.m_integer;
  } else if (v.m_type == Type::Float) {
    m_float = v.m_float;
  } else if (v.m_type == Type::String) {
    m_string = v.m_string;
  }
  m_type = v.m_type;
  return *this;
}

template<>
struct Variable::Converter<bool> {
  static constexpr Type type() { return Type::Boolean; }
  static constexpr bool as_t(const Variable& v) { return v.m_boolean; }
};
template<>
struct Variable::Converter<float> {
  static constexpr Type type() { return Type::Float; }
  static constexpr float as_t(const Variable& v)
  {
    return static_cast<float>(v.m_float);
  }
};
template<>
struct Variable::Converter<double> {
  static constexpr Type type() { return Type::Float; }
  static constexpr double as_t(const Variable& v) { return v.m_float; }
};
template<>
struct Variable::Converter<std::string> {
  static constexpr Type type() { return Type::String; }
  static constexpr const std::string& as_t(const Variable& v)
  {
    return v.m_string;
  }
};
template<typename I>
struct Variable::IntegerConverter {
  static constexpr Type type() { return Type::Integer; }
  static constexpr I as_t(const Variable& v)
  {
    return static_cast<I>(v.m_integer);
  }
};
template<>
struct Variable::Converter<int8_t> : Variable::IntegerConverter<int8_t> {
};
template<>
struct Variable::Converter<int16_t> : Variable::IntegerConverter<int16_t> {
};
template<>
struct Variable::Converter<int32_t> : Variable::IntegerConverter<int32_t> {
};
template<>
struct Variable::Converter<int64_t> : Variable::IntegerConverter<int64_t> {
};
template<>
struct Variable::Converter<uint8_t> : Variable::IntegerConverter<uint8_t> {
};
template<>
struct Variable::Converter<uint16_t> : Variable::IntegerConverter<uint16_t> {
};
template<>
struct Variable::Converter<uint32_t> : Variable::IntegerConverter<uint32_t> {
};
template<>
struct Variable::Converter<uint64_t> : Variable::IntegerConverter<uint64_t> {
};

template<typename T>
inline auto
Variable::as() const
{
  if (name.empty()) {
    throw std::invalid_argument("Can not register command with empty name");
  }
  if (m_commands.count(name)) {
    throw std::invalid_argument(
      "Can not register command '" + name + "' more than once");
  }
  return Variable::Converter<T>::as_t(*this);
}

namespace dispatcher_impl {
namespace {

template<typename T>
inline T
str_decode(const std::string& str)
{
  T tmp;
  std::istringstream is(str);
  is >> tmp;
  if (is.fail()) {
    std::string msg;
    msg += "Could not convert value '";
    msg += str;
    msg += "' to type '";
    msg += typeid(T).name();
    msg += "'";
    throw std::invalid_argument(std::move(msg));
  }
  return tmp;
}

template<typename T>
inline std::string
str_encode(const T& value)
{
  std::ostringstream os;
  os << value;
  if (os.fail()) {
    std::string msg;
    msg += "Could not convert type '";
    msg += typeid(T).name();
    msg += "' to std::string";
    throw std::invalid_argument(std::move(msg));
  }
  return os.str();
}

// Wrap a function that returns a value
template<typename R, typename... Args>
struct NativeInterfaceWrappper {
  std::function<R(Args...)> func;

  std::string operator()(const std::vector<std::string>& args)
  {
    return decode_and_call(args, std::index_sequence_for<Args...>{});
  }
  template<std::size_t... I>
  std::string decode_and_call(
    const std::vector<std::string>& args, std::index_sequence<I...>)
  {
    return str_encode(
      func(str_decode<typename std::decay<Args>::type>(args.at(I))...));
  }
};

// Wrap a function that does not return anything
template<typename... Args>
struct NativeInterfaceWrappper<void, Args...> {
  std::function<void(Args...)> func;

  std::string operator()(const std::vector<std::string>& args)
  {
    return decode_and_call(args, std::index_sequence_for<Args...>{});
  }
  template<std::size_t... I>
  std::string decode_and_call(
    const std::vector<std::string>& args, std::index_sequence<I...>)
  {
    func(str_decode<typename std::decay<Args>::type>(args.at(I))...);
    return std::string();
  }
};

template<typename R, typename... Args>
inline Dispatcher::NativeInterface
make_native_interface(std::function<R(Args...)>&& function)
{
  return NativeInterfaceWrappper<R, Args...>{std::move(function)};
}

} // namespace
} // namespace dispatcher_impl

template<typename R, typename... Args>
inline void
Dispatcher::add(std::string name, std::function<R(Args...)> func)
{
  m_commands[std::move(name)] = Command{
    dispatcher_impl::make_native_interface(std::move(func)), sizeof...(Args)};
}

template<typename R, typename... Args>
inline void
Dispatcher::add(std::string name, R (*func)(Args...))
{
  assert(func && "Function pointer must be non-null");
  add(std::move(name), std::function<R(Args...)>(func));
}

template<typename T, typename R, typename... Args>
inline void
Dispatcher::add(std::string name, R (T::*member_func)(Args...), T* t)
{
  assert(member_func && "Member function pointer must be non-null");
  assert(t && "Object pointer must be non-null");
  add(std::move(name), std::function<R(Args...)>([=](Args... args) {
        return (t->*member_func)(args...);
      }));
}

inline std::string
Dispatcher::call(const std::string& name, const std::vector<std::string>& args)
{
  auto cmd = m_commands.find(name);
  if (cmd == m_commands.end()) {
    throw std::invalid_argument("Unknown command '" + name + "'");
  }
  if (args.size() != cmd->second.nargs) {
    throw std::invalid_argument(
      "Command '" + name + "' expects " + std::to_string(cmd->second.nargs) +
      " arguments but " + std::to_string(args.size()) + " given");
  }
  return cmd->second.func(args);
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
