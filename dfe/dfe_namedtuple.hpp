// Copyright 2015,2018-2019 Moritz Kiehn
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
/// \brief   Allow structs to be accessed like std::tuple
/// \author  Moritz Kiehn <msmk@cern.ch>
/// \date    2015-06-00, Initial version
/// \date    2018-02-09, Major rework
/// \date    2019-09-09, Split i/o components into separate libraries

#pragma once

#include <array>
#include <cstring>
#include <ostream>
#include <string>
#include <tuple>
#include <utility>

/// Enable selected members of a class or struct to be used as a named tuple.
///
/// Provides access to the names via `::names()`, allows conversion
/// to an equivalent `std::tuple<...>` object via `.to_tuple()`, and enables
/// assignment from an equivalent `std::tuple<...>`.
#define DFE_NAMEDTUPLE(name, members...) \
  using Tuple = decltype(std::make_tuple(members)); \
  static constexpr std::size_t N = std::tuple_size<Tuple>::value; \
  static std::array<std::string, N> names() \
  { \
    return ::dfe::namedtuple_impl::unstringify_names<N>((#members)); \
  } \
  Tuple to_tuple() const { return std::make_tuple(members); } \
  template<typename... U> \
  name& operator=(const std::tuple<U...>& other) \
  { \
    std::tie(members) = other; \
    return *this; \
  } \
  template<typename... U> \
  name& operator=(std::tuple<U...>&& other) \
  { \
    std::tie(members) = std::forward<std::tuple<U...>>(other); \
    return *this; \
  } \
  friend std::ostream& operator<<(std::ostream& os, const name& x) \
  { \
    return ::dfe::namedtuple_impl::print_tuple( \
      os, x.to_tuple(), x.names(), std::make_index_sequence<name::N>{}); \
  }

// implementation helpers

namespace dfe {
namespace namedtuple_impl {

// reverse macro stringification
template<std::size_t N>
inline std::array<std::string, N>
unstringify_names(const char* str)
{
  std::array<std::string, N> out;
  std::size_t idx = 0;
  while (const char* mark = std::strchr(str, ',')) {
    out[idx++].assign(str, mark - str);
    // start next search after current mark
    str = mark + 1;
    // skip possible whitespace
    str += std::strspn(str, " ");
  }
  out[idx].assign(str);
  return out;
}

// modified from http://stackoverflow.com/a/6245777
template<typename Tuple, typename Names, std::size_t... I>
inline std::ostream&
print_tuple(
  std::ostream& os, const Tuple& t, const Names& n, std::index_sequence<I...>)
{
  using swallow = int[];
  std::size_t idx = 0;
  (void)swallow{0, (void(
                      os << ((0 < idx++) ? " " : "") << std::get<I>(n) << "="
                         << std::get<I>(t)),
                    0)...};
  return os;
}

} // namespace namedtuple_impl
} // namespace dfe
