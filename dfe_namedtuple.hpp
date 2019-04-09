// Copyright 2015,2018 Moritz Kiehn
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
/// \brief   named tuples for data storage
/// \author  Moritz Kiehn <msmk@cern.ch>
/// \date    2015-06-00, Initial version
/// \date    2018-02-09, Major rework

#pragma once

#include <endian.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <limits>
#include <string>
#include <tuple>
#include <utility>

/// Enable selected members of a class or struct to be used as an named tuple.
#define DFE_NAMEDTUPLE(name, members...) \
  static constexpr size_t N = \
    std::tuple_size<decltype(std::make_tuple(members))>::value; \
  static std::array<std::string, N> names() \
  { \
    return ::dfe::namedtuple_impl::unstringify_names<N>((#members)); \
  } \
  auto to_tuple() const->decltype(std::make_tuple(members)) \
  { \
    return std::make_tuple(members); \
  } \
  friend std::ostream& operator<<(std::ostream& os, const name& x) \
  { \
    return ::dfe::namedtuple_impl::print_tuple( \
      os, x.to_tuple(), x.names(), std::make_index_sequence<name::N>{}); \
  }

namespace dfe {

/// Write records as delimiter-separated values into a text file.
template<typename Namedtuple>
class TextNamedtupleWriter {
public:
  TextNamedtupleWriter() = delete;
  TextNamedtupleWriter(const TextNamedtupleWriter&) = delete;
  TextNamedtupleWriter(TextNamedtupleWriter&&) = default;
  ~TextNamedtupleWriter() = default;
  TextNamedtupleWriter& operator=(const TextNamedtupleWriter&) = delete;
  TextNamedtupleWriter& operator=(TextNamedtupleWriter&&) = default;

  /// Create a file at the given path. Overwrites existing data.
  ///
  /// \param path       Path to the output file
  /// \param delimiter  Delimiter to separate values within one record
  /// \param precision  Output floating point precision
  TextNamedtupleWriter(const std::string& path, char delimiter, int precision);

  /// Append a record to the file.
  void append(const Namedtuple& record);

private:
  template<typename TupleLike, std::size_t... I>
  void write_line(const TupleLike& values, std::index_sequence<I...>);

  std::ofstream m_file;
  char m_delimiter;
};

/// Write records as a comma-separated values into a text file.
template<typename Namedtuple>
class CsvNamedtupleWriter : public TextNamedtupleWriter<Namedtuple> {
public:
  /// Create a csv file at the given path. Overwrites existing data.
  CsvNamedtupleWriter(
    const std::string& path,
    int precision = (std::numeric_limits<double>::max_digits10 + 1))
    : TextNamedtupleWriter<Namedtuple>(path, ',', precision)
  {
  }
};

/// Write records as a tab-separated values into a text file.
template<typename Namedtuple>
class TsvNamedtupleWriter : public TextNamedtupleWriter<Namedtuple> {
public:
  /// Create a tsv file at the given path. Overwrites existing data.
  TsvNamedtupleWriter(
    const std::string& path,
    int precision = (std::numeric_limits<double>::max_digits10 + 1))
    : TextNamedtupleWriter<Namedtuple>(path, '\t', precision)
  {
  }
};

/// Write records into a binary NumPy-compatible `.npy` file.
///
/// See https://docs.scipy.org/doc/numpy/neps/npy-format.html for a detailed
/// explanation of the file format.
template<typename Namedtuple>
class NpyNamedtupleWriter {
public:
  NpyNamedtupleWriter() = delete;
  NpyNamedtupleWriter(const NpyNamedtupleWriter&) = delete;
  NpyNamedtupleWriter(NpyNamedtupleWriter&&) = default;
  ~NpyNamedtupleWriter();
  NpyNamedtupleWriter& operator=(const NpyNamedtupleWriter&) = delete;
  NpyNamedtupleWriter& operator=(NpyNamedtupleWriter&&) = default;

  /// Create a npy file at the given path. Overwrites existing data.
  NpyNamedtupleWriter(const std::string& path);

  /// Append a record to the end of the file.
  void append(const Namedtuple& record);

private:
  void write_header(std::size_t num_tuples);

  std::ofstream m_file;
  std::size_t m_fixed_header_length;
  std::size_t m_num_tuples;
};

// common helpers and implementation details

namespace namedtuple_impl {
namespace {

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
  int idx = 0;
  (void)swallow{0, (void(
                      os << ((0 < idx++) ? " " : "") << std::get<I>(n) << "="
                         << std::get<I>(t)),
                    0)...};
  return os;
}

} // namespace
} // namespace namedtuple_impl

// implementation text writer

template<typename Namedtuple>
inline TextNamedtupleWriter<Namedtuple>::TextNamedtupleWriter(
  const std::string& path, char delimiter, int precision)
  : m_delimiter(delimiter)
{
  // make our life easier. always throw on error
  m_file.exceptions(std::ofstream::badbit | std::ofstream::failbit);
  m_file.open(
    path, std::ios_base::binary | std::ios_base::out | std::ios_base::trunc);
  // set output precision for floating point values
  m_file.precision(precision);
  // write column names as header
  write_line(Namedtuple::names(), std::make_index_sequence<Namedtuple::N>{});
}

template<typename Namedtuple>
inline void
TextNamedtupleWriter<Namedtuple>::append(const Namedtuple& record)
{
  write_line(record.to_tuple(), std::make_index_sequence<Namedtuple::N>{});
}

template<typename Namedtuple>
template<typename TupleLike, std::size_t... I>
void
TextNamedtupleWriter<Namedtuple>::write_line(
  const TupleLike& values, std::index_sequence<I...>)
{
  // this is a bit like magic, here is whats going on:
  // the (void(<expr>), 0) expression evaluates <expr>, ignores its return value
  // by casting it to void, and then returns an integer with value 0 by virtue
  // of the comma operator (yes, ',' can also be an operator with a very weird
  // but helpful function). The ... pack expansion creates this expression for
  // each entry in the index pack I, effectively looping over the indices.
  using swallow = int[];
  std::size_t col = 0;
  (void)swallow{0, (void(
                      m_file << std::get<I>(values)
                             << ((++col < sizeof...(I)) ? m_delimiter : '\n')),
                    0)...};
}

// implementation npy writer

namespace namedtuple_impl {
namespace {

#if (__BYTE_ORDER == __LITTLE_ENDIAN) && (__FLOAT_WORD_ORDER == __BYTE_ORDER)
#define DFE_DTYPE_CODE(c) "<" #c
#elif (__BYTE_ORDER == __BIG_ENDIAN) && (__FLOAT_WORD_ORDER == __BYTE_ORDER)
#define DTYPE_SIGNED_CODE(size) ">" #c
#else
#error Unsupported byte order
#endif
template<typename T>
struct TypeInfo {
};
template<>
struct TypeInfo<uint8_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(u1); }
};
template<>
struct TypeInfo<uint16_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(u2); }
};
template<>
struct TypeInfo<uint32_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(u4); }
};
template<>
struct TypeInfo<uint64_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(u8); }
};
template<>
struct TypeInfo<int8_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(i1); }
};
template<>
struct TypeInfo<int16_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(i2); }
};
template<>
struct TypeInfo<int32_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(i4); }
};
template<>
struct TypeInfo<int64_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(i8); }
};
template<>
struct TypeInfo<float> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(f4); }
};
template<>
struct TypeInfo<double> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(f8); }
};
// not needed after this point. undef to avoid cluttering the namespace
#undef DFE_DTYPE_CODE

template<typename... Types>
inline std::array<const char*, sizeof...(Types)>
dtypes_codes(const std::tuple<Types...>& t)
{
  return {TypeInfo<typename std::decay<Types>::type>::dtype_code()...};
}

template<typename T>
inline std::string
dtypes_description(const T& t)
{
  std::string descr;
  std::size_t n = T::N;
  auto names = t.names();
  auto codes = dtypes_codes(t.to_tuple());
  descr += '[';
  for (decltype(n) i = 0; i < n; ++i) {
    descr += "('";
    descr += names[i];
    descr += "', '";
    descr += codes[i];
    descr += "')";
    if ((i + 1) < n) { descr += ", "; }
  }
  descr += ']';
  return descr;
}

template<typename TupleLike, std::size_t... I>
inline void
write_npy_record(
  std::ostream& os, const TupleLike& values, std::index_sequence<I...>)
{
  // see write implementation in csv writer for explanation
  using swallow = int[];
  (void)swallow{0, (void(os.write(
                      reinterpret_cast<const char*>(&std::get<I>(values)),
                      sizeof(typename std::tuple_element<I, TupleLike>::type))),
                    0)...};
}

} // namespace
} // namespace namedtuple_impl

template<typename Namedtuple>
inline NpyNamedtupleWriter<Namedtuple>::NpyNamedtupleWriter(
  const std::string& path)
  : m_fixed_header_length(0)
  , m_num_tuples(0)
{
  // make our life easier. always throw on error
  m_file.exceptions(std::ofstream::badbit | std::ofstream::failbit);
  m_file.open(
    path, std::ios_base::binary | std::ios_base::out | std::ios_base::trunc);
  // write a header that uses the maximum amount of space, i.e. biggest
  // possible number of ntuples, so that we have enough space when we
  // overwrite it w/ the actual number of tuples at closing time.
  write_header(SIZE_MAX);
  write_header(0);
}

template<typename Namedtuple>
inline NpyNamedtupleWriter<Namedtuple>::~NpyNamedtupleWriter()
{
  if (!m_file.is_open()) { return; }
  write_header(m_num_tuples);
  m_file.close();
}

template<typename Namedtuple>
inline void
NpyNamedtupleWriter<Namedtuple>::append(const Namedtuple& record)
{
  namedtuple_impl::write_npy_record(
    m_file, record.to_tuple(), std::make_index_sequence<Namedtuple::N>{});
  m_num_tuples += 1;
}

template<typename Namedtuple>
inline void
NpyNamedtupleWriter<Namedtuple>::write_header(std::size_t num_tuples)
{
  std::string header;
  // magic
  header += "\x93NUMPY";
  // fixed version number (major, minor)
  header += static_cast<char>(0x1);
  header += static_cast<char>(0x0);
  // placeholder value for the header length (2byte little end. unsigned)
  header += static_cast<char>(0xAF);
  header += static_cast<char>(0xFE);
  // python dict w/ data type and size information
  header += '{';
  header +=
    "'descr': " + namedtuple_impl::dtypes_description(Namedtuple()) + ", ";
  header += "'fortran_order': False, ";
  header += "'shape': (" + std::to_string(num_tuples) + ",), ";
  header += '}';
  // padd w/ spaces for 16 byte alignment of the whole header
  while (((header.size() + 1) % 16) != 0) { header += ' '; }
  // the initial header fixes the available header size. updated headers
  // must always occupy the same space and might require additional
  // padding spaces
  if (m_fixed_header_length == 0) {
    m_fixed_header_length = header.size();
  } else {
    while (header.size() < m_fixed_header_length) { header += ' '; }
  }
  header += '\n';
  // replace the header length place holder
  std::size_t header_length = header.size() - 10;
  header[8] = static_cast<char>(header_length >> 0);
  header[9] = static_cast<char>(header_length >> 8);
  m_file.seekp(0);
  m_file.write(header.data(), header.size());
}

} // namespace dfe
