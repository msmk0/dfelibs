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
/// \brief   Named tuples for data storage
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
#include <sstream>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

/// Enable selected members of a class or struct to be used as an named tuple.
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

namespace dfe {

/// Write records as delimiter-separated values into a text file.
template<typename NamedTuple>
class TextNamedTupleWriter {
public:
  TextNamedTupleWriter() = delete;
  TextNamedTupleWriter(const TextNamedTupleWriter&) = delete;
  TextNamedTupleWriter(TextNamedTupleWriter&&) = default;
  ~TextNamedTupleWriter() = default;
  TextNamedTupleWriter& operator=(const TextNamedTupleWriter&) = delete;
  TextNamedTupleWriter& operator=(TextNamedTupleWriter&&) = default;

  /// Create a file at the given path. Overwrites existing data.
  ///
  /// \param path       Path to the output file
  /// \param delimiter  Delimiter to separate values within one record
  /// \param precision  Output floating point precision
  TextNamedTupleWriter(const std::string& path, char delimiter, int precision);

  /// Append a record to the file.
  void append(const NamedTuple& record);

private:
  template<typename TupleLike, std::size_t... I>
  void write_line(const TupleLike& values, std::index_sequence<I...>);

  std::ofstream m_file;
  char m_delimiter;
};

/// Write records as a comma-separated values into a text file.
template<typename NamedTuple>
class CsvNamedTupleWriter : public TextNamedTupleWriter<NamedTuple> {
public:
  /// Create a csv file at the given path. Overwrites existing data.
  CsvNamedTupleWriter(
    const std::string& path,
    int precision = (std::numeric_limits<double>::max_digits10 + 1))
    : TextNamedTupleWriter<NamedTuple>(path, ',', precision)
  {
  }
};

/// Write records as a tab-separated values into a text file.
template<typename NamedTuple>
class TsvNamedTupleWriter : public TextNamedTupleWriter<NamedTuple> {
public:
  /// Create a tsv file at the given path. Overwrites existing data.
  TsvNamedTupleWriter(
    const std::string& path,
    int precision = (std::numeric_limits<double>::max_digits10 + 1))
    : TextNamedTupleWriter<NamedTuple>(path, '\t', precision)
  {
  }
};

/// Read records as delimiter-separated values from a text file.
///
/// The reader is strict about its input format. Each row **must** contain
/// exactly as many columns as there are values within the record. Every row
/// **must** end with a single newline. The first row is **alway** interpreted
/// as a header but can be skipped. If it is not skipped, the header names
/// in each column **must** match exactly to the record member names.
template<typename NamedTuple>
class TextNamedTupleReader {
public:
  TextNamedTupleReader() = delete;
  TextNamedTupleReader(const TextNamedTupleReader&) = delete;
  TextNamedTupleReader(TextNamedTupleReader&&) = default;
  ~TextNamedTupleReader() = default;
  TextNamedTupleReader& operator=(const TextNamedTupleReader&) = delete;
  TextNamedTupleReader& operator=(TextNamedTupleReader&&) = default;

  /// Open a file at the given path.
  ///
  /// \param path           Path to the input file
  /// \param delimiter      Delimiter to separate values within one record
  /// \param verify_header  false to check header column names, false to skip
  TextNamedTupleReader(
    const std::string& path, char delimiter, bool verify_header);

  /// Read the next record from the file.
  ///
  /// \returns true   if a record was successfully read
  /// \returns false  if no more records are available
  bool read(NamedTuple& record);

private:
  bool read_line();
  template<typename TupleLike, std::size_t... I>
  TupleLike parse_line(std::index_sequence<I...>) const;

  std::ifstream m_file;
  char m_delimiter;
  std::string m_line;
  std::array<std::string, NamedTuple::N> m_columns;
  std::size_t m_num_lines;
};

/// Read records from a comma-separated file.
template<typename NamedTuple>
class CsvNamedTupleReader : public TextNamedTupleReader<NamedTuple> {
public:
  /// Open a csv file at the given path.
  CsvNamedTupleReader(const std::string& path, bool verify_header = true)
    : TextNamedTupleReader<NamedTuple>(path, ',', verify_header)
  {
  }
};

/// Read records from a tab-separated file.
template<typename NamedTuple>
class TsvNamedTupleReader : public TextNamedTupleReader<NamedTuple> {
public:
  /// Open a tsv file at the given path.
  TsvNamedTupleReader(const std::string& path, bool verify_header = true)
    : TextNamedTupleReader<NamedTuple>(path, '\t', verify_header)
  {
  }
};

/// Write records into a binary NumPy-compatible `.npy` file.
///
/// See
/// https://docs.scipy.org/doc/numpy/reference/generated/numpy.lib.format.html
/// for an explanation of the file format.
template<typename NamedTuple>
class NpyNamedTupleWriter {
public:
  NpyNamedTupleWriter() = delete;
  NpyNamedTupleWriter(const NpyNamedTupleWriter&) = delete;
  NpyNamedTupleWriter(NpyNamedTupleWriter&&) = default;
  ~NpyNamedTupleWriter();
  NpyNamedTupleWriter& operator=(const NpyNamedTupleWriter&) = delete;
  NpyNamedTupleWriter& operator=(NpyNamedTupleWriter&&) = default;

  /// Create a npy file at the given path. Overwrites existing data.
  NpyNamedTupleWriter(const std::string& path);

  /// Append a record to the end of the file.
  void append(const NamedTuple& record);

private:
  void write_header(std::size_t num_tuples);
  template<typename TupleLike, std::size_t... I>
  void write_record(const TupleLike& values, std::index_sequence<I...>);

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
  std::size_t idx = 0;
  (void)swallow{0, (void(
                      os << ((0 < idx++) ? " " : "") << std::get<I>(n) << "="
                         << std::get<I>(t)),
                    0)...};
  return os;
}

} // namespace
} // namespace namedtuple_impl

// implementation text writer

template<typename NamedTuple>
inline TextNamedTupleWriter<NamedTuple>::TextNamedTupleWriter(
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
  write_line(NamedTuple::names(), std::make_index_sequence<NamedTuple::N>{});
}

template<typename NamedTuple>
inline void
TextNamedTupleWriter<NamedTuple>::append(const NamedTuple& record)
{
  write_line(record.to_tuple(), std::make_index_sequence<NamedTuple::N>{});
}

template<typename NamedTuple>
template<typename TupleLike, std::size_t... I>
void
TextNamedTupleWriter<NamedTuple>::write_line(
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

// implementation text reader

template<typename NamedTuple>
TextNamedTupleReader<NamedTuple>::TextNamedTupleReader(
  const std::string& path, char delimiter, bool verify_header)
  : m_delimiter(delimiter)
  , m_num_lines(0)
{
  // make our life easier. always throw on error
  m_file.exceptions(std::ofstream::badbit);
  m_file.open(path, std::ios_base::binary | std::ios_base::in);
  if (verify_header) {
    if (!read_line()) { throw std::runtime_error("Invalid file header"); };
    const auto& expected = NamedTuple::names();
    for (std::size_t i = 0; i < NamedTuple::N; ++i) {
      if (expected[i] != m_columns[i]) {
        throw std::runtime_error(
          "Invalid header column=" + std::to_string(i) + " expected='" +
          expected[i] + "' seen='" + m_columns[i] + "'");
      }
    }
  } else {
    m_file.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
  }
}

template<typename NamedTuple>
bool
TextNamedTupleReader<NamedTuple>::read(NamedTuple& record)
{
  if (!read_line()) { return false; }
  record = parse_line<typename NamedTuple::Tuple>(
    std::make_index_sequence<NamedTuple::N>{});
  return true;
}

template<typename NamedTuple>
bool
TextNamedTupleReader<NamedTuple>::read_line()
{
  // read a full line
  std::getline(m_file, m_line, '\n');
  if (!m_file.good()) { return false; }
  // split into columns
  std::size_t icol = 0;
  std::size_t pos = 0;
  for (; (icol < m_columns.size()) and (pos < m_line.size()); ++icol) {
    std::size_t token = m_line.find(m_delimiter, pos);
    if (token == pos) {
      throw std::runtime_error(
        "Empty column " + std::to_string(icol) + " in line " +
        std::to_string(m_num_lines));
    }
    if (token == std::string::npos) {
      // we are at the last column
      m_columns[icol] = m_line.substr(pos);
      pos = m_line.size();
    } else {
      // exclude the delimiter from the extracted column
      m_columns[icol] = m_line.substr(pos, token - pos);
      // continue next search after the delimiter
      pos = token + 1;
    }
  }
  if (icol < m_columns.size()) {
    throw std::runtime_error(
      "Too few columns in line " + std::to_string(m_num_lines));
  }
  if (pos < m_line.size()) {
    throw std::runtime_error(
      "Too many columns in line " + std::to_string(m_num_lines));
  }
  m_num_lines += 1;
  return true;
}

template<typename NamedTuple>
template<typename TupleLike, std::size_t... I>
TupleLike
TextNamedTupleReader<NamedTuple>::parse_line(std::index_sequence<I...>) const
{
  // see write_line implementation in text writer for explanation
  TupleLike values;
  std::istringstream is;
  using swallow = int[];
  (void)swallow{
    0, ((std::istringstream(m_columns[I]) >> std::get<I>(values)), 0)...};
  return values;
}

// implementation npy writer

namespace namedtuple_impl {
namespace {

#if (__BYTE_ORDER == __LITTLE_ENDIAN) && (__FLOAT_WORD_ORDER == __BYTE_ORDER)
#define DFE_NUMPY_DTYPE_CODE(c) "<" #c
#elif (__BYTE_ORDER == __BIG_ENDIAN) && (__FLOAT_WORD_ORDER == __BYTE_ORDER)
#define DFE_NUMPY_DTYPE_CODE(size) ">" #c
#else
#error Unsupported byte order
#endif
template<typename T>
constexpr std::enable_if_t<false, T> kNumpyDtypeCode;
template<>
constexpr const char* kNumpyDtypeCode<uint8_t> = DFE_NUMPY_DTYPE_CODE(u1);
template<>
constexpr const char* kNumpyDtypeCode<uint16_t> = DFE_NUMPY_DTYPE_CODE(u2);
template<>
constexpr const char* kNumpyDtypeCode<uint32_t> = DFE_NUMPY_DTYPE_CODE(u4);
template<>
constexpr const char* kNumpyDtypeCode<uint64_t> = DFE_NUMPY_DTYPE_CODE(u8);
template<>
constexpr const char* kNumpyDtypeCode<int8_t> = DFE_NUMPY_DTYPE_CODE(i1);
template<>
constexpr const char* kNumpyDtypeCode<int16_t> = DFE_NUMPY_DTYPE_CODE(i2);
template<>
constexpr const char* kNumpyDtypeCode<int32_t> = DFE_NUMPY_DTYPE_CODE(i4);
template<>
constexpr const char* kNumpyDtypeCode<int64_t> = DFE_NUMPY_DTYPE_CODE(i8);
template<>
constexpr const char* kNumpyDtypeCode<float> = DFE_NUMPY_DTYPE_CODE(f4);
template<>
constexpr const char* kNumpyDtypeCode<double> = DFE_NUMPY_DTYPE_CODE(f8);
template<>
constexpr const char* kNumpyDtypeCode<bool> = DFE_NUMPY_DTYPE_CODE(b);
// not needed after this point. undef to avoid cluttering the namespace
#undef DFE_NUMPY_DTYPE_CODE

template<typename... Types>
inline std::array<const char*, sizeof...(Types)>
dtypes_codes(const std::tuple<Types...>& t)
{
  return {kNumpyDtypeCode<typename std::decay<Types>::type>...};
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

} // namespace
} // namespace namedtuple_impl

template<typename NamedTuple>
inline NpyNamedTupleWriter<NamedTuple>::NpyNamedTupleWriter(
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

template<typename NamedTuple>
inline NpyNamedTupleWriter<NamedTuple>::~NpyNamedTupleWriter()
{
  if (!m_file.is_open()) { return; }
  write_header(m_num_tuples);
  m_file.close();
}

template<typename NamedTuple>
inline void
NpyNamedTupleWriter<NamedTuple>::append(const NamedTuple& record)
{
  write_record(record.to_tuple(), std::make_index_sequence<NamedTuple::N>{});
  m_num_tuples += 1;
}

template<typename NamedTuple>
inline void
NpyNamedTupleWriter<NamedTuple>::write_header(std::size_t num_tuples)
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
    "'descr': " + namedtuple_impl::dtypes_description(NamedTuple()) + ", ";
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

template<typename NamedTuple>
template<typename TupleLike, std::size_t... I>
inline void
NpyNamedTupleWriter<NamedTuple>::write_record(
  const TupleLike& values, std::index_sequence<I...>)
{
  // see write_line implementation in text writer for explanation
  using swallow = int[];
  (void)swallow{0, (void(m_file.write(
                      reinterpret_cast<const char*>(&std::get<I>(values)),
                      sizeof(typename std::tuple_element<I, TupleLike>::type))),
                    0)...};
}

} // namespace dfe
