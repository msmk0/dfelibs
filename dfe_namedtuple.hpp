// "THE BEER-WARE LICENSE" (Revision 42):
// <msmk@cern.ch> wrote this file.  As long as you retain this notice you
// can do whatever you want with this stuff. If we meet some day, and you think
// this stuff is worth it, you can buy me a beer in return.   Moritz Kiehn

/// \file
/// \brief   named tuples for data storage
/// \author  Moritz Kiehn <msmk@cern.ch>
/// \date    2015-06-00, Initial version
/// \date    2018-02-09, Major rework

#pragma once

#include <endian.h>

#include <array>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <string>
#include <tuple>

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
    return print_tuple( \
      os, x.to_tuple(), x.names(), \
      ::dfe::namedtuple_impl::SequenceGenerator<name::N>()); \
  }

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

// modified from http://stackoverflow.com/a/6245777
template<typename Tuple, typename Names, std::size_t... I>
inline std::ostream&
print_tuple(std::ostream& os, const Tuple& t, const Names& n, Sequence<I...>)
{
  using swallow = int[];
  int idx = 0;
  (void)swallow{0, (void(
                      os << ((0 < idx++) ? " " : "") << std::get<I>(n) << "="
                         << std::get<I>(t)),
                    0)...};
  return os;
}

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
  static constexpr int text_width() { return 3; }
};
template<>
struct TypeInfo<uint16_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(u2); }
  static constexpr int text_width() { return 5; }
};
template<>
struct TypeInfo<uint32_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(u4); }
  static constexpr int text_width() { return 10; }
};
template<>
struct TypeInfo<uint64_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(u8); }
  static constexpr int text_width() { return 20; }
};
template<>
struct TypeInfo<int8_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(i1); }
  static constexpr int text_width() { return 4; }
};
template<>
struct TypeInfo<int16_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(i2); }
  static constexpr int text_width() { return 6; }
};
template<>
struct TypeInfo<int32_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(i4); }
  static constexpr int text_width() { return 11; }
};
template<>
struct TypeInfo<int64_t> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(i8); }
  static constexpr int text_width() { return 21; }
};
template<>
struct TypeInfo<float> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(f4); }
  static constexpr int text_width() { return 10; }
};
template<>
struct TypeInfo<double> {
  static constexpr const char* dtype_code() { return DFE_DTYPE_CODE(f8); }
  static constexpr int text_width() { return 10; }
};
// not needed anymore. avoid cluttering the namespace
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
    if ((i + 1) < n) {
      descr += ", ";
    }
  }
  descr += ']';
  return descr;
}

template<typename... Types>
inline std::array<int, sizeof...(Types)>
text_widths(const std::tuple<Types...>& t)
{
  return {TypeInfo<typename std::decay<Types>::type>::text_width()...};
}

} // namespace namedtuple_impl

/// Write records as comma-separated values into a text file.
template<typename Namedtuple>
class CsvNamedtupleWriter {
public:
  CsvNamedtupleWriter() = delete;
  CsvNamedtupleWriter(const CsvNamedtupleWriter&) = delete;
  CsvNamedtupleWriter& operator=(const CsvNamedtupleWriter&) = delete;
  CsvNamedtupleWriter(CsvNamedtupleWriter&&) = default;
  CsvNamedtupleWriter& operator=(CsvNamedtupleWriter&&) = default;
  ~CsvNamedtupleWriter() = default;
  /// Create a csv file at the given path. Overwrites existing data.
  CsvNamedtupleWriter(std::string path);

  /// Append a record to the end of the file.
  void append(const Namedtuple& record);

private:
  template<typename TupleLike, std::size_t... I>
  void write(const TupleLike& values, namedtuple_impl::Sequence<I...>);

  std::ofstream m_file;
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
  NpyNamedtupleWriter& operator=(const NpyNamedtupleWriter&) = delete;
  NpyNamedtupleWriter(NpyNamedtupleWriter&&) = default;
  NpyNamedtupleWriter& operator=(NpyNamedtupleWriter&&) = default;
  /// Create a npy file at the given path. Overwrites existing data.
  NpyNamedtupleWriter(std::string path);
  ~NpyNamedtupleWriter();

  /// Append a record to the end of the file.
  void append(const Namedtuple& record);

private:
  void write_header(std::size_t num_tuples);
  template<typename TupleLike, std::size_t... I>
  void write(const TupleLike& values, namedtuple_impl::Sequence<I...>);

  std::ofstream m_file;
  std::size_t m_fixed_header_length;
  std::size_t m_num_tuples;
};

/// Write records as a space-separated table into a text file.
template<typename Namedtuple>
class TabularNamedtupleWriter {
public:
  TabularNamedtupleWriter() = delete;
  TabularNamedtupleWriter(const TabularNamedtupleWriter&) = delete;
  TabularNamedtupleWriter& operator=(const TabularNamedtupleWriter&) = delete;
  TabularNamedtupleWriter(TabularNamedtupleWriter&&) = default;
  TabularNamedtupleWriter& operator=(TabularNamedtupleWriter&&) = default;
  ~TabularNamedtupleWriter() = default;
  /// Create a tabular text file at the given path. Overwrites existing data.
  TabularNamedtupleWriter(std::string path);

  /// Append a record to the end of the file.
  void append(const Namedtuple& record);

private:
  template<typename TupleLike, std::size_t... I>
  void write(const TupleLike& values, namedtuple_impl::Sequence<I...>);

  std::ofstream m_file;
  std::array<int, Namedtuple::N> m_widths;
};

} // namespace dfe

// implementation csv writer

template<typename Namedtuple>
dfe::CsvNamedtupleWriter<Namedtuple>::CsvNamedtupleWriter(std::string path)
{
  // make our life easier. always throw on error
  m_file.exceptions(std::ofstream::badbit | std::ofstream::failbit);
  m_file.open(
    path, std::ios_base::binary | std::ios_base::out | std::ios_base::trunc);
  // write column names as header
  write(
    Namedtuple::names(), namedtuple_impl::SequenceGenerator<Namedtuple::N>());
}

template<typename Namedtuple>
void
dfe::CsvNamedtupleWriter<Namedtuple>::append(const Namedtuple& record)
{
  write(record.to_tuple(), namedtuple_impl::SequenceGenerator<Namedtuple::N>());
}

template<typename Namedtuple>
template<typename TupleLike, std::size_t... I>
void
dfe::CsvNamedtupleWriter<Namedtuple>::write(
  const TupleLike& values, namedtuple_impl::Sequence<I...>)
{
  // this is a bit like magic, here is whats going on:
  // the (void(<expr>), 0) expression evaluates <expr>, ignores its return value
  // by casting it to void, and then returns an integer with value 0 by virtue
  // of the comma operator (yes, ',' can also be an operator with a very weird
  // but helpful function). The ... pack expansion creates this expression for
  // each entry in the index pack I, effectively looping over the indices.
  using swallow = int[];
  int col = 0;
  (void)swallow{
    0, (void(m_file << (0 < col++ ? "," : "") << std::get<I>(values)), 0)...};
  m_file << '\n';
}

// implementation npy writer

template<typename Namedtuple>
dfe::NpyNamedtupleWriter<Namedtuple>::NpyNamedtupleWriter(std::string path)
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
dfe::NpyNamedtupleWriter<Namedtuple>::~NpyNamedtupleWriter()
{
  if (!m_file.is_open()) {
    return;
  }
  write_header(m_num_tuples);
  m_file.close();
}

template<typename Namedtuple>
void
dfe::NpyNamedtupleWriter<Namedtuple>::append(const Namedtuple& record)
{
  write(record.to_tuple(), namedtuple_impl::SequenceGenerator<Namedtuple::N>());
  m_num_tuples += 1;
}

template<typename Namedtuple>
void
dfe::NpyNamedtupleWriter<Namedtuple>::write_header(std::size_t num_tuples)
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
  while (((header.size() + 1) % 16) != 0) {
    header += ' ';
  }
  // the initial header fixes the available header size. updated headers
  // must always occupy the same space and might require additional
  // padding spaces
  if (m_fixed_header_length == 0) {
    m_fixed_header_length = header.size();
  } else {
    while (header.size() < m_fixed_header_length) {
      header += ' ';
    }
  }
  header += '\n';
  // replace the header length place holder
  std::size_t header_length = header.size() - 10;
  header[8] = static_cast<char>(header_length >> 0);
  header[9] = static_cast<char>(header_length >> 8);
  m_file.seekp(0);
  m_file.write(header.data(), header.size());
}

template<typename Namedtuple>
template<typename TupleLike, std::size_t... I>
void
dfe::NpyNamedtupleWriter<Namedtuple>::write(
  const TupleLike& values, namedtuple_impl::Sequence<I...>)
{
  // see write implementation in csv writer for explanation
  using swallow = int[];
  (void)swallow{0, (void(m_file.write(
                      reinterpret_cast<const char*>(&std::get<I>(values)),
                      sizeof(typename std::tuple_element<I, TupleLike>::type))),
                    0)...};
}

// implementation tabular writer

template<typename Namedtuple>
dfe::TabularNamedtupleWriter<Namedtuple>::TabularNamedtupleWriter(
  std::string path)
  : m_widths(namedtuple_impl::text_widths(Namedtuple().to_tuple()))
{
  // make our life easier. always throw on error
  m_file.exceptions(std::ofstream::badbit | std::ofstream::failbit);
  m_file.open(
    path, std::ios_base::binary | std::ios_base::out | std::ios_base::trunc);
  // ensure each column is wide enough to hold its title
  auto names = Namedtuple::names();
  for (size_t i = 0; i < Namedtuple::N; ++i) {
    m_widths[i] = std::max<int>(m_widths[i], names[i].size());
  }
  // write column names as header
  write(names, namedtuple_impl::SequenceGenerator<Namedtuple::N>());
}

template<typename Namedtuple>
void
dfe::TabularNamedtupleWriter<Namedtuple>::append(const Namedtuple& record)
{
  write(record.to_tuple(), namedtuple_impl::SequenceGenerator<Namedtuple::N>());
}

template<typename Namedtuple>
template<typename TupleLike, std::size_t... I>
void
dfe::TabularNamedtupleWriter<Namedtuple>::write(
  const TupleLike& values, namedtuple_impl::Sequence<I...>)
{
  // see csv implementation for detailed explanation of whats going on here
  using swallow = int[];
  int col = 0;
  (void)swallow{0, (void(
                      m_file << (0 < col++ ? " " : "") << std::left
                             << std::setw(m_widths[I]) << std::get<I>(values)),
                    0)...};
  m_file << '\n';
}
