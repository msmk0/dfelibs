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
/// \brief   Read/write (d)elimiter-(s)eparated (v)alues text files
/// \author  Moritz Kiehn <msmk@cern.ch>
/// \date    2019-09-09, Split dsv i/o from the namedtuple library

#pragma once

#include <array>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>

namespace dfe {
namespace io_dsv_impl {

/// Write records as delimiter-separated values into a text file.
template<typename NamedTuple>
class DsvWriter {
public:
  DsvWriter() = delete;
  DsvWriter(const DsvWriter&) = delete;
  DsvWriter(DsvWriter&&) = default;
  ~DsvWriter() = default;
  DsvWriter& operator=(const DsvWriter&) = delete;
  DsvWriter& operator=(DsvWriter&&) = default;

  /// Create a file at the given path. Overwrites existing data.
  ///
  /// \param path       Path to the output file
  /// \param delimiter  Delimiter to separate values within one record
  /// \param precision  Output floating point precision
  DsvWriter(const std::string& path, char delimiter, int precision);

  /// Append a record to the file.
  void append(const NamedTuple& record);

private:
  template<typename TupleLike, std::size_t... I>
  void write_line(const TupleLike& values, std::index_sequence<I...>);

  std::ofstream m_file;
  char m_delimiter;
};

/// Read records as delimiter-separated values from a text file.
///
/// The reader is strict about its input format. Each row **must** contain
/// exactly as many columns as there are values within the record. Every row
/// **must** end with a single newline. The first row is **alway** interpreted
/// as a header but can be skipped. If it is not skipped, the header names
/// in each column **must** match exactly to the record member names.
template<typename NamedTuple>
class DsvReader {
public:
  DsvReader() = delete;
  DsvReader(const DsvReader&) = delete;
  DsvReader(DsvReader&&) = default;
  ~DsvReader() = default;
  DsvReader& operator=(const DsvReader&) = delete;
  DsvReader& operator=(DsvReader&&) = default;

  /// Open a file at the given path.
  ///
  /// \param path           Path to the input file
  /// \param delimiter      Delimiter to separate values within one record
  /// \param verify_header  false to check header column names, false to skip
  DsvReader(const std::string& path, char delimiter, bool verify_header);

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

// implementation text writer

template<typename NamedTuple>
inline DsvWriter<NamedTuple>::DsvWriter(
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
DsvWriter<NamedTuple>::append(const NamedTuple& record)
{
  write_line(record.to_tuple(), std::make_index_sequence<NamedTuple::N>{});
}

template<typename NamedTuple>
template<typename TupleLike, std::size_t... I>
inline void
DsvWriter<NamedTuple>::write_line(
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
inline DsvReader<NamedTuple>::DsvReader(
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
inline bool
DsvReader<NamedTuple>::read(NamedTuple& record)
{
  if (!read_line()) { return false; }
  record = parse_line<typename NamedTuple::Tuple>(
    std::make_index_sequence<NamedTuple::N>{});
  return true;
}

template<typename NamedTuple>
inline bool
DsvReader<NamedTuple>::read_line()
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
inline TupleLike
DsvReader<NamedTuple>::parse_line(std::index_sequence<I...>) const
{
  // see write_line implementation in text writer for explanation
  TupleLike values;
  std::istringstream is;
  using swallow = int[];
  (void)swallow{
    0, ((std::istringstream(m_columns[I]) >> std::get<I>(values)), 0)...};
  return values;
}

} // namespace io_dsv_impl

/// Write records as a comma-separated values into a text file.
template<typename NamedTuple>
class CsvNamedTupleWriter : public io_dsv_impl::DsvWriter<NamedTuple> {
public:
  /// Create a csv file at the given path. Overwrites existing data.
  CsvNamedTupleWriter(
    const std::string& path,
    int precision = (std::numeric_limits<double>::max_digits10 + 1))
    : io_dsv_impl::DsvWriter<NamedTuple>(path, ',', precision)
  {
  }
};

/// Write records as a tab-separated values into a text file.
template<typename NamedTuple>
class TsvNamedTupleWriter : public io_dsv_impl::DsvWriter<NamedTuple> {
public:
  /// Create a tsv file at the given path. Overwrites existing data.
  TsvNamedTupleWriter(
    const std::string& path,
    int precision = (std::numeric_limits<double>::max_digits10 + 1))
    : io_dsv_impl::DsvWriter<NamedTuple>(path, '\t', precision)
  {
  }
};

/// Read records from a comma-separated file.
template<typename NamedTuple>
class CsvNamedTupleReader : public io_dsv_impl::DsvReader<NamedTuple> {
public:
  /// Open a csv file at the given path.
  CsvNamedTupleReader(const std::string& path, bool verify_header = true)
    : io_dsv_impl::DsvReader<NamedTuple>(path, ',', verify_header)
  {
  }
};

/// Read records from a tab-separated file.
template<typename NamedTuple>
class TsvNamedTupleReader : public io_dsv_impl::DsvReader<NamedTuple> {
public:
  /// Open a tsv file at the given path.
  TsvNamedTupleReader(const std::string& path, bool verify_header = true)
    : io_dsv_impl::DsvReader<NamedTuple>(path, '\t', verify_header)
  {
  }
};

} // namespace dfe
