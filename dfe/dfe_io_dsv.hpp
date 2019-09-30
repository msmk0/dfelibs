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

#include <algorithm>
#include <array>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace dfe {
namespace io_dsv_impl {

/// Write records as delimiter-separated values into a text file.
template<char Delimiter, typename NamedTuple>
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
  /// \param precision  Output floating point precision
  DsvWriter(
    const std::string& path,
    int precision = std::numeric_limits<double>::max_digits10);

  /// Append a record to the file.
  void append(const NamedTuple& record);

private:
  template<typename TupleLike, std::size_t... I>
  void write_line(const TupleLike& values, std::index_sequence<I...>);

  std::ofstream m_file;
};

/// Read records as delimiter-separated values from a text file.
///
/// The reader is strict about its input format to avoid ambiguities. If
/// header verification is disabled, the first line will be skipped and each
/// line must contain exactly as many columns as there are tuple members in
/// exactly the same order. If header verification is enabled, the first line
/// is interpreted as the header. Names in the header must match exactly to
/// the tuple members but can be in arbitrary order. The file can contain
/// extra columns that are not tuple members. Each following row must have
/// exactly the same number of columns as the header.
template<char Delimiter, typename NamedTuple>
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
  /// \param verify_header  true to check header column names, false to skip
  DsvReader(const std::string& path, bool verify_header = true);

  /// Read the next record from the file.
  ///
  /// \returns true   if a record was successfully read
  /// \returns false  if no more records are available
  bool read(NamedTuple& record);

  /// Read the next record and any extra columns from the file.
  ///
  /// \returns true   if a record was successfully read
  /// \returns false  if no more records are available
  template<typename T>
  bool read(NamedTuple& record, std::vector<T>& extra);

  /// Return the number of additional columns that are not part of the tuple.
  std::size_t num_extra_columns() const { return m_extra_columns.size(); }
  /// Return the number of records read so far.
  std::size_t num_records() const { return m_num_records; }

private:
  bool read_line();
  void parse_header();
  template<typename TupleLike, std::size_t... I>
  void parse_record(TupleLike& record, std::index_sequence<I...>) const;

  std::ifstream m_file;
  std::string m_line;
  std::vector<std::string> m_columns;
  std::size_t m_num_lines = 0;
  std::size_t m_num_records = 0;
  // will be fixed after reading the header
  std::size_t m_num_columns = SIZE_MAX;
  // map the tuple index to the corresponding column index on file.
  std::array<std::size_t, NamedTuple::N> m_tuple_to_column;
  // column indices that do not map to a tuple item.
  std::vector<std::size_t> m_extra_columns;
};

// implementation text writer

template<char Delimiter, typename NamedTuple>
inline DsvWriter<Delimiter, NamedTuple>::DsvWriter(
  const std::string& path, int precision)
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

template<char Delimiter, typename NamedTuple>
inline void
DsvWriter<Delimiter, NamedTuple>::append(const NamedTuple& record)
{
  write_line(record.to_tuple(), std::make_index_sequence<NamedTuple::N>{});
}

template<char Delimiter, typename NamedTuple>
template<typename TupleLike, std::size_t... I>
inline void
DsvWriter<Delimiter, NamedTuple>::write_line(
  const TupleLike& values, std::index_sequence<I...>)
{
  // this is a bit like magic, here is whats going on:
  // the (<expr>, 0) expression evaluates <expr>, ignores its return value
  // and then returns an integer with value 0 by virtue of the comma operator
  // (yes, ',' can also be an operator with a very weird but helpful
  // function). The ... pack expansion creates this expression for each entry
  // in the index pack I, effectively looping over the indices at compile
  // time.
  using swallow = int[];
  (void)swallow{0, (m_file << std::get<I>(values)
                           << (((I + 1) < sizeof...(I)) ? Delimiter : '\n'),
                    0)...};
}

// implementation text reader

template<char Delimiter, typename NamedTuple>
inline DsvReader<Delimiter, NamedTuple>::DsvReader(
  const std::string& path, bool verify_header)
  : m_file(path, std::ios_base::binary | std::ios_base::in)
{
  if (not m_file.is_open() or m_file.fail()) {
    throw std::runtime_error("Could not open file '" + path + "'");
  }
  if (not read_line()) {
    throw std::runtime_error("Could not read header from '" + path + "'");
  }
  if (verify_header) {
    parse_header();
  } else {
    // assume row content is identical in content and order to the tuple
    m_num_columns = NamedTuple::N;
    for (std::size_t i = 0; i < m_tuple_to_column.size(); ++i) {
      m_tuple_to_column[i] = i;
    }
  }
}

template<char Delimiter, typename NamedTuple>
inline bool
DsvReader<Delimiter, NamedTuple>::read(NamedTuple& record)
{
  if (not read_line()) { return false; }
  // check for consistent entries per-line
  if (m_columns.size() < m_num_columns) {
    throw std::runtime_error(
      "Too few columns in line " + std::to_string(m_num_lines));
  }
  if (m_num_columns < m_columns.size()) {
    throw std::runtime_error(
      "Too many columns in line " + std::to_string(m_num_lines));
  }
  // convert to tuple
  typename NamedTuple::Tuple values;
  parse_record(values, std::make_index_sequence<NamedTuple::N>{});
  record = values;
  m_num_records += 1;
  return true;
}

template<char Delimiter, typename NamedTuple>
template<typename T>
inline bool
DsvReader<Delimiter, NamedTuple>::read(
  NamedTuple& record, std::vector<T>& extra)
{
  if (not read(record)) { return false; }
  extra.clear();
  for (auto i : m_extra_columns) {
    std::istringstream is(m_columns[i]);
    extra.push_back({});
    is >> extra.back();
  }
  return true;
}

template<char Delimiter, typename NamedTuple>
inline bool
DsvReader<Delimiter, NamedTuple>::read_line()
{
  // read the next line and check for both end-of-file and errors
  std::getline(m_file, m_line);
  m_num_lines += 1;
  if (m_file.eof()) { return false; }
  if (m_file.fail()) {
    throw std::runtime_error(
      "Could not read line " + std::to_string(m_num_lines));
  }

  // split the line into columns
  m_columns.clear();
  for (std::string::size_type pos = 0; pos < m_line.size();) {
    auto del = m_line.find_first_of(Delimiter, pos);
    if (del == std::string::npos) {
      // reached the end of the line; also determines the last column
      m_columns.emplace_back(m_line, pos);
      break;
    } else {
      m_columns.emplace_back(m_line, pos, del - pos);
      // start next column search after the delimiter
      pos = del + 1;
    }
  }
  return true;
}

template<char Delimiter, typename NamedTuple>
inline void
DsvReader<Delimiter, NamedTuple>::parse_header()
{
  const auto& names = NamedTuple::names();
  // check that all columns are available
  for (const auto& name : names) {
    auto it = std::find(m_columns.begin(), m_columns.end(), name);
    if (it == m_columns.end()) {
      throw std::runtime_error("Missing header column '" + name + "'");
    }
  }
  // determine column order
  for (std::size_t i = 0; i < m_columns.size(); ++i) {
    // find the position of the column in the tuple.
    auto it = std::find(names.begin(), names.end(), m_columns[i]);
    if (it != names.end()) {
      // establish mapping between column and tuple item position
      m_tuple_to_column[std::distance(names.begin(), it)] = i;
    } else {
      // record non-tuple columns
      m_extra_columns.push_back(i);
    }
  }
  // fix number of columns for all future reads
  m_num_columns = m_columns.size();
}

template<char Delimiter, typename NamedTuple>
template<typename TupleLike, std::size_t... I>
inline void
DsvReader<Delimiter, NamedTuple>::parse_record(
  TupleLike& record, std::index_sequence<I...>) const
{
  // see write_line implementation in text writer for explanation
  using swallow = int[];
  // allow different column ordering on file compared to the namedtuple
  (void)swallow{0, (std::istringstream(m_columns[m_tuple_to_column[I]]) >>
                      std::get<I>(record),
                    0)...};
}

} // namespace io_dsv_impl

/// Write records as a comma-separated values into a text file.
template<typename NamedTuple>
using CsvNamedTupleWriter = io_dsv_impl::DsvWriter<',', NamedTuple>;

/// Read tuple-like records from a comma-separated file.
template<typename NamedTuple>
using CsvNamedTupleReader = io_dsv_impl::DsvReader<',', NamedTuple>;

/// Write tuple-like records as tab-separated values into a text file.
template<typename NamedTuple>
using TsvNamedTupleWriter = io_dsv_impl::DsvWriter<'\t', NamedTuple>;

/// Read tuple-like records from a tab-separated file.
template<typename NamedTuple>
using TsvNamedTupleReader = io_dsv_impl::DsvReader<'\t', NamedTuple>;

} // namespace dfe
