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
    int precision = (std::numeric_limits<double>::max_digits10 + 1));

  /// Append a record to the file.
  void append(const NamedTuple& record);

private:
  template<typename TupleLike, std::size_t... I>
  void write_line(const TupleLike& values, std::index_sequence<I...>);

  std::ofstream m_file;
};

/// Read records as delimiter-separated values from a text file.
///
/// The reader is strict about its input format. Each row **must** contain
/// exactly as many columns as there are values within the record. Every row
/// **must** end with a single newline. The first row is **alway** interpreted
/// as a header but can be skipped. If it is not skipped, the header names
/// in each column **must** match exactly to the record member names.
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

private:
  bool read_line();
  void parse_header() const;
  template<typename TupleLike, std::size_t... I>
  void parse_record(TupleLike& record, std::index_sequence<I...>) const;

  std::ifstream m_file;
  std::array<std::string, NamedTuple::N> m_columns;
  std::size_t m_num_lines;
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
  // (yes, ',' can also be an operator with a very weird but helpful function).
  // The ... pack expansion creates this expression for each entry in the index
  // pack I, effectively looping over the indices at compile time.
  using swallow = int[];
  (void)swallow{0, (m_file << std::get<I>(values)
                           << (((I + 1) < sizeof...(I)) ? Delimiter : '\n'),
                    0)...};
}

// implementation text reader

template<char Delimiter, typename NamedTuple>
inline DsvReader<Delimiter, NamedTuple>::DsvReader(
  const std::string& path, bool verify_header)
  : m_num_lines(0)
{
  // make our life easier. always throw on error
  m_file.exceptions(std::ofstream::badbit);
  m_file.open(path, std::ios_base::binary | std::ios_base::in);
  if (verify_header) {
    if (not read_line()) { throw std::runtime_error("Invalid file header"); };
    parse_header();
  } else {
    m_file.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
  }
}

template<char Delimiter, typename NamedTuple>
inline bool
DsvReader<Delimiter, NamedTuple>::read(NamedTuple& record)
{
  if (not read_line()) { return false; }
  typename NamedTuple::Tuple values;
  parse_record(values, std::make_index_sequence<NamedTuple::N>{});
  record = values;
  return true;
}

template<char Delimiter, typename NamedTuple>
inline bool
DsvReader<Delimiter, NamedTuple>::read_line()
{
  constexpr std::ifstream::int_type end_of_column = Delimiter;
  constexpr std::ifstream::int_type end_of_line = '\n';
  constexpr std::ifstream::int_type end_of_file =
    std::ifstream::traits_type::eof();
  constexpr std::size_t expected_columns = NamedTuple::N;

  // read single line and split it into columns
  std::size_t ncolumns = 0;
  std::string column;
  while (true) {
    auto c = m_file.get();
    if (c == end_of_file) {
      return false;
    } else if (c == end_of_line) {
      // end-of-line terminates the last column
      m_columns[ncolumns++] = column;
      break;
    } else if (c == end_of_column) {
      m_columns[ncolumns++] = column;
      // end-of-column token means there should be at aleast one more column
      // and we can already check now for column count mismatches
      if (expected_columns < (ncolumns + 1)) {
        throw std::runtime_error(
          "Too many columns in line " + std::to_string(m_num_lines));
      }
      column.clear();
    } else {
      column.push_back(c);
    }
  }
  if (ncolumns < expected_columns) {
    throw std::runtime_error(
      "Too few columns in line " + std::to_string(m_num_lines));
  }
  m_num_lines += 1;
  return true;
}

template<char Delimiter, typename NamedTuple>
inline void
DsvReader<Delimiter, NamedTuple>::parse_header() const
{
  // ensure correct column names
  const auto& expected = NamedTuple::names();
  for (std::size_t i = 0; i < NamedTuple::N; ++i) {
    if (expected[i] != m_columns[i]) {
      throw std::runtime_error(
        "Invalid header column=" + std::to_string(i) + " expected='" +
        expected[i] + "' seen='" + m_columns[i] + "'");
    }
  }
}

template<char Delimiter, typename NamedTuple>
template<typename TupleLike, std::size_t... I>
inline void
DsvReader<Delimiter, NamedTuple>::parse_record(
  TupleLike& record, std::index_sequence<I...>) const
{
  // see write_line implementation in text writer for explanation
  using swallow = int[];
  (void)swallow{
    0, (std::istringstream(m_columns[I]) >> std::get<I>(record), 0)...};
}

} // namespace io_dsv_impl

/// Write records as a comma-separated values into a text file.
template<typename NamedTuple>
using CsvNamedTupleWriter = io_dsv_impl::DsvWriter<',', NamedTuple>;

/// Read records from a comma-separated file.
template<typename NamedTuple>
using CsvNamedTupleReader = io_dsv_impl::DsvReader<',', NamedTuple>;

/// Write records as a tab-separated values into a text file.
template<typename NamedTuple>
using TsvNamedTupleWriter = io_dsv_impl::DsvWriter<'\t', NamedTuple>;

/// Read records from a tab-separated file.
template<typename NamedTuple>
using TsvNamedTupleReader = io_dsv_impl::DsvReader<'\t', NamedTuple>;

} // namespace dfe
