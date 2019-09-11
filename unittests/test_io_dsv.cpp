/// \file
/// \brief Unit tests for (d)elimiter-(s)eparated (v)alues i/o

#include <boost/test/unit_test.hpp>

#include "record.hpp"
#include <dfe/dfe_io_dsv.hpp>
#include <dfe/dfe_namedtuple.hpp>

BOOST_TEST_DONT_PRINT_LOG_VALUE(Record::Tuple)

#define TEST_READER_RECORDS(reader) \
  do { \
    Record record; \
    for (size_t i = 0; reader.read(record); ++i) { \
      auto expected = make_record(i); \
      BOOST_TEST( \
        record.to_tuple() == expected.to_tuple(), \
        "inconsistent record " << i << " expected=(" << expected << ") read=(" \
                               << record << ")"); \
      BOOST_TEST(record.THIS_IS_UNUSED == Record().THIS_IS_UNUSED); \
    } \
  } while (false)

// full write/read chain tests

constexpr size_t kNRecords = 1024;

BOOST_AUTO_TEST_CASE(csv_namedtuple_write_read)
{
  // write some data
  {
    dfe::CsvNamedTupleWriter<Record> writer("test.csv");

    for (size_t i = 0; i < kNRecords; ++i) {
      BOOST_CHECK_NO_THROW(writer.append(make_record(i)));
    }
  }
  // read the data back
  {
    dfe::CsvNamedTupleReader<Record> reader("test.csv");

    TEST_READER_RECORDS(reader);
    BOOST_TEST(reader.num_records() == kNRecords);
  }
  // read the data back w/o verifying the header
  {
    dfe::CsvNamedTupleReader<Record> reader("test.csv", false);

    TEST_READER_RECORDS(reader);
    BOOST_TEST(reader.num_records() == kNRecords);
  }
}

BOOST_AUTO_TEST_CASE(tsv_namedtuple_write_read)
{
  // write some data
  {
    dfe::TsvNamedTupleWriter<Record> writer("test.tsv");

    for (size_t i = 0; i < kNRecords; ++i) {
      BOOST_CHECK_NO_THROW(writer.append(make_record(i)));
    }
  }
  // read the data back
  {
    dfe::TsvNamedTupleReader<Record> reader("test.tsv");

    TEST_READER_RECORDS(reader);
    BOOST_TEST(reader.num_records() == kNRecords);
  }
  // read the data back w/o verifying the header
  {
    dfe::TsvNamedTupleReader<Record> reader("test.tsv", false);

    TEST_READER_RECORDS(reader);
    BOOST_TEST(reader.num_records() == kNRecords);
  }
}

// construct a path to an example data file
std::string
make_data_path(const char* filename)
{
  std::string path = DFE_UNITTESTS_DIRECTORY;
  path += "/data/namedtuple-";
  path += filename;
  return path;
}

// read Pandas-generated files

constexpr size_t kNOnfile = 32;

// w/ reordered columns

BOOST_AUTO_TEST_CASE(csv_namedtuple_read_reordered)
{
  std::string path = make_data_path("reordered_columns.csv");
  dfe::CsvNamedTupleReader<Record> reader(path);

  TEST_READER_RECORDS(reader);
  BOOST_TEST(reader.num_records() == kNOnfile);
}

BOOST_AUTO_TEST_CASE(tsv_namedtuple_read_reordered)
{
  std::string path = make_data_path("reordered_columns.tsv");
  dfe::TsvNamedTupleReader<Record> reader(path);

  TEST_READER_RECORDS(reader);
  BOOST_TEST(reader.num_records() == kNOnfile);
}

// w/ reordered and additional columns

#define TEST_READER_RECORDS_EXTRA(reader, nextra) \
  do { \
    Record record; \
    std::vector<int> extra; \
    for (size_t i = 0; reader.read(record, extra); ++i) { \
      auto expected = make_record(i); \
      BOOST_TEST( \
        record.to_tuple() == expected.to_tuple(), \
        "inconsistent record " << i << " expected=(" << expected << ") read=(" \
                               << record << ")"); \
      BOOST_TEST(record.THIS_IS_UNUSED == Record().THIS_IS_UNUSED); \
      BOOST_TEST(extra.size() == nextra); \
      for (auto x : extra) { BOOST_TEST(x == i); } \
    } \
  } while (false)

BOOST_AUTO_TEST_CASE(csv_namedtuple_read_extra_columns)
{
  std::string path = make_data_path("extra_columns.csv");
  dfe::CsvNamedTupleReader<Record> reader(path);

  TEST_READER_RECORDS_EXTRA(reader, 3);
  BOOST_TEST(reader.num_records() == kNOnfile);
  BOOST_TEST(reader.num_extra_columns() == 3);
}

BOOST_AUTO_TEST_CASE(tsv_namedtuple_read_extra_columns)
{
  std::string path = make_data_path("extra_columns.tsv");
  dfe::TsvNamedTupleReader<Record> reader(path);

  TEST_READER_RECORDS_EXTRA(reader, 3);
  BOOST_TEST(reader.num_records() == kNOnfile);
  BOOST_TEST(reader.num_extra_columns() == 3);
}

// failure tests for readers

BOOST_AUTO_TEST_CASE(tsv_namedtuple_read_bad_files)
{
  using Reader = dfe::CsvNamedTupleReader<Record>;

  Record r;
  BOOST_CHECK_THROW(Reader("does/not/exist.tsv"), std::runtime_error);
  BOOST_CHECK_THROW(
    Reader(make_data_path("bad_header1.tsv")), std::runtime_error);
  BOOST_CHECK_THROW(
    Reader(make_data_path("bad_header2.tsv")), std::runtime_error);
  BOOST_CHECK_THROW(
    Reader(make_data_path("bad_header3.tsv")), std::runtime_error);
  BOOST_CHECK_THROW(
    Reader(make_data_path("missing_columns.tsv")), std::runtime_error);
  BOOST_CHECK_THROW(
    Reader(make_data_path("too_few_columns.tsv")).read(r), std::runtime_error);
  BOOST_CHECK_THROW(
    Reader(make_data_path("too_many_columns.tsv")).read(r), std::runtime_error);
}
