/// \file
/// \brief Unit tests for (d)elimiter-(s)eparated (v)alues i/o

#include <boost/test/unit_test.hpp>

#include "record.hpp"
#include <dfe/dfe_io_dsv.hpp>
#include <dfe/dfe_namedtuple.hpp>

static constexpr size_t kNRecords = 1024;

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

// failure tests for readers

// construct a path to an example data file
std::string
make_data_path(const char* filename)
{
  std::string path = DFE_UNITTESTS_DIRECTORY;
  path += "/data/namedtuple-";
  path += filename;
  path += ".csv";
  return path;
}

BOOST_AUTO_TEST_CASE(tsv_namedtuple_read_bad_files)
{
  using Reader = dfe::CsvNamedTupleReader<Record>;

  Record r;
  BOOST_CHECK_THROW(Reader("does/not/exist.tsv"), std::runtime_error);
  BOOST_CHECK_THROW(Reader(make_data_path("bad_header1")), std::runtime_error);
  BOOST_CHECK_THROW(Reader(make_data_path("bad_header2")), std::runtime_error);
  BOOST_CHECK_THROW(Reader(make_data_path("bad_header3")), std::runtime_error);
  BOOST_CHECK_THROW(
    Reader(make_data_path("too_few_columns")).read(r), std::runtime_error);
  BOOST_CHECK_THROW(
    Reader(make_data_path("too_many_columns")).read(r), std::runtime_error);
}
