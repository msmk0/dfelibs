/// \file
/// \brief Unit tests for named tuple readers and writers

#include <boost/test/unit_test.hpp>
#include <iostream>

#include <cstdint>

#include <dfe/dfe_namedtuple.hpp>

static constexpr size_t kNRecords = 1024;

struct Record {
  int16_t x = 0;
  int32_t y = 0;
  int64_t z = 0;
  uint64_t a = 0;
  bool THIS_IS_UNUSED = false;
  float b = 0;
  double c = 0;
  bool d = false;

  DFE_NAMEDTUPLE(Record, x, y, z, a, b, c, d)
};

Record
make_record(size_t i)
{
  Record r;
  r.x = i;
  r.y = -2 * i;
  r.z = 4 * i;
  r.a = 8 * i;
  r.THIS_IS_UNUSED = ((i % 2) == 0);
  r.b = 0.23126121f * i;
  r.c = -42.53425 * i;
  r.d = ((i % 2) != 0);
  return r;
}

BOOST_TEST_DONT_PRINT_LOG_VALUE(Record::Tuple)

BOOST_AUTO_TEST_CASE(namedtuple_names)
{
  auto example = make_record(123);
  BOOST_TEST(example.names().size() == 7);
  BOOST_TEST(example.names().at(0) == "x");
  BOOST_TEST(example.names().at(1) == "y");
  BOOST_TEST(example.names().at(2) == "z");
  BOOST_TEST(example.names().at(3) == "a");
  BOOST_TEST(example.names().at(4) == "b");
  BOOST_TEST(example.names().at(5) == "c");
  BOOST_TEST(example.names().at(6) == "d");
}

BOOST_AUTO_TEST_CASE(nametuple_assign)
{
  // check default values
  Record r;
  BOOST_TEST(r.x == 0);
  BOOST_TEST(r.y == 0);
  BOOST_TEST(r.z == 0);
  BOOST_TEST(r.a == 0);
  BOOST_TEST(r.b == 0.0f);
  BOOST_TEST(r.c == 0.0);
  BOOST_TEST(not r.d);
  // assign namedtuple from a regular tuple
  r = std::make_tuple(-1, 1, 2, -3, 1.23f, 6.54, true);
  BOOST_TEST(r.x == -1);
  BOOST_TEST(r.y == 1);
  BOOST_TEST(r.z == 2);
  BOOST_TEST(r.a == -3);
  BOOST_TEST(r.b == 1.23f);
  BOOST_TEST(r.c == 6.54);
  BOOST_TEST(r.d);
}

// full write/read chain tests

BOOST_AUTO_TEST_CASE(namedtuple_csv)
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

    Record record;
    size_t n = 0;
    while (reader.read(record)) {
      BOOST_TEST(
        record.to_tuple() == make_record(n).to_tuple(),
        "inconsistent record " << n);
      // this should remain untouched, i.e. at the default value
      BOOST_TEST(record.THIS_IS_UNUSED == Record().THIS_IS_UNUSED);
      n += 1;
    }
    BOOST_TEST(n == kNRecords);
  }
}

BOOST_AUTO_TEST_CASE(namedtuple_tsv)
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

    Record record;
    size_t n = 0;
    while (reader.read(record)) {
      BOOST_TEST(
        record.to_tuple() == make_record(n).to_tuple(),
        "inconsistent record " << n);
      // this should remain untouched, i.e. at the default value
      BOOST_TEST(record.THIS_IS_UNUSED == Record().THIS_IS_UNUSED);
      n += 1;
    }
    BOOST_TEST(n == kNRecords);
  }
}

BOOST_AUTO_TEST_CASE(namedtuple_npy)
{
  // write some data
  {
    dfe::NpyNamedTupleWriter<Record> writer("test.npy");

    for (size_t i = 0; i < kNRecords; ++i) {
      BOOST_CHECK_NO_THROW(writer.append(make_record(i)));
    }
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

BOOST_AUTO_TEST_CASE(namedtuple_tsv_reader)
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
