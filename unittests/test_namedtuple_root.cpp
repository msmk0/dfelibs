/// \file
/// \brief Unit tests for named tuple root readers and writer

#include <boost/test/unit_test.hpp>
#include <iostream>

#include <cstdint>

#include <dfe_namedtuple.hpp>
#include <dfe_namedtuple_root.hpp>

static constexpr size_t kNRecords = 1024;

struct Record {
  // regular types
  float f;
  double d;
  unsigned long ul;
  unsigned int ui;
  long l;
  int i;
  // stdtypes
  uint64_t u64;
  uint32_t u32;
  uint16_t u16;
  uint8_t u8;
  int64_t i64;
  int32_t i32;
  int16_t i16;
  int8_t i8;
  // ROOT types
  UChar_t ru8;
  UShort_t ru16;
  UInt_t ru32;
  ULong64_t ru64;
  Char_t ri8;
  Short_t ri16;
  Int_t ri32;
  Long64_t ri64;
  Bool_t rb;

  DFE_NAMEDTUPLE(
    Record, u64, u32, u16, u8, i64, i32, i16, i8, ul, ui, l, i, f, d, ru8, ru16,
    ru32, ru64, ri8, ri16, ri32, ri64, rb);
};

Record
make_record(size_t i)
{
  Record r;
  r.f = 0.234f * i;
  r.d = -0.234 * i;
  r.ul = 32 * i;
  r.ui = 16 * i;
  r.l = -32 * i;
  r.i = -16 * i;
  r.u64 = 8 * i;
  r.u32 = 4 * i;
  r.u16 = 2 * i;
  r.u8 = i;
  r.i64 = -8 * i;
  r.i32 = -4 * i;
  r.i16 = -2 * i;
  r.i8 = -1 * i;
  r.ru8 = UINT8_MAX;
  r.ru16 = UINT16_MAX;
  r.ru32 = UINT32_MAX;
  r.ru64 = UINT64_MAX;
  r.ri8 = INT8_MIN;
  r.ri16 = INT16_MIN;
  r.ri32 = INT32_MIN;
  r.ri64 = INT64_MIN;
  r.rb = ((i % 4) == 0u);
  return r;
}

BOOST_TEST_DONT_PRINT_LOG_VALUE(Record::Tuple)

BOOST_AUTO_TEST_CASE(namedtuple_root)
{
  // write some data
  {
    dfe::RootNamedTupleWriter<Record> writer("test.root", "records");

    for (size_t i = 0; i < kNRecords; ++i) {
      BOOST_CHECK_NO_THROW(writer.append(make_record(i)));
    }
  }
  // TODO read the data back
}

// TODO failure tests
