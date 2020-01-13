/// \file
/// \brief Demonstrate the basic named tuple writer functionality

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <random>

#include <dfe/dfe_io_dsv.hpp>
#include <dfe/dfe_io_numpy.hpp>
#ifdef DFE_USE_IO_ROOT
#include <dfe/dfe_io_root.hpp>
#endif
#include <dfe/dfe_namedtuple.hpp>

struct Data {
  uint32_t dac0;
  uint32_t temperature;
  int64_t timestamp;
  float humidity;
  int unused;

  DFE_NAMEDTUPLE(Data, dac0, temperature, timestamp, humidity);
};

int
main(int argc, char* argv[])
{
  // text writers
  dfe::NamedTupleCsvWriter<Data> csv("test.csv");
  dfe::NamedTupleTsvWriter<Data> tsv("test.tsv");
  // numpy writer
  dfe::NamedTupleNumpyWriter<Data> npy("test.npy");
#ifdef DFE_USE_IO_ROOT
  // (optional) ROOT writer
  dfe::NamedTupleRootWriter<Data> roo("test.root", "records");
#endif

  // random data generators
  auto rng = std::ranlux48(12345);
  auto rnd_dac0 = std::uniform_int_distribution<uint32_t>(32, 511);
  auto rnd_temp = std::uniform_int_distribution<uint32_t>(2400, 2800);
  auto rnd_jitr = std::uniform_int_distribution<int64_t>(-10, 10);
  auto rnd_hmdt = std::normal_distribution<float>(35.0, 5.0);

  for (int i = 0; i < 1024; ++i) {
    Data x;
    x.dac0 = rnd_dac0(rng);
    x.temperature = rnd_temp(rng);
    x.timestamp = 1000 * i + rnd_jitr(rng);
    x.humidity = rnd_hmdt(rng);
    x.unused = i;

    csv.append(x);
    tsv.append(x);
    npy.append(x);
#ifdef DFE_USE_IO_ROOT
    roo.append(x);
#endif

    std::cout << "entry " << i << ": " << x << std::endl;
  }

  return EXIT_SUCCESS;
}
