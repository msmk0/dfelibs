/// \file
/// \brief Demonstrate dfe::RootNamedTupleWriter functionality

#include <cstdlib>
#include <iostream>
#include <random>

#include <dfe_namedtuple.hpp>
#include <dfe_namedtuple_root.hpp>

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
  // open ROOT file
  dfe::RootNamedTupleWriter<Data> writer("test.root", "data");

  // random data generators
  auto rng = std::ranlux48();
  auto rnd_dac0 = std::uniform_int_distribution<uint32_t>(32, 511);
  auto rnd_temp = std::uniform_int_distribution<uint32_t>(2400, 2800);
  auto rnd_jitr = std::uniform_int_distribution<int64_t>(-10, 10);
  auto rnd_hmdt = std::normal_distribution<float>(35.0, 5.0);

  // add a few random entries
  for (int i = 0; i < 2048; ++i) {
    Data x;
    x.dac0 = rnd_dac0(rng);
    x.temperature = rnd_temp(rng);
    x.timestamp = 1000 * i + rnd_jitr(rng);
    x.humidity = rnd_hmdt(rng);
    x.unused = i;
    writer.append(x);

    std::cout << "entry " << i << ": " << x << std::endl;
  }

  return EXIT_SUCCESS;
}
