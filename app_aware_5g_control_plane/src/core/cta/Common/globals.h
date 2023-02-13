#ifndef GLOBALS_H_
#define GLOBALS_H_

#define MAX_PKT_SIZE 1024
#define MAX_PKT_BURST 32

// Ports configuration
#define CPF_PORT 1
#define RX_PORT 2
#define TX_PORT 3

#include <rte_mempool.h>
#include <vector>

using namespace std;

extern volatile bool force_quit;

union bytesToNumber {
  uint8_t buff[2];
  uint16_t value;
};

union gutiExtractor {
  uint8_t buff[8];
  unsigned long int guti;
};

union logicalClockExtractor {
  uint8_t buff[8];
  unsigned long int logical_clock;
};

struct cpf_actions {
  int action;
  vector<int> config;
};

struct CTAConfig {
  int serializer;
  int number_of_cpfs;
  int replicas;
  vector<float> *cpu_loads;
};

struct LBInfo {
  int id;
  struct rte_ring *tx_ring;
  struct CTAConfig *config;
  vector<int> traffic_distribution;
  vector<int> dequeue_ratios;
};

struct LBRtnInfo {

  struct rte_ring **lb_rtn_rings;
  struct CTAConfig *config;
};

extern struct rte_mempool *global_mempool;
const int RX_DEQUEUE_THREADS = 7;
const int TX_DEQUEUE_THREADS = 4;

#endif
