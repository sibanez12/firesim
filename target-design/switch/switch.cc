#include <functional>
#include <queue>
#include <algorithm>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <cstdlib>
#include <arpa/inet.h>
#include <string>
#include <sstream>
#include <random>

#include <time.h>
#include <netinet/ether.h>
#include <net/ethernet.h>
#include "Packet.h"
#include "EthLayer.h"
#include "IPv4Layer.h"

#define IGNORE_PRINTF

#ifdef IGNORE_PRINTF
#define printf(fmt, ...) (0)
#endif

// param: link latency in cycles
// assuming 3.2 GHz, this number / 3.2 = link latency in ns
// e.g. setting this to 35000 gives you 35000/3.2 = 10937.5 ns latency
// IMPORTANT: this must be a multiple of 7
//
// THIS IS SET BY A COMMAND LINE ARGUMENT. DO NOT CHANGE IT HERE.
//#define LINKLATENCY 6405
int LINKLATENCY = 0;

// param: switching latency in cycles
// assuming 3.2 GHz, this number / 3.2 = switching latency in ns
//
// THIS IS SET BY A COMMAND LINE ARGUMENT. DO NOT CHANGE IT HERE.
int switchlat = 0;

#define SWITCHLATENCY (switchlat)

// param: numerator and denominator of bandwidth throttle
// Used to throttle outbound bandwidth from port
//
// THESE ARE SET BY A COMMAND LINE ARGUMENT. DO NOT CHANGE IT HERE.
int throttle_numer = 1;
int throttle_denom = 1;

// uncomment to use a limited output buffer size, OUTPUT_BUF_SIZE
//#define LIMITED_BUFSIZE

// size of output buffers, in # of flits
// only if LIMITED BUFSIZE is set
// TODO: expose in manager
#define OUTPUT_BUF_SIZE (131072L)

// pull in # clients config
#define NUMCLIENTSCONFIG
#include "switchconfig.h"
#undef NUMCLIENTSCONFIG

// DO NOT TOUCH
#define NUM_TOKENS (LINKLATENCY)
#define TOKENS_PER_BIGTOKEN (7)
#define BIGTOKEN_BYTES (64)
#define NUM_BIGTOKENS (NUM_TOKENS/TOKENS_PER_BIGTOKEN)
#define BUFSIZE_BYTES (NUM_BIGTOKENS*BIGTOKEN_BYTES)

// DO NOT TOUCH
#define SWITCHLAT_NUM_TOKENS (SWITCHLATENCY)
#define SWITCHLAT_NUM_BIGTOKENS (SWITCHLAT_NUM_TOKENS/TOKENS_PER_BIGTOKEN)
#define SWITCHLAT_BUFSIZE_BYTES (SWITCHLAT_NUM_BIGTOKENS*BIGTOKEN_BYTES)

uint64_t this_iter_cycles_start = 0;

// pull in mac2port array
#define MACPORTSCONFIG
#include "switchconfig.h"
#undef MACPORTSCONFIG

#include "flit.h"
#include "baseport.h"
#include "shmemport.h"
#include "socketport.h"
#include "sshport.h"

#define ETHER_HEADER_SIZE          14
#define IP_DST_FIELD_OFFSET        16 // Dest field immediately after, in same 64-bit flit
#define IP_SUBNET_OFFSET           2
#define IP_HEADER_SIZE             20 // TODO: Not always, just currently the case with L-NIC.

#define LNIC_DATA_FLAG_MASK        0b1
#define LNIC_ACK_FLAG_MASK         0b10
#define LNIC_NACK_FLAG_MASK        0b100
#define LNIC_PULL_FLAG_MASK        0b1000
#define LNIC_CHOP_FLAG_MASK        0b10000
#define LNIC_HEADER_MSG_LEN_OFFSET 5
#define LNIC_PACKET_CHOPPED_SIZE   72 // Bytes, the minimum L-NIC packet size
#define LNIC_HEADER_SIZE           30

#define MICA_R_TYPE 1
#define MICA_W_TYPE 2
#define MICA_VALUE_SIZE_WORDS 64
#define MICA_KEY_SIZE_WORDS   2
struct __attribute__((__packed__)) mica_hdr_t {
  uint64_t op_type;
  uint64_t key[MICA_KEY_SIZE_WORDS];
  uint64_t value[MICA_VALUE_SIZE_WORDS];
};

#define CHAINREP_FLAGS_FROM_TESTER    (1 << 7)
#define CHAINREP_FLAGS_OP_READ        (1 << 6)
#define CHAINREP_FLAGS_OP_WRITE       (1 << 5)
#define CHAINREP_CHAIN_SIZE           3
#define CHAINREP_VALUE_SIZE_WORDS     8
#define CHAINREP_KEY_SIZE_WORDS       2
struct __attribute__((__packed__)) chainrep_w_hdr_t {
  uint8_t flags;
  uint8_t seq;
  uint8_t node_cnt;
  uint8_t client_ctx;
  uint32_t client_ip;
  uint64_t nodes[2];
  uint64_t key[CHAINREP_KEY_SIZE_WORDS];
  uint64_t value[CHAINREP_VALUE_SIZE_WORDS];
};
struct __attribute__((__packed__)) chainrep_r_hdr_t {
  uint8_t flags;
  uint8_t seq;
  uint8_t node_cnt;
  uint8_t client_ctx;
  uint32_t client_ip;
  uint64_t key[CHAINREP_KEY_SIZE_WORDS];
};


// Comment this out to disable pkt trimming
#define TRIM_PKTS

// TODO: these should really be exposed via config_runtime.ini
#define LOG_QUEUE_SIZE
#define LOG_EVENTS
#define LOG_ALL_PACKETS

// Pull in load generator parameters, if any
#define LOADGENSTATS
#include "switchconfig.h"
#undef LOADGENSTATS
#ifdef USE_LOAD_GEN
#include "LnicLayer.h"
#include "AppLayer.h"
#include "PayloadLayer.h"
class parsed_packet_t {
 private:
    pcpp::Packet* pcpp_packet;
 public:
    pcpp::EthLayer* eth;
    pcpp::IPv4Layer* ip;
    pcpp::LnicLayer* lnic;
    pcpp::AppLayer* app;
    switchpacket* tsp;

    parsed_packet_t() {
        eth = nullptr;
        ip = nullptr;
        lnic = nullptr;
        app = nullptr;
        tsp = nullptr;
        pcpp_packet = nullptr;
    }

    ~parsed_packet_t() {
        if (pcpp_packet != nullptr) {
            delete pcpp_packet;
        }
    }

    bool parse(switchpacket* tsp) {
        uint64_t packet_size_bytes = tsp->amtwritten * sizeof(uint64_t);
        struct timeval format_time;
        format_time.tv_sec = tsp->timestamp / 1000000000;
        format_time.tv_usec = (tsp->timestamp % 1000000000) / 1000;
        pcpp::RawPacket raw_packet((const uint8_t*)tsp->dat, 200*sizeof(uint64_t), format_time, false, pcpp::LINKTYPE_ETHERNET);
        pcpp::Packet* parsed_packet = new pcpp::Packet(&raw_packet);
        pcpp::EthLayer* eth_layer = parsed_packet->getLayerOfType<pcpp::EthLayer>();
        pcpp::IPv4Layer* ip_layer = parsed_packet->getLayerOfType<pcpp::IPv4Layer>();
        pcpp::LnicLayer* lnic_layer = (pcpp::LnicLayer*)parsed_packet->getLayerOfType(pcpp::LNIC, 0);
        pcpp::AppLayer* app_layer = (pcpp::AppLayer*)parsed_packet->getLayerOfType(pcpp::GenericPayload, 0);
        if (!eth_layer || !ip_layer || !lnic_layer || !app_layer) {
            if (!eth_layer) fprintf(stdout, "Null eth layer\n");
            if (!ip_layer) fprintf(stdout, "Null ip layer\n");
            if (!lnic_layer) fprintf(stdout, "Null lnic layer\n");
            if (!app_layer) fprintf(stdout, "Null app layer\n");
            this->eth = nullptr;
            this->ip = nullptr;
            this->lnic = nullptr;
            this->app = nullptr;
            this->tsp = nullptr;
            delete parsed_packet;
            this->pcpp_packet = nullptr;
            return false;
        }
        this->eth = eth_layer;
        this->ip = ip_layer;
        this->lnic = lnic_layer;
        this->app = app_layer;
        this->tsp = tsp;
        this->pcpp_packet = parsed_packet;
        return true;
    }
};
#define LOAD_GEN_MAC "08:55:66:77:88:08"
#define LOAD_GEN_IP "10.0.0.1"
#define NIC_MAC "00:26:E1:00:00:00"
#define NIC_IP "10.0.0.2"
#define MAX_TX_MSG_ID 127
#define APP_HEADER_SIZE 16
uint64_t tx_request_count = 0;
uint64_t rx_request_count = 0;
uint64_t request_rate_lambda_inverse;
bool load_generator_complete = false;
bool request_tx_done = false;
uint64_t request_tx_done_time;
uint64_t next_threshold = 0;
uint16_t global_tx_msg_id = 0;
bool start_message_received = false;
uint64_t global_start_message_count = 0;
std::exponential_distribution<double>* gen_dist;
std::default_random_engine* gen_rand;
std::exponential_distribution<double>* service_exp_dist;
std::uniform_int_distribution<>* service_key_uniform_dist;
std::default_random_engine* dist_rand;
std::normal_distribution<double>* service_normal_high;
std::normal_distribution<double>* service_normal_low;
std::binomial_distribution<int>* service_select_dist;
bool load_gen_hook(switchpacket* tsp);
void generate_load_packets();
void check_request_timeout();
#endif

// These are both set by command-line arguments. Don't change them here.
int HIGH_PRIORITY_OBUF_SIZE = 0;
int LOW_PRIORITY_OBUF_SIZE = 0;

// TODO: replace these port mapping hacks with a mac -> port mapping,
// could be hardcoded

BasePort * ports[NUMPORTS];
void send_with_priority(uint16_t port, switchpacket* tsp);

// State to keep track of the last queue size samples.
// Index 0 is high-priority, index 1 is low-priority.
int last_qsize_samples[NUMPORTS][2];

/* switch from input ports to output ports */
void do_fast_switching() {
#pragma omp parallel for
    for (int port = 0; port < NUMPORTS; port++) {
        ports[port]->setup_send_buf();
    }


// preprocess from raw input port to packets
#pragma omp parallel for
for (int port = 0; port < NUMPORTS; port++) {
    BasePort * current_port = ports[port];
    uint8_t * input_port_buf = current_port->current_input_buf;

    for (int tokenno = 0; tokenno < NUM_TOKENS; tokenno++) {
        if (is_valid_flit(input_port_buf, tokenno)) {
            uint64_t flit = get_flit(input_port_buf, tokenno);

            switchpacket * sp;
            if (!(current_port->input_in_progress)) {
                sp = (switchpacket*)calloc(sizeof(switchpacket), 1);
                current_port->input_in_progress = sp;

                // here is where we inject switching latency. this is min port-to-port latency
                sp->timestamp = this_iter_cycles_start + tokenno + SWITCHLATENCY;
                sp->sender = port;
            }
            sp = current_port->input_in_progress;

            sp->dat[sp->amtwritten++] = flit;
            if (is_last_flit(input_port_buf, tokenno)) {
                current_port->input_in_progress = NULL;
                if (current_port->push_input(sp)) {
                    printf("packet timestamp: %ld, len: %ld, sender: %d\n",
                            this_iter_cycles_start + tokenno,
                            sp->amtwritten, port);
                }
            }
        }
    }
}

// next do the switching. but this switching is just shuffling pointers,
// so it should be fast. it has to be serial though...

// NO PARALLEL!
// shift pointers to output queues, but in order. basically.
// until the input queues have no more complete packets
// 1) find the next switchpacket with the lowest timestamp across all the inputports
// 2) look at its mac, copy it into the right ports
//          i) if it's a broadcast: sorry, you have to make N-1 copies of it...
//          to put into the other queues

struct tspacket {
    uint64_t timestamp;
    switchpacket * switchpack;

    bool operator<(const tspacket &o) const
    {
        return timestamp > o.timestamp;
    }
};

typedef struct tspacket tspacket;


// TODO thread safe priority queue? could do in parallel?
std::priority_queue<tspacket> pqueue;

for (int i = 0; i < NUMPORTS; i++) {
    while (!(ports[i]->inputqueue.empty())) {
        switchpacket * sp = ports[i]->inputqueue.front();
        ports[i]->inputqueue.pop();
        pqueue.push( tspacket { sp->timestamp, sp });
    }
}

// next, put back into individual output queues
while (!pqueue.empty()) {
    switchpacket * tsp = pqueue.top().switchpack;
    pqueue.pop();

    struct timeval format_time;
    format_time.tv_sec = tsp->timestamp / 1000000000;
    format_time.tv_usec = (tsp->timestamp % 1000000000) / 1000;
    pcpp::RawPacket raw_packet((const uint8_t*)tsp->dat, 200*sizeof(uint64_t), format_time, false, pcpp::LINKTYPE_ETHERNET);
    pcpp::Packet parsed_packet(&raw_packet);
    pcpp::EthLayer* ethernet_layer = parsed_packet.getLayerOfType<pcpp::EthLayer>();
    pcpp::IPv4Layer* ip_layer = parsed_packet.getLayerOfType<pcpp::IPv4Layer>();
    if (ethernet_layer == NULL) {
        fprintf(stdout, "NULL ethernet layer\n");
        free(tsp);
        continue;
    }
    if (ip_layer == NULL) {
        fprintf(stdout, "NULL ip layer from %d with amtread %d and amtwritten %d\n", tsp->sender, tsp->amtread, tsp->amtwritten);
        if (ethernet_layer != NULL) {
            fprintf(stdout, "Source MAC %s, dest MAC %s\n", ethernet_layer->getSourceMac().toString().c_str(), ethernet_layer->getDestMac().toString().c_str());
        }
        for (int i = 0; i < tsp->amtwritten; i++) {
            fprintf(stdout, "%d: %#lx\n", i, __builtin_bswap64(tsp->dat[i]));
        }
        free(tsp);
        continue;
    }

// If this is a load generator, we need to do something completely different with all incoming packets.
#ifdef USE_LOAD_GEN
    if (load_gen_hook(tsp)) {
      free(tsp);
      continue;
    }
#endif

    int flit_offset_doublebytes = (ETHER_HEADER_SIZE + IP_DST_FIELD_OFFSET + IP_SUBNET_OFFSET) / sizeof(uint16_t);
    uint16_t switching_flit = ((uint16_t*)tsp->dat)[flit_offset_doublebytes];

    uint16_t send_to_port = get_port_from_flit(switching_flit, 0);
    if (send_to_port == UNKNOWN_ADDRESS) {
        fprintf(stdout, "Packet with unknown destination address, dropping\n");
        free(tsp);
        // Do nothing for a packet with an unknown destination address
    } else if (send_to_port == BROADCAST_ADJUSTED) {
#define ADDUPLINK (NUMUPLINKS > 0 ? 1 : 0)
        // this will only send broadcasts to the first (zeroeth) uplink.
        // on a switch receiving broadcast packet from an uplink, this should
        // automatically prevent switch from sending the broadcast to any uplink
        for (int i = 0; i < NUMDOWNLINKS + ADDUPLINK; i++) {
            if (i != tsp->sender ) {
                switchpacket * tsp2 = (switchpacket*)malloc(sizeof(switchpacket));
                memcpy(tsp2, tsp, sizeof(switchpacket));
                send_with_priority(i, tsp2);
            }
        }
        free(tsp);
    } else {
        send_with_priority(send_to_port, tsp);
    }
}

#ifdef USE_LOAD_GEN
generate_load_packets();
check_request_timeout();
#endif

// Log queue sizes if logging is enabled
#ifdef LOG_QUEUE_SIZE
for (int i = 0; i < NUMPORTS; i++) {
    if (ports[i]->outputqueue_high_size != last_qsize_samples[i][0] || ports[i]->outputqueue_low_size != last_qsize_samples[i][1]) {
        last_qsize_samples[i][0] = ports[i]->outputqueue_high_size;
        last_qsize_samples[i][1] = ports[i]->outputqueue_low_size;
        fprintf(stdout, "&&CSV&&QueueSize,%ld,%d,%d,%d\n", this_iter_cycles_start, i, ports[i]->outputqueue_high_size, ports[i]->outputqueue_low_size);
    }
}
#endif

// finally in parallel, flush whatever we can to the output queues based on timestamp

#pragma omp parallel for
for (int port = 0; port < NUMPORTS; port++) {
    BasePort * thisport = ports[port];
    thisport->write_flits_to_output();
}

}

// Load generator specific code begin
#ifdef USE_LOAD_GEN
void print_packet(char* direction, parsed_packet_t* packet) {
    fprintf(stdout, "%s IP(src=%s, dst=%s), %s, %s, packet_len=%d\n", direction,
            packet->ip->getSrcIpAddress().toString().c_str(), packet->ip->getDstIpAddress().toString().c_str(),
            packet->lnic->toString().c_str(), packet->app->toString().c_str(), packet->tsp->amtwritten * sizeof(uint64_t));
}

bool count_start_message() {
    global_start_message_count++;
    if (strcmp(test_type, "ONE_CONTEXT_FOUR_CORES") == 0) {
        return global_start_message_count >= 4 * NUMPORTS;
    } else if (strcmp(test_type, "FOUR_CONTEXTS_FOUR_CORES") == 0) {
        return global_start_message_count >= 4 * NUMPORTS;
    } else if (strcmp(test_type, "TWO_CONTEXTS_FOUR_SHARED_CORES") == 0) {
        return global_start_message_count >= 8 * NUMPORTS;
    } else if ((strcmp(test_type, "DIF_PRIORITY_LNIC_DRIVEN") == 0) ||
              (strcmp(test_type, "DIF_PRIORITY_TIMER_DRIVEN") == 0) ||
              (strcmp(test_type, "HIGH_PRIORITY_C1_STALL") == 0) ||
              (strcmp(test_type, "LOW_PRIORITY_C1_STALL") == 0)) {
        return global_start_message_count >= 2 * NUMPORTS;
    } else {
        fprintf(stdout, "Unknown test type: %s\n", test_type);
        exit(-1);
    }
}

double get_avg_service_time() {
    // Compute avg_service_time
    double avg_service_time;
    if (strcmp(service_dist_type, "FIXED") == 0) {
        avg_service_time = (double)fixed_dist_cycles;
    } else if (strcmp(service_dist_type, "EXP") == 0) {
        avg_service_time = exp_dist_scale_factor * (1.0 / exp_dist_decay_const);
    } else if (strcmp(service_dist_type, "BIMODAL") == 0) {
        avg_service_time = bimodal_dist_high_mean * bimodal_dist_fraction_high + bimodal_dist_low_mean * (1.0 - bimodal_dist_fraction_high);
    } else {
        fprintf(stdout, "Unknown distribution type: %s\n", service_dist_type);
        exit(-1);
    }
    return avg_service_time;
}

void log_packet_response_time(parsed_packet_t* packet) {
    // TODO: We need to print a header as well to record what the parameters for this run were.
    uint64_t service_time = be64toh(packet->app->getAppHeader()->service_time);
    uint64_t sent_time = be64toh(packet->app->getAppHeader()->sent_time);
    uint16_t src_context = be16toh(packet->lnic->getLnicHeader()->src_context);
    uint64_t recv_time = packet->tsp->timestamp; // TODO: This accounts for tokens, even though sends don't. Is that a problem?
    uint64_t iter_time = this_iter_cycles_start;
    uint64_t delta_time = (recv_time > sent_time) ? (recv_time - sent_time) : 0;
    fprintf(stdout, "&&CSV&&ResponseTimes,%ld,%ld,%ld,%ld,%ld,%d,%f,%ld\n",
      service_time, delta_time, sent_time, recv_time, iter_time, src_context, get_avg_service_time(), request_rate_lambda_inverse);
}

void update_load() {
    // We've sent and received all required requests for the current load.
    // OR we've timed out and some requests were dropped.
    // Check if we are done or if we should move to the next load.
    if (request_rate_lambda_inverse <= request_rate_lambda_inverse_stop) {
        load_generator_complete = true;
        fprintf(stdout, "---- Load Generator Complete! ----\n");
        // TODO: Maybe we should send a "DONE" msg to the server and have it shutdown gracefully?
    } else {
        // Move to the next load
        // Update the load generation distribution
        request_rate_lambda_inverse -= request_rate_lambda_inverse_dec;
        double request_rate_lambda = 1.0 / (double)request_rate_lambda_inverse;
        std::exponential_distribution<double>::param_type new_lambda(request_rate_lambda);
        gen_dist->param(new_lambda);
        // Reset accounting state
        tx_request_count = 0;
        rx_request_count = 0;
        request_tx_done = false;
        fprintf(stdout, "---- New Avg Arrival Time: %ld ----\n", request_rate_lambda_inverse);
    }
}

void check_request_timeout() {
    // It's possible that the DUT dropped some requests so we will never receive all the responses.
    // In this case, we need to timeout and move onto the next load.
    // NOTE: if we timeout too early then we will receive too many responses in the next iteration ...
    uint64_t timeout_cycles = get_avg_service_time() * num_requests * 2;
    if (!load_generator_complete && request_tx_done && (this_iter_cycles_start >= request_tx_done_time + timeout_cycles)) {
        fprintf(stdout, "---- Timeout! Not all responses received! ----\n");
        update_load();
    }
}

bool should_generate_packet_this_cycle() {
    if (!start_message_received || (tx_request_count >= num_requests)) {
        return false;
    }
    if (this_iter_cycles_start >= next_threshold) {
        // compute when the next request should be sent
        if (strcmp(request_dist_type, "FIXED") == 0) {
            next_threshold = this_iter_cycles_start + request_rate_lambda_inverse;
        } else if (strcmp(request_dist_type, "EXP") == 0) {
            next_threshold = this_iter_cycles_start + (uint64_t)(*gen_dist)(*gen_rand);
        } else {
            fprintf(stdout, "Unknown distribution type: %s\n", service_dist_type);
            exit(-1);
        }
        return true;
    }
    return false;
}

uint64_t get_service_time(int &dist) {
    if (strcmp(service_dist_type, "FIXED") == 0) {
        dist = 0;
        return fixed_dist_cycles;
    } else if (strcmp(service_dist_type, "EXP") == 0) {
        double exp_value = exp_dist_scale_factor * (*service_exp_dist)(*dist_rand);
        dist = 0;
        return std::min(std::max((uint64_t)exp_value, min_service_time), max_service_time);
    } else if (strcmp(service_dist_type, "BIMODAL") == 0) {
        double service_low = (*service_normal_low)(*dist_rand);
        double service_high = (*service_normal_high)(*dist_rand);
        int select_high = (*service_select_dist)(*dist_rand);
        if (select_high) {
            dist = 1;
            return std::min(std::max((uint64_t)service_high, min_service_time), max_service_time);
        } else {
            dist = 0;
            return std::min(std::max((uint64_t)service_low, min_service_time), max_service_time);
        }
    } else {
        fprintf(stdout, "Unknown distribution type: %s\n", service_dist_type);
        exit(-1);
    }

}

uint64_t get_service_key(int context_id) {
  return (max_service_key * context_id) + (*service_key_uniform_dist)(*dist_rand);
}

uint16_t get_next_tx_msg_id() {
    uint16_t to_return = global_tx_msg_id;
    global_tx_msg_id++;
    if (global_tx_msg_id == MAX_TX_MSG_ID) {
        global_tx_msg_id = 0;
    }
    return to_return;
}

void send_load_packet(uint16_t dst_context, uint64_t service_time, uint64_t sent_time) {
    // Build the new ethernet/ip packet layers
    pcpp::EthLayer new_eth_layer(pcpp::MacAddress(LOAD_GEN_MAC), pcpp::MacAddress(NIC_MAC));
    pcpp::IPv4Layer new_ip_layer(pcpp::IPv4Address(std::string(LOAD_GEN_IP)), pcpp::IPv4Address(std::string(NIC_IP)));
    new_ip_layer.getIPv4Header()->ipId = htons(1);
    new_ip_layer.getIPv4Header()->timeToLive = 64;
    new_ip_layer.getIPv4Header()->protocol = 153; // Protocol code for LNIC

    uint16_t tx_msg_id = get_next_tx_msg_id();

    // Build the new lnic and application packet layers
    pcpp::LnicLayer new_lnic_layer(0, 0, 0, 0, 0, 0, 0, 0, 0);
    new_lnic_layer.getLnicHeader()->flags = (uint8_t)LNIC_DATA_FLAG_MASK;
    new_lnic_layer.getLnicHeader()->src_context = htons(0);
    new_lnic_layer.getLnicHeader()->dst_context = htons(dst_context);
    new_lnic_layer.getLnicHeader()->tx_msg_id = htons(tx_msg_id);
    pcpp::AppLayer new_app_layer(service_time, sent_time);

    uint16_t msg_len = new_app_layer.getHeaderLen();
    pcpp::PayloadLayer new_payload_layer(0, 0, false);

    if (strcmp(load_type, "MICA") == 0) {
      struct mica_hdr_t mica_hdr;
      uint16_t mica_hdr_size;
      mica_hdr.key[0] = htobe64(get_service_key(dst_context));
      mica_hdr.key[1] = htobe64(0x0);
      if (tx_msg_id % 2 == 0) {
        mica_hdr.op_type = htobe64(MICA_W_TYPE);
        mica_hdr.value[0] = htobe64(0x7);
        mica_hdr_size = sizeof(mica_hdr);
      }
      else {
        mica_hdr.op_type = htobe64(MICA_R_TYPE);
        mica_hdr_size = sizeof(mica_hdr) - sizeof(mica_hdr.value);
      }
      new_payload_layer = pcpp::PayloadLayer((uint8_t*)&mica_hdr, mica_hdr_size, false);
      msg_len += new_payload_layer.getHeaderLen();
    }
    else if (strcmp(load_type, "CHAINREP") == 0) {
      struct chainrep_w_hdr_t w_hdr;
#define CHAINREP_CLIENT_IP 0x0a000002
#define CHAINREP_NODE1_IP  0x0a000003
      uint32_t node_ips[] = {CHAINREP_NODE1_IP+0, CHAINREP_NODE1_IP+1, CHAINREP_NODE1_IP+2};
      uint8_t node_ctxs[] = {0, 0, 0};
      w_hdr.flags = CHAINREP_FLAGS_OP_WRITE;
      w_hdr.seq = 0;
      w_hdr.node_cnt = CHAINREP_CHAIN_SIZE - 1;
      w_hdr.client_ctx = 0;
      w_hdr.client_ip = htobe32(CHAINREP_CLIENT_IP);
      for (unsigned i = 1; i < CHAINREP_CHAIN_SIZE; i++)
        w_hdr.nodes[i-1] = htobe64(((uint64_t)node_ctxs[i] << 32) | node_ips[i]);
      w_hdr.key[0] = htobe64(get_service_key(dst_context));
      w_hdr.key[1] = htobe64(0x0);
      w_hdr.value[0] = htobe64(0x7);
      new_payload_layer = pcpp::PayloadLayer((uint8_t*)&w_hdr, sizeof(w_hdr), false);
      msg_len += new_payload_layer.getHeaderLen();
    }
    else if (strcmp(load_type, "CHAINREP_READ") == 0) {
      struct chainrep_r_hdr_t r_hdr;
      r_hdr.flags = CHAINREP_FLAGS_OP_READ;
      r_hdr.seq = 0;
      r_hdr.node_cnt = 0;
      r_hdr.client_ctx = 0;
      r_hdr.client_ip = htobe32(CHAINREP_CLIENT_IP);
      r_hdr.key[0] = htobe64(get_service_key(dst_context));
      r_hdr.key[1] = htobe64(0x0);
      new_payload_layer = pcpp::PayloadLayer((uint8_t*)&r_hdr, sizeof(r_hdr), false);
      msg_len += new_payload_layer.getHeaderLen();
    }

    new_lnic_layer.getLnicHeader()->msg_len = htons(msg_len);

    // Join the layers into a new packet
    uint64_t data_packet_size_bytes = ETHER_HEADER_SIZE + IP_HEADER_SIZE + LNIC_HEADER_SIZE + msg_len;
    pcpp::Packet new_packet(data_packet_size_bytes);
    new_packet.addLayer(&new_eth_layer);
    new_packet.addLayer(&new_ip_layer);
    new_packet.addLayer(&new_lnic_layer);
    new_packet.addLayer(&new_app_layer);
    if (strcmp(load_type, "MICA") == 0 ||
        strcmp(load_type, "CHAINREP") == 0 ||
        strcmp(load_type, "CHAINREP_READ") == 0
        )
      new_packet.addLayer(&new_payload_layer);

    new_packet.computeCalculateFields();

    // Convert the packet to a switchpacket
    switchpacket* new_tsp = (switchpacket*)calloc(sizeof(switchpacket), 1);
    new_tsp->timestamp = this_iter_cycles_start;
    new_tsp->amtwritten = data_packet_size_bytes / sizeof(uint64_t);
    new_tsp->amtread = 0;
    new_tsp->sender = 0;
    memcpy(new_tsp->dat, new_packet.getRawPacket()->getRawData(), data_packet_size_bytes);

    // Verify and log the switchpacket
    // TODO: For now we only work with port 0.
    parsed_packet_t sent_packet;
    if (!sent_packet.parse(new_tsp)) {
        fprintf(stdout, "Invalid generated packet.\n");
        free(new_tsp);
        return;
    }
#ifdef LOG_ALL_PACKETS
    print_packet("LOAD", &sent_packet);
#endif
    send_with_priority(0, new_tsp);
    tx_request_count++;
    fprintf(stdout, "&&CSV&&RequestStats,%ld,%ld,%d,%f,%ld\n",
      sent_time, service_time, dst_context, get_avg_service_time(), request_rate_lambda_inverse);
    // check if we are done sending requests
    if (tx_request_count >= num_requests) {
        request_tx_done = true;
        request_tx_done_time = sent_time;
    }
}

// Returns true if this packet is for the load generator, otherwise returns
// false, indicating that the switch should process the packet as usual
bool load_gen_hook(switchpacket* tsp) {
    // Parse and log the incoming packet
    parsed_packet_t packet;
    bool is_valid = packet.parse(tsp);
    if (!is_valid) {
        fprintf(stdout, "Invalid received packet.\n");
        return true;
    }
    // Ignore packets that aren't for the load generator:
    if (packet.ip->getDstIpAddress() != pcpp::IPv4Address(std::string(LOAD_GEN_IP))) {
      return false;
    }
#ifdef LOG_ALL_PACKETS
    print_packet("RECV", &packet);
#endif
    // Send ACK+PULL responses to DATA packets
    // TODO: This only works for one-packet messages for now
    if (packet.lnic->getLnicHeader()->flags & LNIC_DATA_FLAG_MASK) {
        // Calculate the ACK+PULL values
        pcpp::lnichdr* lnic_hdr = packet.lnic->getLnicHeader();
        uint16_t pull_offset = lnic_hdr->pkt_offset + rtt_pkts;
        uint8_t flags = LNIC_ACK_FLAG_MASK | LNIC_PULL_FLAG_MASK;
        uint64_t ack_packet_size_bytes = ETHER_HEADER_SIZE + IP_HEADER_SIZE + LNIC_HEADER_SIZE + APP_HEADER_SIZE;

        // Build the new packet layers
        pcpp::EthLayer new_eth_layer(packet.eth->getDestMac(), packet.eth->getSourceMac());
        pcpp::IPv4Layer new_ip_layer(packet.ip->getDstIpAddress(), packet.ip->getSrcIpAddress());
        new_ip_layer.getIPv4Header()->ipId = htons(1);
        new_ip_layer.getIPv4Header()->timeToLive = 64;
        new_ip_layer.getIPv4Header()->protocol = 153; // Protocol code for LNIC
        pcpp::LnicLayer new_lnic_layer(flags, ntohs(lnic_hdr->dst_context), ntohs(lnic_hdr->src_context),
                                       ntohs(lnic_hdr->msg_len), lnic_hdr->pkt_offset, pull_offset,
                                       ntohs(lnic_hdr->tx_msg_id), ntohs(lnic_hdr->buf_ptr), lnic_hdr->buf_size_class);
        pcpp::AppLayer new_app_layer(0, 0);

        // Join the layers into a new packet
        pcpp::Packet new_packet(ack_packet_size_bytes);
        new_packet.addLayer(&new_eth_layer);
        new_packet.addLayer(&new_ip_layer);
        new_packet.addLayer(&new_lnic_layer);
        new_packet.addLayer(&new_app_layer);
        new_packet.computeCalculateFields();

        // Convert the packet to a switchpacket
        switchpacket* new_tsp = (switchpacket*)calloc(sizeof(switchpacket), 1);
        new_tsp->timestamp = tsp->timestamp;
        new_tsp->amtwritten = ack_packet_size_bytes / sizeof(uint64_t);
        new_tsp->amtread = 0;
        new_tsp->sender = 0;
        memcpy(new_tsp->dat, new_packet.getRawPacket()->getRawData(), ack_packet_size_bytes);

        // Verify and log the switchpacket
        parsed_packet_t sent_packet;
        if (!sent_packet.parse(new_tsp)) {
            fprintf(stdout, "Invalid sent packet.\n");
            free(new_tsp);
            return true;
        }
#ifdef LOG_ALL_PACKETS
        print_packet("SEND", &sent_packet);
#endif
        send_with_priority(tsp->sender, new_tsp);

        // Check for nanoPU startup messages
        if (!start_message_received) {
            if(count_start_message()) {
                start_message_received = true;
                fprintf(stdout, "---- All Start Msgs Received! ---\n");
            }
        } else {
            log_packet_response_time(&packet);
            rx_request_count++;
            if (rx_request_count >= num_requests) {
                // All responses received -- move to the next load
                update_load();
            }
        }
    }
    return true;
}

// Figure out which load packets to generate.
// TODO: This should really have an enum instead of a strcmp.
void generate_load_packets() {
    if (!should_generate_packet_this_cycle()) {
        return;
    }
    int dist; // indicates which distribution is selected
    uint64_t service_time = get_service_time(dist);
    uint64_t sent_time = this_iter_cycles_start; // TODO: Check this

    if (strcmp(test_type, "ONE_CONTEXT_FOUR_CORES") == 0) {
        send_load_packet(0, service_time, sent_time);
    } else if (strcmp(test_type, "FOUR_CONTEXTS_FOUR_CORES") == 0) {
        send_load_packet(rand() % 4, service_time, sent_time);
    } else if (strcmp(test_type, "TWO_CONTEXTS_FOUR_SHARED_CORES") == 0) {
        // send request to context 0 if low distribution is selected
        // send request to context 1 if high distribution is selected
        send_load_packet(dist, service_time, sent_time);
    } else if ((strcmp(test_type, "DIF_PRIORITY_LNIC_DRIVEN") == 0) ||
               (strcmp(test_type, "DIF_PRIORITY_TIMER_DRIVEN") == 0) ||
               (strcmp(test_type, "HIGH_PRIORITY_C1_STALL") == 0) ||
               (strcmp(test_type, "LOW_PRIORITY_C1_STALL") == 0)) {
        send_load_packet(rand() % 2, service_time, sent_time);
    } else {
        fprintf(stdout, "Unknown test type: %s\n", test_type);
        exit(-1);
    }
}

// Load generator specific code end.
#endif

void send_with_priority(uint16_t port, switchpacket* tsp) {
    uint8_t lnic_header_flags = *((uint8_t*)tsp->dat + ETHER_HEADER_SIZE + IP_HEADER_SIZE);
    bool is_data = lnic_header_flags & LNIC_DATA_FLAG_MASK;
    bool is_ack = lnic_header_flags & LNIC_ACK_FLAG_MASK;
    bool is_nack = lnic_header_flags & LNIC_NACK_FLAG_MASK;
    bool is_pull = lnic_header_flags & LNIC_PULL_FLAG_MASK;
    bool is_chop = lnic_header_flags & LNIC_CHOP_FLAG_MASK;
    uint64_t packet_size_bytes = tsp->amtwritten * sizeof(uint64_t);
    
    uint64_t lnic_msg_len_bytes_offset = (uint64_t)tsp->dat + ETHER_HEADER_SIZE + IP_HEADER_SIZE + LNIC_HEADER_MSG_LEN_OFFSET;
    uint16_t lnic_msg_len_bytes = *(uint16_t*)lnic_msg_len_bytes_offset;
    lnic_msg_len_bytes = __builtin_bswap16(lnic_msg_len_bytes);

    uint64_t lnic_src_context_offset = (uint64_t)tsp->dat + ETHER_HEADER_SIZE + IP_HEADER_SIZE + 1;
    uint64_t lnic_dst_context_offset = (uint64_t)tsp->dat + ETHER_HEADER_SIZE + IP_HEADER_SIZE + 3;
    uint16_t lnic_src_context = __builtin_bswap16(*(uint16_t*)lnic_src_context_offset);
    uint16_t lnic_dst_context = __builtin_bswap16(*(uint16_t*)lnic_dst_context_offset);

    uint64_t packet_data_size = packet_size_bytes - ETHER_HEADER_SIZE - IP_HEADER_SIZE - LNIC_HEADER_SIZE;
    uint64_t packet_msg_words_offset = (uint64_t)tsp->dat + ETHER_HEADER_SIZE + IP_HEADER_SIZE + LNIC_HEADER_SIZE;
    uint64_t* packet_msg_words = (uint64_t*)packet_msg_words_offset;

#ifdef LOG_ALL_PACKETS
    struct timeval format_time;
    format_time.tv_sec = tsp->timestamp / 1000000000;
    format_time.tv_usec = (tsp->timestamp % 1000000000) / 1000;
    pcpp::RawPacket raw_packet((const uint8_t*)tsp->dat, 200*sizeof(uint64_t), format_time, false, pcpp::LINKTYPE_ETHERNET);
    pcpp::Packet parsed_packet(&raw_packet);
    pcpp::IPv4Layer* ip_layer = parsed_packet.getLayerOfType<pcpp::IPv4Layer>();
    std::string ip_src_addr = ip_layer->getSrcIpAddress().toString();
    std::string ip_dst_addr = ip_layer->getDstIpAddress().toString();
    std::string flags_str;
    flags_str += is_data ? "DATA" : "";
    flags_str += is_ack ? " ACK" : "";
    flags_str += is_nack ? " NACK" : "";
    flags_str += is_pull ? " PULL" : "";
    flags_str += is_chop ? " CHOP" : "";
    fprintf(stdout, "%ld: IP(src=%s, dst=%s), LNIC(flags=%s, msg_len=%d, src_context=%d, dst_context=%d), packet_len=%d, port=%d\n",
                     tsp->timestamp, ip_src_addr.c_str(), ip_dst_addr.c_str(), flags_str.c_str(), lnic_msg_len_bytes,
                     lnic_src_context, lnic_dst_context, packet_size_bytes, port);
#endif // LOG_ALL_PACKETS

    if (is_data && !is_chop) {
        // Regular data, send to low priority queue or chop and send to high priority
        // queue if low priority queue is full.
        if (packet_size_bytes + ports[port]->outputqueue_low_size < LOW_PRIORITY_OBUF_SIZE) {
            ports[port]->outputqueue_low.push(tsp);
            ports[port]->outputqueue_low_size += packet_size_bytes;
        } else {
#ifdef TRIM_PKTS
            // Try to chop the packet
            if (LNIC_PACKET_CHOPPED_SIZE + ports[port]->outputqueue_high_size < HIGH_PRIORITY_OBUF_SIZE) {
#ifdef LOG_EVENTS
                fprintf(stdout, "&&CSV&&Events,Chopped,%ld,%d\n", this_iter_cycles_start, port);
#endif // LOG_EVENTS
                switchpacket * tsp2 = (switchpacket*)calloc(sizeof(switchpacket), 1);
                tsp2->timestamp = tsp->timestamp;
                tsp2->amtwritten = LNIC_PACKET_CHOPPED_SIZE / sizeof(uint64_t);
                tsp2->amtread = tsp->amtread;
                tsp2->sender = tsp->sender;
                memcpy(tsp2->dat, tsp->dat, ETHER_HEADER_SIZE + IP_HEADER_SIZE + LNIC_HEADER_SIZE);
                uint64_t lnic_flag_offset = (uint64_t)tsp2->dat + ETHER_HEADER_SIZE + IP_HEADER_SIZE;
                *(uint8_t*)(lnic_flag_offset) |= LNIC_CHOP_FLAG_MASK;
                free(tsp);
                ports[port]->outputqueue_high.push(tsp2);
                ports[port]->outputqueue_high_size += LNIC_PACKET_CHOPPED_SIZE;

            } else {
                // TODO: We should really drop the lowest priority packet sometimes, not always the newly arrived packet
#ifdef LOG_EVENTS
                fprintf(stdout, "&&CSV&&Events,DroppedData,%ld,%d\n", this_iter_cycles_start, port);
#endif // LOG_EVENTS
                free(tsp);
            }
#else // TRIM_PKTS is not defined
#ifdef LOG_EVENTS
            fprintf(stdout, "&&CSV&&Events,DroppedData,%ld,%d\n", this_iter_cycles_start, port);
#endif // LOG_EVENTS
            free(tsp);
#endif // TRIM_PKTS
        }
    } else if ((is_data && is_chop) || (!is_data && !is_chop)) {
        // Chopped data or control, send to high priority output queue
        if (packet_size_bytes + ports[port]->outputqueue_high_size < HIGH_PRIORITY_OBUF_SIZE) {
            ports[port]->outputqueue_high.push(tsp);
            ports[port]->outputqueue_high_size += packet_size_bytes;
        } else {
#ifdef LOG_EVENTS
            fprintf(stdout, "&&CSV&&Events,DroppedControl,%ld,%d\n", this_iter_cycles_start, port);
#endif
            free(tsp);
        }
    } else {
        fprintf(stdout, "Invalid combination: Chopped control packet. Dropping.\n");
        free(tsp);
        // Chopped control packet. This shouldn't be possible.
    }
}

static void simplify_frac(int n, int d, int *nn, int *dd)
{
    int a = n, b = d;

    // compute GCD
    while (b > 0) {
        int t = b;
        b = a % b;
        a = t;
    }

    *nn = n / a;
    *dd = d / a;
}

int main (int argc, char *argv[]) {
    int bandwidth;

    if (argc < 6) {
        // if insufficient args, error out
        fprintf(stdout, "usage: ./switch LINKLATENCY SWITCHLATENCY BANDWIDTH HIGH_PRIORITY_OBUF_SIZE LOW_PRIORITY_OBUF_SIZE\n");
        fprintf(stdout, "insufficient args provided\n.");
        fprintf(stdout, "LINKLATENCY and SWITCHLATENCY should be provided in cycles.\n");
        fprintf(stdout, "BANDWIDTH should be provided in Gbps\n");
        fprintf(stdout, "OBUF SIZES should be provided in bytes.\n");
        exit(1);
    }

    LINKLATENCY = atoi(argv[1]);
    switchlat = atoi(argv[2]);
    bandwidth = atoi(argv[3]);
    HIGH_PRIORITY_OBUF_SIZE = atoi(argv[4]);
    LOW_PRIORITY_OBUF_SIZE = atoi(argv[5]);

#ifdef USE_LOAD_GEN
    request_rate_lambda_inverse = request_rate_lambda_inverse_start;
    double request_rate_lambda = 1.0 / (double)request_rate_lambda_inverse;
    gen_rand = new std::default_random_engine;
    gen_dist = new std::exponential_distribution<double>(request_rate_lambda);
    dist_rand = new std::default_random_engine;
    service_key_uniform_dist = new std::uniform_int_distribution<>(min_service_key, max_service_key);
    service_exp_dist = new std::exponential_distribution<double>(exp_dist_decay_const);
    service_normal_high = new std::normal_distribution<double>(bimodal_dist_high_mean, bimodal_dist_high_stdev);
    service_normal_low = new std::normal_distribution<double>(bimodal_dist_low_mean, bimodal_dist_low_stdev);
    service_select_dist = new std::binomial_distribution<int>(1, bimodal_dist_fraction_high);
    fprintf(stdout, "---- New Avg Arrival Time: %ld ----\n", request_rate_lambda_inverse);
#endif

    simplify_frac(bandwidth, 200, &throttle_numer, &throttle_denom);

    fprintf(stdout, "Using link latency: %d\n", LINKLATENCY);
    fprintf(stdout, "Using switching latency: %d\n", SWITCHLATENCY);
    fprintf(stdout, "BW throttle set to %d/%d\n", throttle_numer, throttle_denom);
    fprintf(stdout, "High priority obuf size: %d\n", HIGH_PRIORITY_OBUF_SIZE);
    fprintf(stdout, "Low priority obuf size: %d\n", LOW_PRIORITY_OBUF_SIZE);

    if ((LINKLATENCY % 7) != 0) {
        // if invalid link latency, error out.
        fprintf(stdout, "INVALID LINKLATENCY. Currently must be multiple of 7 cycles.\n");
        exit(1);
    }

    omp_set_num_threads(NUMPORTS); // we parallelize over ports, so max threads = # ports

#ifdef LOG_QUEUE_SIZE
    // initialize last_qsize_samples
    for (int p = 0; p < NUMPORTS; p++) {
        last_qsize_samples[p][0] = 0; // high-priority
        last_qsize_samples[p][1] = 0; // low-priority
        fprintf(stdout, "&&CSV&&QueueSize,%ld,%d,%d,%d\n", this_iter_cycles_start, p, 0, 0);
    }
#endif

#define PORTSETUPCONFIG
#include "switchconfig.h"
#undef PORTSETUPCONFIG

    while (true) {

        // handle sends
#pragma omp parallel for
        for (int port = 0; port < NUMPORTS; port++) {
            ports[port]->send();
        }

        // handle receives. these are blocking per port
#pragma omp parallel for
        for (int port = 0; port < NUMPORTS; port++) {
            ports[port]->recv();
        }
 
#pragma omp parallel for
        for (int port = 0; port < NUMPORTS; port++) {
            ports[port]->tick_pre();
        }

        do_fast_switching();

        this_iter_cycles_start += LINKLATENCY; // keep track of time

        // some ports need to handle extra stuff after each iteration
        // e.g. shmem ports swapping shared buffers
#pragma omp parallel for
        for (int port = 0; port < NUMPORTS; port++) {
            ports[port]->tick();
        }

    }
}
