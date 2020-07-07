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
#define LNIC_PACKET_CHOPPED_SIZE   128 // Bytes, the minimum L-NIC packet size

#define HIGH_PRIORITY_OBUF_SIZE    (300L)
#define LOW_PRIORITY_OBUF_SIZE     (300L)

// TODO: replace these port mapping hacks with a mac -> port mapping,
// could be hardcoded

BasePort * ports[NUMPORTS];
void send_with_priority(uint16_t port, switchpacket* tsp);

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
    if (ethernet_layer == NULL || ip_layer == NULL) {
        if (ethernet_layer == NULL) {
            fprintf(stdout, "NULL ethernet layer\n");
        }
        if (ip_layer == NULL) {
            fprintf(stdout, "NULL ip layer\n");
        }
    } else {
        //fprintf(stdout, "Source MAC %s, dest MAC %s\n", ethernet_layer->getSourceMac().toString().c_str(), ethernet_layer->getDestMac().toString().c_str());
        fprintf(stdout, "Source IP %s, dest IP %s\n", ip_layer->getSrcIpAddress().toString().c_str(), ip_layer->getDstIpAddress().toString().c_str());
        //fprintf(stdout, "Header length is %d\n", ip_layer->getHeaderLen());
    }

    int flit_offset_doublebytes = (ETHER_HEADER_SIZE + IP_DST_FIELD_OFFSET + IP_SUBNET_OFFSET) / sizeof(uint16_t);
    uint16_t switching_flit = ((uint16_t*)tsp->dat)[flit_offset_doublebytes];

    uint16_t send_to_port = get_port_from_flit(switching_flit, 0);
    if (send_to_port == UNKNOWN_ADDRESS) {
        fprintf(stdout, "Packet with unknown destination address, dropping\n");
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
// finally in parallel, flush whatever we can to the output queues based on timestamp

#pragma omp parallel for
for (int port = 0; port < NUMPORTS; port++) {
    BasePort * thisport = ports[port];
    thisport->write_flits_to_output();
}

}

void send_with_priority(uint16_t port, switchpacket* tsp) {
    uint8_t lnic_header_flags = *((uint8_t*)tsp->dat + ETHER_HEADER_SIZE + IP_HEADER_SIZE);
    bool is_data = lnic_header_flags & LNIC_DATA_FLAG_MASK;
    bool is_ack = lnic_header_flags & LNIC_ACK_FLAG_MASK;
    bool is_nack = lnic_header_flags & LNIC_NACK_FLAG_MASK;
    bool is_pull = lnic_header_flags & LNIC_PULL_FLAG_MASK;
    bool is_chop = lnic_header_flags & LNIC_CHOP_FLAG_MASK;
    uint64_t packet_size_bytes = tsp->amtwritten * sizeof(uint64_t);
    
    //fprintf(stdout, "Packet info: header_flags %#lx is_data %d is_chop %d packet_size_bytes %d\n", lnic_header_flags, is_data, is_chop, packet_size_bytes);

    uint64_t lnic_msg_len_bytes_offset = (uint64_t)tsp->dat + ETHER_HEADER_SIZE + IP_HEADER_SIZE + LNIC_HEADER_MSG_LEN_OFFSET;
    uint16_t lnic_msg_len_bytes = *(uint16_t*)lnic_msg_len_bytes_offset;
    lnic_msg_len_bytes = __builtin_bswap16(lnic_msg_len_bytes);
    //fprintf(stdout, "Lnic msg len bytes is %d\n", lnic_msg_len_bytes);
    std::string to_send = "Flags: ";
    to_send += (is_data ? "DATA " : "");
    to_send += (is_ack ? "ACK " : "");
    to_send += (is_nack ? "NACK " : "");
    to_send += (is_pull ? "PULL " : "");
    to_send += (is_chop ? "CHOP " : "");
    fprintf(stdout, "%s Port: %d\n", to_send.c_str(), port);
    fprintf(stdout, "High obuf at %d low obuf at %d\n", ports[port]->outputqueue_high_size, ports[port]->outputqueue_low_size);

    if (is_data && !is_chop) {
        // Regular data, send to low priority queue or chop and send to high priority
        // queue if low priority queue is full.
        if (packet_size_bytes + ports[port]->outputqueue_low_size < LOW_PRIORITY_OBUF_SIZE) {
            fprintf(stdout, "Sending as regular data\n");
            ports[port]->outputqueue_low.push(tsp);
            ports[port]->outputqueue_low_size += packet_size_bytes;
        } else {
            // Try to chop the packet
            if (LNIC_PACKET_CHOPPED_SIZE + ports[port]->outputqueue_high_size < HIGH_PRIORITY_OBUF_SIZE) {
                fprintf(stdout, "Sending as chopped\n");
                tsp->amtwritten = LNIC_PACKET_CHOPPED_SIZE / sizeof(uint64_t);
                *((uint8_t*)tsp->dat + ETHER_HEADER_SIZE + IP_HEADER_SIZE) |= LNIC_CHOP_FLAG_MASK;
                ports[port]->outputqueue_high.push(tsp);
                ports[port]->outputqueue_high_size += LNIC_PACKET_CHOPPED_SIZE;

            } else {
                // TODO: We should really drop the lowest priority packet sometimes, not always the newly arrived packet
                fprintf(stdout, "Both queues full for chopped packet on port %d, dropping\n", port);
            }
        }
    } else if ((is_data && is_chop) || (!is_data && !is_chop)) {
        // Chopped data or control, send to high priority output queue
        if (packet_size_bytes + ports[port]->outputqueue_high_size < HIGH_PRIORITY_OBUF_SIZE) {
            fprintf(stdout, "Sending as control\n");
            ports[port]->outputqueue_high.push(tsp);
            ports[port]->outputqueue_high_size += packet_size_bytes;
        } else {
            fprintf(stdout, "High priority queue for port %d full, dropping packet\n", port);
        }
    } else {
        fprintf(stdout, "Invalid combination: Chopped control packet. Dropping.\n");
        // Chopped control packet. This shouldn't be possible.
    }
    fprintf(stdout, "\n");
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

    if (argc < 4) {
        // if insufficient args, error out
        fprintf(stdout, "usage: ./switch LINKLATENCY SWITCHLATENCY BANDWIDTH\n");
        fprintf(stdout, "insufficient args provided\n.");
        fprintf(stdout, "LINKLATENCY and SWITCHLATENCY should be provided in cycles.\n");
        fprintf(stdout, "BANDWIDTH should be provided in Gbps\n");
        exit(1);
    }

    LINKLATENCY = atoi(argv[1]);
    switchlat = atoi(argv[2]);
    bandwidth = atoi(argv[3]);

    simplify_frac(bandwidth, 200, &throttle_numer, &throttle_denom);

    fprintf(stdout, "Using link latency: %d\n", LINKLATENCY);
    fprintf(stdout, "Using switching latency: %d\n", SWITCHLATENCY);
    fprintf(stdout, "BW throttle set to %d/%d\n", throttle_numer, throttle_denom);

    if ((LINKLATENCY % 7) != 0) {
        // if invalid link latency, error out.
        fprintf(stdout, "INVALID LINKLATENCY. Currently must be multiple of 7 cycles.\n");
        exit(1);
    }

    omp_set_num_threads(NUMPORTS); // we parallelize over ports, so max threads = # ports

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
