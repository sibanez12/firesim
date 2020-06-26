//See LICENSE for license details

#ifndef __LNIC_H
#define __LNIC_H

#include "bridges/bridge_driver.h"
#include <vector>

// TODO this should not be hardcoded here.
#define MAX_BANDWIDTH 200

#ifdef LNICBRIDGEMODULE_struct_guard
class lnic_t: public bridge_driver_t
{
    public:
        lnic_t(simif_t* sim, std::vector<std::string> &args,
            LNICBRIDGEMODULE_struct *addrs, int lnicno,
            long dma_addr);
        ~lnic_t();

        virtual void init();
        virtual void tick();
        virtual bool terminate() { return false; };
        virtual int exit_code() { return 0; }
        virtual void finish() {};

    private:
        simif_t* sim;

        uint64_t nic_mac_lendian;
        uint64_t switch_mac_lendian;
        uint32_t nic_ip_lendian;
        
        char * pcis_read_bufs[2];
        char * pcis_write_bufs[2];
        int rlimit_inc, rlimit_period, rlimit_size;
	int pause_threshold, pause_quanta, pause_refresh;

        // link latency in cycles
        // assuming 3.2 GHz, this number / 3.2 = link latency in ns
        // e.g. setting this to 6405 gives you 6405/3.2 = 2001.5625 ns latency
        // IMPORTANT: this must be a multiple of 7
        int LINKLATENCY;
        FILE * niclog;
        LNICBRIDGEMODULE_struct *mmio_addrs;
        bool loopback;

        // checking for token loss
        uint32_t next_token_from_fpga = 0x0;
        uint32_t next_token_from_socket = 0x0;

        uint64_t iter = 0;

        int currentround = 0;

        // only for TOKENVERIFY
        uint64_t timeelapsed_cycles = 0;

        long dma_addr;
};
#endif // LNICBRIDGEMODULE_struct_guard

#endif // __LNIC_H
