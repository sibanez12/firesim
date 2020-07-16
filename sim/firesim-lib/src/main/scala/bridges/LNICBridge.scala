//See LICENSE for license details
package firesim
package bridges

import chisel3._
import chisel3.util._
import chisel3.experimental.{DataMirror, Direction}
import freechips.rocketchip.config.{Parameters, Field}
import freechips.rocketchip.diplomacy.AddressSet
import freechips.rocketchip.util._

import midas.widgets._
import testchipip.{StreamIO, StreamChannel}
import junctions.{NastiIO, NastiKey}
import lnic.{NICIO, NICIOvonly}

object LNICTokenQueueConsts {
  val TOKENS_PER_BIG_TOKEN = 7
  val BIG_TOKEN_WIDTH = (TOKENS_PER_BIG_TOKEN + 1) * 64
  val TOKEN_QUEUE_DEPTH = 6144
}
import LNICTokenQueueConsts._

case object LoopbackLNIC extends Field[Boolean](false)

class LNICBridge(implicit p: Parameters) extends BlackBox with Bridge[HostPortIO[NICIOvonly], LNICBridgeModule] {
  val io = IO(Flipped(new NICIOvonly))
  val bridgeIO = HostPort(io)
  val constructorArg = None
  generateAnnotations()
}

object LNICBridge {
  def apply(nicIO: NICIOvonly)(implicit p: Parameters): LNICBridge = {
    val ep = Module(new LNICBridge)
    ep.io.out <> nicIO.out
    nicIO.in <> ep.io.in
    nicIO.nic_mac_addr := ep.io.nic_mac_addr
    nicIO.switch_mac_addr := ep.io.switch_mac_addr
    nicIO.nic_ip_addr := ep.io.nic_ip_addr
    nicIO.timeout_cycles := ep.io.timeout_cycles
    nicIO.rtt_pkts := ep.io.rtt_pkts

    ep
  }
}

/* on a NIC token transaction:
 * 1) simulation driver feeds an empty token to start:
 *  data_in is garbage or real value (if exists)
 *  data_in_valid is 0 or 1 respectively
 *  data_out_ready is true (say host can always accept)
 *
 * 2) target responds:
 *  data_out garbage or real value (if exists)
 *  data_out_valid 0 or 1 respectively
 *  data_in_ready would be 1, so driver knows how to construct the next token if there was data to send
 *
 *  repeat
 */

class LNICReadyValidLast extends Bundle {
  val data_last = Bool()
  val ready = Bool()
  val valid = Bool()
}

class LNICBIGToken extends Bundle {
  val data = Vec(7, UInt(64.W))
  val rvls = Vec(7, new LNICReadyValidLast())
  val pad = UInt(43.W)
}

class HostToLNICToken extends Bundle {
  val data_in = new StreamChannel(64)
  val data_in_valid = Bool()
  val data_out_ready = Bool()
}

class LNICToHostToken extends Bundle {
  val data_out = new StreamChannel(64)
  val data_out_valid = Bool()
  val data_in_ready = Bool()
}

class BigTokenToLNICTokenAdapter extends Module {
  val io = IO(new Bundle {
    val htnt = DecoupledIO(new HostToLNICToken)
    val pcie_in = Flipped(DecoupledIO(UInt(512.W)))
  })

  val pcieBundled = io.pcie_in.bits.asTypeOf(new LNICBIGToken)

  val xactHelper = DecoupledHelper(io.htnt.ready, io.pcie_in.valid)

  val loopIter = RegInit(0.U(32.W))
  when (io.htnt.fire()) {
    loopIter := Mux(loopIter === 6.U, 0.U, loopIter + 1.U)
  }

  io.htnt.bits.data_in.data := pcieBundled.data(loopIter)
  io.htnt.bits.data_in.keep := 0xFF.U
  io.htnt.bits.data_in.last := pcieBundled.rvls(loopIter).data_last
  io.htnt.bits.data_in_valid := pcieBundled.rvls(loopIter).valid
  io.htnt.bits.data_out_ready := pcieBundled.rvls(loopIter).ready
  io.htnt.valid := xactHelper.fire(io.htnt.ready)
  io.pcie_in.ready := xactHelper.fire(io.pcie_in.valid, loopIter === 6.U)
}

class LNICTokenToBigTokenAdapter extends Module {
  val io = IO(new Bundle {
    val ntht = Flipped(DecoupledIO(new LNICToHostToken))
    val pcie_out = DecoupledIO(UInt(512.W))
  })

  // step one, buffer 7 elems into registers. note that the 7th element is here 
  // just for convenience. in reality, it is not used since we're bypassing to
  // remove a cycle of latency
  val NTHT_BUF = Reg(Vec(7, new LNICToHostToken))
  val specialCounter = RegInit(0.U(32.W))

  when (io.ntht.valid) {
    NTHT_BUF(specialCounter) := io.ntht.bits
  }

  io.ntht.ready := (specialCounter === 6.U && io.pcie_out.ready) || (specialCounter =/= 6.U)
  io.pcie_out.valid := specialCounter === 6.U && io.ntht.valid
  when ((specialCounter =/= 6.U) && io.ntht.valid) {
    specialCounter := specialCounter + 1.U
  } .elsewhen ((specialCounter === 6.U) && io.ntht.valid && io.pcie_out.ready) {
    specialCounter := 0.U
  } .otherwise {
    specialCounter := specialCounter
  }
  // step two, connect 6 elems + latest one to output (7 items)
  // TODO: attach pcie_out to data

  // debug check to help check we're not losing tokens somewhere
  val token_trace_counter = RegInit(0.U(43.W))
  when (io.pcie_out.fire()) {
    token_trace_counter := token_trace_counter + 1.U
  } .otherwise {
    token_trace_counter := token_trace_counter
  }

  val out = Wire(new LNICBIGToken)
  for (i <- 0 until 6) {
    out.data(i) := NTHT_BUF(i).data_out.data
    out.rvls(i).data_last := NTHT_BUF(i).data_out.last
    out.rvls(i).ready := NTHT_BUF(i).data_in_ready
    out.rvls(i).valid := NTHT_BUF(i).data_out_valid
  }
  out.data(6) := io.ntht.bits.data_out.data
  out.rvls(6).data_last := io.ntht.bits.data_out.last
  out.rvls(6).ready := io.ntht.bits.data_in_ready
  out.rvls(6).valid := io.ntht.bits.data_out_valid
  out.pad := token_trace_counter

  io.pcie_out.bits := out.asUInt
}

class HostToLNICTokenGenerator(nTokens: Int)(implicit p: Parameters) extends Module {
  val io = IO(new Bundle {
    val out = Decoupled(new HostToLNICToken)
    val in = Flipped(Decoupled(new LNICToHostToken))
  })

  val s_init :: s_seed :: s_forward :: Nil = Enum(3)
  val state = RegInit(s_init)

  val (_, seedDone) = Counter(state === s_seed && io.out.fire(), nTokens)

  io.out.valid := state === s_seed || (state === s_forward && io.in.valid)
  io.out.bits.data_in_valid := state === s_forward && io.in.bits.data_out_valid
  io.out.bits.data_in := io.in.bits.data_out
  io.out.bits.data_out_ready := state === s_seed || io.in.bits.data_in_ready
  io.in.ready := state === s_forward && io.out.ready

  when (state === s_init) { state := s_seed }
  when (seedDone) { state := s_forward }
}

class LNICBridgeModule(implicit p: Parameters) extends BridgeModule[HostPortIO[NICIOvonly]]()(p)
    with BidirectionalDMA {
  val io = IO(new WidgetIO)
  val hPort = IO(HostPort(Flipped(new NICIOvonly)))
  // DMA mixin parameters
  lazy val fromHostCPUQueueDepth = TOKEN_QUEUE_DEPTH
  lazy val toHostCPUQueueDepth   = TOKEN_QUEUE_DEPTH
  // Biancolin: Need to look into this
  lazy val dmaSize = BigInt((BIG_TOKEN_WIDTH / 8) * TOKEN_QUEUE_DEPTH)

  val htnt_queue = Module(new Queue(new HostToLNICToken, 10))
  val ntht_queue = Module(new Queue(new LNICToHostToken, 10))

  val bigtokenToNIC = Module(new BigTokenToLNICTokenAdapter)
  val NICtokenToBig = Module(new LNICTokenToBigTokenAdapter)

  val target = hPort.hBits
  val tFireHelper = DecoupledHelper(hPort.toHost.hValid,
                                    hPort.fromHost.hReady)
  val tFire = tFireHelper.fire

  if (p(LoopbackLNIC)) {
    val tokenGen = Module(new HostToLNICTokenGenerator(10))
    htnt_queue.io.enq <> tokenGen.io.out
    tokenGen.io.in <> ntht_queue.io.deq
    NICtokenToBig.io.ntht.valid := false.B
    NICtokenToBig.io.ntht.bits := DontCare
    bigtokenToNIC.io.htnt.ready := false.B
  } else {
    NICtokenToBig.io.ntht <> ntht_queue.io.deq
    htnt_queue.io.enq <> bigtokenToNIC.io.htnt
  }

  hPort.toHost.hReady := ntht_queue.io.enq.ready
  ntht_queue.io.enq.valid := hPort.toHost.hValid
  ntht_queue.io.enq.bits.data_out := target.out.bits
  ntht_queue.io.enq.bits.data_out_valid := target.out.valid
  ntht_queue.io.enq.bits.data_in_ready := true.B //target.in.ready

  hPort.fromHost.hValid := htnt_queue.io.deq.valid
  htnt_queue.io.deq.ready := hPort.fromHost.hReady
  target.in.bits := htnt_queue.io.deq.bits.data_in
  target.in.valid := htnt_queue.io.deq.bits.data_in_valid
  //target.out.ready := htnt_queue.io.deq.bits.data_out_ready

  bigtokenToNIC.io.pcie_in <> incomingPCISdat.io.deq
  outgoingPCISdat.io.enq <> NICtokenToBig.io.pcie_out


  if (p(LoopbackLNIC)) {
    target.nic_mac_addr := 0.U
    target.switch_mac_addr := 0.U
    target.nic_ip_addr := 0.U
    target.timeout_cycles := 0.U
    target.rtt_pkts := 0.U
  } else {
    val nic_mac_addr_reg_upper = Reg(UInt(32.W))
    val nic_mac_addr_reg_lower = Reg(UInt(32.W))
    val switch_mac_addr_reg_upper = Reg(UInt(32.W))
    val switch_mac_addr_reg_lower = Reg(UInt(32.W))
    val nic_ip_addr_reg = Reg(UInt(32.W))
    val timeout_cycles_reg_upper = Reg(UInt(32.W))
    val timeout_cycles_reg_lower = Reg(UInt(32.W))
    val rtt_pkts_reg = Reg(UInt(32.W))

    target.nic_mac_addr := Cat(nic_mac_addr_reg_upper, nic_mac_addr_reg_lower)
    target.switch_mac_addr := Cat(switch_mac_addr_reg_upper, switch_mac_addr_reg_lower)
    target.nic_ip_addr := nic_ip_addr_reg
    target.timeout_cycles := Cat(timeout_cycles_reg_upper, timeout_cycles_reg_lower)
    target.rtt_pkts := rtt_pkts_reg

    attach(nic_mac_addr_reg_upper, "nic_mac_addr_upper", WriteOnly)
    attach(nic_mac_addr_reg_lower, "nic_mac_addr_lower", WriteOnly)
    attach(switch_mac_addr_reg_upper, "switch_mac_addr_upper", WriteOnly)
    attach(switch_mac_addr_reg_lower, "switch_mac_addr_lower", WriteOnly)
    attach(nic_ip_addr_reg, "nic_ip_addr", WriteOnly)
    attach(timeout_cycles_reg_upper, "timeout_cycles_upper", WriteOnly)
    attach(timeout_cycles_reg_lower, "timeout_cycles_lower", WriteOnly)
    attach(rtt_pkts_reg, "rtt_pkts", WriteOnly)
  }

  genROReg(!tFire, "done")

  genCRFile()
}
