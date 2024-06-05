#pragma once
#include <queue>
#include <tuple>
#include <vector>

#include "common/fallback.h"
#include "dregress.h"
#include "fragmentation.h"
#include "kernel_interface_handler.h"
#include "prepare.h"
#include "worker.h"

namespace dataplane
{

class SlowWorker
{
public:
	struct Config
	{
		uint32_t SWICMPOutRateLimit;
		bool use_kernel_interface;
	};

private:
	pthread_barrier_t* m_run_barrier;
	bool m_stop = false;
	std::vector<tPortId> m_ports_serviced;
	std::vector<cWorker*> m_workers_serviced;
	std::vector<worker_gc_t*> m_gcs_to_service;
	cWorker* m_slow_worker;
	std::queue<
	        std::tuple<
	                rte_mbuf*,
	                common::globalBase::tFlow>>
	        m_slow_worker_mbufs;
	rte_mempool* m_mempool; // from cControlPlane
	common::slowworker::stats_t m_stats;
	fragmentation_t m_fragmentation;
	dregress_t m_dregress;
	uint32_t m_icmp_out_remainder;
	Config m_config;
	dataplane::KernelInterfaceWorker m_kni_worker;

public:
	SlowWorker(cWorker* worker,
	           const std::vector<tPortId>& ports_to_service,
			   KernelInterfaceWorker&& kni,
	           rte_mempool* mempool,
	           pthread_barrier_t* run);

	cWorker* GetWorker() { return m_slow_worker; }
	const dataplane::KernelInterfaceWorker& KniWorker() const { return m_kni_worker;}
	const dataplane::base::generation& CurrentBase() { return m_slow_worker->CurrentBase(); }
	const fragmentation_t& Fragmentation() const { return m_fragmentation; }
	dregress_t& Dregress() { return m_dregress; }
	const dregress_t& Dregress() const { return m_dregress; }
	void freeWorkerPacket(rte_ring* ring_to_free_mbuf, rte_mbuf* mbuf);
	rte_mbuf* convertMempool(rte_ring* ring_to_free_mbuf, rte_mbuf* old_mbuf);
	rte_mempool* Mempool() {return m_mempool;}
	void PreparePacket(rte_mbuf* mbuf) { m_slow_worker->preparePacket(mbuf); }
	void sendPacketToSlowWorker(rte_mbuf* mbuf, const common::globalBase::tFlow& flow);
	const common::slowworker::stats_t& Stats() const { return m_stats; }
	unsigned ring_handle(rte_ring* ring_to_free_mbuf, rte_ring* ring);

	void handlePacket_icmp_translate_v6_to_v4(rte_mbuf* mbuf);
	void handlePacket_icmp_translate_v4_to_v6(rte_mbuf* mbuf);
	void handlePacket_fragment(rte_mbuf* mbuf);

	void handlePacket_dregress(rte_mbuf* mbuf)
	{
		m_dregress.insert(mbuf);
	}

	void handlePacket_farm(rte_mbuf* mbuf)
	{
		m_stats.farm_packets++;
		m_slow_worker->slowWorkerFarmHandleFragment(mbuf);
	}

	void handlePacket_repeat(rte_mbuf* mbuf)
	{
		const rte_ether_hdr* ethernetHeader = rte_pktmbuf_mtod(mbuf, rte_ether_hdr*);
		dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

		if (ethernetHeader->ether_type == rte_cpu_to_be_16(RTE_ETHER_TYPE_VLAN))
		{
			const rte_vlan_hdr* vlanHeader = rte_pktmbuf_mtod_offset(mbuf, rte_vlan_hdr*, sizeof(rte_ether_hdr));

			metadata->flow.data.logicalPortId = CALCULATE_LOGICALPORT_ID(metadata->fromPortId, rte_be_to_cpu_16(vlanHeader->vlan_tci));
		}
		else
		{
			metadata->flow.data.logicalPortId = CALCULATE_LOGICALPORT_ID(metadata->fromPortId, 0);
		}

		/// @todo: opt
		m_slow_worker->preparePacket(mbuf);

		const auto& base = m_slow_worker->CurrentBase();
		const auto& logicalPort = base.globalBase->logicalPorts[metadata->flow.data.logicalPortId];

		m_stats.repeat_packets++;
		sendPacketToSlowWorker(mbuf, logicalPort.flow);
	}

	void handlePacket_fw_state_sync(rte_mbuf* mbuf)
	{
		dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

		const auto& base = m_slow_worker->CurrentBase();
		const auto& fw_state_config = base.globalBase->fw_state_sync_configs[metadata->flow.data.aclId];

		metadata->network_headerType = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV6);
		metadata->network_headerOffset = sizeof(rte_ether_hdr) + sizeof(rte_vlan_hdr);
		metadata->transport_headerType = IPPROTO_UDP;
		metadata->transport_headerOffset = metadata->network_headerOffset + sizeof(rte_ipv6_hdr);

		generic_rte_ether_hdr* ethernetHeader = rte_pktmbuf_mtod(mbuf, generic_rte_ether_hdr*);
		ethernetHeader->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_VLAN);
		rte_ether_addr_copy(&fw_state_config.ether_address_destination, &ethernetHeader->dst_addr);

		rte_vlan_hdr* vlanHeader = rte_pktmbuf_mtod_offset(mbuf, rte_vlan_hdr*, sizeof(rte_ether_hdr));
		vlanHeader->eth_proto = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV6);

		rte_ipv6_hdr* ipv6Header = rte_pktmbuf_mtod_offset(mbuf, rte_ipv6_hdr*, metadata->network_headerOffset);
		ipv6Header->vtc_flow = rte_cpu_to_be_32(0x6 << 28);
		ipv6Header->payload_len = rte_cpu_to_be_16(sizeof(rte_udp_hdr) + sizeof(dataplane::globalBase::fw_state_sync_frame_t));
		ipv6Header->proto = IPPROTO_UDP;
		ipv6Header->hop_limits = 64;
		memcpy(ipv6Header->src_addr, fw_state_config.ipv6_address_source.bytes, 16);
		memcpy(ipv6Header->dst_addr, fw_state_config.ipv6_address_multicast.bytes, 16);

		rte_udp_hdr* udpHeader = rte_pktmbuf_mtod_offset(mbuf, rte_udp_hdr*, metadata->network_headerOffset + sizeof(rte_ipv6_hdr));
		udpHeader->src_port = fw_state_config.port_multicast; // IPFW reuses the same port for both src and dst.
		udpHeader->dst_port = fw_state_config.port_multicast;
		udpHeader->dgram_len = rte_cpu_to_be_16(sizeof(rte_udp_hdr) + sizeof(dataplane::globalBase::fw_state_sync_frame_t));
		udpHeader->dgram_cksum = 0;
		udpHeader->dgram_cksum = rte_ipv6_udptcp_cksum(ipv6Header, udpHeader);

		// Iterate for all interested ports.
		for (unsigned int port_id = 0; port_id < fw_state_config.flows_size; port_id++)
		{
			rte_mbuf* mbuf_clone = rte_pktmbuf_alloc(m_mempool);
			if (mbuf_clone == nullptr)
			{
				m_slow_worker->Stats().fwsync_multicast_egress_drops++;
				continue;
			}

			*YADECAP_METADATA(mbuf_clone) = *YADECAP_METADATA(mbuf);

			memcpy(rte_pktmbuf_mtod(mbuf_clone, char*),
			       rte_pktmbuf_mtod(mbuf, char*),
			       mbuf->data_len);
			mbuf_clone->data_len = mbuf->data_len;
			mbuf_clone->pkt_len = mbuf->pkt_len;

			const auto& flow = fw_state_config.flows[port_id];
			m_slow_worker->Stats().fwsync_multicast_egress_packets++;
			sendPacketToSlowWorker(mbuf_clone, flow);
		}

		if (!fw_state_config.ipv6_address_unicast.empty())
		{
			memcpy(ipv6Header->src_addr, fw_state_config.ipv6_address_unicast_source.bytes, 16);
			memcpy(ipv6Header->dst_addr, fw_state_config.ipv6_address_unicast.bytes, 16);
			udpHeader->src_port = fw_state_config.port_unicast;
			udpHeader->dst_port = fw_state_config.port_unicast;
			udpHeader->dgram_cksum = 0;
			udpHeader->dgram_cksum = rte_ipv6_udptcp_cksum(ipv6Header, udpHeader);

			rte_mbuf* mbuf_clone = rte_pktmbuf_alloc(m_mempool);
			if (mbuf_clone == nullptr)
			{
				m_slow_worker->Stats().fwsync_unicast_egress_drops++;
			}
			else
			{
				*YADECAP_METADATA(mbuf_clone) = *YADECAP_METADATA(mbuf);

				memcpy(rte_pktmbuf_mtod(mbuf_clone, char*),
				       rte_pktmbuf_mtod(mbuf, char*),
				       mbuf->data_len);
				mbuf_clone->data_len = mbuf->data_len;
				mbuf_clone->pkt_len = mbuf->pkt_len;

				m_slow_worker->Stats().fwsync_unicast_egress_packets++;
				sendPacketToSlowWorker(mbuf_clone, fw_state_config.ingress_flow);
			}
		}

		rte_pktmbuf_free(mbuf);
	}

	bool handlePacket_fw_state_sync_ingress(rte_mbuf* mbuf);

	void handlePacket_balancer_icmp_forward(rte_mbuf* mbuf);

	void handlePacketFromForwardingPlane(rte_mbuf* mbuf)
	{
		dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

		if (handlePacket_fw_state_sync_ingress(mbuf))
		{
			m_stats.fwsync_multicast_ingress_packets++;
			rte_pktmbuf_free(mbuf);
			return;
		}

#ifdef CONFIG_YADECAP_AUTOTEST
		if (metadata->flow.type != common::globalBase::eFlowType::slowWorker_kni_local)
		{
			// drop by default in tests
			m_stats.slowworker_drops++;
			rte_pktmbuf_free(mbuf);
			return;
		}
		rte_ether_hdr* ethernetHeader = rte_pktmbuf_mtod(mbuf, rte_ether_hdr*);
		memset(ethernetHeader->dst_addr.addr_bytes,
		       0x71,
		       6);

#endif

		if (!m_config.use_kernel_interface)
		{
			// TODO stats
			unsigned txSize = rte_eth_tx_burst(metadata->fromPortId, 0, &mbuf, 1);
			if (!txSize)
			{
				rte_pktmbuf_free(mbuf);
			}
			return;
		}
		else
		{
			m_kni_worker.HandlePacketFromForwardingPlane(mbuf);
		}
	}

	void HandleWorkerRings()
	{
		for (unsigned nIter = 0; nIter < YANET_CONFIG_RING_PRIORITY_RATIO; nIter++)
		{
			for (unsigned hIter = 0; hIter < YANET_CONFIG_RING_PRIORITY_RATIO; hIter++)
			{
				unsigned hProcessed = 0;
				for (cWorker* worker : m_workers_serviced)
				{
					hProcessed += ring_handle(worker->ring_toFreePackets, worker->ring_highPriority);
				}
				if (!hProcessed)
				{
					break;
				}
			}

			unsigned nProcessed = 0;
			for (cWorker* worker : m_workers_serviced)
			{
				nProcessed += ring_handle(worker->ring_toFreePackets, worker->ring_normalPriority);
			}
			if (!nProcessed)
			{
				break;
			}
		}
		for (cWorker* worker : m_workers_serviced)
		{
			ring_handle(worker->ring_toFreePackets, worker->ring_lowPriority);
		}
	}

	// \brief dequeue packets from worker_gc's ring to slowworker
	void DequeueGC();

	void Iteration()
	{
		m_slow_worker->slowWorkerBeforeHandlePackets();

		HandleWorkerRings();
		if (m_config.use_kernel_interface)
		{
			m_kni_worker.Flush();
		}

		DequeueGC();
		m_fragmentation.handle();
		m_dregress.handle();

		if (m_config.use_kernel_interface)
		{
			m_kni_worker.ForwardToPhy();
			m_kni_worker.RecvFree();
		}

		/// push packets to slow worker
		while (!m_slow_worker_mbufs.empty())
		{
			for (unsigned int i = 0;
			     i < CONFIG_YADECAP_MBUFS_BURST_SIZE;
			     i++)
			{
				if (m_slow_worker_mbufs.empty())
				{
					break;
				}

				auto& tuple = m_slow_worker_mbufs.front();
				m_slow_worker->slowWorkerFlow(std::get<0>(tuple), std::get<1>(tuple));

				m_slow_worker_mbufs.pop();
			}

			m_slow_worker->slowWorkerHandlePackets();
		}

		m_slow_worker->slowWorkerAfterHandlePackets();
#ifdef CONFIG_YADECAP_AUTOTEST
		std::this_thread::sleep_for(std::chrono::microseconds{1});
#endif // CONFIG_YADECAP_AUTOTEST
	}
	void WaitInit();
	void Start()
	{
		WaitInit();
		while (!m_stop)
		{
			Iteration();
		}
	}
	void operator()()
	{
		Start();
	}
};

} // namespace dataplane
