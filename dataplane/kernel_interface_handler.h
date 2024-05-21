#pragma once
#include <vector>

#include <rte_ethdev.h>

#include "base.h"
#include "metadata.h"


namespace dataplane
{

struct sKniStats
{
	uint64_t ipackets = 0;
	uint64_t ibytes = 0;
	uint64_t idropped = 0;
	uint64_t opackets = 0;
	uint64_t obytes = 0;
	uint64_t odropped = 0;
};

class KernelInterface
{
	tPortId m_port;
	tQueueId m_queue;
	rte_mbuf* m_burst[CONFIG_YADECAP_MBUFS_BURST_SIZE];
	uint16_t m_burst_length = 0;

public:
	KernelInterface() = default;
	KernelInterface(tPortId port, tQueueId queue) :
	        m_port{port}, m_queue{queue}
	{
	}
	void Flush()
	{
		auto sent = rte_eth_tx_burst(m_port, m_queue, m_burst, m_burst_length);
		const auto remain = m_burst_length - sent;
		if (remain)
		{
			rte_pktmbuf_free_bulk(m_burst + sent, remain);
		}
		m_burst_length = 0;
	}
	struct DirectionStats
	{
		uint64_t bytes;
		uint64_t packets;
		uint64_t dropped;
	};
	DirectionStats FlushTracked()
	{
		DirectionStats stats;
		stats.bytes = std::accumulate(m_burst, m_burst + m_burst_length, 0, [](uint64_t total, rte_mbuf* mbuf) {
			return total + rte_pktmbuf_pkt_len(mbuf);
		});
		stats.packets = rte_eth_tx_burst(m_port, m_queue, m_burst, m_burst_length);

		stats.dropped = m_burst_length - stats.packets;
		if (stats.dropped)
		{
			stats.bytes = std::accumulate(m_burst, m_burst + m_burst_length, stats.bytes, [](uint64_t total, rte_mbuf* mbuf) {
				return total - rte_pktmbuf_pkt_len(mbuf);
			});
			rte_pktmbuf_free_bulk(m_burst + stats.packets, stats.dropped);
		}
		m_burst_length = 0;

		return stats;
	}
	const tPortId& Port() const
	{
		return m_port;
	}
	void Push(rte_mbuf* mbuf)
	{
		if (m_burst_length == YANET_CONFIG_BURST_SIZE)
		{
			Flush();
		}
		m_burst[m_burst_length++] = mbuf;
	}
	const tQueueId& Queue() const
	{
		return m_queue;
	}
};

class KernelInterfaceWorker
{
	std::size_t m_port_count = 0;
	std::array<tPortId, CONFIG_YADECAP_PORTS_SIZE> m_dpdk_ports;
	std::array<tQueueId, CONFIG_YADECAP_PORTS_SIZE> m_dpdk_queues;
	std::array<sKniStats, CONFIG_YADECAP_PORTS_SIZE> m_stats;
#if DEPRECATED
	std::array<tPortId, CONFIG_YADECAP_PORTS_SIZE> m_forward_port_ids;
#endif
	std::array<KernelInterface, CONFIG_YADECAP_PORTS_SIZE> m_forward;
	std::array<KernelInterface, CONFIG_YADECAP_PORTS_SIZE> m_in_dump;
	std::array<KernelInterface, CONFIG_YADECAP_PORTS_SIZE> m_out_dump;
	std::array<KernelInterface, CONFIG_YADECAP_PORTS_SIZE> m_drop_dump;
	const dataplane::base::PortMapper* m_port_mapper;
	uint64_t m_unknown_dump_interface = 0;
	KernelInterfaceWorker() {}
	bool AddInterface(std::array<std::pair<tPortId,tQueueId>, 4> psnqs)
	{
		m_forward[m_port_count] = KernelInterface{psnqs[0].first, psnqs[0].second};
		m_forward[m_port_count] = KernelInterface{psnqs[1].first, psnqs[1].second};
		m_forward[m_port_count] = KernelInterface{psnqs[2].first, psnqs[2].second};
		m_forward[m_port_count] = KernelInterface{psnqs[3].first, psnqs[3].second};
		m_port_count++;
	}
#if DEPRECATED
	static std::optional<KernelInterfaceHandle> InitInterface(KernelInterface& iface,
	                                                          const std::string& name,
	                                                          tPortId port,
	                                                          tQueueId queue,
	                                                          rte_mempool* mempool,
	                                                          uint64_t queue_size)
	{
		auto h = KernelInterfaceHandle::MakeKernelInterfaceHandle(name, port, mempool, queue_size);
		if (h)
		{
			iface = {h.value().Id(), queue};
		}
		return h;
	}
#endif
	/**
	 * @brief Receive packets from interface and free them.
	 * @param iface Interface to receive packets from.
	 */
	void RecvFree(const KernelInterface& iface)
	{
		rte_mbuf* burst[CONFIG_YADECAP_MBUFS_BURST_SIZE];
		auto len = rte_eth_rx_burst(iface.Port(), iface.Queue(), burst, CONFIG_YADECAP_MBUFS_BURST_SIZE);
		rte_pktmbuf_free_bulk(burst, len);
	}

public:
	KernelInterfaceWorker(const KernelInterfaceWorker&) = delete;
	KernelInterfaceWorker(KernelInterfaceWorker&&);
	KernelInterfaceWorker& operator=(const KernelInterfaceWorker&) = delete;
	KernelInterfaceWorker& operator=(KernelInterfaceWorker&&);
	~KernelInterfaceWorker() = default;
	void Iteration()
	{

	}
#if DEPRECATED
	static std::optional<KernelInterfaceWorker> MakeKernelInterfaceWorker(
	        const std::vector<std::pair<tPortId, const std::string&>>& ports,
	        tQueueId queue_id,
	        rte_mempool* mempool,
	        uint64_t queue_size)
	{
		KernelInterfaceWorker kniworker;
		for (int i = 0; i < ports.size(); ++i)
		{
			const auto& [phy_id, name] = ports[i];
			auto forward = InitInterface(kniworker.m_forward[i], name, phy_id, queue_id, mempool, queue_size);
			auto in_dump = InitInterface(kniworker.m_in_dump[i], "in." + name, phy_id, queue_id, mempool, queue_size);
			auto out_dump = InitInterface(kniworker.m_out_dump[i], "out." + name, phy_id, queue_id, mempool, queue_size);
			auto drop_dump = InitInterface(kniworker.m_drop_dump[i], "drop." + name, phy_id, queue_id, mempool, queue_size);

			if (!forward || !in_dump || !out_dump || !drop_dump)
			{
				return std::optional<KernelInterfaceWorker>{};
			}
			kniworker.m_handles.emplace_back(std::move(forward.value()),
			                                 std::move(in_dump.value()),
			                                 std::move(out_dump.value()),
			                                 std::move(drop_dump.value()));
			++kniworker.m_port_count;
		}
		return std::optional<KernelInterfaceWorker>{std::move(kniworker)};
	}

	/// @brief Set kernel interface up via ioctl
	[[nodiscard]] bool SetInterfacesUp()
	{
		for (int i = 0; i < m_port_count; ++i)
		{
			if (!m_handles[i].forward.SetUp())
			{
				return false;
			}
		}
		return true;
	}
#endif

	/// @brief Transmit accumulated packets. Those that could not be sent are freed
	void Flush()
	{
		for (int i = 0; i < m_port_count; ++i)
		{
			const auto& delta = m_forward[i].FlushTracked();
			m_stats[i].opackets += delta.packets;
			m_stats[i].obytes += delta.bytes;
			m_stats[i].odropped += delta.dropped;
			m_in_dump[i].Flush();
			m_out_dump[i].Flush();
			m_drop_dump[i].Flush();
		}
	}
	/// @brief Receive from in.X/out.X/drop.X interfaces and free packets
	void RecvFree()
	{
		for (int i = 0; i < m_port_count; ++i)
		{
			RecvFree(m_in_dump[i]);
			RecvFree(m_out_dump[i]);
			RecvFree(m_drop_dump[i]);
		}
	}
	/// @brief Receive packets from kernel interface and send to physical port
	void ForwardToPhy()
	{
		for (int i = 0; i < m_port_count; ++i)
		{
			rte_mbuf* burst[CONFIG_YADECAP_MBUFS_BURST_SIZE];
			auto packets = rte_eth_rx_burst(m_forward[i].Port(), m_forward[i].Queue(), burst, CONFIG_YADECAP_MBUFS_BURST_SIZE);
			uint64_t bytes = std::accumulate(burst, burst + packets, 0, [](uint64_t total, rte_mbuf* mbuf) {
				return total + rte_pktmbuf_pkt_len(mbuf);
			});
			auto transmitted = rte_eth_tx_burst(m_dpdk_ports[i], m_dpdk_queues[i], burst, packets);
			const auto remain = packets - transmitted;

			if (remain)
			{
				bytes = std::accumulate(burst, burst + packets, bytes, [](uint64_t total, rte_mbuf* mbuf) {
					return total - rte_pktmbuf_pkt_len(mbuf);
				});
				rte_pktmbuf_free_bulk(burst + transmitted, remain);
			}

			auto& stats = m_stats[i];
			stats.opackets += transmitted;
			stats.obytes += bytes;
			stats.odropped += remain;
		}
	}
#if DEPRECATED
	void HandlePacketDump(rte_mbuf* mbuf)
	{
		dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

		if (!m_port_mapper->ValidDpdk(metadata->flow.data.dump.id))
		{
			m_unknown_dump_interface++;
			rte_pktmbuf_free(mbuf);
			return;
		}
		const auto local_port_id = m_port_mapper->ToLogical(metadata->flow.data.dump.id);

		using dumpType = common::globalBase::dump_type_e;
		switch (metadata->flow.data.dump.type)
		{
			case dumpType::physicalPort_ingress:
				m_in_dump[local_port_id].Push(mbuf);
				break;
			case dumpType::physicalPort_egress:
				m_out_dump[local_port_id].Push(mbuf);
				break;
			case dumpType::physicalPort_drop:
				m_drop_dump[local_port_id].Push(mbuf);
				break;
			default:
				m_unknown_dump_interface++;
				rte_pktmbuf_free(mbuf);
		}
	}
#endif
};
} // namespace dataplane