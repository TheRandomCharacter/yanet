#pragma once
#include <iterator>
#include <vector>

#include <rte_ethdev.h>

#include "base.h"
#include "metadata.h"

namespace dataplane
{

struct Endpoint
{
	tPortId port;
	tQueueId queue;
};

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
	KernelInterface(const Endpoint& e) :
	        m_port{e.port}, m_queue{e.queue}
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
	DirectionStats PushTracked(rte_mbuf* mbuf)
	{
		DirectionStats res;
		if (m_burst_length == YANET_CONFIG_BURST_SIZE)
		{
			res = FlushTracked();
		}
		m_burst[m_burst_length++] = mbuf;
		return res;
	}
	const tQueueId& Queue() const
	{
		return m_queue;
	}
};

struct KernelInterfaceBundleConfig
{
	Endpoint phy;
	Endpoint forward;
	Endpoint in_dump;
	Endpoint out_dump;
	Endpoint drop_dump;
};

struct KernelInterfaceWorkerConfig
{
	std::vector<KernelInterfaceBundleConfig> interfaces;
	dataplane::base::PortMapper* port_mapper;
};

class KernelInterfaceWorker
{
public:
	template<typename T>
	using PortArray = std::array<T, CONFIG_YADECAP_PORTS_SIZE>;
	template<typename T>
	using ConstPortArrayRange = std::pair<typename PortArray<T>::const_iterator, typename PortArray<T>::const_iterator>;

private:
	std::size_t size_ = 0;
	PortArray<tPortId> phy_ports_;
	PortArray<tQueueId> phy_queues_;
	PortArray<sKniStats> stats_;
	PortArray<KernelInterface> forward_;
	PortArray<KernelInterface> in_dump_;
	PortArray<KernelInterface> out_dump_;
	PortArray<KernelInterface> drop_dump_;
	const dataplane::base::PortMapper* port_mapper_;
	uint64_t unknown_dump_interface_ = 0;

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
	KernelInterfaceWorker(const KernelInterfaceWorkerConfig& config) :
	        port_mapper_{config.port_mapper}
	{
		for (const auto& iface : config.interfaces)
		{
			phy_ports_[size_] = iface.phy.port;
			phy_queues_[size_] = iface.phy.queue;
			forward_[size_] = KernelInterface{iface.forward};
			in_dump_[size_] = KernelInterface{iface.in_dump};
			out_dump_[size_] = KernelInterface{iface.out_dump};
			drop_dump_[size_] = KernelInterface{iface.drop_dump};
			++size_;
		}
	}

	ConstPortArrayRange<tPortId> PortsIds() const
	{
		return {phy_ports_.begin(), phy_ports_.begin() + size_};
	}
	ConstPortArrayRange<sKniStats> PortsStats() const
	{
		return {stats_.begin(), stats_.begin() + size_};
	}
	std::optional<std::reference_wrapper<const sKniStats>> PortStats(tPortId pid) const
	{
		for (std::size_t i = 0; i < size_; ++i)
		{
			if (phy_ports_[i] == pid)
			{
				return stats_[i];
			}
		}
		return {};
	}

	/// @brief Transmit accumulated packets. Those that could not be sent are freed
	void Flush()
	{
		for (std::size_t i = 0; i < size_; ++i)
		{
			const auto& delta = forward_[i].FlushTracked();
			stats_[i].opackets += delta.packets;
			stats_[i].obytes += delta.bytes;
			stats_[i].odropped += delta.dropped;
			in_dump_[i].Flush();
			out_dump_[i].Flush();
			drop_dump_[i].Flush();
		}
	}
	/// @brief Receive from in.X/out.X/drop.X interfaces and free packets
	void RecvFree()
	{
		for (std::size_t i = 0; i < size_; ++i)
		{
			RecvFree(in_dump_[i]);
			RecvFree(out_dump_[i]);
			RecvFree(drop_dump_[i]);
		}
	}
	/// @brief Receive packets from kernel interface and send to physical port
	void ForwardToPhy()
	{
		for (std::size_t i = 0; i < size_; ++i)
		{
			rte_mbuf* burst[CONFIG_YADECAP_MBUFS_BURST_SIZE];
			auto packets = rte_eth_rx_burst(forward_[i].Port(), forward_[i].Queue(), burst, CONFIG_YADECAP_MBUFS_BURST_SIZE);
			uint64_t bytes = std::accumulate(burst, burst + packets, 0, [](uint64_t total, rte_mbuf* mbuf) {
				return total + rte_pktmbuf_pkt_len(mbuf);
			});
			auto transmitted = rte_eth_tx_burst(phy_ports_[i], phy_queues_[i], burst, packets);
			const auto remain = packets - transmitted;

			if (remain)
			{
				bytes = std::accumulate(burst, burst + packets, bytes, [](uint64_t total, rte_mbuf* mbuf) {
					return total - rte_pktmbuf_pkt_len(mbuf);
				});
				rte_pktmbuf_free_bulk(burst + transmitted, remain);
			}

			auto& stats = stats_[i];
			stats.opackets += transmitted;
			stats.obytes += bytes;
			stats.odropped += remain;
		}
	}
	void HandlePacketDump(rte_mbuf* mbuf)
	{
		dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

		if (!port_mapper_->ValidDpdk(metadata->flow.data.dump.id))
		{
			unknown_dump_interface_++;
			rte_pktmbuf_free(mbuf);
			return;
		}
		const auto local_port_id = port_mapper_->ToLogical(metadata->flow.data.dump.id);

		using dumpType = common::globalBase::dump_type_e;
		switch (metadata->flow.data.dump.type)
		{
			case dumpType::physicalPort_ingress:
				in_dump_[local_port_id].Push(mbuf);
				break;
			case dumpType::physicalPort_egress:
				out_dump_[local_port_id].Push(mbuf);
				break;
			case dumpType::physicalPort_drop:
				drop_dump_[local_port_id].Push(mbuf);
				break;
			default:
				unknown_dump_interface_++;
				rte_pktmbuf_free(mbuf);
		}
	}
	void HandlePacketFromForwardingPlane(rte_mbuf* mbuf)
	{
		dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

		const auto i = port_mapper_->ToLogical(metadata->fromPortId);
		const auto& delta = forward_[i].PushTracked(mbuf);
		stats_[i].opackets += delta.packets;
		stats_[i].obytes += delta.bytes;
		stats_[i].odropped += delta.dropped;
	}
};
} // namespace dataplane