#pragma once
#include <vector>

#include "base.h"
#include "metadata.h"

#include "burst.h"
#include "kernel_interface_handle.h"

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
	dpdk::Burst m_burst;

public:
	KernelInterface() = default;
	KernelInterface(tPortId port, tQueueId queue) :
	        m_port{port}, m_queue{queue}
	{
	}
	void Flush()
	{
		m_burst.Tx(m_port, m_queue).Free();
	}
	struct DirectionStats
	{
		uint64_t bytes;
		uint64_t packets;
		uint64_t dropped;
	};
	DirectionStats FlushTracked()
	{
		const auto& [bytes, packets] = m_burst.TxTracked(m_port, m_queue);
		return DirectionStats{bytes, packets, Free()};
	}
	uint16_t Free()
	{
		return m_burst.Free();
	}
	const tPortId& Port() const
	{
		return m_port;
	}
	void Push(rte_mbuf* mbuf)
	{
		if (!m_burst.Push(mbuf))
		{
			Flush();
			m_burst.PushUnsafe(mbuf);
		}
	}
	const tQueueId& Queue() const
	{
		return m_queue;
	}
};

class KernelInterfaceWorker
{
	struct HandleBundle
	{
		KernelInterfaceHandle forward;
		KernelInterfaceHandle in_dump;
		KernelInterfaceHandle out_dump;
		KernelInterfaceHandle drop_dump;
	};
	std::size_t m_port_count;
	std::vector<HandleBundle> m_handles;
	std::array<sKniStats, CONFIG_YADECAP_PORTS_SIZE> m_stats;
	std::array<KernelInterface, CONFIG_YADECAP_PORTS_SIZE> m_forward;
	std::array<KernelInterface, CONFIG_YADECAP_PORTS_SIZE> m_in_dump;
	std::array<KernelInterface, CONFIG_YADECAP_PORTS_SIZE> m_out_dump;
	std::array<KernelInterface, CONFIG_YADECAP_PORTS_SIZE> m_drop_dump;
	dpdk::Burst m_operative;
	const dataplane::base::PortMapper* m_port_mapper;
	uint64_t m_unknown_dump_interface = 0;
	KernelInterfaceWorker() {}
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
	/**
	 * @brief Receive packets from interface and free them.
	 * @param iface Interface to receive packets from.
	 */
	void RecvFree(const KernelInterface& iface)
	{
		m_operative.Rx(iface.Port(), iface.Queue()).Free();
	}

public:
	KernelInterfaceWorker(const KernelInterfaceWorker&) = delete;
	KernelInterfaceWorker(KernelInterfaceWorker&&) = default;
	KernelInterfaceWorker& operator=(const KernelInterfaceWorker&) = delete;
	KernelInterfaceWorker& operator=(KernelInterfaceWorker&&) = default;
	~KernelInterfaceWorker() = default;
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
			auto& stats = m_stats[i];
			const auto& [packets, bytes] = m_operative.Rx(m_handles[i].forward.Id(), 0)
			                                       .TxTracked(m_port_mapper->ToDpdk(i), 0);
			stats.opackets += packets;
			stats.obytes += bytes;
			stats.odropped += m_operative.Free();
		}
	}
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
};
} // namespace dataplane