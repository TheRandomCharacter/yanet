#pragma once
#include <queue>
#include <tuple>
#include <vector>

#include "common/fallback.h"
#include "dpdk.h"
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
		uint32_t SWICMPOutRateLimit = 0;
		bool use_kernel_interface;
	};

private:
	pthread_barrier_t* m_run_barrier;
	bool m_stop = false;
	std::vector<tPortId> m_ports_serviced;
	std::vector<cWorker*> m_workers_serviced;
	std::vector<dpdk::RingConn<rte_mbuf*>> from_gcs_;
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
	           const std::vector<cWorker*>& workers_to_service,
	           std::vector<dpdk::RingConn<rte_mbuf*>>&& from_gcs,
	           KernelInterfaceWorker&& kni,
	           rte_mempool* mempool,
	           pthread_barrier_t* run,
	           bool use_kni,
	           uint32_t sw_icmp_out_rate_limit);
	SlowWorker(const SlowWorker&) = delete;
	SlowWorker(SlowWorker&& other);

	cWorker* GetWorker() { return m_slow_worker; }
	const dataplane::KernelInterfaceWorker& KniWorker() const { return m_kni_worker; }
	const dataplane::base::generation& CurrentBase() { return m_slow_worker->CurrentBase(); }
	const fragmentation_t& Fragmentation() const { return m_fragmentation; }
	dregress_t& Dregress() { return m_dregress; }
	const dregress_t& Dregress() const { return m_dregress; }
	void freeWorkerPacket(rte_ring* ring_to_free_mbuf, rte_mbuf* mbuf);
	rte_mbuf* convertMempool(rte_ring* ring_to_free_mbuf, rte_mbuf* old_mbuf);
	rte_mempool* Mempool() { return m_mempool; }
	void PreparePacket(rte_mbuf* mbuf) { m_slow_worker->preparePacket(mbuf); }
	void sendPacketToSlowWorker(rte_mbuf* mbuf, const common::globalBase::tFlow& flow);
	void ResetIcmpOutRemainder(uint32_t limit) { m_icmp_out_remainder = limit; }

	const common::slowworker::stats_t& Stats() const { return m_stats; }
	unsigned ring_handle(rte_ring* ring_to_free_mbuf, rte_ring* ring);
	void handlePacket_icmp_translate_v6_to_v4(rte_mbuf* mbuf);
	void handlePacket_icmp_translate_v4_to_v6(rte_mbuf* mbuf);
	void handlePacket_fragment(rte_mbuf* mbuf);
	void handlePacket_dregress(rte_mbuf* mbuf);
	void handlePacket_farm(rte_mbuf* mbuf);
	void handlePacket_repeat(rte_mbuf* mbuf);
	void handlePacket_fw_state_sync(rte_mbuf* mbuf);
	bool handlePacket_fw_state_sync_ingress(rte_mbuf* mbuf);
	void handlePacket_balancer_icmp_forward(rte_mbuf* mbuf);
	void handlePacketFromForwardingPlane(rte_mbuf* mbuf);
	void HandleWorkerRings();

	// \brief dequeue packets from worker_gc's ring to slowworker
	void DequeueGC();

	void WaitInit();
	void Start()
	{
		WaitInit();
		while (!m_stop)
		{
			Iteration();
		}
	}
	void operator()() { Start(); }

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
};

} // namespace dataplane
