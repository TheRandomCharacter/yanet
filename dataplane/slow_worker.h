#pragma once
#include <queue>
#include <tuple>

#include "worker.h"

namespace dataplane
{

class SlowWorker
{
	pthread_barrier_t* m_init_ports_barrier;
	pthread_barrier_t* m_run_barrier;
	bool m_stop = false;
	std::vector<tPortId> m_ports_serviced;
	std::vector<cWorker*> m_workers_serviced;
	cWorker* m_slow_worker;
	std::queue<
	        std::tuple<
	                rte_mbuf*,
	                common::globalBase::tFlow>>
	        m_slow_worker_mbufs;
	rte_mempool* m_mempool; // from cControlPlane

public:
	SlowWorker(cWorker* worker,
	           const std::vector<tPortId>& ports_to_service,
	           rte_mempool* mempool,
	           pthread_barrier_t* init_ports,
	           pthread_barrier_t* run) :
	        m_init_ports_barrier(init_ports),
	        m_run_barrier(run),
	        m_ports_serviced(ports_to_service),
	        m_slow_worker(worker),
	        m_mempool(mempool)
	{
	}

	cWorker* GetWorker() { return m_slow_worker; }
	void Iteration()
	{
		m_slow_worker->slowWorkerBeforeHandlePackets();
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
	}
	void WaitInit()
	{
		int rc = pthread_barrier_wait(m_init_ports_barrier);
		if (rc == PTHREAD_BARRIER_SERIAL_THREAD)
		{
			pthread_barrier_destroy(m_init_ports_barrier);
		}
		else if (rc)
		{
			YADECAP_LOG_ERROR("init_ports_barrier pthread_barrier_wait() = %d\n", rc);
			abort();
		}
		rc = pthread_barrier_wait(m_run_barrier);
		if (rc == PTHREAD_BARRIER_SERIAL_THREAD)
		{
			pthread_barrier_destroy(m_run_barrier);
		}
		else if (rc)
		{
			YADECAP_LOG_ERROR("run_barrier pthread_barrier_wait() = %d\n", rc);
			abort();
		}
	}
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