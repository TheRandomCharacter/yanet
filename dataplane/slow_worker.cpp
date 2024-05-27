#include "slow_worker.h"
#include "dataplane.h"

namespace dataplane
{
void SlowWorker::WaitInit()
{
	YANET_LOG_ERROR("Reaching ports barrier (slow)\n");
	m_slow_worker->dataPlane->InitPortsBarrier();

	YANET_LOG_ERROR("WaitRun\n");
	auto rc = pthread_barrier_wait(m_run_barrier);
	if (rc == PTHREAD_BARRIER_SERIAL_THREAD)
	{
		pthread_barrier_destroy(m_run_barrier);
	}
	else if (rc)
	{
		YADECAP_LOG_ERROR("run_barrier pthread_barrier_wait() = %d\n", rc);
		abort();
	}
	YANET_LOG_ERROR("Running\n");
}

} // namesoace dataplane