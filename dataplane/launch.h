#include <optional>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_lcore.h>
#include <thread>

namespace dpdk
{
namespace internal
{

template<typename F>
struct lcore_func_args
{
	F& ftor;
	pthread_barrier_t* init_bar;
	F** location;
};

template<typename F>
int lcore_func(void* arg)
{
	auto args = reinterpret_cast<lcore_func_args<F>*>(arg);
	F ftor = std::move(args->ftor);
	*(args->location) = &ftor;
	pthread_barrier_wait(args->init_bar);
	ftor();
	return 0;
}

} // namespace internal


/*
 * \brief Moves provided functor \p ftor on stack of lcore with id \p core_id and calls it
 * \warning Returned pointer will dangle after ftor() returns
 */
template<typename F>
std::optional<F*> RunAsWorkerOnCore(unsigned core_id, F&& ftor)
{
	pthread_barrier_t init_bar;
	if (pthread_barrier_init(&init_bar, nullptr, 2))
	{
		printf("failed to init barrier for moving functor to core %d\n", core_id);
		return std::nullopt;
	}
	F* location;
	internal::lcore_func_args<F> args{ftor, &init_bar, &location};
	int res = rte_eal_remote_launch(internal::lcore_func<F>, reinterpret_cast<void*>(&args), core_id);
	if (res == -1)
	{
		return std::nullopt;
	}
	pthread_barrier_wait(&init_bar);
	pthread_barrier_destroy(&init_bar);
	return location;
}

} // namespace dpdk