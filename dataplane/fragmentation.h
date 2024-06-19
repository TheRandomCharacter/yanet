#pragma once

#include <cinttypes>

#include <atomic>
#include <map>
#include <tuple>
#include <variant>

#include <rte_mbuf.h>

#include "common/type.h"

#include "type.h"

namespace fragmentation
{

using key_ipv4_t = std::tuple<common::globalBase::eFlowType, ///< @todo
                              uint64_t, ///< @todo
                              common::ipv4_address_t,
                              common::ipv4_address_t,
                              uint16_t>; ///< packet_id

using key_ipv6_t = std::tuple<common::globalBase::eFlowType, ///< @todo
                              uint64_t, ///< @todo
                              common::ipv6_address_t,
                              common::ipv6_address_t,
                              uint32_t>; ///< identification

using key_t = std::variant<key_ipv4_t,
                           key_ipv6_t>;

using value_t = std::tuple<std::map<uint32_t, ///< range_from
                                    std::tuple<uint32_t, ///< range_to
                                               rte_mbuf*>>,
                           uint16_t, ///< first packet time
                           uint16_t>; ///< last packet time

}

class fragmentation_t
{
public:
	using OnCollected = std::function<void(rte_mbuf*, const common::globalBase::tFlow&)>;
	fragmentation_t(OnCollected callback,
	                uint64_t timeout_first,
	                uint64_t timeout_last,
	                uint64_t packets_per_flow,
	                uint64_t size_limit);
	fragmentation_t(fragmentation_t&& other);
	~fragmentation_t();

	fragmentation_t& operator=(fragmentation_t&& other);

public:
	common::fragmentation::stats_t getStats() const;
	OnCollected& Callback() {return callback_;}

	void insert(rte_mbuf* mbuf);
	void handle();

protected:
	bool isTimeout(const fragmentation::value_t& value);
	bool isCollected(const fragmentation::value_t& value);
	bool isIntersect(const fragmentation::value_t& value, const uint32_t& range_from, const uint32_t& range_to);
protected:

	OnCollected callback_;

	uint64_t timeout_first_;
	uint64_t timeout_last_;
	uint64_t packets_per_flow_;
	uint64_t size_limit_;

	common::fragmentation::stats_t stats_;

	std::map<fragmentation::key_t, fragmentation::value_t> fragments_;
};
