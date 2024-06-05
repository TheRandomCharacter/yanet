#include "slow_worker.h"

#include "dataplane.h"
#include "icmp_translations.h"

namespace dataplane
{
SlowWorker::SlowWorker(cWorker* worker,
                       const std::vector<tPortId>& ports_to_service,
					   KernelInterfaceWorker&& kni,
                       rte_mempool* mempool,
                       pthread_barrier_t* run) :
        m_run_barrier{run},
        m_ports_serviced{ports_to_service},
        m_slow_worker{worker},
        m_mempool{mempool},
        m_fragmentation(
                [&](rte_mbuf* pkt, const common::globalBase::tFlow& flow) {
	                m_slow_worker->dataPlane->controlPlane->sendPacketToSlowWorker(pkt, flow);
                },
                m_slow_worker->dataPlane->getConfigValues().fragmentation_timeout_first,
                m_slow_worker->dataPlane->getConfigValues().fragmentation_timeout_last,
                m_slow_worker->dataPlane->getConfigValues().fragmentation_packets_per_flow,
                m_slow_worker->dataPlane->getConfigValues().fragmentation_timeout_first),
        m_dregress(this,
                   m_slow_worker->dataPlane,
                   static_cast<uint32_t>(m_slow_worker->dataPlane->getConfigValues().gc_step)), // @TODO fix mismatch in type of config value and actually used one
		m_kni_worker{std::move(kni)}
{
}

void SlowWorker::freeWorkerPacket(rte_ring* ring_to_free_mbuf,
                                  rte_mbuf* mbuf)
{
	if (ring_to_free_mbuf == m_slow_worker->ring_toFreePackets)
	{
		rte_pktmbuf_free(mbuf);
		return;
	}

	while (rte_ring_sp_enqueue(ring_to_free_mbuf, mbuf) != 0)
	{
		std::this_thread::yield();
	}
}

rte_mbuf* SlowWorker::convertMempool(rte_ring* ring_to_free_mbuf, rte_mbuf* old_mbuf)
{
	/// we dont support attached mbufs

	rte_mbuf* mbuf = rte_pktmbuf_alloc(m_mempool);
	if (!mbuf)
	{
		m_stats.mempool_is_empty++;

		freeWorkerPacket(ring_to_free_mbuf, old_mbuf);
		return nullptr;
	}

	*YADECAP_METADATA(mbuf) = *YADECAP_METADATA(old_mbuf);

	/// @todo: rte_pktmbuf_append() and check error

	memcpy(rte_pktmbuf_mtod(mbuf, char*),
	       rte_pktmbuf_mtod(old_mbuf, char*),
	       old_mbuf->data_len);

	mbuf->data_len = old_mbuf->data_len;
	mbuf->pkt_len = old_mbuf->pkt_len;

	freeWorkerPacket(ring_to_free_mbuf, old_mbuf);

	if (rte_mbuf_refcnt_read(mbuf) != 1)
	{
		YADECAP_LOG_ERROR("something wrong\n");
	}

	return mbuf;
}

void SlowWorker::sendPacketToSlowWorker(rte_mbuf* mbuf, const common::globalBase::tFlow& flow)
{
	/// we dont support attached mbufs

	if (m_slow_worker_mbufs.size() >= 1024) ///< @todo: variable
	{
		m_stats.slowworker_drops++;
		rte_pktmbuf_free(mbuf);
		return;
	}

	m_stats.slowworker_packets++;
	m_slow_worker_mbufs.emplace(mbuf, flow);
}

unsigned SlowWorker::ring_handle(rte_ring* ring_to_free_mbuf,
                                 rte_ring* ring)
{
	rte_mbuf* mbufs[CONFIG_YADECAP_MBUFS_BURST_SIZE];

	unsigned rxSize = rte_ring_sc_dequeue_burst(ring,
	                                            (void**)mbufs,
	                                            CONFIG_YADECAP_MBUFS_BURST_SIZE,
	                                            nullptr);

#ifdef CONFIG_YADECAP_AUTOTEST
	if (rxSize)
	{
		std::this_thread::sleep_for(std::chrono::microseconds{400});
	}
#endif // CONFIG_YADECAP_AUTOTEST

	for (uint16_t mbuf_i = 0; mbuf_i < rxSize; mbuf_i++)
	{
		rte_mbuf* mbuf = convertMempool(ring_to_free_mbuf, mbufs[mbuf_i]);
		if (!mbuf)
		{
			continue;
		}

		dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

		if (metadata->flow.type == common::globalBase::eFlowType::slowWorker_nat64stateless_ingress_icmp)
		{
			handlePacket_icmp_translate_v6_to_v4(mbuf);
		}
		else if (metadata->flow.type == common::globalBase::eFlowType::slowWorker_nat64stateless_ingress_fragmentation)
		{
			metadata->flow.type = common::globalBase::eFlowType::nat64stateless_ingress_checked;
			handlePacket_fragment(mbuf);
		}
		else if (metadata->flow.type == common::globalBase::eFlowType::slowWorker_nat64stateless_egress_icmp)
		{
			handlePacket_icmp_translate_v4_to_v6(mbuf);
		}
		else if (metadata->flow.type == common::globalBase::eFlowType::slowWorker_nat64stateless_egress_fragmentation)
		{
			metadata->flow.type = common::globalBase::eFlowType::nat64stateless_egress_checked;
			handlePacket_fragment(mbuf);
		}
		else if (metadata->flow.type == common::globalBase::eFlowType::slowWorker_dregress)
		{
			handlePacket_dregress(mbuf);
		}
		else if (metadata->flow.type == common::globalBase::eFlowType::slowWorker_nat64stateless_egress_farm)
		{
			handlePacket_farm(mbuf);
		}
		else if (metadata->flow.type == common::globalBase::eFlowType::slowWorker_dump)
		{
			m_kni_worker.HandlePacketDump(mbuf);
		}
		else if (metadata->flow.type == common::globalBase::eFlowType::slowWorker_repeat)
		{
			handlePacket_repeat(mbuf);
		}
		else if (metadata->flow.type == common::globalBase::eFlowType::slowWorker_fw_sync)
		{
			handlePacket_fw_state_sync(mbuf);
		}
		else if (metadata->flow.type == common::globalBase::eFlowType::slowWorker_balancer_icmp_forward)
		{
			handlePacket_balancer_icmp_forward(mbuf);
		}
		else
		{
			handlePacketFromForwardingPlane(mbuf);
		}
	}
	return rxSize;
}

void SlowWorker::handlePacket_icmp_translate_v6_to_v4(rte_mbuf* mbuf)
{
	dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

	const auto& base = m_slow_worker->CurrentBase();
	const auto& nat64stateless = base.globalBase->nat64statelesses[metadata->flow.data.nat64stateless.id];
	const auto& translation = base.globalBase->nat64statelessTranslations[metadata->flow.data.nat64stateless.translationId];

	m_slow_worker->slowWorkerTranslation(mbuf, nat64stateless, translation, true);

	if (do_icmp_translate_v6_to_v4(mbuf, translation))
	{
		m_slow_worker->Stats().nat64stateless_ingressPackets++;
		sendPacketToSlowWorker(mbuf, nat64stateless.flow);
	}
	else
	{
		m_slow_worker->Stats().nat64stateless_ingressUnknownICMP++;
		rte_pktmbuf_free(mbuf);
	}
}

void SlowWorker::handlePacket_icmp_translate_v4_to_v6(rte_mbuf* mbuf)
{
	dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

	const auto& base = m_slow_worker->CurrentBase();
	const auto& nat64stateless = base.globalBase->nat64statelesses[metadata->flow.data.nat64stateless.id];
	const auto& translation = base.globalBase->nat64statelessTranslations[metadata->flow.data.nat64stateless.translationId];

	m_slow_worker->slowWorkerTranslation(mbuf, nat64stateless, translation, false);

	if (do_icmp_translate_v4_to_v6(mbuf, translation))
	{
		m_slow_worker->Stats().nat64stateless_egressPackets++;
		sendPacketToSlowWorker(mbuf, nat64stateless.flow);
	}
	else
	{
		m_slow_worker->Stats().nat64stateless_egressUnknownICMP++;
		rte_pktmbuf_free(mbuf);
	}
}

void SlowWorker::handlePacket_fragment(rte_mbuf* mbuf)
{
	dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

	const auto& base = m_slow_worker->CurrentBase();
	const auto& nat64stateless = base.globalBase->nat64statelesses[metadata->flow.data.nat64stateless.id];

	if (nat64stateless.defrag_farm_prefix.empty() || metadata->network_headerType != rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4) || nat64stateless.farm)
	{
		m_fragmentation.insert(mbuf);
		return;
	}

	m_stats.tofarm_packets++;
	m_slow_worker->slowWorkerHandleFragment(mbuf);
	sendPacketToSlowWorker(mbuf, nat64stateless.flow);
}

bool SlowWorker::handlePacket_fw_state_sync_ingress(rte_mbuf* mbuf)
{
	dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

	generic_rte_ether_hdr* ethernetHeader = rte_pktmbuf_mtod(mbuf, generic_rte_ether_hdr*);
	if ((ethernetHeader->dst_addr.addr_bytes[0] & 1) == 0)
	{
		return false;
	}

	// Confirmed multicast packet.
	// Try to match against our multicast groups.
	if (ethernetHeader->ether_type != rte_cpu_to_be_16(RTE_ETHER_TYPE_VLAN))
	{
		return false;
	}

	rte_vlan_hdr* vlanHeader = rte_pktmbuf_mtod_offset(mbuf, rte_vlan_hdr*, sizeof(rte_ether_hdr));
	if (vlanHeader->eth_proto != rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV6))
	{
		return false;
	}

	rte_ipv6_hdr* ipv6Header = rte_pktmbuf_mtod_offset(mbuf, rte_ipv6_hdr*, sizeof(rte_ether_hdr) + sizeof(rte_vlan_hdr));
	if (metadata->transport_headerType != IPPROTO_UDP)
	{
		return false;
	}

	const auto udp_payload_len = rte_be_to_cpu_16(ipv6Header->payload_len) - sizeof(rte_udp_hdr);
	// Can contain multiple states per sync packet.
	if (udp_payload_len % sizeof(dataplane::globalBase::fw_state_sync_frame_t) != 0)
	{
		return false;
	}

	tAclId aclId;
	{
		auto fw_state_multicast_acl_ids = m_slow_worker->dataPlane->controlPlane->fw_state_multicast_acl_ids.Accessor();
		auto it = fw_state_multicast_acl_ids->find(common::ipv6_address_t(ipv6Header->dst_addr));
		if (it == fw_state_multicast_acl_ids->end())
		{
			return false;
		}

		aclId = it->second;
	}

	const auto& base = m_slow_worker->CurrentBase();
	const auto& fw_state_config = base.globalBase->fw_state_sync_configs[aclId];

	if (memcmp(ipv6Header->src_addr, fw_state_config.ipv6_address_source.bytes, 16) == 0)
	{
		// Ignore self-generated packets.
		return false;
	}

	rte_udp_hdr* udpHeader = rte_pktmbuf_mtod_offset(mbuf, rte_udp_hdr*, sizeof(rte_ether_hdr) + sizeof(rte_vlan_hdr) + sizeof(rte_ipv6_hdr));
	if (udpHeader->dst_port != fw_state_config.port_multicast)
	{
		return false;
	}

	for (size_t idx = 0; idx < udp_payload_len / sizeof(dataplane::globalBase::fw_state_sync_frame_t); ++idx)
	{
		dataplane::globalBase::fw_state_sync_frame_t* payload = rte_pktmbuf_mtod_offset(
		        mbuf,
		        dataplane::globalBase::fw_state_sync_frame_t*,
		        sizeof(rte_ether_hdr) + sizeof(rte_vlan_hdr) + sizeof(rte_ipv6_hdr) + sizeof(rte_udp_hdr) + idx * sizeof(dataplane::globalBase::fw_state_sync_frame_t));

		if (payload->addr_type == 6)
		{
			dataplane::globalBase::fw6_state_key_t key;
			key.proto = payload->proto;
			key.__nap = 0;
			// Swap src and dst addresses.
			memcpy(key.dst_addr.bytes, payload->src_ip6.bytes, 16);
			memcpy(key.src_addr.bytes, payload->dst_ip6.bytes, 16);

			if (payload->proto == IPPROTO_TCP || payload->proto == IPPROTO_UDP)
			{
				// Swap src and dst ports.
				key.dst_port = payload->src_port;
				key.src_port = payload->dst_port;
			}
			else
			{
				key.dst_port = 0;
				key.src_port = 0;
			}

			dataplane::globalBase::fw_state_value_t value;
			value.type = static_cast<dataplane::globalBase::fw_state_type>(payload->proto);
			value.owner = dataplane::globalBase::fw_state_owner_e::external;
			value.last_seen = m_slow_worker->CurrentTime();
			value.flow = fw_state_config.ingress_flow;
			value.acl_id = aclId;
			value.last_sync = m_slow_worker->CurrentTime();
			value.packets_since_last_sync = 0;
			value.packets_backward = 0;
			value.packets_forward = 0;
			value.tcp.unpack(payload->flags);

			auto& dataPlane = m_slow_worker->dataPlane;
			uint32_t state_timeout = dataPlane->getConfigValues().stateful_firewall_other_protocols_timeout;
			if (payload->proto == IPPROTO_UDP)
			{
				state_timeout = dataPlane->getConfigValues().stateful_firewall_udp_timeout;
			}
			else if (payload->proto == IPPROTO_TCP)
			{
				state_timeout = dataPlane->getConfigValues().stateful_firewall_tcp_timeout;
				uint8_t flags = value.tcp.src_flags | value.tcp.dst_flags;
				if (flags & (uint8_t)common::fwstate::tcp_flags_e::ACK)
				{
					state_timeout = dataPlane->getConfigValues().stateful_firewall_tcp_syn_ack_timeout;
				}
				else if (flags & (uint8_t)common::fwstate::tcp_flags_e::SYN)
				{
					state_timeout = dataPlane->getConfigValues().stateful_firewall_tcp_syn_timeout;
				}
				if (flags & (uint8_t)common::fwstate::tcp_flags_e::FIN)
				{
					state_timeout = dataPlane->getConfigValues().stateful_firewall_tcp_fin_timeout;
				}
			}
			value.state_timeout = state_timeout;

			for (auto& [socketId, globalBaseAtomic] : m_slow_worker->dataPlane->globalBaseAtomics)
			{
				(void)socketId;

				dataplane::globalBase::fw_state_value_t* lookup_value;
				dataplane::spinlock_nonrecursive_t* locker;
				const uint32_t hash = globalBaseAtomic->fw6_state->lookup(key, lookup_value, locker);
				if (lookup_value)
				{
					// Keep state alive for us even if there were no packets received.
					// Do not reset other counters.
					lookup_value->last_seen = m_slow_worker->CurrentTime();
					lookup_value->tcp.src_flags |= value.tcp.src_flags;
					lookup_value->tcp.dst_flags |= value.tcp.dst_flags;
					lookup_value->state_timeout = std::max(lookup_value->state_timeout, value.state_timeout);
				}
				else
				{
					globalBaseAtomic->fw6_state->insert(hash, key, value);
				}
				locker->unlock();
			}
		}
		else if (payload->addr_type == 4)
		{
			dataplane::globalBase::fw4_state_key_t key;
			key.proto = payload->proto;
			key.__nap = 0;
			// Swap src and dst addresses.
			key.dst_addr.address = payload->src_ip;
			key.src_addr.address = payload->dst_ip;

			if (payload->proto == IPPROTO_TCP || payload->proto == IPPROTO_UDP)
			{
				// Swap src and dst ports.
				key.dst_port = payload->src_port;
				key.src_port = payload->dst_port;
			}
			else
			{
				key.dst_port = 0;
				key.src_port = 0;
			}

			dataplane::globalBase::fw_state_value_t value;
			value.type = static_cast<dataplane::globalBase::fw_state_type>(payload->proto);
			value.owner = dataplane::globalBase::fw_state_owner_e::external;
			value.last_seen = m_slow_worker->CurrentTime();
			value.flow = fw_state_config.ingress_flow;
			value.acl_id = aclId;
			value.last_sync = m_slow_worker->CurrentTime();
			value.packets_since_last_sync = 0;
			value.packets_backward = 0;
			value.packets_forward = 0;
			value.tcp.unpack(payload->flags);

			auto& cfg = m_slow_worker->dataPlane->getConfigValues();
			uint32_t state_timeout = cfg.stateful_firewall_other_protocols_timeout;
			if (payload->proto == IPPROTO_UDP)
			{
				state_timeout = cfg.stateful_firewall_udp_timeout;
			}
			else if (payload->proto == IPPROTO_TCP)
			{
				state_timeout = cfg.stateful_firewall_tcp_timeout;
				uint8_t flags = value.tcp.src_flags | value.tcp.dst_flags;
				if (flags & (uint8_t)common::fwstate::tcp_flags_e::ACK)
				{
					state_timeout = cfg.stateful_firewall_tcp_syn_ack_timeout;
				}
				else if (flags & (uint8_t)common::fwstate::tcp_flags_e::SYN)
				{
					state_timeout = cfg.stateful_firewall_tcp_syn_timeout;
				}
				if (flags & (uint8_t)common::fwstate::tcp_flags_e::FIN)
				{
					state_timeout = cfg.stateful_firewall_tcp_fin_timeout;
				}
			}
			value.state_timeout = state_timeout;

			for (auto& [socketId, globalBaseAtomic] : m_slow_worker->dataPlane->globalBaseAtomics)
			{
				(void)socketId;

				dataplane::globalBase::fw_state_value_t* lookup_value;
				dataplane::spinlock_nonrecursive_t* locker;
				const uint32_t hash = globalBaseAtomic->fw4_state->lookup(key, lookup_value, locker);
				if (lookup_value)
				{
					// Keep state alive for us even if there were no packets received.
					// Do not reset other counters.
					lookup_value->last_seen = m_slow_worker->CurrentTime();
					lookup_value->tcp.src_flags |= value.tcp.src_flags;
					lookup_value->tcp.dst_flags |= value.tcp.dst_flags;
					lookup_value->state_timeout = std::max(lookup_value->state_timeout, value.state_timeout);
				}
				else
				{
					globalBaseAtomic->fw4_state->insert(hash, key, value);
				}
				locker->unlock();
			}
		}
	}

	return true;
}

void SlowWorker::handlePacket_balancer_icmp_forward(rte_mbuf* mbuf)
{
	if (m_config.SWICMPOutRateLimit != 0)
	{
		if (m_icmp_out_remainder == 0)
		{
			m_slow_worker->IncrementCounter(common::globalBase::static_counter_type::balancer_icmp_out_rate_limit_reached);
			rte_pktmbuf_free(mbuf);
			return;
		}

		--m_icmp_out_remainder;
	}

	auto vip_to_balancers = m_slow_worker->dataPlane->controlPlane->vip_to_balancers.Accessor();
	auto vip_vport_proto = m_slow_worker->dataPlane->controlPlane->vip_vport_proto.Accessor();

	const auto& base = m_slow_worker->CurrentBase();

	dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

	common::ip_address_t original_src_from_icmp_payload;
	common::ip_address_t src_from_ip_header;
	uint16_t original_src_port_from_icmp_payload;

	uint32_t balancer_id = metadata->flow.data.balancer.id;

	dataplane::metadata inner_metadata;

	if (metadata->transport_headerType == IPPROTO_ICMP)
	{
		rte_ipv4_hdr* ipv4Header = rte_pktmbuf_mtod_offset(mbuf, rte_ipv4_hdr*, metadata->network_headerOffset);
		src_from_ip_header = common::ip_address_t(rte_be_to_cpu_32(ipv4Header->src_addr));

		rte_ipv4_hdr* icmpPayloadIpv4Header = rte_pktmbuf_mtod_offset(mbuf, rte_ipv4_hdr*, metadata->transport_headerOffset + sizeof(icmpv4_header_t));
		original_src_from_icmp_payload = common::ip_address_t(rte_be_to_cpu_32(icmpPayloadIpv4Header->src_addr));

		inner_metadata.network_headerType = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);
		inner_metadata.network_headerOffset = metadata->transport_headerOffset + sizeof(icmpv4_header_t);
	}
	else
	{
		rte_ipv6_hdr* ipv6Header = rte_pktmbuf_mtod_offset(mbuf, rte_ipv6_hdr*, metadata->network_headerOffset);
		src_from_ip_header = common::ip_address_t(ipv6Header->src_addr);

		rte_ipv6_hdr* icmpPayloadIpv6Header = rte_pktmbuf_mtod_offset(mbuf, rte_ipv6_hdr*, metadata->transport_headerOffset + sizeof(icmpv6_header_t));
		original_src_from_icmp_payload = common::ip_address_t(icmpPayloadIpv6Header->src_addr);

		inner_metadata.network_headerType = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV6);
		inner_metadata.network_headerOffset = metadata->transport_headerOffset + sizeof(icmpv6_header_t);
	}

	if (!prepareL3(mbuf, &inner_metadata))
	{
		/* we are not suppossed to get in here anyway, same check was done earlier by balancer_icmp_forward_handle(),
		   but we needed to call prepareL3() to determine icmp payload original packets transport header offset */
		if (inner_metadata.network_headerType == rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4))
		{
			m_slow_worker->IncrementCounter(common::globalBase::static_counter_type::balancer_icmp_drop_icmpv4_payload_too_short_ip);
		}
		else
		{
			m_slow_worker->IncrementCounter(common::globalBase::static_counter_type::balancer_icmp_drop_icmpv6_payload_too_short_ip);
		}

		rte_pktmbuf_free(mbuf);
		return;
	}

	if (inner_metadata.transport_headerType != IPPROTO_TCP && inner_metadata.transport_headerType != IPPROTO_UDP)
	{
		// not supported protocol for cloning and distributing, drop
		rte_pktmbuf_free(mbuf);
		m_slow_worker->IncrementCounter(common::globalBase::static_counter_type::balancer_icmp_drop_unexpected_transport_protocol);
		return;
	}

	// check whether ICMP payload is too short to contain "offending" packet's IP header and ports is performed earlier by balancer_icmp_forward_handle()
	void* icmpPayloadTransportHeader = rte_pktmbuf_mtod_offset(mbuf, void*, inner_metadata.transport_headerOffset);

	// both TCP and UDP headers have src port (16 bits) as the first field
	original_src_port_from_icmp_payload = rte_be_to_cpu_16(*(uint16_t*)icmpPayloadTransportHeader);

	if (vip_to_balancers->size() <= balancer_id)
	{
		// no vip_to_balancers table for this balancer_id
		rte_pktmbuf_free(mbuf);
		m_slow_worker->IncrementCounter(common::globalBase::static_counter_type::balancer_icmp_drop_no_unrdup_table_for_balancer_id);
		return;
	}

	if (!(*vip_to_balancers)[balancer_id].count(original_src_from_icmp_payload))
	{
		// vip is not listed in unrdup config - neighbor balancers are unknown, drop
		rte_pktmbuf_free(mbuf);
		m_slow_worker->IncrementCounter(common::globalBase::static_counter_type::balancer_icmp_drop_unrdup_vip_not_found);
		return;
	}

	if (vip_vport_proto->size() <= balancer_id)
	{
		// no vip_vport_proto table for this balancer_id
		rte_pktmbuf_free(mbuf);
		m_slow_worker->IncrementCounter(common::globalBase::static_counter_type::balancer_icmp_drop_no_vip_vport_proto_table_for_balancer_id);
		return;
	}

	if (!(*vip_vport_proto)[balancer_id].count({original_src_from_icmp_payload, original_src_port_from_icmp_payload, inner_metadata.transport_headerType}))
	{
		// such combination of vip-vport-protocol is absent, don't clone, drop
		rte_pktmbuf_free(mbuf);
		m_slow_worker->IncrementCounter(common::globalBase::static_counter_type::balancer_icmp_drop_unknown_service);
		return;
	}

	const auto& neighbor_balancers = (*vip_to_balancers)[balancer_id][original_src_from_icmp_payload];

	for (const auto& neighbor_balancer : neighbor_balancers)
	{
		// will not send a cloned packet if source address in "balancer" section of controlplane.conf is absent
		if (neighbor_balancer.is_ipv4() && !base.globalBase->balancers[metadata->flow.data.balancer.id].source_ipv4.address)
		{
			m_slow_worker->IncrementCounter(common::globalBase::static_counter_type::balancer_icmp_no_balancer_src_ipv4);
			continue;
		}

		if (neighbor_balancer.is_ipv6() && base.globalBase->balancers[metadata->flow.data.balancer.id].source_ipv6.empty())
		{
			m_slow_worker->IncrementCounter(common::globalBase::static_counter_type::balancer_icmp_no_balancer_src_ipv6);
			continue;
		}

		rte_mbuf* mbuf_clone = rte_pktmbuf_alloc(m_mempool);
		if (mbuf_clone == nullptr)
		{
			m_slow_worker->IncrementCounter(common::globalBase::static_counter_type::balancer_icmp_failed_to_clone);
			continue;
		}

		*YADECAP_METADATA(mbuf_clone) = *YADECAP_METADATA(mbuf);
		dataplane::metadata* clone_metadata = YADECAP_METADATA(mbuf_clone);

		rte_memcpy(rte_pktmbuf_mtod(mbuf_clone, char*),
		           rte_pktmbuf_mtod(mbuf, char*),
		           mbuf->data_len);

		if (neighbor_balancer.is_ipv4())
		{
			rte_pktmbuf_prepend(mbuf_clone, sizeof(rte_ipv4_hdr));
			memmove(rte_pktmbuf_mtod(mbuf_clone, char*),
			        rte_pktmbuf_mtod_offset(mbuf_clone, char*, sizeof(rte_ipv4_hdr)),
			        clone_metadata->network_headerOffset);

			rte_ipv4_hdr* outerIpv4Header = rte_pktmbuf_mtod_offset(mbuf_clone, rte_ipv4_hdr*, clone_metadata->network_headerOffset);

			outerIpv4Header->src_addr = base.globalBase->balancers[metadata->flow.data.balancer.id].source_ipv4.address;
			outerIpv4Header->dst_addr = rte_cpu_to_be_32(neighbor_balancer.get_ipv4());

			outerIpv4Header->version_ihl = 0x45;
			outerIpv4Header->type_of_service = 0x00;
			outerIpv4Header->packet_id = rte_cpu_to_be_16(0x01);
			outerIpv4Header->fragment_offset = 0;
			outerIpv4Header->time_to_live = 64;

			outerIpv4Header->total_length = rte_cpu_to_be_16((uint16_t)(mbuf->pkt_len - clone_metadata->network_headerOffset + sizeof(rte_ipv4_hdr)));

			if (metadata->network_headerType == rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4))
			{
				outerIpv4Header->next_proto_id = IPPROTO_IPIP;
			}
			else
			{
				outerIpv4Header->next_proto_id = IPPROTO_IPV6;
			}

			yanet_ipv4_checksum(outerIpv4Header);

			mbuf_clone->data_len = mbuf->data_len + sizeof(rte_ipv4_hdr);
			mbuf_clone->pkt_len = mbuf->pkt_len + sizeof(rte_ipv4_hdr);

			// might need to change next protocol type in ethernet/vlan header in cloned packet

			rte_ether_hdr* ethernetHeader = rte_pktmbuf_mtod(mbuf_clone, rte_ether_hdr*);
			if (ethernetHeader->ether_type == rte_cpu_to_be_16(RTE_ETHER_TYPE_VLAN))
			{
				rte_vlan_hdr* vlanHeader = rte_pktmbuf_mtod_offset(mbuf_clone, rte_vlan_hdr*, sizeof(rte_ether_hdr));
				vlanHeader->eth_proto = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);
			}
			else
			{
				ethernetHeader->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);
			}
		}
		else if (neighbor_balancer.is_ipv6())
		{
			rte_pktmbuf_prepend(mbuf_clone, sizeof(rte_ipv6_hdr));
			memmove(rte_pktmbuf_mtod(mbuf_clone, char*),
			        rte_pktmbuf_mtod_offset(mbuf_clone, char*, sizeof(rte_ipv6_hdr)),
			        clone_metadata->network_headerOffset);

			rte_ipv6_hdr* outerIpv6Header = rte_pktmbuf_mtod_offset(mbuf_clone, rte_ipv6_hdr*, clone_metadata->network_headerOffset);

			rte_memcpy(outerIpv6Header->src_addr, base.globalBase->balancers[metadata->flow.data.balancer.id].source_ipv6.bytes, sizeof(outerIpv6Header->src_addr));
			if (src_from_ip_header.is_ipv6())
			{
				((uint32_t*)outerIpv6Header->src_addr)[2] = ((uint32_t*)src_from_ip_header.get_ipv6().data())[2] ^ ((uint32_t*)src_from_ip_header.get_ipv6().data())[3];
			}
			else
			{
				((uint32_t*)outerIpv6Header->src_addr)[2] = src_from_ip_header.get_ipv4();
			}
			rte_memcpy(outerIpv6Header->dst_addr, neighbor_balancer.get_ipv6().data(), sizeof(outerIpv6Header->dst_addr));

			outerIpv6Header->vtc_flow = rte_cpu_to_be_32((0x6 << 28));
			outerIpv6Header->payload_len = rte_cpu_to_be_16((uint16_t)(mbuf->pkt_len - clone_metadata->network_headerOffset));
			outerIpv6Header->hop_limits = 64;

			if (metadata->network_headerType == rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4))
			{
				outerIpv6Header->proto = IPPROTO_IPIP;
			}
			else
			{
				outerIpv6Header->proto = IPPROTO_IPV6;
			}

			mbuf_clone->data_len = mbuf->data_len + sizeof(rte_ipv6_hdr);
			mbuf_clone->pkt_len = mbuf->pkt_len + sizeof(rte_ipv6_hdr);

			// might need to change next protocol type in ethernet/vlan header in cloned packet

			rte_ether_hdr* ethernetHeader = rte_pktmbuf_mtod(mbuf_clone, rte_ether_hdr*);
			if (ethernetHeader->ether_type == rte_cpu_to_be_16(RTE_ETHER_TYPE_VLAN))
			{
				rte_vlan_hdr* vlanHeader = rte_pktmbuf_mtod_offset(mbuf_clone, rte_vlan_hdr*, sizeof(rte_ether_hdr));
				vlanHeader->eth_proto = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV6);
			}
			else
			{
				ethernetHeader->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV6);
			}
		}

		m_slow_worker->IncrementCounter(common::globalBase::static_counter_type::balancer_icmp_clone_forwarded);

		const auto& flow = base.globalBase->balancers[metadata->flow.data.balancer.id].flow;

		m_slow_worker->preparePacket(mbuf_clone);
		sendPacketToSlowWorker(mbuf_clone, flow);
	}

	// packet itself is not going anywhere, only its clones with prepended header
	rte_pktmbuf_free(mbuf);
}

void SlowWorker::DequeueGC()
{
	for (worker_gc_t* worker_gc : m_gcs_to_service)
	{
		rte_mbuf* mbufs[CONFIG_YADECAP_MBUFS_BURST_SIZE];

		unsigned rxSize = rte_ring_sc_dequeue_burst(worker_gc->ring_to_slowworker,
		                                            (void**)mbufs,
		                                            CONFIG_YADECAP_MBUFS_BURST_SIZE,
		                                            nullptr);

		for (uint16_t mbuf_i = 0; mbuf_i < rxSize; mbuf_i++)
		{
			rte_mbuf* mbuf = convertMempool(worker_gc->ring_to_free_mbuf, mbufs[mbuf_i]);
			if (!mbuf)
			{
				continue;
			}

			dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

			sendPacketToSlowWorker(mbuf, metadata->flow);
		}
	}
}

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

} // namespace dataplane
