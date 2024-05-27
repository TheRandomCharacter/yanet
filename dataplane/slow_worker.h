#pragma once
#include <queue>
#include <tuple>

#include "common/fallback.h"
#include "icmp.h"
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

public:
	SlowWorker(cWorker* worker,
	           const std::vector<tPortId>& ports_to_service,
	           rte_mempool* mempool,
	           pthread_barrier_t* run) :
	        m_run_barrier(run),
	        m_ports_serviced(ports_to_service),
	        m_slow_worker(worker),
	        m_mempool(mempool)
	{
	}

	cWorker* GetWorker() { return m_slow_worker; }
	void freeWorkerPacket(rte_ring* ring_to_free_mbuf,
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
	rte_mbuf* convertMempool(rte_ring* ring_to_free_mbuf,
	                         rte_mbuf* old_mbuf)
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
	void sendPacketToSlowWorker(rte_mbuf* mbuf,
	                            const common::globalBase::tFlow& flow)
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

	unsigned ring_handle(rte_ring* ring_to_free_mbuf,
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
				handlePacket_dump(mbuf);
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

	static bool do_icmp_translate_v6_to_v4(rte_mbuf* mbuf,
	                                       const dataplane::globalBase::nat64stateless_translation_t& translation)
	{
		dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

		rte_ipv4_hdr* ipv4ExtHeader = rte_pktmbuf_mtod_offset(mbuf, rte_ipv4_hdr*, metadata->network_headerOffset);

		if (ipv4ExtHeader->next_proto_id != IPPROTO_ICMP)
		{
			return false;
		}

		unsigned int payloadExtLen = rte_be_to_cpu_16(ipv4ExtHeader->total_length) - sizeof(rte_ipv4_hdr);

		if (payloadExtLen < sizeof(icmp_header_t*) + sizeof(rte_ipv6_hdr))
		{
			return false;
		}

		icmp_header_t* icmpHeader = rte_pktmbuf_mtod_offset(mbuf, icmp_header_t*, metadata->network_headerOffset + sizeof(rte_ipv4_hdr));
		uint8_t type = icmpHeader->type;
		uint8_t code = icmpHeader->code;

		if (type == ICMP6_DST_UNREACH)
		{
			type = ICMP_DEST_UNREACH;

			if (code == ICMP6_DST_UNREACH_NOROUTE || code == ICMP6_DST_UNREACH_BEYONDSCOPE ||
			    code == ICMP6_DST_UNREACH_ADDR)
			{
				code = ICMP_HOST_UNREACH;
			}
			else if (code == ICMP6_DST_UNREACH_ADMIN)
			{
				code = ICMP_HOST_ANO;
			}
			else if (code == ICMP6_DST_UNREACH_NOPORT)
			{
				code = ICMP_PORT_UNREACH;
			}
			else
			{
				return false;
			}
		}
		else if (type == ICMP6_PACKET_TOO_BIG)
		{
			type = ICMP_DEST_UNREACH;
			code = ICMP_FRAG_NEEDED;

			uint32_t mtu = rte_be_to_cpu_32(icmpHeader->data32[0]) - 20;
			icmpHeader->data32[0] = 0;
			icmpHeader->data16[1] = rte_cpu_to_be_16(mtu);
		}
		else if (type == ICMP6_TIME_EXCEEDED)
		{
			type = ICMP_TIME_EXCEEDED;
		}
		else if (type == ICMP6_PARAM_PROB)
		{

			if (code == ICMP6_PARAMPROB_HEADER)
			{
				type = ICMP_PARAMETERPROB;
				code = 0;
				uint32_t ptr = rte_be_to_cpu_32(icmpHeader->data32[0]);
				icmpHeader->data32[0] = 0;

				if (ptr == 0 || ptr == 1)
				{
					/// unchanged
				}
				else if (ptr == 4 || ptr == 5)
				{
					ptr = 2;
				}
				else if (ptr == 6)
				{
					ptr = 9;
				}
				else if (ptr == 7)
				{
					ptr = 8;
				}
				else if (ptr >= 8 && ptr < 24)
				{
					ptr = 12;
				}
				else if (ptr >= 24 && ptr < 40)
				{
					ptr = 16;
				}
				else
				{
					return false;
				}

				icmpHeader->data8[0] = ptr;
			}
			else if (code == ICMP6_PARAMPROB_NEXTHEADER)
			{
				type = ICMP_DEST_UNREACH;
				code = ICMP_PROT_UNREACH;
				icmpHeader->data32[0] = 0;
			}
		}
		else
		{
			return false;
		}

		icmpHeader->type = type;
		icmpHeader->code = code;

		rte_ipv6_hdr* ipv6PayloadHeader = rte_pktmbuf_mtod_offset(mbuf, rte_ipv6_hdr*, metadata->network_headerOffset + sizeof(rte_ipv4_hdr) + sizeof(icmp_header_t));

		/// @todo: think about it.
		if (memcmp(ipv6PayloadHeader->dst_addr, translation.ipv6Address.bytes, 16))
		{
			return false;
		}

		if (memcmp(ipv6PayloadHeader->src_addr, translation.ipv6DestinationAddress.bytes, 12))
		{
			return false;
		}

		uint32_t addressSource = *(uint32_t*)&ipv6PayloadHeader->src_addr[12];

		if (addressSource != ipv4ExtHeader->dst_addr)
		{
			return false;
		}

		uint32_t addressDestination = *(uint32_t*)&ipv6PayloadHeader->dst_addr[12];
		addressDestination = translation.ipv4Address.address;

		uint16_t checksum6 = yanet_checksum(&ipv6PayloadHeader->src_addr[0], 32);
		uint16_t payloadLength = rte_be_to_cpu_16(ipv6PayloadHeader->payload_len);

		unsigned int ipv6PayloadHeaderSize = sizeof(rte_ipv6_hdr);

		uint16_t packet_id = 0x3421; ///< @todo: nat64statelessPacketId;
		uint16_t fragment_offset = 0; ///< @todo: rte_cpu_to_be_16(RTE_IPV4_HDR_DF_FLAG);
		uint8_t nextPayloadHeader = ipv6PayloadHeader->proto;

		if (nextPayloadHeader == IPPROTO_FRAGMENT)
		{
			if (payloadExtLen < sizeof(icmp_header_t*) + sizeof(rte_ipv6_hdr) + sizeof(tIPv6ExtensionFragment*))
			{
				return false;
			}
			tIPv6ExtensionFragment* extension = (tIPv6ExtensionFragment*)(((char*)ipv6PayloadHeader) + ipv6PayloadHeaderSize);
			packet_id = static_cast<uint16_t>(extension->identification >> 16);
			fragment_offset = rte_cpu_to_be_16(rte_be_to_cpu_16(extension->offsetFlagM) >> 3);
			fragment_offset |= (extension->offsetFlagM & 0x0100) >> 3;

			nextPayloadHeader = extension->nextHeader;

			ipv6PayloadHeaderSize += 8;
		}

		if (nextPayloadHeader == IPPROTO_HOPOPTS ||
		    nextPayloadHeader == IPPROTO_ROUTING ||
		    nextPayloadHeader == IPPROTO_FRAGMENT ||
		    nextPayloadHeader == IPPROTO_NONE ||
		    nextPayloadHeader == IPPROTO_DSTOPTS ||
		    nextPayloadHeader == IPPROTO_MH)
		{
			/// @todo: ipv6 extensions

			return false;
		}

		if (nextPayloadHeader == IPPROTO_ICMPV6)
		{
			nextPayloadHeader = IPPROTO_ICMP;
		}

		rte_ipv4_hdr* ipv4PayloadHeader = (rte_ipv4_hdr*)((char*)ipv6PayloadHeader + ipv6PayloadHeaderSize - sizeof(rte_ipv4_hdr));

		ipv4PayloadHeader->version_ihl = 0x45;
		ipv4PayloadHeader->type_of_service = (rte_be_to_cpu_32(ipv6PayloadHeader->vtc_flow) >> 20) & 0xFF;
		ipv4PayloadHeader->total_length = rte_cpu_to_be_16(payloadLength + 20 - (ipv6PayloadHeaderSize - 40));
		ipv4PayloadHeader->packet_id = packet_id;
		ipv4PayloadHeader->fragment_offset = fragment_offset;
		ipv4PayloadHeader->time_to_live = ipv6PayloadHeader->hop_limits;
		ipv4PayloadHeader->next_proto_id = nextPayloadHeader;
		ipv4PayloadHeader->src_addr = addressSource;
		ipv4PayloadHeader->dst_addr = addressDestination;

		yanet_ipv4_checksum(ipv4PayloadHeader);

		uint16_t checksum4 = yanet_checksum(&ipv4PayloadHeader->src_addr, 8);

		{
			unsigned int delta = ipv6PayloadHeaderSize - sizeof(rte_ipv4_hdr);

			memcpy(rte_pktmbuf_mtod_offset(mbuf, char*, delta),
			       rte_pktmbuf_mtod(mbuf, char*),
			       metadata->network_headerOffset + sizeof(rte_ipv4_hdr) + sizeof(icmp_header_t));

			rte_pktmbuf_adj(mbuf, delta);

			icmpHeader = (icmp_header_t*)((char*)icmpHeader + delta);
			ipv4ExtHeader = (rte_ipv4_hdr*)((char*)ipv4ExtHeader + delta);

			uint16_t csum = ~ipv4ExtHeader->hdr_checksum;
			csum = csum_minus(csum, ipv4ExtHeader->total_length);
			ipv4ExtHeader->total_length = rte_cpu_to_be_16(rte_be_to_cpu_16(ipv4ExtHeader->total_length) - delta);
			csum = csum_plus(csum, ipv4ExtHeader->total_length);
			ipv4ExtHeader->hdr_checksum = (csum == 0xffff) ? csum : ~csum;
		}

		if ((fragment_offset & 0xFF1F) == 0)
		{
			if (nextPayloadHeader == IPPROTO_TCP)
			{
				/// @todo: check packet size

				rte_tcp_hdr* tcpPayloadHeader = (rte_tcp_hdr*)((char*)ipv4PayloadHeader + sizeof(rte_ipv4_hdr));
				yanet_tcp_checksum_v6_to_v4(tcpPayloadHeader, checksum6, checksum4);
			}
			else if (nextPayloadHeader == IPPROTO_UDP)
			{
				/// @todo: check packet size

				rte_udp_hdr* udpPayloadHeader = (rte_udp_hdr*)((char*)ipv4PayloadHeader + sizeof(rte_ipv4_hdr));
				yanet_udp_checksum_v6_to_v4(udpPayloadHeader, checksum6, checksum4);
			}
			else if (nextPayloadHeader == IPPROTO_ICMP)
			{
				/// @todo: check packet size

				icmp_header_t* icmpPayloadHeader = (icmp_header_t*)((char*)ipv4PayloadHeader + sizeof(rte_ipv4_hdr));

				if ((fragment_offset & 0xFF3F) != 0 ||
				    !yanet_icmp_translate_v6_to_v4(icmpPayloadHeader,
				                                   payloadLength,
				                                   checksum6))
				{
					return false;
				}
			}
			else
			{
				return false;
			}
		}

		icmpHeader->checksum = 0;
		uint32_t sum = __rte_raw_cksum(icmpHeader, sizeof(icmp_header_t) + sizeof(rte_ipv4_hdr) + payloadLength, 0);
		icmpHeader->checksum = ~__rte_raw_cksum_reduce(sum);

		return true;
	}

	void handlePacket_icmp_translate_v6_to_v4(rte_mbuf* mbuf)
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

	static bool do_icmp_translate_v4_to_v6(rte_mbuf* mbuf,
	                                       const dataplane::globalBase::nat64stateless_translation_t& translation)
	{
		dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

		rte_ipv6_hdr* ipv6ExtHeader = rte_pktmbuf_mtod_offset(mbuf, rte_ipv6_hdr*, metadata->network_headerOffset);

		if (ipv6ExtHeader->proto != IPPROTO_ICMPV6)
		{
			return false;
		}

		unsigned int ipv6ExtHeaderSize = sizeof(rte_ipv6_hdr);

		unsigned int payloadLen = rte_be_to_cpu_16(ipv6ExtHeader->payload_len);

		if (payloadLen < sizeof(icmp_header_t) + sizeof(rte_ipv4_hdr))
		{
			return false;
		}

		icmp_header_t* icmpHeader = rte_pktmbuf_mtod_offset(mbuf, icmp_header_t*, metadata->network_headerOffset + ipv6ExtHeaderSize);
		uint8_t type = icmpHeader->type;
		uint8_t code = icmpHeader->code;

		if (type == ICMP_DEST_UNREACH)
		{
			type = ICMP6_DST_UNREACH;

			if (code == ICMP_NET_UNREACH || code == ICMP_HOST_UNREACH ||
			    code == ICMP_SR_FAILED || code == ICMP_NET_UNKNOWN ||
			    code == ICMP_HOST_UNKNOWN || code == ICMP_HOST_ISOLATED ||
			    code == ICMP_NET_UNR_TOS || code == ICMP_HOST_UNR_TOS)
			{
				code = ICMP6_DST_UNREACH_NOROUTE;
			}
			else if (code == ICMP_PORT_UNREACH)
			{
				code = ICMP6_DST_UNREACH_NOPORT;
			}
			else if (code == ICMP_NET_ANO || code == ICMP_HOST_ANO ||
			         code == ICMP_PKT_FILTERED || code == ICMP_PREC_CUTOFF)
			{
				code = ICMP6_DST_UNREACH_ADMIN;
			}
			else if (code == ICMP_PROT_UNREACH)
			{
				type = ICMP6_PARAM_PROB;
				code = ICMP6_PARAMPROB_NEXTHEADER;

				icmpHeader->data32[0] = rte_cpu_to_be_32(6);
			}
			else if (code == ICMP_FRAG_NEEDED)
			{
				type = ICMP6_PACKET_TOO_BIG;
				code = 0;

				uint32_t mtu = rte_be_to_cpu_16(icmpHeader->data16[1]) + 20;
				if (mtu < 1280)
				{
					mtu = 1280;
				}

				icmpHeader->data32[0] = rte_cpu_to_be_32(mtu);
			}
			else
			{
				return false;
			}
		}
		else if (type == ICMP_TIME_EXCEEDED)
		{
			type = ICMP6_TIME_EXCEEDED;
		}
		else if (type == ICMP_PARAMETERPROB)
		{
			if (code != 0 && code != 2)
			{
				return false;
			}

			uint8_t ptr = icmpHeader->data8[0];

			if (ptr == 0 || ptr == 1)
			{
				/// unchanged
			}
			else if (ptr == 2 || ptr == 3)
			{
				ptr = 4;
			}
			else if (ptr == 8)
			{
				ptr = 7;
			}
			else if (ptr == 9)
			{
				ptr = 6;
			}
			else if (ptr >= 12 && ptr < 16)
			{
				ptr = 8;
			}
			else if (ptr >= 16 && ptr < 20)
			{
				ptr = 24;
			}
			else
			{
				return false;
			}

			type = ICMP6_PARAM_PROB;
			code = ICMP6_PARAMPROB_HEADER;

			icmpHeader->data32[0] = rte_cpu_to_be_32(ptr);
		}
		else
		{
			return false;
		}

		icmpHeader->type = type;
		icmpHeader->code = code;

		rte_ipv4_hdr* ipv4PayloadHeader = rte_pktmbuf_mtod_offset(mbuf, rte_ipv4_hdr*, metadata->network_headerOffset + ipv6ExtHeaderSize + sizeof(icmp_header_t));

		/// @todo: check ipv4 payload header length

		if (ipv4PayloadHeader->src_addr != translation.ipv4Address.address)
		{
			return false;
		}

		unsigned int ipv6PayloadHeaderSize = sizeof(rte_ipv6_hdr);

		if ((ipv4PayloadHeader->fragment_offset & 0xFF3F) != 0)
		{
			ipv6PayloadHeaderSize += 8;
		}

		{
			unsigned int delta = ipv6PayloadHeaderSize - sizeof(rte_ipv4_hdr);

			rte_pktmbuf_prepend(mbuf, delta);

			memcpy(rte_pktmbuf_mtod(mbuf, char*),
			       rte_pktmbuf_mtod_offset(mbuf, char*, delta),
			       metadata->network_headerOffset + ipv6ExtHeaderSize + sizeof(icmp_header_t));

			icmpHeader = (icmp_header_t*)((char*)icmpHeader - delta);
			ipv6ExtHeader = (rte_ipv6_hdr*)((char*)ipv6ExtHeader - delta);
			payloadLen += delta;
			ipv6ExtHeader->payload_len = rte_cpu_to_be_16(payloadLen);
		}

		rte_ipv6_hdr* ipv6PayloadHeader = rte_pktmbuf_mtod_offset(mbuf, rte_ipv6_hdr*, metadata->network_headerOffset + ipv6ExtHeaderSize + sizeof(icmp_header_t));

		uint32_t addressDestination = ipv4PayloadHeader->dst_addr;
		uint16_t checksum4 = yanet_checksum(&ipv4PayloadHeader->src_addr, 8);

		uint16_t fragment_offset = ipv4PayloadHeader->fragment_offset;
		uint8_t nextPayloadHeader = ipv4PayloadHeader->next_proto_id;

		if (nextPayloadHeader == IPPROTO_ICMP)
		{
			nextPayloadHeader = IPPROTO_ICMPV6;
		}

		if ((fragment_offset & 0xFF3F) != 0)
		{
			tIPv6ExtensionFragment* extension = (tIPv6ExtensionFragment*)(((char*)ipv6PayloadHeader) + sizeof(rte_ipv6_hdr));
			extension->nextHeader = nextPayloadHeader;
			extension->reserved = 0;
			extension->offsetFlagM = rte_cpu_to_be_16(rte_be_to_cpu_16(fragment_offset) << 3);
			extension->offsetFlagM |= (fragment_offset & 0x0020) << 3;

			/// @todo:  it is not original identification.
			extension->identification = ipv4PayloadHeader->packet_id;

			ipv6PayloadHeader->proto = IPPROTO_FRAGMENT;
		}
		else
		{
			ipv6PayloadHeader->proto = nextPayloadHeader;
		}

		ipv6PayloadHeader->vtc_flow = rte_cpu_to_be_32((0x6 << 28) | (ipv4PayloadHeader->type_of_service << 20));
		ipv6PayloadHeader->payload_len = rte_cpu_to_be_16(rte_be_to_cpu_16(ipv4PayloadHeader->total_length) - 20 + (ipv6PayloadHeaderSize - 40));
		ipv6PayloadHeader->hop_limits = ipv4PayloadHeader->time_to_live;
		;

		memcpy(ipv6PayloadHeader->src_addr, translation.ipv6Address.bytes, 16);

		if (memcmp(ipv6PayloadHeader->src_addr, ipv6ExtHeader->dst_addr, 16))
		{
			return false;
		}

		memcpy(&ipv6PayloadHeader->dst_addr[0], translation.ipv6DestinationAddress.bytes, 12);
		memcpy(&ipv6PayloadHeader->dst_addr[12], &addressDestination, 4);

		uint16_t checksum6 = yanet_checksum(&ipv6PayloadHeader->src_addr[0], 32);

		if ((fragment_offset & 0xFF1F) == 0)
		{
			if (nextPayloadHeader == IPPROTO_TCP)
			{
				/// @todo: check packet size

				rte_tcp_hdr* tcpPayloadHeader = (rte_tcp_hdr*)((char*)ipv6PayloadHeader + ipv6PayloadHeaderSize);
				yanet_tcp_checksum_v4_to_v6(tcpPayloadHeader, checksum4, checksum6);
			}
			else if (nextPayloadHeader == IPPROTO_UDP)
			{
				/// @todo: check packet size

				rte_udp_hdr* udpPayloadHeader = (rte_udp_hdr*)((char*)ipv6PayloadHeader + ipv6PayloadHeaderSize);
				yanet_udp_checksum_v4_to_v6(udpPayloadHeader, checksum4, checksum6);
			}
			else if (nextPayloadHeader == IPPROTO_ICMPV6)
			{
				/// @todo: check packet size

				icmp_header_t* icmpPayloadHeader = (icmp_header_t*)((char*)ipv6PayloadHeader + ipv6PayloadHeaderSize);

				if ((fragment_offset & 0xFF3F) != 0 ||
				    !yanet_icmp_translate_v4_to_v6(icmpPayloadHeader,
				                                   rte_be_to_cpu_16(ipv6PayloadHeader->payload_len),
				                                   checksum6))
				{
					return false;
				}
			}
			else
			{
				return false;
			}
		}

		icmpHeader->checksum = 0;
		uint32_t sum = __rte_raw_cksum(ipv6ExtHeader->src_addr, 16, 0);
		sum = __rte_raw_cksum(ipv6ExtHeader->dst_addr, 16, sum);

		uint32_t tmp = ((uint32_t)rte_cpu_to_be_16(IPPROTO_ICMPV6) << 16) + rte_cpu_to_be_16(payloadLen);
		sum = __rte_raw_cksum(&tmp, 4, sum);
		sum = __rte_raw_cksum(icmpHeader, payloadLen, sum);

		icmpHeader->checksum = ~__rte_raw_cksum_reduce(sum);

		return true;
	}

	void handlePacket_icmp_translate_v4_to_v6(rte_mbuf* mbuf)
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

	void handlePacket_fragment(rte_mbuf* mbuf)
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

	void handlePacket_dregress(rte_mbuf* mbuf)
	{
		m_dregress.insert(mbuf);
	}

	void handlePacket_farm(rte_mbuf* mbuf)
	{
		m_stats.farm_packets++;
		m_slow_worker->slowWorkerFarmHandleFragment(mbuf);
	}

	void handlePacket_dump(rte_mbuf* mbuf)
	{
		dataplane::metadata* metadata = YADECAP_METADATA(mbuf);

		const auto& portmapper = m_slow_worker->basePermanently.ports;
		if (!portmapper.ValidDpdk(metadata->flow.data.dump.id))
		{
			m_stats.unknown_dump_interface++;
			rte_pktmbuf_free(mbuf);
			return;
		}
		const auto local_port_id = portmapper.ToLogical(metadata->flow.data.dump.id);

		auto push = [this, mbuf](KniPortData& iface) {
			if (iface.mbufs_count == CONFIG_YADECAP_MBUFS_BURST_SIZE)
			{
				flush_kernel_interface(iface);
				iface.mbufs[iface.mbufs_count++] = mbuf;
			}
		};

		if (metadata->flow.data.dump.type == common::globalBase::dump_type_e::physicalPort_ingress)
		{
			push(in_dump_kernel_interfaces[local_port_id]);
			return;
		}
		else if (metadata->flow.data.dump.type == common::globalBase::dump_type_e::physicalPort_egress)
		{
			push(out_dump_kernel_interfaces[local_port_id]);
			return;
		}
		else if (metadata->flow.data.dump.type == common::globalBase::dump_type_e::physicalPort_drop)
		{
			push(drop_dump_kernel_interfaces[local_port_id]);
			return;
		}

		m_stats.unknown_dump_interface++;
		rte_pktmbuf_free(mbuf);
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

	bool handlePacket_fw_state_sync_ingress(rte_mbuf* mbuf)
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

	void handlePacket_balancer_icmp_forward(rte_mbuf* mbuf)
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
			stats.slowworker_drops++;
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
			const auto& portmapper = m_slow_worker->basePermanently.ports;

			auto& iface = kernel_interfaces[portmapper.ToLogical(metadata->fromPortId)];
			if (iface.mbufs_count == CONFIG_YADECAP_MBUFS_BURST_SIZE)
			{
				flush_kernel_interface(iface, kernel_stats[portmapper.ToLogical(metadata->fromPortId)]);
				iface.mbufs[iface.mbufs_count++] = mbuf;
			}
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
	void DequeueGC()
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

	void Iteration()
	{
		YANET_LOG_ERROR("slow worker iteration\n");
		m_slow_worker->slowWorkerBeforeHandlePackets();

		HandleWorkerRings();

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