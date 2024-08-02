#pragma once

#include "common/type.h"
#include "common/utils.h"
#include "common/variant_trait_map.h"

namespace common
{

namespace acl
{
class dump_t
{
public:
	dump_t() :
	        dump_id(0),
	        dump_tag("")
	{}

	dump_t(std::string dump_tag) :
	        dump_id(0),
	        dump_tag(std::move(dump_tag))
	{}

	bool operator==(const dump_t& o) const
	{
		return std::tie(dump_id, dump_tag) ==
		       std::tie(o.dump_id, o.dump_tag);
	}

	bool operator!=(const dump_t& o) const
	{
		return !operator==(o);
	}

	constexpr bool operator<(const dump_t& o) const
	{
		return std::tie(dump_id, dump_tag) <
		       std::tie(o.dump_id, o.dump_tag);
	}

	void pop(stream_in_t& stream)
	{
		stream.pop(dump_id);
		stream.pop(dump_tag);
	}

	void push(stream_out_t& stream) const
	{
		stream.push(dump_id);
		stream.push(dump_tag);
	}

	uint64_t dump_id;
	std::string dump_tag;
};

struct check_state_t
{
	// Unique identifier for hash calculation
	static constexpr int64_t HASH_IDENTIFIER = 12345;

	bool operator==([[maybe_unused]] const check_state_t& o) const
	{
		return true; // TODO: why do we need this operator?
	}

	constexpr bool operator<([[maybe_unused]] const check_state_t& o) const
	{
		return true; // TODO: why do we need this operator?
	}
};

struct state_timeout_t
{
	state_timeout_t() :
	        timeout(0)
	{}

	state_timeout_t(uint32_t timeout) :
	        timeout(timeout)
	{}

	bool operator==(const state_timeout_t& o) const
	{
		return timeout == o.timeout;
	}

	bool operator!=(const state_timeout_t& o) const
	{
		return !operator==(o);
	}

	constexpr bool operator<(const state_timeout_t& o) const
	{
		return timeout < o.timeout;
	}

	void pop(stream_in_t& stream)
	{
		stream.pop(timeout);
	}

	void push(stream_out_t& stream) const
	{
		stream.push(timeout);
	}

	uint32_t timeout;
};

} // namespace acl

// TODO: When rewriting the current ACL library into LibFilter, we could consider using inheritance.
// All "action" classes should implement some operators and pop/push methods, so it's beneficial to enforce this
// at the language level. However, if the same structures are used in the dataplane, it could impact performance
// due to extra pointer dereferences in vtable, if compiler does not succeed in devirtualization.
// Anyway, this is something to think about.

/**
 * @brief Represents an action that dumps packets to a specified ring.
 */
struct DumpAction final
{
	// Maximum count of DumpAction objects allowed.
	static constexpr size_t MAX_COUNT = YANET_CONFIG_DUMP_ID_SIZE;
	// The identifier for the dump ring.
	uint64_t dump_id;

	DumpAction(const acl::dump_t& dump_action) :
	        dump_id(dump_action.dump_id){};

	DumpAction() :
	        dump_id(0){};

	[[nodiscard]] bool terminating() const { return false; }

	void pop(stream_in_t& stream)
	{
		stream.pop(dump_id);
	}

	void push(stream_out_t& stream) const
	{
		stream.push(dump_id);
	}

	[[nodiscard]] std::string to_string() const
	{
		std::ostringstream oss;
		oss << "DumpAction(dump_id=" << dump_id << ")";
		return oss.str();
	}
};

/**
 * @brief Represents an action that processes a packet flow.
 *
 * Only one FlowAction is allowed in an Actions object.
 */
struct FlowAction final
{
	// Maximum count of FlowAction objects allowed.
	static constexpr size_t MAX_COUNT = 1;
	// The flow associated with this action.
	globalBase::tFlow flow;
	// Timeout for the state
	std::optional<uint32_t> timeout;

	FlowAction(const globalBase::tFlow& flow) :
	        flow(flow){};

	FlowAction(globalBase::tFlow&& flow) :
	        flow(std::move(flow)) {}

	FlowAction() :
	        flow(globalBase::tFlow()) {}

	[[nodiscard]] bool terminating() const { return true; }

	void pop(stream_in_t& stream)
	{
		stream.pop(flow);
		stream.pop(timeout);
	}

	void push(stream_out_t& stream) const
	{
		stream.push(flow);
		stream.push(timeout);
	}

	[[nodiscard]] std::string to_string() const
	{
		std::ostringstream oss;
		oss << "FlowAction(flow=" << flow.to_string() << ", timeout = "
		    << (timeout.has_value() ? std::to_string(timeout.value()) : "not specified")
		    << ")";
		return oss.str();
	}
};

/**
 * @brief Represents an action that checks the dynamic firewall state.
 *
 * This class doesn't have any specific info to store,
 * because check-state rule doesn't need anything.
 */
struct CheckStateAction final
{
	static constexpr size_t MAX_COUNT = 1;

	CheckStateAction(const acl::check_state_t&){};
	CheckStateAction() = default;

	[[nodiscard]] bool terminating() const { return false; }

	void pop(stream_in_t& stream)
	{
		stream.pop(reinterpret_cast<uint8_t(&)[sizeof(*this)]>(*this));
	}

	void push(stream_out_t& stream) const
	{
		stream.push(reinterpret_cast<const uint8_t(&)[sizeof(*this)]>(*this));
	}

	[[nodiscard]] std::string to_string() const
	{
		return "CheckStateAction()";
	}
};

/**
 * @brief Represents an action that sets timeout for the dynamic firewall rule.
 */
struct StateTimeoutAction final
{
	// Maximum count of StateTimeoutActions objects allowed.
	// We have one here since only the last timeout matters.
	static constexpr size_t MAX_COUNT = 1;
	// Timeout in seconds
	uint32_t timeout;

	StateTimeoutAction(const acl::state_timeout_t& timeout_action) :
	        timeout(timeout_action.timeout){};

	StateTimeoutAction() :
	        timeout(0){};

	[[nodiscard]] bool terminating() const { return false; }

	void pop(stream_in_t& stream)
	{
		stream.pop(timeout);
	}

	void push(stream_out_t& stream) const
	{
		stream.push(timeout);
	}

	[[nodiscard]] std::string to_string() const
	{
		std::ostringstream oss;
		oss << "StateTimeoutAction(timeout=" << timeout << ")";
		return oss.str();
	}
};

using RawAction = std::variant<FlowAction, DumpAction, CheckStateAction, StateTimeoutAction>;

/**
 * @brief Represents a generic action.
 */
struct Action
{
	RawAction raw_action;

	Action() :
	        raw_action(FlowAction()) {}

	template<typename T>
	Action(T action) :
	        raw_action(std::move(action)) {}

	void pop(stream_in_t& stream)
	{
		stream.pop(raw_action);
	}
	void push(stream_out_t& stream) const
	{
		stream.push(raw_action);
	}

	[[nodiscard]] std::string to_string() const
	{
		return std::visit([](auto&& action) { return action.to_string(); }, raw_action);
	}
};

namespace acl
{
/**
 * This struct is used for an intermediate representation of an object that
 * describes a list of actions that needs to be performed on a packet that matched some group.
 *
 * Such objects are created in total_table_t::compile.
 *
 * This representation is intermediate cause the Actions objects that are we going to use in dataplane
 * will have slightly different representation of the internal vector based on whether we have
 * a "check-state" action or not. This way we can reduce a number of branching in the dataplane and
 * also reduce the size of the object since we will use std::variant to hold either a "check-state"-object,
 * or a regular one (see common::Actions definition below).
 * Also, it manages the StateTimeoutActions to reduce number of actions in the dataplane by saving only the
 * last timeout action.
 */
struct IntermediateActions
{
	template<typename T>
	struct has_max_count_one
	{
		static constexpr bool value = (T::MAX_COUNT == 1);
	};

	/**
	 * Trait to check if we should store the first occurrence
	 *
	 * Define first-matters actions here.
	 */
	template<typename T>
	struct is_first_matters
	{
		static constexpr bool value = std::is_same_v<T, CheckStateAction>;
	};

	/**
	 * Trait to check if we should store the last occurrence
	 *
	 * Define last-matters actions here.
	 */
	template<typename T>
	struct is_last_matters
	{
		static constexpr bool value = std::is_same_v<T, StateTimeoutAction>;
	};

	std::vector<Action> path{};
	std::array<size_t, std::variant_size_v<RawAction>> action_counts = {0};
	// Using VariantTraitMap to store indices of actions with MAX_COUNT=1.
	using OptionalIndexMap = utils::VariantTraitMap<RawAction, has_max_count_one, std::optional<std::ptrdiff_t>>;
	OptionalIndexMap indices{};

	IntermediateActions() = default;
	IntermediateActions(const Action& action) { add(action); }

	/**
	 * @brief Adds an action to the current path while adhering to the following:
	 *
	 * Either:
	 * - Only the first occurrence of an action is kept.
	 * - Only the last occurrence of an action is kept (previous occurrences are removed).
	 * - Actions are added as long as they don't exceed their defined `MAX_COUNT`.
	 *
	 * Adding a new action type is as simple as placing the action type in the corresponding group.
	 */
	void add(const Action& action)
	{
		size_t variant_index = action.raw_action.index();

		// Extract the type info at the beginning.
		std::visit([&](auto&& actual_action) {
			using T = std::decay_t<decltype(actual_action)>;

			if constexpr (has_max_count_one<T>::value)
			{
				handle_unique_action<T>(action, variant_index);
			}
			else if (action_counts[variant_index] < T::MAX_COUNT)
			{
				add_to_path(action, variant_index);
			}
		},
		           action.raw_action);
	}

	/**
	 * @brief Retrieves a pointer to a unique action of type T from the path.
	 *
	 * This method returns a pointer of type T if it exists in the path.
	 * The action type T must have MAX_COUNT == 1.
	 *
	 * @tparam T The action type to retrieve.
	 * @return Pointer to the action of type T.
	 */
	template<typename T>
	T* get()
	{
		static_assert(has_max_count_one<T>::value, "Can get only unique actions from path");

		return indices.get<T>().has_value() ? &get_action<T>() : nullptr;
	}

	/**
	 * @brief Retrieves a const reference to a unique action of type T from the path.
	 *
	 * This method returns a const pointer to the action of type T
	 * if it exists in the path. The action type T must have MAX_COUNT == 1.
	 *
	 * @tparam T The action type to retrieve.
	 * @return A const pointer to the action of type T.
	 */
	template<typename T>
	[[nodiscard]] const T* get() const
	{
		static_assert(has_max_count_one<T>::value, "Can get only unique actions from path");

		return indices.get<T>().has_value() ? &get_action<T>() : nullptr;
	}

	/**
	 * @brief Removes a unique action of type T from the path.
	 *
	 * This method removes the action of type T from the path if it exists.
	 * The action type T must have MAX_COUNT == 1.
	 *
	 * @tparam T The action type to remove.
	 */
	template<typename T>
	void remove()
	{
		static_assert(has_max_count_one<T>::value, "Can remove only unique actions from path");

		if (auto& path_index = indices.get<T>())
		{
			remove_action_at(path_index.value());
		}
	}

private:
	// We're interested in storing only the first or last occurrence
	template<typename T>
	void handle_unique_action(const Action& action, size_t variant_index)
	{
		if constexpr (is_first_matters<T>::value)
		{
			handle_first_matters_action<T>(action, variant_index);
		}
		else if constexpr (is_last_matters<T>::value)
		{
			handle_last_matters_action<T>(action, variant_index);
		}
		else if constexpr (std::is_same_v<T, FlowAction>)
		{
			/*
			 * FlowAction should only appear once in the `path`. If a second
			 * FlowAction is added, this indicates an error in YANET's
			 * `total_table_t::compile()`. Ideally, this should not happen,
			 * but if it does, we log an error rather than crashing the application.
			 * In that case, we will use the last occurrence of FlowAction and proceed.
			 */
			if (indices.get<T>().has_value())
			{
				YANET_LOG_ERROR("Multiple FlowAction instances detected in the "
				                "path. Check total_table_t::compile(). Will use "
				                "the last occurrence.\n");
			}

			handle_last_matters_action<T>(action, variant_index);
		}
		else
		{
			static_assert(utils::always_false<T>::value, "Not all unique actions with MAX_COUNT = 1 are properly categorized. "
			                                             "Please add the missing actions to either `is_first_matters` or `is_last_matters` "
			                                             "to ensure their index is tracked. Tracking the index of such actions could "
			                                             "enhance dataplane performance if this information is utilized in "
			                                             "`value_t::compile()`.");
		}
	}

	// Only store the first occurrence.
	template<typename T>
	void handle_first_matters_action(const Action& action, size_t variant_index)
	{
		auto& path_index = indices.get<T>();

		if (!path_index.has_value())
		{
			add_to_path(action, variant_index);
			path_index = path.size() - 1;
		}
		// Ignore subsequent occurrences as we're only interested in the first.
	}

	// Only store the last occurrence.
	template<typename T>
	void handle_last_matters_action(const Action& action, size_t variant_index)
	{
		// Remove the previous occurrence
		remove<T>();

		// Add the new occurrence
		auto& path_index = indices.get<T>();

		add_to_path(action, variant_index);
		path_index = path.size() - 1;
	}

	// Add the action to the path and increment its count.
	void add_to_path(const Action& action, size_t variant_index)
	{
		path.push_back(action);
		action_counts[variant_index]++;
	}

	// Retrieves reference to an action of type T. Action should exist.
	template<typename T>
	T& get_action()
	{
		size_t path_index = indices.get<T>().value();
		return std::get<T>(path[path_index].raw_action);
	}

	// Remove action at index and adjust saved indices
	void remove_action_at(std::ptrdiff_t path_index)
	{
		path.erase(path.begin() + path_index);
		adjust_indices_after_removal(path_index);
	}

	// Loop through each type in FilteredTypes and adjust the corresponding index
	void adjust_indices_after_removal(std::ptrdiff_t removed_index)
	{
		std::apply([&](auto&&... types) {
			(adjust_index<std::decay_t<decltype(types)>>(removed_index), ...);
		},
		           OptionalIndexMap::Types{});
	}

	template<typename T>
	void adjust_index(std::ptrdiff_t removed_index)
	{
		auto& path_index = indices.get<T>();
		if (path_index && *path_index > removed_index)
		{
			*path_index -= 1;
		}
	}
};

} // namespace acl

template<bool HasCheckState>
class BaseActions;

template<>
class BaseActions<false>
{
private:
	std::vector<Action> path_{};

public:
	BaseActions() = default;

	BaseActions(acl::IntermediateActions&& actions) :
	        path_(std::move(actions.path)) {}

	[[nodiscard]] const Action& get_last() const
	{
		assert(!path_.empty());
		return path_.back();
	}

	Action& get_last()
	{
		assert(!path_.empty());
		return path_.back();
	}

	[[nodiscard]] const std::vector<Action>& get_actions() const
	{
		return path_;
	}

	[[nodiscard]] const common::globalBase::tFlow& get_flow() const
	{
		assert(std::holds_alternative<FlowAction>(get_last().raw_action));
		return std::get<FlowAction>(get_last().raw_action).flow;
	}

	[[nodiscard]] common::globalBase::tFlow& get_flow()
	{
		assert(std::holds_alternative<FlowAction>(get_last().raw_action));
		return std::get<FlowAction>(get_last().raw_action).flow;
	}

	bool operator<(const BaseActions& second) const
	{
		return get_flow() < second.get_flow();
	}

	void pop(stream_in_t& stream)
	{
		stream.pop(path_);
	}

	void push(stream_out_t& stream) const
	{
		stream.push(path_);
	}
};

template<>
class BaseActions<true>
{
private:
	std::vector<Action> path_{};
	// TODO: This is a prefix of a path_, in C++-20 I would use std::span to avoid extra copying
	std::vector<Action> check_state_path_{};

public:
	BaseActions() = default;
	BaseActions(acl::IntermediateActions&& actions)
	{
		assert(actions.indices.get<common::CheckStateAction>().has_value());
		auto check_state_index = actions.indices.get<common::CheckStateAction>().value();

		path_ = std::move(actions.path);

		// check_state_path_ is the prefix up to the check-state action inclusively
		check_state_path_.assign(path_.begin(), path_.begin() + check_state_index + 1);

		// Remove the check-state action from the main path_
		path_.erase(path_.begin() + check_state_index);
	}

	[[nodiscard]] const std::vector<Action>& get_actions() const
	{
		return path_;
	}

	[[nodiscard]] const std::vector<Action>& get_check_state_actions() const
	{
		return check_state_path_;
	}

	[[nodiscard]] const Action& get_last() const
	{
		assert(!path_.empty());
		return path_.back();
	}

	Action& get_last()
	{
		assert(!path_.empty());
		return path_.back();
	}

	[[nodiscard]] const common::globalBase::tFlow& get_flow() const
	{
		assert(std::holds_alternative<FlowAction>(get_last().raw_action));
		return std::get<FlowAction>(get_last().raw_action).flow;
	}

	[[nodiscard]] common::globalBase::tFlow& get_flow()
	{
		assert(std::holds_alternative<FlowAction>(get_last().raw_action));
		return std::get<FlowAction>(get_last().raw_action).flow;
	}

	bool operator<(const BaseActions& second) const
	{
		return get_flow() < second.get_flow();
	}

	void pop(stream_in_t& stream)
	{
		stream.pop(path_);
		stream.pop(check_state_path_);
	}

	void push(stream_out_t& stream) const
	{
		stream.push(path_);
		stream.push(check_state_path_);
	}
};

/**
 * The Actions type is defined as a std::variant to efficiently handle two possible states of action sequences:
 * - BaseActions<true>: This specialization is used when the action sequence contains a check-state action.
 * - BaseActions<false>: This specialization is used when the action sequence does not contain a check-state action.
 *
 * This approach allows us to avoid runtime branching to check for the presence of a check-state action, thereby
 * enhancing performance. Instead, the decision is made once when constructing the Actions object.
 *
 * During packet processing in the dataplane, this enables a more efficient execution path, as the
 * type of Actions being processed (with or without check-state) can be resolved at compile time using std::visit.
 * We will still have one extra branch on packet cause we need to know whether it will require a check-state, but
 * that will be only once. Once the result of a check-state is determined, we will choose correct path and execute it
 * without any additional checks.
 */
using Actions = std::variant<BaseActions<true>, BaseActions<false>>;

} // namespace common
