#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/list.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_limit_percent.hpp"
#include "duckdb/planner/query_node/bound_select_node.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::PlanFilter(unique_ptr<Expression> condition, unique_ptr<LogicalOperator> root) {
	PlanSubqueries(&condition, &root);
	auto filter = make_unique<LogicalFilter>(move(condition));
	filter->AddChild(move(root));
	return move(filter);
}

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundSelectNode &boundSelectNode) {
	unique_ptr<LogicalOperator> root;
	D_ASSERT(boundSelectNode.from_table);
	root = CreatePlan(*boundSelectNode.from_table);
	D_ASSERT(root);

	// plan the sample clause
	if (boundSelectNode.sample_options) {
		root = make_unique<LogicalSample>(move(boundSelectNode.sample_options), move(root));
	}

	if (boundSelectNode.where_clause) {
		root = PlanFilter(move(boundSelectNode.where_clause), move(root));
	}

	if (!boundSelectNode.aggregates.empty() || !boundSelectNode.groups.group_expressions.empty()) {
		if (!boundSelectNode.groups.group_expressions.empty()) {
			// visit the groups
			for (auto &group : boundSelectNode.groups.group_expressions) {
				PlanSubqueries(&group, &root);
			}
		}
		// now visit all aggregate expressions
		for (auto &expr : boundSelectNode.aggregates) {
			PlanSubqueries(&expr, &root);
		}
		// finally create the aggregate node with the group_index and aggregate_index as obtained from the binder
		auto aggregate =
		    make_unique<LogicalAggregate>(boundSelectNode.group_index, boundSelectNode.aggregate_index, move(boundSelectNode.aggregates));
		aggregate->groups = move(boundSelectNode.groups.group_expressions);
		aggregate->groupings_index = boundSelectNode.groupings_index;
		aggregate->grouping_sets = move(boundSelectNode.groups.grouping_sets);
		aggregate->grouping_functions = move(boundSelectNode.grouping_functions);

		aggregate->AddChild(move(root));
		root = move(aggregate);
	} else if (!boundSelectNode.groups.grouping_sets.empty()) {
		// edge case: we have grouping sets but no groups or aggregates
		// this can only happen if we have e.g. select 1 from tbl group by ();
		// just output a dummy scan
		root = make_unique_base<LogicalOperator, LogicalDummyScan>(boundSelectNode.group_index);
	}

	if (boundSelectNode.having) {
		PlanSubqueries(&boundSelectNode.having, &root);
		auto having = make_unique<LogicalFilter>(move(boundSelectNode.having));

		having->AddChild(move(root));
		root = move(having);
	}

	if (!boundSelectNode.windows.empty()) {
		auto win = make_unique<LogicalWindow>(boundSelectNode.window_index);
		win->expressions = move(boundSelectNode.windows);
		// visit the window expressions
		for (auto &expr : win->expressions) {
			PlanSubqueries(&expr, &root);
		}
		D_ASSERT(!win->expressions.empty());
		win->AddChild(move(root));
		root = move(win);
	}

	if (boundSelectNode.qualify) {
		PlanSubqueries(&boundSelectNode.qualify, &root);
		auto qualify = make_unique<LogicalFilter>(move(boundSelectNode.qualify));

		qualify->AddChild(move(root));
		root = move(qualify);
	}

	if (!boundSelectNode.unnests.empty()) {
		auto unnest = make_unique<LogicalUnnest>(boundSelectNode.unnest_index);
		unnest->expressions = move(boundSelectNode.unnests);
		// visit the unnest expressions
		for (auto &expr : unnest->expressions) {
			PlanSubqueries(&expr, &root);
		}
		D_ASSERT(!unnest->expressions.empty());
		unnest->AddChild(move(root));
		root = move(unnest);
	}

	for (auto &expr : boundSelectNode.select_list) {
		PlanSubqueries(&expr, &root);
	}

	// create the projection
	auto proj = make_unique<LogicalProjection>(boundSelectNode.projection_index, move(boundSelectNode.select_list));
	auto &projection = *proj;
	proj->AddChild(move(root));
	root = move(proj);

	// finish the plan by handling the elements of the QueryNode
	root = VisitQueryNode(boundSelectNode, move(root));

	// add a prune node if necessary
	if (boundSelectNode.need_prune) {
		D_ASSERT(root);
		vector<unique_ptr<Expression>> prune_expressions;
		for (idx_t i = 0; i < boundSelectNode.column_count; i++) {
			prune_expressions.push_back(make_unique<BoundColumnRefExpression>(
			    projection.expressions[i]->return_type, ColumnBinding(boundSelectNode.projection_index, i)));
		}
		auto prune = make_unique<LogicalProjection>(boundSelectNode.prune_index, move(prune_expressions));
		prune->AddChild(move(root));
		root = move(prune);
	}

	return root;
}

} // namespace duckdb
