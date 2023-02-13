#include "duckdb/execution/physical_plan_generator.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/execution/column_binding_resolver.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

    class DependencyExtractor : public LogicalOperatorVisitor {
    public:
        explicit DependencyExtractor(unordered_set<CatalogEntry *> &dependencies) : dependencies(dependencies) {
        }

    protected:
        unique_ptr<Expression> VisitReplace(BoundFunctionExpression &expr, unique_ptr<Expression> *expr_ptr) override {
            // extract dependencies from the bound function expression
            if (expr.function.dependency) {
                expr.function.dependency(expr, dependencies);
            }
            return nullptr;
        }

    private:
        unordered_set<CatalogEntry *> &dependencies;
    };

    PhysicalPlanGenerator::PhysicalPlanGenerator(ClientContext &context) : context(context) {
    }

    PhysicalPlanGenerator::~PhysicalPlanGenerator() {
    }

    unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(unique_ptr<LogicalOperator> logicalPlan) {
        auto &profiler = QueryProfiler::Get(context);

        // first resolve column references
        profiler.StartPhase("column_binding");
        ColumnBindingResolver columnBindingResolver;
        columnBindingResolver.VisitOperator(*logicalPlan);
        profiler.EndPhase();

        // now resolve types of all the operators
        profiler.StartPhase("resolve_types");
        logicalPlan->ResolveOperatorTypes();
        profiler.EndPhase();

        // extract dependencies from the logical physicalPlan
        DependencyExtractor dependencyExtractor(dependencies);
        dependencyExtractor.VisitOperator(*logicalPlan);

        // then create the main physical physicalPlan
        profiler.StartPhase("create_plan");
        auto physicalPlan = CreatePlan(*logicalPlan);
        profiler.EndPhase();

        physicalPlan->Verify();
        return physicalPlan;
    }

    unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalOperator &logicalPlan) {
        logicalPlan.estimated_cardinality = logicalPlan.EstimateCardinality(context);
        unique_ptr<PhysicalOperator> physicalPlan = nullptr;

        switch (logicalPlan.type) {
            case LogicalOperatorType::LOGICAL_GET:
                physicalPlan = CreatePlan((LogicalGet &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_PROJECTION:
                physicalPlan = CreatePlan((LogicalProjection &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
                physicalPlan = CreatePlan((LogicalEmptyResult &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_FILTER:
                physicalPlan = CreatePlan((LogicalFilter &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
                physicalPlan = CreatePlan((LogicalAggregate &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_WINDOW:
                physicalPlan = CreatePlan((LogicalWindow &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_UNNEST:
                physicalPlan = CreatePlan((LogicalUnnest &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_LIMIT:
                physicalPlan = CreatePlan((LogicalLimit &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_LIMIT_PERCENT:
                physicalPlan = CreatePlan((LogicalLimitPercent &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_SAMPLE:
                physicalPlan = CreatePlan((LogicalSample &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_ORDER_BY:
                physicalPlan = CreatePlan((LogicalOrder &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_TOP_N:
                physicalPlan = CreatePlan((LogicalTopN &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_COPY_TO_FILE:
                physicalPlan = CreatePlan((LogicalCopyToFile &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
                physicalPlan = CreatePlan((LogicalDummyScan &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_ANY_JOIN:
                physicalPlan = CreatePlan((LogicalAnyJoin &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_DELIM_JOIN:
                physicalPlan = CreatePlan((LogicalDelimJoin &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
                physicalPlan = CreatePlan((LogicalComparisonJoin &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
                physicalPlan = CreatePlan((LogicalCrossProduct &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_UNION:
            case LogicalOperatorType::LOGICAL_EXCEPT:
            case LogicalOperatorType::LOGICAL_INTERSECT:
                physicalPlan = CreatePlan((LogicalSetOperation &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_INSERT:
                physicalPlan = CreatePlan((LogicalInsert &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_DELETE:
                physicalPlan = CreatePlan((LogicalDelete &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_CHUNK_GET:
                physicalPlan = CreatePlan((LogicalColumnDataGet &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_DELIM_GET:
                physicalPlan = CreatePlan((LogicalDelimGet &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
                physicalPlan = CreatePlan((LogicalExpressionGet &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_UPDATE:
                physicalPlan = CreatePlan((LogicalUpdate &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_CREATE_TABLE:
                physicalPlan = CreatePlan((LogicalCreateTable &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_CREATE_INDEX:
                physicalPlan = CreatePlan((LogicalCreateIndex &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_EXPLAIN:
                physicalPlan = CreatePlan((LogicalExplain &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_SHOW:
                physicalPlan = CreatePlan((LogicalShow &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_DISTINCT:
                physicalPlan = CreatePlan((LogicalDistinct &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_PREPARE:
                physicalPlan = CreatePlan((LogicalPrepare &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_EXECUTE:
                physicalPlan = CreatePlan((LogicalExecute &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_CREATE_VIEW:
            case LogicalOperatorType::LOGICAL_CREATE_SEQUENCE:
            case LogicalOperatorType::LOGICAL_CREATE_SCHEMA:
            case LogicalOperatorType::LOGICAL_CREATE_MACRO:
            case LogicalOperatorType::LOGICAL_CREATE_TYPE:
                physicalPlan = CreatePlan((LogicalCreate &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_PRAGMA:
                physicalPlan = CreatePlan((LogicalPragma &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_TRANSACTION:
            case LogicalOperatorType::LOGICAL_ALTER:
            case LogicalOperatorType::LOGICAL_DROP:
            case LogicalOperatorType::LOGICAL_VACUUM:
            case LogicalOperatorType::LOGICAL_LOAD:
                physicalPlan = CreatePlan((LogicalSimple &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_RECURSIVE_CTE:
                physicalPlan = CreatePlan((LogicalRecursiveCTE &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_CTE_REF:
                physicalPlan = CreatePlan((LogicalCTERef &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_EXPORT:
                physicalPlan = CreatePlan((LogicalExport &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_SET:
                physicalPlan = CreatePlan((LogicalSet &) logicalPlan);
                break;
            case LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR:
                physicalPlan = ((LogicalExtensionOperator &) logicalPlan).CreatePlan(context, *this);

                if (!physicalPlan) {
                    throw InternalException("Missing PhysicalOperator for Extension Operator");
                }
                break;
            default: {
                throw NotImplementedException("Unimplemented logical operator type!");
            }
        }

        if (logicalPlan.estimated_props) {
            physicalPlan->estimated_cardinality = logicalPlan.estimated_props->GetCardinality<idx_t>();
            physicalPlan->estimated_props = logicalPlan.estimated_props->Copy();
        } else {
            physicalPlan->estimated_props = make_unique<EstimatedProperties>();
        }

        return physicalPlan;
    }

} // namespace duckdb
