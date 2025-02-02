#include "duckdb/parser/parsed_data/create_collation_info.hpp"

namespace duckdb {

CreateCollationInfo::CreateCollationInfo(string name_p, ScalarFunction function_p, bool combinable_p,
                                         bool not_required_for_equality_p)
    : CreateInfo(CatalogType::COLLATION_ENTRY), function(move(function_p)), combinable(combinable_p),
      not_required_for_equality(not_required_for_equality_p) {
	this->name = move(name_p);
	internal = true;
}

void CreateCollationInfo::SerializeInternal(Serializer &) const {
	throw NotImplementedException("Cannot serialize '%s'", CatalogTypeToString(type));
}

unique_ptr<CreateInfo> CreateCollationInfo::Copy() const {
	auto result = make_unique<CreateCollationInfo>(name, function, combinable, not_required_for_equality);
	CopyProperties(*result);
	return move(result);
}

} // namespace duckdb
