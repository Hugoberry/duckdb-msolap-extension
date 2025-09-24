#define DUCKDB_EXTENSION_MAIN

#include "msolap_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

struct MsolapDummyData : public TableFunctionData, public GlobalTableFunctionState {
    bool data_returned = false;
    
    MsolapDummyData() {}

    idx_t MaxThreads() const override {
        return 1;
    }
};

//-----------------------------------------------------------------------------
// Table function implementation for non-Windows platforms
//-----------------------------------------------------------------------------

static unique_ptr<FunctionData> MsolapDummyBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {

    return_types={LogicalType::VARCHAR};
    names= {"MSOLAP extension is only supported on Windows platforms due to COM/OLEDB dependencies"};
    
    return make_uniq<MsolapDummyData>();
}

static unique_ptr<GlobalTableFunctionState> MsolapDummyInitGlobalState(ClientContext &context,
                                                                  TableFunctionInitInput &input) {
    // Initialize the global state
    return make_uniq<MsolapDummyData>();
}

static void MsolapDummyScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = data.global_state->Cast<MsolapDummyData>();
    if (state.data_returned) {
        output.SetCardinality(0);
        return;
    }
    state.data_returned = true;
    
}

//-----------------------------------------------------------------------------
// Extension loading
//-----------------------------------------------------------------------------

static void LoadInternal(ExtensionLoader &loader) {
    // Register the MSOLAP dummy function for non-Windows platforms
    TableFunction msolap_function("msolap", {LogicalType::VARCHAR, LogicalType::VARCHAR}, 
                                  MsolapDummyScan, MsolapDummyBind, MsolapDummyInitGlobalState);
    
    loader.RegisterFunction(msolap_function);
}

void MsolapExtension::Load(ExtensionLoader &loader) {
    // Register the MSOLAP dummy function for non-Windows platforms
    TableFunction msolap_function("msolap", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                                 MsolapDummyScan, MsolapDummyBind, MsolapDummyInitGlobalState);
    
    CreateTableFunctionInfo msolap_info(msolap_function);
    loader.RegisterFunction(msolap_info);
}


} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(msolap,loader) {
    duckdb::MsolapExtension::Load(loader);
}

DUCKDB_EXTENSION_API const char *msolap_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif