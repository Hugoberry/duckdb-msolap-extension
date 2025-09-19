#define DUCKDB_EXTENSION_MAIN

#include "msolap_extension.hpp"
#include "msolap_scanner.hpp"
#include "msolap_utils.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
    // Register MSOLAP table function
    MSOLAPScanFunction msolap_scan_fun;
    loader.RegisterFunction(msolap_scan_fun);
}

void MsolapExtension::Load(ExtensionLoader &loader) {
    LoadInternal(loader);
}


} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(msolap, loader) {
    duckdb::MsolapExtension::Load(loader);
}

DUCKDB_EXTENSION_API const char *msolap_version() {
    return duckdb::DuckDB::LibraryVersion();
}

}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif