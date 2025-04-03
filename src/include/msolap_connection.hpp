//===----------------------------------------------------------------------===//
//                         DuckDB
//
// msolap_connection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include <windows.h>
#include <oledb.h>
#include <oledberr.h>
#include <string>
#include <memory>

namespace duckdb {

// MSOLAP CLSID (MSOLAP.8)
const CLSID CLSID_MSOLAP =
{ 0xDBC724B0, 0xDD86, 0x4772, { 0xBB, 0x5A, 0xFC, 0xC6, 0xCA, 0xB2, 0xFC, 0x1A } };

class MSOLAPConnection {
public:
    MSOLAPConnection();
    ~MSOLAPConnection();
    
    // Disable copy constructors
    MSOLAPConnection(const MSOLAPConnection &other) = delete;
    MSOLAPConnection &operator=(const MSOLAPConnection &) = delete;
    
    // Enable move constructors
    MSOLAPConnection(MSOLAPConnection &&other) noexcept;
    MSOLAPConnection &operator=(MSOLAPConnection &&) noexcept;
    
    // Connect to MSOLAP using a connection string
    static MSOLAPConnection Connect(const std::string &connection_string);
    
    // Execute a DAX query and return an interface to process results
    IRowset* ExecuteQuery(const std::string &dax_query);
    
    // Get column information from a rowset
    bool GetColumnInfo(IRowset *rowset, std::vector<std::string> &names, std::vector<LogicalType> &types);
    
    // Check if connection is open
    bool IsOpen() const;
    
    // Close connection
    void Close();

    // Initialize COM if needed
    static void InitializeCOM();
private:
    // Parse connection string and set properties
    void ParseConnectionString(const std::string &connection_string);
    
    
    // COM interfaces
    IDBInitialize* pIDBInitialize;
    IDBCreateCommand* pIDBCreateCommand;
    
    // Connection properties
    std::wstring server_name;
    std::wstring database_name;
    
    // COM initialization flag
    static bool com_initialized;
};
class ComInitializer {
    public:
        ComInitializer() {
            HRESULT hr = CoInitialize(NULL);
            if (FAILED(hr) && hr != S_FALSE) {
                throw std::runtime_error("COM initialization failed");
            }
            initialized = (hr == S_OK); // Only track if we actually initialized
        }
        
        ~ComInitializer() {
            if (initialized) {
                CoUninitialize();
            }
        }
        
        // Explicit "no copy" policy to prevent accidental copying
        // ComInitializer(const ComInitializer&) = delete;
        // ComInitializer& operator=(const ComInitializer&) = delete;
        
    private:
        bool initialized;
    };

} // namespace duckdb