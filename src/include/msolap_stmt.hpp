#pragma once

#include <string>
#include <vector>
#include <windows.h>
#include <oledb.h>
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

// Type-specific column data structures
struct INT_DATA {
    DBSTATUS dwStatus;
    DBLENGTH dwLength;
    INT64 value;
};

struct FLOAT_DATA {
    DBSTATUS dwStatus;
    DBLENGTH dwLength;
    double value;
};

struct BOOL_DATA {
    DBSTATUS dwStatus;
    DBLENGTH dwLength;
    BOOL value;
};

struct STRING_DATA {
    DBSTATUS dwStatus;
    DBLENGTH dwLength;
    WCHAR data[4096]; // Use a fixed buffer size for strings
};

struct DATE_DATA {
    DBSTATUS dwStatus;
    DBLENGTH dwLength;
    DBTIMESTAMP value;
};

struct VARIANT_DATA {
    DBSTATUS dwStatus;
    DBLENGTH dwLength;
    VARIANT var;
};

// Enum to track the column type for each column
enum class MSOLAPColumnType {
    INTEGER,
    FLOAT,
    BOOLEAN,
    STRING,
    DATE,
    VARIANT
};

class MSOLAPDB;

class MSOLAPStatement {
public:
    MSOLAPStatement();
    MSOLAPStatement(MSOLAPDB& db, const std::string& dax_query);
    ~MSOLAPStatement();

    // Prevent copying
    MSOLAPStatement(const MSOLAPStatement&) = delete;
    MSOLAPStatement& operator=(const MSOLAPStatement&) = delete;

    // Allow moving
    MSOLAPStatement(MSOLAPStatement&& other) noexcept;
    MSOLAPStatement& operator=(MSOLAPStatement&& other) noexcept;

    // Execute the statement
    bool Execute();
    
    // Move to the next row in the result set
    // Returns true if there is a row, false if no more rows
    bool Step();
    
    // Get the number of columns in the result set
    DBORDINAL GetColumnCount() const;
    
    // Get the name of a column
    std::string GetColumnName(DBORDINAL column) const;
    
    // Get the type of a column
    DBTYPE GetColumnType(DBORDINAL column) const;
    
    // Get logical types for all columns
    std::vector<LogicalType> GetColumnTypes() const;
    
    // Get names for all columns
    std::vector<std::string> GetColumnNames() const;
    
    // Get a value from the current row
    Value GetValue(DBORDINAL column, const LogicalType& type);
    
    // Close the statement
    void Close();
    
    // Check if the statement is open
    bool IsOpen() const { return pICommand != nullptr; }

    // Track the binding type for each column
    // std::vector<BindingType> column_binding_types;

    // Track the column type for each column
    std::vector<MSOLAPColumnType> column_types;
    
    // New direct type access methods
    bool IsNull(DBORDINAL column) const;
    int64_t GetInt64(DBORDINAL column) const;
    double GetDouble(DBORDINAL column) const;
    string_t GetString(DBORDINAL column, Vector& result_vector) const;
    bool GetBoolean(DBORDINAL column) const;
    timestamp_t GetTimestamp(DBORDINAL column) const;

private:
    // Set up column bindings
    void SetupBindings();
    
    // Get value from a variant
    Value GetVariantValue(VARIANT* var, const LogicalType& type);
    
    // Free resources
    void FreeResources();

    // Command object and related interfaces
    ICommand* pICommand;
    ICommandText* pICommandText;
    IRowset* pIRowset;
    IAccessor* pIAccessor;
    
    // Column information
    DBCOLUMNINFO* pColumnInfo;
    WCHAR* pStringsBuffer;
    DBORDINAL cColumns;
    
    // Accessor handle
    HACCESSOR hAccessor;
    
    // Current row handle
    HROW hRow;
    
    // Row data buffer
    BYTE* pRowData;
    
    // Bindings for columns
    std::vector<DBBINDING> bindings;
    
    // State tracking
    bool has_row;
    bool executed;

    // Data buffers for each type
    std::vector<BYTE*> type_buffers;
    std::vector<size_t> type_buffer_sizes;
};

} // namespace duckdb