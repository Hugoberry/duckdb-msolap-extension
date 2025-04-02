#pragma once

#include <string>
#include <vector>
#include <windows.h>
#include <oledb.h>
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

struct COLUMNDATA {
    DBSTATUS dwStatus;
    DBLENGTH dwLength;
    union {
        INT16 i16Val;
        INT32 i32Val;
        INT64 i64Val;
        float fltVal;
        double dblVal;
        VARIANT_BOOL boolVal;
        DATE dateVal;
        CY cyVal;
        VARIANT varVal;
        char strVal[4096]; // Buffer for string data
        BYTE byteData[1];  // Generic byte data
    } data;
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
};

} // namespace duckdb