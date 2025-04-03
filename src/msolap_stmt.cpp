#include "msolap_stmt.hpp"
#include "msolap_db.hpp"
#include <comdef.h>

namespace duckdb {

MSOLAPStatement::MSOLAPStatement()
    : pICommand(nullptr),
      pICommandText(nullptr),
      pIRowset(nullptr),
      pIAccessor(nullptr),
      pColumnInfo(nullptr),
      pStringsBuffer(nullptr),
      cColumns(0),
      hAccessor(NULL),
      hRow(NULL),
      pRowData(nullptr),
      has_row(false),
      executed(false) {
}

MSOLAPStatement::MSOLAPStatement(MSOLAPDB& db, const std::string& dax_query)
    : pICommand(nullptr),
      pICommandText(nullptr),
      pIRowset(nullptr),
      pIAccessor(nullptr),
      pColumnInfo(nullptr),
      pStringsBuffer(nullptr),
      cColumns(0),
      hAccessor(NULL),
      hRow(NULL),
      pRowData(nullptr),
      has_row(false),
      executed(false) {
    
    HRESULT hr;
    
    // Create a command object
    hr = db.pIDBCreateCommand->CreateCommand(NULL, IID_ICommand, (IUnknown**)&pICommand);
    if (FAILED(hr)) {
        throw MSOLAPException(hr, "Failed to create command object");
    }
    
    // Get the ICommandText interface
    hr = pICommand->QueryInterface(IID_ICommandText, (void**)&pICommandText);
    if (FAILED(hr)) {
        ::SafeRelease(&pICommand);
        throw MSOLAPException(hr, "Failed to get ICommandText interface");
    }
    
    // Set the command text
    BSTR bstrQuery = StringToBSTR(dax_query);
    hr = pICommandText->SetCommandText(DBGUID_DEFAULT, bstrQuery);
    ::SysFreeString(bstrQuery);
    
    if (FAILED(hr)) {
        ::SafeRelease(&pICommandText);
        ::SafeRelease(&pICommand);
        throw MSOLAPException(hr, "Failed to set command text");
    }
    
    // Set command timeout property if available
    ICommandProperties* pICommandProperties = NULL;
    hr = pICommand->QueryInterface(IID_ICommandProperties, (void**)&pICommandProperties);
    if (SUCCEEDED(hr)) {
        DBPROP prop;
        DBPROPSET propset;
        
        // Initialize the property
        prop.dwPropertyID = DBPROP_COMMANDTIMEOUT;
        prop.dwOptions = DBPROPOPTIONS_REQUIRED;
        prop.vValue.vt = VT_I4;
        prop.vValue.lVal = db.timeout_seconds;
        prop.colid = DB_NULLID;
        
        // Initialize the property set
        propset.guidPropertySet = DBPROPSET_ROWSET;
        propset.cProperties = 1;
        propset.rgProperties = &prop;
        
        // Set the property
        pICommandProperties->SetProperties(1, &propset);
        ::SafeRelease(&pICommandProperties);
    }
}

MSOLAPStatement::~MSOLAPStatement() {
    Close();
}

MSOLAPStatement::MSOLAPStatement(MSOLAPStatement&& other) noexcept
    : pICommand(other.pICommand),
      pICommandText(other.pICommandText),
      pIRowset(other.pIRowset),
      pIAccessor(other.pIAccessor),
      pColumnInfo(other.pColumnInfo),
      pStringsBuffer(other.pStringsBuffer),
      cColumns(other.cColumns),
      hAccessor(other.hAccessor),
      hRow(other.hRow),
      pRowData(other.pRowData),
      bindings(std::move(other.bindings)),
      has_row(other.has_row),
      executed(other.executed) {
      
    // Clear the moved-from object's state
    other.pICommand = nullptr;
    other.pICommandText = nullptr;
    other.pIRowset = nullptr;
    other.pIAccessor = nullptr;
    other.pColumnInfo = nullptr;
    other.pStringsBuffer = nullptr;
    other.cColumns = 0;
    other.hAccessor = NULL;
    other.hRow = NULL;
    other.pRowData = nullptr;
    other.has_row = false;
    other.executed = false;
}

MSOLAPStatement& MSOLAPStatement::operator=(MSOLAPStatement&& other) noexcept {
    if (this != &other) {
        // Clean up existing resources
        Close();
        
        // Move resources from other
        pICommand = other.pICommand;
        pICommandText = other.pICommandText;
        pIRowset = other.pIRowset;
        pIAccessor = other.pIAccessor;
        pColumnInfo = other.pColumnInfo;
        pStringsBuffer = other.pStringsBuffer;
        cColumns = other.cColumns;
        hAccessor = other.hAccessor;
        hRow = other.hRow;
        pRowData = other.pRowData;
        bindings = std::move(other.bindings);
        has_row = other.has_row;
        executed = other.executed;
        
        // Clear the moved-from object's state
        other.pICommand = nullptr;
        other.pICommandText = nullptr;
        other.pIRowset = nullptr;
        other.pIAccessor = nullptr;
        other.pColumnInfo = nullptr;
        other.pStringsBuffer = nullptr;
        other.cColumns = 0;
        other.hAccessor = NULL;
        other.hRow = NULL;
        other.pRowData = nullptr;
        other.has_row = false;
        other.executed = false;
    }
    return *this;
}
void MSOLAPStatement::SetupBindings() {
    if (cColumns == 0) {
        return;
    }
    
    // Create bindings for each column
    bindings.resize(cColumns);
    column_types.resize(cColumns);
    
    DWORD dwOffset = 0;
    
    // First pass: calculate the buffer size needed and assign binding types
    size_t total_buffer_size = 0;
    
    for (DBORDINAL i = 0; i < cColumns; i++) {
        MSOLAPColumnType column_type;
        size_t type_size = 0;
        
        // Assign the appropriate column type based on column type
        switch (pColumnInfo[i].wType) {
            case DBTYPE_I1:
            case DBTYPE_I2:
            case DBTYPE_I4:
            case DBTYPE_I8:
            case DBTYPE_UI1:
            case DBTYPE_UI2:
            case DBTYPE_UI4:
            case DBTYPE_UI8:
            // case DBTYPE_INT:
            // case DBTYPE_UINT:
                column_type = MSOLAPColumnType::INTEGER;
                type_size = sizeof(INT_DATA);
                break;
                
            case DBTYPE_R4:
            case DBTYPE_R8:
            case DBTYPE_CY:
            case DBTYPE_DECIMAL:
            case DBTYPE_NUMERIC:
                column_type = MSOLAPColumnType::FLOAT;
                type_size = sizeof(FLOAT_DATA);
                break;
                
            case DBTYPE_BOOL:
                column_type = MSOLAPColumnType::BOOLEAN;
                type_size = sizeof(BOOL_DATA);
                break;
                
            case DBTYPE_DATE:
            case DBTYPE_DBDATE:
            case DBTYPE_DBTIME:
            case DBTYPE_DBTIMESTAMP:
                column_type = MSOLAPColumnType::DATE;
                type_size = sizeof(DATE_DATA);
                break;
                
            case DBTYPE_BSTR:
            case DBTYPE_STR:
            case DBTYPE_WSTR:
                column_type = MSOLAPColumnType::STRING;
                type_size = sizeof(STRING_DATA);
                break;
                
            default:
                // Fallback to VARIANT for unsupported or complex types
                column_type = MSOLAPColumnType::VARIANT;
                type_size = sizeof(VARIANT_DATA);
                break;
        }
        
        column_types[i] = column_type;
        total_buffer_size += type_size;
        type_buffer_sizes.push_back(type_size);
    }
    
    // Allocate a single buffer for all columns
    pRowData = new BYTE[total_buffer_size];
    memset(pRowData, 0, total_buffer_size);
    
    // Second pass: setup bindings with appropriate types
    dwOffset = 0;
    for (DBORDINAL i = 0; i < cColumns; i++) {
        // Initialize binding common properties
        bindings[i].iOrdinal = pColumnInfo[i].iOrdinal;
        bindings[i].obValue = dwOffset + offsetof(INT_DATA, value); // This offset is adjusted below
        bindings[i].obLength = dwOffset + offsetof(INT_DATA, dwLength);
        bindings[i].obStatus = dwOffset + offsetof(INT_DATA, dwStatus);
        bindings[i].pTypeInfo = NULL;
        bindings[i].pObject = NULL;
        bindings[i].pBindExt = NULL;
        bindings[i].dwFlags = 0;
        bindings[i].eParamIO = DBPARAMIO_NOTPARAM;
        bindings[i].dwPart = DBPART_VALUE | DBPART_LENGTH | DBPART_STATUS;
        bindings[i].dwMemOwner = DBMEMOWNER_CLIENTOWNED;
        bindings[i].bPrecision = 0;
        bindings[i].bScale = 0;
        
        // Store the buffer pointer for this column
        type_buffers.push_back(pRowData + dwOffset);
        
        // Setup type-specific bindings
        switch (column_types[i]) {
            case MSOLAPColumnType::INTEGER:
                bindings[i].wType = DBTYPE_I8;
                bindings[i].cbMaxLen = sizeof(INT64);
                bindings[i].obValue = dwOffset + offsetof(INT_DATA, value);
                break;
                
            case MSOLAPColumnType::FLOAT:
                bindings[i].wType = DBTYPE_R8;
                bindings[i].cbMaxLen = sizeof(double);
                bindings[i].obValue = dwOffset + offsetof(FLOAT_DATA, value);
                break;
                
            case MSOLAPColumnType::BOOLEAN:
                bindings[i].wType = DBTYPE_BOOL;
                bindings[i].cbMaxLen = sizeof(BOOL);
                bindings[i].obValue = dwOffset + offsetof(BOOL_DATA, value);
                break;
                
            case MSOLAPColumnType::STRING:
                bindings[i].wType = DBTYPE_WSTR;
                bindings[i].cbMaxLen = sizeof(((STRING_DATA*)0)->data);
                bindings[i].obValue = dwOffset + offsetof(STRING_DATA, data);
                break;
                
            case MSOLAPColumnType::DATE:
                bindings[i].wType = DBTYPE_DBTIMESTAMP;
                bindings[i].cbMaxLen = sizeof(DBTIMESTAMP);
                bindings[i].obValue = dwOffset + offsetof(DATE_DATA, value);
                break;
                
            case MSOLAPColumnType::VARIANT:
                bindings[i].wType = DBTYPE_VARIANT;
                bindings[i].cbMaxLen = sizeof(VARIANT);
                bindings[i].obValue = dwOffset + offsetof(VARIANT_DATA, var);
                break;
        }
        
        // Move to the next buffer position
        dwOffset += type_buffer_sizes[i];
    }
    
    // Create the accessor with correct buffer size
    HRESULT hr = pIAccessor->CreateAccessor(
        DBACCESSOR_ROWDATA,
        cColumns,
        bindings.data(),
        total_buffer_size,
        &hAccessor,
        NULL);
    
    if (FAILED(hr)) {
        throw MSOLAPException(hr, "Failed to create accessor");
    }
}

bool MSOLAPStatement::Execute() {
    if (executed) {
        return true;
    }
    
    HRESULT hr;
    
    // Execute the command
    hr = pICommand->Execute(NULL, IID_IRowset, NULL, NULL, (IUnknown**)&pIRowset);
    if (FAILED(hr)) {
        throw MSOLAPException(hr, "Failed to execute command");
    }
    
    // Get column information
    IColumnsInfo* pIColumnsInfo = NULL;
    hr = pIRowset->QueryInterface(IID_IColumnsInfo, (void**)&pIColumnsInfo);
    if (FAILED(hr)) {
        ::SafeRelease(&pIRowset);
        throw MSOLAPException(hr, "Failed to get IColumnsInfo interface");
    }
    
    hr = pIColumnsInfo->GetColumnInfo(&cColumns, &pColumnInfo, &pStringsBuffer);
    ::SafeRelease(&pIColumnsInfo);
    
    if (FAILED(hr)) {
        ::SafeRelease(&pIRowset);
        throw MSOLAPException(hr, "Failed to get column info");
    }
    
    // Get the IAccessor interface
    hr = pIRowset->QueryInterface(IID_IAccessor, (void**)&pIAccessor);
    if (FAILED(hr)) {
        ::SafeRelease(&pIRowset);
        throw MSOLAPException(hr, "Failed to get IAccessor interface");
    }
    
    // Setup bindings for the columns
    SetupBindings();
    
    executed = true;
    return true;
}

bool MSOLAPStatement::Step() {
    if (!executed) {
        Execute();
    }
    
    if (has_row) {
        // Release the previous row
        pIRowset->ReleaseRows(1, &hRow, NULL, NULL, NULL);
        has_row = false;
    }
    
    HRESULT hr;
    DBCOUNTITEM cRowsObtained = 0;
    
    // Get the next row - using double indirection with an array of HROWs
    HROW* phRows = &hRow; // array of one HROW
    hr = pIRowset->GetNextRows(DB_NULL_HCHAPTER, 0, 1, &cRowsObtained, &phRows);
    
    if (hr == DB_S_ENDOFROWSET || cRowsObtained == 0) {
        // No more rows
        return false;
    }
    
    if (FAILED(hr)) {
        throw MSOLAPException(hr, "Failed to get next row");
    }
    
    // GetNextRows will always set hRow since we're using phRows = &hRow
    
    // Get the data for the row
    hr = pIRowset->GetData(hRow, hAccessor, pRowData);
    if (FAILED(hr)) {
        pIRowset->ReleaseRows(1, &hRow, NULL, NULL, NULL);
        throw MSOLAPException(hr, "Failed to get row data");
    }
    
    has_row = true;
    return true;
}

DBORDINAL MSOLAPStatement::GetColumnCount() const {
    return cColumns;
}

std::string MSOLAPStatement::GetColumnName(DBORDINAL column) const {
    if (column >= cColumns) {
        throw MSOLAPException("Column index out of range");
    }
    
    return BSTRToString(pColumnInfo[column].pwszName);
}

DBTYPE MSOLAPStatement::GetColumnType(DBORDINAL column) const {
    if (column >= cColumns) {
        throw MSOLAPException("Column index out of range");
    }
    
    return pColumnInfo[column].wType;
}

std::vector<LogicalType> MSOLAPStatement::GetColumnTypes() const {
    std::vector<LogicalType> types;
    types.reserve(cColumns);
    
    try {
        for (DBORDINAL i = 0; i < cColumns; i++) {
            types.push_back(DBTypeToLogicalType(pColumnInfo[i].wType));
        }
    } catch (const std::exception& e) {
        throw MSOLAPException("Error in GetColumnTypes: " + std::string(e.what()));
    } catch (...) {
        throw MSOLAPException("Unknown error in GetColumnTypes");
    }
    
    return types;
}

std::vector<std::string> MSOLAPStatement::GetColumnNames() const {
    std::vector<std::string> names;
    names.reserve(cColumns);
    
    try {
        for (DBORDINAL i = 0; i < cColumns; i++) {
            // Use direct pointer comparison to check if name exists
            if (pColumnInfo[i].pwszName != nullptr) {
                // Try to extract just the actual column name by looking for patterns
                std::string fullName = BSTRToString(pColumnInfo[i].pwszName);
                
                // Based on your OLAP data pattern, try to extract just the main part
                // Looking for patterns like Customer[CustomerKey]
                size_t openBracket = fullName.find('[');
                size_t closeBracket = fullName.find(']');
                
                if (openBracket != std::string::npos && closeBracket != std::string::npos && openBracket < closeBracket) {
                    // Extract just the name inside brackets
                    std::string extractedName = fullName.substr(openBracket + 1, closeBracket - openBracket - 1);
                    names.push_back(extractedName);
                } else {
                    // Fallback to sanitized full name
                    names.push_back(SanitizeColumnName(fullName));
                }
            } else {
                names.push_back("Column_" + std::to_string(i));
            }
        }
    } catch (...) {
        // Reset and use default names if there's any error
        names.clear();
        for (DBORDINAL i = 0; i < cColumns; i++) {
            names.push_back("Column_" + std::to_string(i));
        }
    }
    
    return names;
}

Value MSOLAPStatement::GetValue(DBORDINAL column, const LogicalType& type) {
    if (IsNull(column)) {
        return Value(type);
    }
    
    try {
        switch (type.id()) {
            case LogicalTypeId::SMALLINT:
            case LogicalTypeId::INTEGER:
            case LogicalTypeId::BIGINT:
                if (column_types[column] == MSOLAPColumnType::INTEGER) {
                    return Value::BIGINT(GetInt64(column));
                }
                break;
                
            case LogicalTypeId::FLOAT:
            case LogicalTypeId::DOUBLE:
                if (column_types[column] == MSOLAPColumnType::FLOAT) {
                    return Value::DOUBLE(GetDouble(column));
                }
                break;
                
            case LogicalTypeId::VARCHAR:
                if (column_types[column] == MSOLAPColumnType::STRING) {
                    Vector result_vec(LogicalType::VARCHAR);
                    return Value(GetString(column, result_vec));
                }
                break;
                
            case LogicalTypeId::BOOLEAN:
                if (column_types[column] == MSOLAPColumnType::BOOLEAN) {
                    return Value::BOOLEAN(GetBoolean(column));
                }
                break;
                
            case LogicalTypeId::TIMESTAMP:
                if (column_types[column] == MSOLAPColumnType::DATE) {
                    return Value::TIMESTAMP(GetTimestamp(column));
                }
                break;
        }
        
        // If we reach here, we need to use the variant fallback
        if (column_types[column] == MSOLAPColumnType::VARIANT) {
            VARIANT_DATA* pData = (VARIANT_DATA*)(type_buffers[column]);
            if (pData->dwStatus != DBSTATUS_S_OK) {
                return Value(type);
            }
            
            switch (type.id()) {
                case LogicalTypeId::SMALLINT:
                case LogicalTypeId::INTEGER:
                case LogicalTypeId::BIGINT:
                    return Value::BIGINT(ConvertVariantToInt64(&pData->var));
                    
                case LogicalTypeId::FLOAT:
                case LogicalTypeId::DOUBLE:
                    return Value::DOUBLE(ConvertVariantToDouble(&pData->var));
                    
                case LogicalTypeId::VARCHAR:
                {
                    Vector result_vec(LogicalType::VARCHAR);
                    return Value(ConvertVariantToString(&pData->var, result_vec));
                }
                    
                case LogicalTypeId::BOOLEAN:
                    return Value::BOOLEAN(ConvertVariantToBool(&pData->var));
                    
                case LogicalTypeId::TIMESTAMP:
                    return Value::TIMESTAMP(ConvertVariantToTimestamp(&pData->var));
                    
                default:
                {
                    Vector result_vec(LogicalType::VARCHAR);
                    return Value(ConvertVariantToString(&pData->var, result_vec));
                }
            }
        }
        
        // This should not happen, but let's handle it anyway
        throw MSOLAPException("Unsupported column type conversion");
    } catch (const std::exception& e) {
        throw MSOLAPException(std::string("Error getting value: ") + e.what());
    }
}

Value MSOLAPStatement::GetVariantValue(VARIANT* var, const LogicalType& type) {
    
    switch (type.id()) {
        case LogicalTypeId::SMALLINT:
        case LogicalTypeId::INTEGER:
        case LogicalTypeId::BIGINT:
            return Value::BIGINT(ConvertVariantToInt64(var));
            
        case LogicalTypeId::FLOAT:
        case LogicalTypeId::DOUBLE:
            return Value::DOUBLE(ConvertVariantToDouble(var));
            
        case LogicalTypeId::VARCHAR:
        {
            // Create a temporary vector for string conversion
            Vector result_vec(LogicalType::VARCHAR);
            auto str_val = ConvertVariantToString(var, result_vec);
            return Value(str_val);
        }
            
        case LogicalTypeId::BOOLEAN:
            return Value::BOOLEAN(ConvertVariantToBool(var));
            
        case LogicalTypeId::TIMESTAMP:
            return Value::TIMESTAMP(ConvertVariantToTimestamp(var));
            
        case LogicalTypeId::DECIMAL:
            return Value::DOUBLE(ConvertVariantToDouble(var));
            
        default:
        {
            // Default to string for unsupported types
            Vector result_vec(LogicalType::VARCHAR);
            return Value(ConvertVariantToString(var, result_vec));
        }
    }
}

void MSOLAPStatement::Close() {
    FreeResources();
    
    ::SafeRelease(&pIAccessor);
    ::SafeRelease(&pIRowset);
    ::SafeRelease(&pICommandText);
    ::SafeRelease(&pICommand);
    
    has_row = false;
    executed = false;
}

void MSOLAPStatement::FreeResources() {
    if (has_row && pIRowset) {
        pIRowset->ReleaseRows(1, &hRow, NULL, NULL, NULL);
        has_row = false;
    }
    
    if (hAccessor && pIAccessor) {
        pIAccessor->ReleaseAccessor(hAccessor, NULL);
        hAccessor = NULL;
    }
    
    if (pColumnInfo) {
        CoTaskMemFree(pColumnInfo);
        pColumnInfo = nullptr;
    }
    
    if (pStringsBuffer) {
        CoTaskMemFree(pStringsBuffer);
        pStringsBuffer = nullptr;
    }
    
    if (pRowData) {
        delete[] pRowData;
        pRowData = nullptr;
    }
    
    cColumns = 0;
    bindings.clear();
    column_types.clear();
    type_buffers.clear();
    type_buffer_sizes.clear();
}

bool MSOLAPStatement::IsNull(DBORDINAL column) const {
    if (!has_row) {
        throw MSOLAPException("No current row");
    }
    
    if (column >= cColumns) {
        throw MSOLAPException("Column index out of range");
    }
    
    // Get the status field which is at the same position for all data structures
    DBSTATUS* pStatus = (DBSTATUS*)(type_buffers[column]);
    return *pStatus != DBSTATUS_S_OK;
}

int64_t MSOLAPStatement::GetInt64(DBORDINAL column) const {
    if (!has_row) {
        throw MSOLAPException("No current row");
    }
    
    if (column >= cColumns) {
        throw MSOLAPException("Column index out of range");
    }
    
    if (column_types[column] != MSOLAPColumnType::INTEGER) {
        throw MSOLAPException("Column is not an integer type");
    }
    
    INT_DATA* pData = (INT_DATA*)(type_buffers[column]);
    if (pData->dwStatus != DBSTATUS_S_OK) {
        return 0;
    }
    
    return pData->value;
}

double MSOLAPStatement::GetDouble(DBORDINAL column) const {
    if (!has_row) {
        throw MSOLAPException("No current row");
    }
    
    if (column >= cColumns) {
        throw MSOLAPException("Column index out of range");
    }
    
    if (column_types[column] != MSOLAPColumnType::FLOAT) {
        throw MSOLAPException("Column is not a float type");
    }
    
    FLOAT_DATA* pData = (FLOAT_DATA*)(type_buffers[column]);
    if (pData->dwStatus != DBSTATUS_S_OK) {
        return 0.0;
    }
    
    return pData->value;
}

bool MSOLAPStatement::GetBoolean(DBORDINAL column) const {
    if (!has_row) {
        throw MSOLAPException("No current row");
    }
    
    if (column >= cColumns) {
        throw MSOLAPException("Column index out of range");
    }
    
    if (column_types[column] != MSOLAPColumnType::BOOLEAN) {
        throw MSOLAPException("Column is not a boolean type");
    }
    
    BOOL_DATA* pData = (BOOL_DATA*)(type_buffers[column]);
    if (pData->dwStatus != DBSTATUS_S_OK) {
        return false;
    }
    
    return pData->value != FALSE;
}

string_t MSOLAPStatement::GetString(DBORDINAL column, Vector& result_vector) const {
    if (!has_row) {
        throw MSOLAPException("No current row");
    }
    
    if (column >= cColumns) {
        throw MSOLAPException("Column index out of range");
    }
    
    if (column_types[column] != MSOLAPColumnType::STRING) {
        throw MSOLAPException("Column is not a string type");
    }
    
    STRING_DATA* pData = (STRING_DATA*)(type_buffers[column]);
    if (pData->dwStatus != DBSTATUS_S_OK) {
        return string_t();
    }
    
    // Convert from WCHAR to UTF-8 string
    std::string utf8_str = BSTRToString((BSTR)pData->data);
    return StringVector::AddString(result_vector, utf8_str);
}

timestamp_t MSOLAPStatement::GetTimestamp(DBORDINAL column) const {
    if (!has_row) {
        throw MSOLAPException("No current row");
    }
    
    if (column >= cColumns) {
        throw MSOLAPException("Column index out of range");
    }
    
    if (column_types[column] != MSOLAPColumnType::DATE) {
        throw MSOLAPException("Column is not a date type");
    }
    
    DATE_DATA* pData = (DATE_DATA*)(type_buffers[column]);
    if (pData->dwStatus != DBSTATUS_S_OK) {
        return timestamp_t(0);
    }
    
    // Convert DBTIMESTAMP to timestamp_t
    SYSTEMTIME sysTime;
    sysTime.wYear = pData->value.year;
    sysTime.wMonth = pData->value.month;
    sysTime.wDay = pData->value.day;
    sysTime.wHour = pData->value.hour;
    sysTime.wMinute = pData->value.minute;
    sysTime.wSecond = pData->value.second;
    sysTime.wMilliseconds = pData->value.fraction / 1000000;
    
    date_t date = Date::FromDate(sysTime.wYear, sysTime.wMonth, sysTime.wDay);
    dtime_t time = dtime_t(sysTime.wHour * Interval::MICROS_PER_HOUR + 
                          sysTime.wMinute * Interval::MICROS_PER_MINUTE + 
                          sysTime.wSecond * Interval::MICROS_PER_SEC +
                          pData->value.fraction / 1000);
    
    return Timestamp::FromDatetime(date, time);
}

} // namespace duckdb