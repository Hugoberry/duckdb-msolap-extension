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
    
    DWORD dwOffset = 0;
    
    for (DBORDINAL i = 0; i < cColumns; i++) {
        // Determine buffer size and type based on column type
        DWORD bufferSize = 0;
        DBTYPE bindType = pColumnInfo[i].wType;
        
        // Adjust type and buffer size based on the column data type
        switch (pColumnInfo[i].wType) {
            case DBTYPE_I2:
                bufferSize = sizeof(INT16);
                break;
            case DBTYPE_I4:
                bufferSize = sizeof(INT32);
                break;
            case DBTYPE_I8:
                bufferSize = sizeof(INT64);
                break;
            case DBTYPE_R4:
                bufferSize = sizeof(float);
                break;
            case DBTYPE_R8:
                bufferSize = sizeof(double);
                break;
            case DBTYPE_CY:
                bufferSize = sizeof(CY);
                break;
            case DBTYPE_BOOL:
                bufferSize = sizeof(VARIANT_BOOL);
                break;
            case DBTYPE_DATE:
                bufferSize = sizeof(DATE);
                break;
            case DBTYPE_BSTR:
                bufferSize = 4096; // Allocate a reasonable size for strings
                break;
            default:
                // Use VARIANT for any types we don't handle specifically
                bindType = DBTYPE_VARIANT;
                bufferSize = sizeof(VARIANT);
                break;
        }
        
        // Initialize binding fully
        bindings[i].iOrdinal = pColumnInfo[i].iOrdinal;  // Use actual column ordinal
        bindings[i].obValue = dwOffset + offsetof(COLUMNDATA, data);
        bindings[i].obLength = dwOffset + offsetof(COLUMNDATA, dwLength);
        bindings[i].obStatus = dwOffset + offsetof(COLUMNDATA, dwStatus);
        bindings[i].pTypeInfo = NULL;
        bindings[i].pObject = NULL;
        bindings[i].pBindExt = NULL;
        bindings[i].cbMaxLen = bufferSize;
        bindings[i].dwFlags = 0;
        bindings[i].eParamIO = DBPARAMIO_NOTPARAM;
        bindings[i].dwPart = DBPART_VALUE | DBPART_LENGTH | DBPART_STATUS;
        bindings[i].dwMemOwner = DBMEMOWNER_CLIENTOWNED;
        bindings[i].wType = bindType;
        bindings[i].bPrecision = 0;
        bindings[i].bScale = 0;
        
        // Move to next COLUMNDATA structure
        dwOffset += sizeof(COLUMNDATA);
    }
    
    // Create the accessor with correct buffer size
    HRESULT hr = pIAccessor->CreateAccessor(
        DBACCESSOR_ROWDATA,
        cColumns,
        bindings.data(),
        dwOffset,
        &hAccessor,
        NULL);
    
    if (FAILED(hr)) {
        throw MSOLAPException(hr, "Failed to create accessor");
    }
    
    // Allocate memory for row data
    pRowData = new BYTE[dwOffset];
    memset(pRowData, 0, dwOffset);
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
    if (!has_row) {
        throw MSOLAPException("No current row");
    }
    
    if (column >= cColumns) {
        throw MSOLAPException("Column index out of range");
    }
    
    // Calculate the offset to the COLUMNDATA structure for this column
    COLUMNDATA* pColData = (COLUMNDATA*)(pRowData + (column * sizeof(COLUMNDATA)));
    
    // Get status
    if (pColData->dwStatus != DBSTATUS_S_OK) {
        // Handle NULL or error
        return Value(type);
    }
    
    // Get value directly based on data type
    DBTYPE columnType = pColumnInfo[column].wType;
    
    switch (type.id()) {
        case LogicalTypeId::SMALLINT:
            if (columnType == DBTYPE_I2)
                return Value::SMALLINT(pColData->data.i16Val);
            else if (columnType == DBTYPE_I4)
                return Value::SMALLINT((int16_t)pColData->data.i32Val);
            else if (columnType == DBTYPE_I8)
                return Value::SMALLINT((int16_t)pColData->data.i64Val);
            else if (columnType == DBTYPE_R4)
                return Value::SMALLINT((int16_t)pColData->data.fltVal);
            else if (columnType == DBTYPE_R8)
                return Value::SMALLINT((int16_t)pColData->data.dblVal);
            else if (columnType == DBTYPE_VARIANT)
                return Value::SMALLINT((int16_t)ConvertVariantToInt64(&pColData->data.varVal));
            break;
            
        case LogicalTypeId::INTEGER:
            if (columnType == DBTYPE_I2)
                return Value::INTEGER((int32_t)pColData->data.i16Val);
            else if (columnType == DBTYPE_I4)
                return Value::INTEGER(pColData->data.i32Val);
            else if (columnType == DBTYPE_I8)
                return Value::INTEGER((int32_t)pColData->data.i64Val);
            else if (columnType == DBTYPE_R4)
                return Value::INTEGER((int32_t)pColData->data.fltVal);
            else if (columnType == DBTYPE_R8)
                return Value::INTEGER((int32_t)pColData->data.dblVal);
            else if (columnType == DBTYPE_VARIANT)
                return Value::INTEGER((int32_t)ConvertVariantToInt64(&pColData->data.varVal));
            break;
            
        case LogicalTypeId::BIGINT:
            if (columnType == DBTYPE_I2)
                return Value::BIGINT((int64_t)pColData->data.i16Val);
            else if (columnType == DBTYPE_I4)
                return Value::BIGINT((int64_t)pColData->data.i32Val);
            else if (columnType == DBTYPE_I8)
                return Value::BIGINT(pColData->data.i64Val);
            else if (columnType == DBTYPE_R4)
                return Value::BIGINT((int64_t)pColData->data.fltVal);
            else if (columnType == DBTYPE_R8)
                return Value::BIGINT((int64_t)pColData->data.dblVal);
            else if (columnType == DBTYPE_VARIANT)
                return Value::BIGINT(ConvertVariantToInt64(&pColData->data.varVal));
            break;
            
        case LogicalTypeId::FLOAT:
            if (columnType == DBTYPE_I2)
                return Value::FLOAT((float)pColData->data.i16Val);
            else if (columnType == DBTYPE_I4)
                return Value::FLOAT((float)pColData->data.i32Val);
            else if (columnType == DBTYPE_I8)
                return Value::FLOAT((float)pColData->data.i64Val);
            else if (columnType == DBTYPE_R4)
                return Value::FLOAT(pColData->data.fltVal);
            else if (columnType == DBTYPE_R8)
                return Value::FLOAT((float)pColData->data.dblVal);
            else if (columnType == DBTYPE_VARIANT)
                return Value::FLOAT((float)ConvertVariantToDouble(&pColData->data.varVal));
            break;
            
        case LogicalTypeId::DOUBLE:
            if (columnType == DBTYPE_I2)
                return Value::DOUBLE((double)pColData->data.i16Val);
            else if (columnType == DBTYPE_I4)
                return Value::DOUBLE((double)pColData->data.i32Val);
            else if (columnType == DBTYPE_I8)
                return Value::DOUBLE((double)pColData->data.i64Val);
            else if (columnType == DBTYPE_R4)
                return Value::DOUBLE((double)pColData->data.fltVal);
            else if (columnType == DBTYPE_R8)
                return Value::DOUBLE(pColData->data.dblVal);
            else if (columnType == DBTYPE_CY)
                return Value::DOUBLE(static_cast<double>(pColData->data.cyVal.int64) / 10000.0);
            else if (columnType == DBTYPE_VARIANT)
                return Value::DOUBLE(ConvertVariantToDouble(&pColData->data.varVal));
            break;
            
        case LogicalTypeId::VARCHAR:
            if (columnType == DBTYPE_BSTR) {
                std::string str(pColData->data.strVal, pColData->dwLength);
                Vector result_vec(LogicalType::VARCHAR);
                return Value(StringVector::AddString(result_vec, str));
            } else if (columnType == DBTYPE_VARIANT) {
                Vector result_vec(LogicalType::VARCHAR);
                return Value(ConvertVariantToString(&pColData->data.varVal, result_vec));
            } else {
                // Convert numeric types to string
                std::string result;
                switch (columnType) {
                    case DBTYPE_I2: result = std::to_string(pColData->data.i16Val); break;
                    case DBTYPE_I4: result = std::to_string(pColData->data.i32Val); break;
                    case DBTYPE_I8: result = std::to_string(pColData->data.i64Val); break;
                    case DBTYPE_R4: result = std::to_string(pColData->data.fltVal); break;
                    case DBTYPE_R8: result = std::to_string(pColData->data.dblVal); break;
                    default: result = "[Unsupported Type]"; break;
                }
                Vector result_vec(LogicalType::VARCHAR);
                return Value(StringVector::AddString(result_vec, result));
            }
            break;
            
        case LogicalTypeId::BOOLEAN:
            if (columnType == DBTYPE_BOOL)
                return Value::BOOLEAN(pColData->data.boolVal != VARIANT_FALSE);
            else if (columnType == DBTYPE_I2)
                return Value::BOOLEAN(pColData->data.i16Val != 0);
            else if (columnType == DBTYPE_I4)
                return Value::BOOLEAN(pColData->data.i32Val != 0);
            else if (columnType == DBTYPE_I8)
                return Value::BOOLEAN(pColData->data.i64Val != 0);
            else if (columnType == DBTYPE_VARIANT)
                return Value::BOOLEAN(ConvertVariantToBool(&pColData->data.varVal));
            break;
            
        case LogicalTypeId::TIMESTAMP:
            if (columnType == DBTYPE_DATE) {
                // Convert OLE date to timestamp
                SYSTEMTIME sysTime;
                VariantTimeToSystemTime(pColData->data.dateVal, &sysTime);
                
                date_t date = Date::FromDate(sysTime.wYear, sysTime.wMonth, sysTime.wDay);
                dtime_t time = dtime_t(sysTime.wHour * Interval::MICROS_PER_HOUR + 
                                      sysTime.wMinute * Interval::MICROS_PER_MINUTE + 
                                      sysTime.wSecond * Interval::MICROS_PER_SEC);
                
                return Value::TIMESTAMP(Timestamp::FromDatetime(date, time));
            } else if (columnType == DBTYPE_VARIANT) {
                return Value::TIMESTAMP(ConvertVariantToTimestamp(&pColData->data.varVal));
            }
            break;
            
        case LogicalTypeId::DECIMAL:
            if (columnType == DBTYPE_CY)
                return Value::DOUBLE(static_cast<double>(pColData->data.cyVal.int64) / 10000.0);
            else if (columnType == DBTYPE_I2)
                return Value::DOUBLE((double)pColData->data.i16Val);
            else if (columnType == DBTYPE_I4)
                return Value::DOUBLE((double)pColData->data.i32Val);
            else if (columnType == DBTYPE_I8)
                return Value::DOUBLE((double)pColData->data.i64Val);
            else if (columnType == DBTYPE_R4)
                return Value::DOUBLE((double)pColData->data.fltVal);
            else if (columnType == DBTYPE_R8)
                return Value::DOUBLE(pColData->data.dblVal);
            else if (columnType == DBTYPE_VARIANT)
                return Value::DOUBLE(ConvertVariantToDouble(&pColData->data.varVal));
            break;
            
        default:
            // For any type we don't handle specifically, try to convert to string
            if (columnType == DBTYPE_VARIANT) {
                Vector result_vec(LogicalType::VARCHAR);
                return Value(ConvertVariantToString(&pColData->data.varVal, result_vec));
            } else {
                // Try to convert to string
                std::string result = "[Unsupported Type]";
                Vector result_vec(LogicalType::VARCHAR);
                return Value(StringVector::AddString(result_vec, result));
            }
            break;
    }
    
    // Fallback to using VARIANT conversion for unsupported combinations
    if (columnType == DBTYPE_VARIANT) {
        return GetVariantValue(&pColData->data.varVal, type);
    }
    
    // Default to NULL if we can't handle the conversion
    return Value(type);
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
}

} // namespace duckdb