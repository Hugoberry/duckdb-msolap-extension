#include "msolap_connection.hpp"
#include "msolap_utils.hpp"
#include <stdexcept>

namespace duckdb {

// Initialize static member
bool MSOLAPConnection::com_initialized = false;

MSOLAPConnection::MSOLAPConnection() 
    : pIDBInitialize(nullptr), pIDBCreateCommand(nullptr), integrated_security(false) {
}

MSOLAPConnection::MSOLAPConnection(MSOLAPConnection &&other) noexcept
    : pIDBInitialize(nullptr), pIDBCreateCommand(nullptr), integrated_security(false) {
    std::swap(pIDBInitialize, other.pIDBInitialize);
    std::swap(pIDBCreateCommand, other.pIDBCreateCommand);
    std::swap(server_name, other.server_name);
    std::swap(database_name, other.database_name);
    std::swap(username, other.username);
    std::swap(password, other.password);
    std::swap(integrated_security, other.integrated_security);
    std::swap(encryption, other.encryption);
}

MSOLAPConnection &MSOLAPConnection::operator=(MSOLAPConnection &&other) noexcept {
    std::swap(pIDBInitialize, other.pIDBInitialize);
    std::swap(pIDBCreateCommand, other.pIDBCreateCommand);
    std::swap(server_name, other.server_name);
    std::swap(database_name, other.database_name);
    std::swap(username, other.username);
    std::swap(password, other.password);
    std::swap(integrated_security, other.integrated_security);
    std::swap(encryption, other.encryption);
    return *this;
}

MSOLAPConnection::~MSOLAPConnection() {
    Close();
}

void MSOLAPConnection::InitializeCOM() {
    static thread_local ComInitializer initializer;
}

void MSOLAPConnection::ParseConnectionString(const std::string &connection_string) {
    // Simple parsing of connection string
    // Format expected: "Server=server_name;Database=database_name;User ID=username;Password=password;"
    
    // Initialize connection type
    conn_type = LOCAL_SERVER;
    std::map<std::string, std::string> properties;
    
    size_t pos = 0;
    std::string token;
    std::string str = connection_string;
    
    while ((pos = str.find(';')) != std::string::npos) {
        token = str.substr(0, pos);
        
        // Find key-value separator
        size_t sep_pos = token.find('=');
        if (sep_pos != std::string::npos) {
            std::string key = token.substr(0, sep_pos);
            std::string value = token.substr(sep_pos + 1);
            properties[key] = value;
        }
        
        str.erase(0, pos + 1);
    }
    
    // Handle the last token after the last semicolon
    if (!str.empty()) {
        size_t sep_pos = str.find('=');
        if (sep_pos != std::string::npos) {
            std::string key = str.substr(0, sep_pos);
            std::string value = str.substr(sep_pos + 1);
            properties[key] = value;
        }
    }
    
    // Extract connection properties
    
    // Server/Data Source - check for Power BI or Azure format
    auto server_it = properties.find("Server");
    if (server_it == properties.end()) {
        server_it = properties.find("Data Source"); // Alternative name
    }
    
    if (server_it != properties.end()) {
        std::string server = server_it->second;
        server_name = WindowsUtil::UTF8ToUnicode(server.c_str());
        
        // Check if this is a Power BI URL
        if (server.find("powerbi://") == 0) {
            conn_type = POWERBI;
            
            // Parse the Power BI URL: powerbi://api.powerbi.com/v1.0/contoso.com/Sales
            std::string url = server.substr(10); // Remove "powerbi://"
            
            // Find the domain part
            size_t domain_start = 0;
            size_t domain_end = url.find('/', domain_start);
            if (domain_end != std::string::npos) {
                std::string domain = url.substr(domain_start, domain_end - domain_start);
                
                // Extract rest of the URL
                std::string path = url.substr(domain_end + 1);
                
                // Parse path components
                size_t first_slash = path.find('/');
                if (first_slash != std::string::npos) {
                    std::string version = path.substr(0, first_slash);
                    path = path.substr(first_slash + 1);
                    
                    size_t workspace_end = path.find('/');
                    if (workspace_end != std::string::npos) {
                        std::string tenant = path.substr(0, workspace_end);
                        std::string dataset = path.substr(workspace_end + 1);
                        
                        tenant_id = WindowsUtil::UTF8ToUnicode(tenant.c_str());
                        workspace = WindowsUtil::UTF8ToUnicode(dataset.c_str());
                    }
                }
            }
        }
        // Check if this is an Azure Analysis Services URL
        else if (server.find("asazure://") == 0) {
            conn_type = AZURE_SERVER;
            
            // Parse the Azure URL: asazure://westus.asazure.windows.net/myserver
            std::string url = server.substr(9); // Remove "asazure://"
            
            // Find the domain part
            size_t domain_start = 0;
            size_t domain_end = url.find('/', domain_start);
            if (domain_end != std::string::npos) {
                std::string domain = url.substr(domain_start, domain_end - domain_start);
                
                // The server name is after the last slash
                std::string server_name_part = url.substr(domain_end + 1);
                
                // Extract region from domain
                size_t region_end = domain.find('.');
                if (region_end != std::string::npos) {
                    region = WindowsUtil::UTF8ToUnicode(domain.substr(0, region_end).c_str());
                }
                
                // Update server_name with the parsed server part
                server_name = WindowsUtil::UTF8ToUnicode(server_name_part.c_str());
            }
        }
    } else {
        server_name = L"localhost";
    }
    
    // Database (optional)
    auto db_it = properties.find("Database");
    if (db_it == properties.end()) {
        db_it = properties.find("Catalog"); // Alternative name
    }
    if (db_it != properties.end()) {
        database_name = WindowsUtil::UTF8ToUnicode(db_it->second.c_str());
    } else {
        database_name = L"";
    }
    
    // Authentication properties
    
    // Integrated Security (Windows Auth)
    integrated_security = false;
    auto integrated_it = properties.find("Integrated Security");
    if (integrated_it != properties.end()) {
        std::string value = integrated_it->second;
        // Convert to lowercase for case-insensitive comparison
        std::transform(value.begin(), value.end(), value.begin(), 
                      [](unsigned char c){ return std::tolower(c); });
        integrated_security = (value == "true" || value == "sspi" || value == "yes");
    }
    
    // Username
    auto user_it = properties.find("User ID");
    if (user_it == properties.end()) {
        user_it = properties.find("UID"); // Alternative name
    }
    if (user_it != properties.end()) {
        username = WindowsUtil::UTF8ToUnicode(user_it->second.c_str());
    } else {
        username = L"";
    }
    
    // Password
    auto pwd_it = properties.find("Password");
    if (pwd_it == properties.end()) {
        pwd_it = properties.find("PWD"); // Alternative name
    }
    if (pwd_it != properties.end()) {
        password = WindowsUtil::UTF8ToUnicode(pwd_it->second.c_str());
    } else {
        password = L"";
    }
    
    // Encryption
    auto encrypt_it = properties.find("Encrypt");
    if (encrypt_it != properties.end()) {
        encryption = WindowsUtil::UTF8ToUnicode(encrypt_it->second.c_str());
    } else {
        encryption = L"";
    }

    // Additional properties for Azure/Power BI connections
    
    // Application ID for AAD auth
    auto app_id_it = properties.find("Application ID");
    if (app_id_it == properties.end()) {
        app_id_it = properties.find("AppId"); // Alternative name
    }
    if (app_id_it != properties.end()) {
        application_id = WindowsUtil::UTF8ToUnicode(app_id_it->second.c_str());
    }
    
    // Tenant ID for AAD auth (if not already extracted from URL)
    if (tenant_id.empty()) {
        auto tenant_it = properties.find("Tenant ID");
        if (tenant_it == properties.end()) {
            tenant_it = properties.find("TenantId"); // Alternative name
        }
        if (tenant_it != properties.end()) {
            tenant_id = WindowsUtil::UTF8ToUnicode(tenant_it->second.c_str());
        }
    }
    
    // Application Key/Secret
    auto app_key_it = properties.find("Application Key");
    if (app_key_it == properties.end()) {
        app_key_it = properties.find("AppKey"); // Alternative name
    }
    if (app_key_it != properties.end()) {
        app_key = WindowsUtil::UTF8ToUnicode(app_key_it->second.c_str());
    }
    
    // Authority URL
    auto authority_it = properties.find("Authority");
    if (authority_it != properties.end()) {
        authority = WindowsUtil::UTF8ToUnicode(authority_it->second.c_str());
    } else {
        // Default authority
        authority = L"https://login.microsoftonline.com";
    }
    
    // Resource URL
    auto resource_it = properties.find("Resource");
    if (resource_it != properties.end()) {
        resource = WindowsUtil::UTF8ToUnicode(resource_it->second.c_str());
    } else {
        // Default resource
        if (conn_type == POWERBI) {
            resource = L"https://analysis.windows.net/powerbi/api";
        } else if (conn_type == AZURE_SERVER) {
            resource = L"https://analysis.windows.net/powerbi/api";
        }
    }
}

std::wstring MSOLAPConnection::GetDefaultCatalog() {
    if (!pIDBInitialize) {
        return L"";
    }
    
    // Get the session to query for available catalogs
    IDBCreateSession* pIDBCreateSession = NULL;
    HRESULT hr = pIDBInitialize->QueryInterface(IID_IDBCreateSession, (void**)&pIDBCreateSession);
    if (FAILED(hr)) {
        return L"";
    }
    
    // Create a session
    IDBCreateCommand* pIDBCreateCmd = NULL;
    hr = pIDBCreateSession->CreateSession(NULL, IID_IDBCreateCommand, (IUnknown**)&pIDBCreateCmd);
    MSOLAPUtils::SafeRelease(&pIDBCreateSession);
    if (FAILED(hr)) {
        return L"";
    }
    
    // Create a command
    ICommand* pICommand = NULL;
    hr = pIDBCreateCmd->CreateCommand(NULL, IID_ICommand, (IUnknown**)&pICommand);
    MSOLAPUtils::SafeRelease(&pIDBCreateCmd);
    if (FAILED(hr)) {
        return L"";
    }
    
    // Set the command text to query for catalogs
    ICommandText* pICommandText = NULL;
    hr = pICommand->QueryInterface(IID_ICommandText, (void**)&pICommandText);
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&pICommand);
        return L"";
    }
    
    // DISCOVER_DATABASES query
    std::wstring query = L"SELECT [CATALOG_NAME] FROM $SYSTEM.DBSCHEMA_CATALOGS ORDER BY [CATALOG_NAME]";
    hr = pICommandText->SetCommandText(DBGUID_DEFAULT, query.c_str());
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&pICommandText);
        MSOLAPUtils::SafeRelease(&pICommand);
        return L"";
    }
    
    // Execute the command
    IRowset* pIRowset = NULL;
    hr = pICommand->Execute(NULL, IID_IRowset, NULL, NULL, (IUnknown**)&pIRowset);
    MSOLAPUtils::SafeRelease(&pICommandText);
    MSOLAPUtils::SafeRelease(&pICommand);
    if (FAILED(hr)) {
        return L"";
    }
    
    // Get the accessor
    IAccessor* pIAccessor = NULL;
    hr = pIRowset->QueryInterface(IID_IAccessor, (void**)&pIAccessor);
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&pIRowset);
        return L"";
    }
    
    // Set up binding for the catalog name column
    DBBINDING binding;
    binding.iOrdinal = 1; // 1-based ordinal
    binding.obValue = offsetof(CatalogData, var);
    binding.obLength = offsetof(CatalogData, dwLength);
    binding.obStatus = offsetof(CatalogData, dwStatus);
    binding.pTypeInfo = NULL;
    binding.pObject = NULL;
    binding.pBindExt = NULL;
    binding.cbMaxLen = sizeof(VARIANT);
    binding.dwFlags = 0;
    binding.eParamIO = DBPARAMIO_NOTPARAM;
    binding.dwPart = DBPART_VALUE | DBPART_LENGTH | DBPART_STATUS;
    binding.dwMemOwner = DBMEMOWNER_CLIENTOWNED;
    binding.wType = DBTYPE_VARIANT;
    binding.bPrecision = 0;
    binding.bScale = 0;
    
    // Create accessor
    HACCESSOR hAccessor;
    hr = pIAccessor->CreateAccessor(DBACCESSOR_ROWDATA, 1, &binding, sizeof(CatalogData), &hAccessor, NULL);
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&pIAccessor);
        MSOLAPUtils::SafeRelease(&pIRowset);
        return L"";
    }
    
    // Get first row
    HROW hRow;
    HROW* pRows = &hRow;
    DBCOUNTITEM cRowsObtained = 0;
    hr = pIRowset->GetNextRows(0, 0, 1, &cRowsObtained, &pRows);
    
    std::wstring result;
    if (SUCCEEDED(hr) && cRowsObtained > 0) {
        // Get the row data
        CatalogData catalog_data;
        memset(&catalog_data, 0, sizeof(CatalogData));
        
        hr = pIRowset->GetData(hRow, hAccessor, &catalog_data);
        if (SUCCEEDED(hr) && catalog_data.dwStatus == DBSTATUS_S_OK) {
            // Convert VARIANT to wstring
            if (catalog_data.var.vt == VT_BSTR && catalog_data.var.bstrVal) {
                result = catalog_data.var.bstrVal;
            }
            // Clear variant
            VariantClear(&catalog_data.var);
        }
        
        // Release the row handle
        pIRowset->ReleaseRows(1, pRows, NULL, NULL, NULL);
    }
    
    // Cleanup
    pIAccessor->ReleaseAccessor(hAccessor, NULL);
    MSOLAPUtils::SafeRelease(&pIAccessor);
    MSOLAPUtils::SafeRelease(&pIRowset);
    
    return result;
}

MSOLAPConnection MSOLAPConnection::Connect(const std::string &connection_string) {
    MSOLAPConnection connection;
    
    // Initialize COM
    InitializeCOM();
    
    // Parse connection string
    connection.ParseConnectionString(connection_string);
    
    // Create data source
    HRESULT hr = CoCreateInstance(CLSID_MSOLAP, NULL, CLSCTX_INPROC_SERVER,
        IID_IDBInitialize, (void**)&connection.pIDBInitialize);
        
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to create MSOLAP provider: " + MSOLAPUtils::GetErrorMessage(hr));
    }
    
    // Get the IDBProperties interface
    IDBProperties* pIDBProperties = NULL;
    hr = connection.pIDBInitialize->QueryInterface(IID_IDBProperties, (void**)&pIDBProperties);
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&connection.pIDBInitialize);
        throw std::runtime_error("Failed to get IDBProperties: " + MSOLAPUtils::GetErrorMessage(hr));
    }
    
    // Check if we need to get the default catalog
    bool need_default_catalog = connection.database_name.empty();
    
    // Count the number of properties we need to set
    // Base properties plus additional ones for Azure/Power BI
    int propCount = need_default_catalog ? 2 : 3; // Data Source, Mode, (Catalog if provided)
    
    // Add authentication properties based on connection type
    switch(connection.conn_type) {
        case LOCAL_SERVER:
            // Regular authentication properties
            if (connection.integrated_security) {
                propCount++;
            } else if (!connection.username.empty()) {
                propCount += 2; // Username + Password
            }
            break;
            
        case AZURE_SERVER:
        case POWERBI:
            // For Azure/Power BI, we need to add AAD authentication properties
            if (!connection.application_id.empty()) {
                propCount++; // Application ID
                
                if (!connection.app_key.empty()) {
                    propCount++; // Application Key
                }
            } else if (!connection.username.empty()) {
                propCount += 2; // Username + Password
            } else if (connection.integrated_security) {
                propCount++; // Integrated Security
            }
            
            // Always add Authority and Resource for AAD auth
            propCount += 2;
            
            // Add tenant ID if available
            if (!connection.tenant_id.empty()) {
                propCount++;
            }
            break;
    }
    
    // Add encryption property if specified
    if (!connection.encryption.empty()) {
        propCount++;
    }
    
    // Set the properties for the connection
    DBPROP* dbProps = new DBPROP[propCount];
    DBPROPSET dbPropSet;

    // Initialize the property structures
    ZeroMemory(dbProps, sizeof(DBPROP) * propCount);

    int propIndex = 0;
    
    // Set the Data Source property - this varies by connection type
    dbProps[propIndex].dwPropertyID = DBPROP_INIT_DATASOURCE;
    dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
    dbProps[propIndex].vValue.vt = VT_BSTR;
    
    if (connection.conn_type == POWERBI) {
        // For Power BI: use the original connection string data source
        // powerbi://api.powerbi.com/v1.0/contoso.com/Sales
        std::wstring orig_server_str = L"powerbi://api.powerbi.com/v1.0/";
        orig_server_str += connection.tenant_id;
        if (!connection.workspace.empty()) {
            orig_server_str += L"/";
            orig_server_str += connection.workspace;
        }
        dbProps[propIndex].vValue.bstrVal = SysAllocString(orig_server_str.c_str());
    } else if (connection.conn_type == AZURE_SERVER) {
        // For Azure Analysis Services: construct the asazure:// URL
        std::wstring azure_server_str = L"asazure://";
        if (!connection.region.empty()) {
            azure_server_str += connection.region;
            azure_server_str += L".";
        }
        azure_server_str += L"asazure.windows.net/";
        azure_server_str += connection.server_name;
        dbProps[propIndex].vValue.bstrVal = SysAllocString(azure_server_str.c_str());
    } else {
        // Regular server connection
        dbProps[propIndex].vValue.bstrVal = SysAllocString(connection.server_name.c_str());
    }
    propIndex++;

    // Set the Catalog property (database name) only if provided
    if (!need_default_catalog) {
        dbProps[propIndex].dwPropertyID = DBPROP_INIT_CATALOG;
        dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
        dbProps[propIndex].vValue.vt = VT_BSTR;
        dbProps[propIndex].vValue.bstrVal = SysAllocString(connection.database_name.c_str());
        propIndex++;
    }

    // Set the Mode property to read-only
    dbProps[propIndex].dwPropertyID = DBPROP_INIT_MODE;
    dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
    dbProps[propIndex].vValue.vt = VT_I4;
    dbProps[propIndex].vValue.lVal = DB_MODE_READ; // Read-only mode
    propIndex++;
    
    // Set authentication properties based on connection type
    switch(connection.conn_type) {
        case LOCAL_SERVER:
            // Regular authentication
            if (connection.integrated_security) {
                dbProps[propIndex].dwPropertyID = DBPROP_AUTH_INTEGRATED;
                dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
                dbProps[propIndex].vValue.vt = VT_BSTR;
                dbProps[propIndex].vValue.bstrVal = SysAllocString(L"SSPI");
                propIndex++;
            } else if (!connection.username.empty()) {
                // Username
                dbProps[propIndex].dwPropertyID = DBPROP_AUTH_USERID;
                dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
                dbProps[propIndex].vValue.vt = VT_BSTR;
                dbProps[propIndex].vValue.bstrVal = SysAllocString(connection.username.c_str());
                propIndex++;
                
                // Password
                dbProps[propIndex].dwPropertyID = DBPROP_AUTH_PASSWORD;
                dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
                dbProps[propIndex].vValue.vt = VT_BSTR;
                dbProps[propIndex].vValue.bstrVal = SysAllocString(connection.password.c_str());
                propIndex++;
            }
            break;
            
        case AZURE_SERVER:
        case POWERBI:
            // Azure/Power BI specific properties
            
            // Set the authentication method for AAD
            if (!connection.application_id.empty()) {
                // Service Principal auth
                
                // Application ID
                dbProps[propIndex].dwPropertyID = 1; // MSOLAP specific property ID for AppID
                dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
                dbProps[propIndex].vValue.vt = VT_BSTR;
                dbProps[propIndex].vValue.bstrVal = SysAllocString(connection.application_id.c_str());
                propIndex++;
                
                // Application Key
                if (!connection.app_key.empty()) {
                    dbProps[propIndex].dwPropertyID = 2; // MSOLAP specific property ID for AppKey
                    dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
                    dbProps[propIndex].vValue.vt = VT_BSTR;
                    dbProps[propIndex].vValue.bstrVal = SysAllocString(connection.app_key.c_str());
                    propIndex++;
                }
            } else if (!connection.username.empty()) {
                // Username/Password auth
                dbProps[propIndex].dwPropertyID = DBPROP_AUTH_USERID;
                dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
                dbProps[propIndex].vValue.vt = VT_BSTR;
                dbProps[propIndex].vValue.bstrVal = SysAllocString(connection.username.c_str());
                propIndex++;
                
                dbProps[propIndex].dwPropertyID = DBPROP_AUTH_PASSWORD;
                dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
                dbProps[propIndex].vValue.vt = VT_BSTR;
                dbProps[propIndex].vValue.bstrVal = SysAllocString(connection.password.c_str());
                propIndex++;
            } else if (connection.integrated_security) {
                // Windows authentication
                dbProps[propIndex].dwPropertyID = DBPROP_AUTH_INTEGRATED;
                dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
                dbProps[propIndex].vValue.vt = VT_BSTR;
                dbProps[propIndex].vValue.bstrVal = SysAllocString(L"SSPI");
                propIndex++;
            }
            
            // Authority (common for Azure/Power BI)
            dbProps[propIndex].dwPropertyID = 3; // MSOLAP specific property ID for Authority
            dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
            dbProps[propIndex].vValue.vt = VT_BSTR;
            dbProps[propIndex].vValue.bstrVal = SysAllocString(connection.authority.c_str());
            propIndex++;
            
            // Resource (common for Azure/Power BI)
            dbProps[propIndex].dwPropertyID = 4; // MSOLAP specific property ID for Resource
            dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
            dbProps[propIndex].vValue.vt = VT_BSTR;
            dbProps[propIndex].vValue.bstrVal = SysAllocString(connection.resource.c_str());
            propIndex++;
            
            // Tenant ID if available
            if (!connection.tenant_id.empty()) {
                dbProps[propIndex].dwPropertyID = 5; // MSOLAP specific property ID for TenantID
                dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
                dbProps[propIndex].vValue.vt = VT_BSTR;
                dbProps[propIndex].vValue.bstrVal = SysAllocString(connection.tenant_id.c_str());
                propIndex++;
            }
            break;
    }
    
    // Set encryption if specified
    // if (!connection.encryption.empty()) {
    //     dbProps[propIndex].dwPropertyID = DBPROP_INIT_ENCRYPT;
    //     dbProps[propIndex].dwOptions = DBPROPOPTIONS_REQUIRED;
    //     dbProps[propIndex].vValue.vt = VT_BSTR;
    //     dbProps[propIndex].vValue.bstrVal = SysAllocString(connection.encryption.c_str());
    //     propIndex++;
    // }

    // Set up the property set
    dbPropSet.guidPropertySet = DBPROPSET_DBINIT;
    dbPropSet.cProperties = propCount;
    dbPropSet.rgProperties = dbProps;

    // Set the initialization properties
    hr = pIDBProperties->SetProperties(1, &dbPropSet);

    // Free the BSTR allocations
    for (int i = 0; i < propCount; i++) {
        if (dbProps[i].vValue.vt == VT_BSTR && dbProps[i].vValue.bstrVal) {
            SysFreeString(dbProps[i].vValue.bstrVal);
        }
    }

    delete[] dbProps;
    
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&pIDBProperties);
        MSOLAPUtils::SafeRelease(&connection.pIDBInitialize);
        throw std::runtime_error("Failed to set connection properties: " + MSOLAPUtils::GetErrorMessage(hr));
    }

    // Initialize the data source
    hr = connection.pIDBInitialize->Initialize();

    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&pIDBProperties);
        MSOLAPUtils::SafeRelease(&connection.pIDBInitialize);
        throw std::runtime_error("Failed to initialize data source: " + MSOLAPUtils::GetErrorMessage(hr));
    }

    // If no catalog was specified, try to get the default catalog
    if (need_default_catalog) {
        connection.database_name = connection.GetDefaultCatalog();
        
        // If we found a default catalog, set it
        if (!connection.database_name.empty()) {
            // Create property for catalog
            DBPROP dbProp;
            DBPROPSET dbCatalogPropSet;
            
            ZeroMemory(&dbProp, sizeof(dbProp));
            
            // Set the Catalog property
            dbProp.dwPropertyID = DBPROP_INIT_CATALOG;
            dbProp.dwOptions = DBPROPOPTIONS_REQUIRED;
            dbProp.vValue.vt = VT_BSTR;
            dbProp.vValue.bstrVal = SysAllocString(connection.database_name.c_str());
            
            // Set up the property set
            dbCatalogPropSet.guidPropertySet = DBPROPSET_DBINIT;
            dbCatalogPropSet.cProperties = 1;
            dbCatalogPropSet.rgProperties = &dbProp;
            
            // Set the catalog property
            hr = pIDBProperties->SetProperties(1, &dbCatalogPropSet);
            
            // Free BSTR
            SysFreeString(dbProp.vValue.bstrVal);
            
            if (SUCCEEDED(hr)) {
                // Re-initialize with the catalog
                connection.pIDBInitialize->Uninitialize();
                hr = connection.pIDBInitialize->Initialize();
                if (FAILED(hr)) {
                    MSOLAPUtils::SafeRelease(&pIDBProperties);
                    MSOLAPUtils::SafeRelease(&connection.pIDBInitialize);
                    throw std::runtime_error("Failed to reinitialize with default catalog: " + MSOLAPUtils::GetErrorMessage(hr));
                }
            }
        }
    }
    
    MSOLAPUtils::SafeRelease(&pIDBProperties);

    // Create a session
    IDBCreateSession* pIDBCreateSession = NULL;
    hr = connection.pIDBInitialize->QueryInterface(IID_IDBCreateSession, (void**)&pIDBCreateSession);
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&connection.pIDBInitialize);
        throw std::runtime_error("Failed to get IDBCreateSession: " + MSOLAPUtils::GetErrorMessage(hr));
    }

    hr = pIDBCreateSession->CreateSession(NULL, IID_IDBCreateCommand, (IUnknown**)&connection.pIDBCreateCommand);
    MSOLAPUtils::SafeRelease(&pIDBCreateSession);
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&connection.pIDBInitialize);
        throw std::runtime_error("Failed to create session: " + MSOLAPUtils::GetErrorMessage(hr));
    }
    
    return connection;
}

IRowset* MSOLAPConnection::ExecuteQuery(const std::string &dax_query) {
    if (!IsOpen()) {
        throw std::runtime_error("Connection is not open");
    }
    
    // Convert query to wide string
    std::wstring wquery = WindowsUtil::UTF8ToUnicode(dax_query.c_str());
    
    // Create command object
    ICommand* pICommand = NULL;
    HRESULT hr = pIDBCreateCommand->CreateCommand(NULL, IID_ICommand, (IUnknown**)&pICommand);
    if (FAILED(hr)) {
        throw std::runtime_error("Failed to create command: " + MSOLAPUtils::GetErrorMessage(hr));
    }

    // Set the command text
    ICommandText* pICommandText = NULL;
    hr = pICommand->QueryInterface(IID_ICommandText, (void**)&pICommandText);
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&pICommand);
        throw std::runtime_error("Failed to get ICommandText: " + MSOLAPUtils::GetErrorMessage(hr));
    }

    hr = pICommandText->SetCommandText(DBGUID_DEFAULT, wquery.c_str());
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&pICommandText);
        MSOLAPUtils::SafeRelease(&pICommand);
        throw std::runtime_error("Failed to set command text: " + MSOLAPUtils::GetErrorMessage(hr));
    }

    // Set rowset properties to disable bookmarks/rowid column
    ICommandProperties* pICommandProperties = NULL;
    hr = pICommand->QueryInterface(IID_ICommandProperties, (void**)&pICommandProperties);
    if (SUCCEEDED(hr)) {
        DBPROP dbProps[1];
        DBPROPSET dbPropSet;

        // Initialize the property structures
        ZeroMemory(dbProps, sizeof(dbProps));

        // Set the Bookmarks property to false to exclude rowid column
        dbProps[0].dwPropertyID = DBPROP_BOOKMARKS;
        dbProps[0].dwOptions = DBPROPOPTIONS_REQUIRED;
        dbProps[0].vValue.vt = VT_BOOL;
        dbProps[0].vValue.boolVal = VARIANT_FALSE;  // Disable bookmarks

        // Set up the property set
        dbPropSet.guidPropertySet = DBPROPSET_ROWSET;
        dbPropSet.cProperties = 1;
        dbPropSet.rgProperties = dbProps;

        // Set the properties
        pICommandProperties->SetProperties(1, &dbPropSet);
        
        MSOLAPUtils::SafeRelease(&pICommandProperties);
    }

    // Execute the command
    IRowset* pIRowset = NULL;
    hr = pICommand->Execute(NULL, IID_IRowset, NULL, NULL, (IUnknown**)&pIRowset);
    MSOLAPUtils::SafeRelease(&pICommandText);
    MSOLAPUtils::SafeRelease(&pICommand);

    if (FAILED(hr)) {
        throw std::runtime_error("Query execution failed: " + MSOLAPUtils::GetErrorMessage(hr));
    }
    
    return pIRowset;
}

bool MSOLAPConnection::GetColumnInfo(IRowset *rowset, std::vector<std::string> &names, std::vector<LogicalType> &types) {
    if (!rowset) {
        return false;
    }
    
    // Get column information using IColumnsInfo
    IColumnsInfo* pIColumnsInfo = NULL;
    HRESULT hr = rowset->QueryInterface(IID_IColumnsInfo, (void**)&pIColumnsInfo);
    if (FAILED(hr)) {
        return false;
    }

    // Get column information
    DBORDINAL cColumns;
    WCHAR* pStringsBuffer = NULL;
    DBCOLUMNINFO* pColumnInfo = NULL;

    hr = pIColumnsInfo->GetColumnInfo(&cColumns, &pColumnInfo, &pStringsBuffer);
    if (FAILED(hr)) {
        MSOLAPUtils::SafeRelease(&pIColumnsInfo);
        return false;
    }

    // Process column information
    names.clear();
    types.clear();
    
    for (DBORDINAL i = 0; i < cColumns; i++) {
        std::string column_name;
        if (pColumnInfo[i].pwszName) {
            // Sanitize column name (replace [] with _)
            column_name = MSOLAPUtils::SanitizeColumnName(pColumnInfo[i].pwszName);
        } else {
            column_name = "Column" + std::to_string(i);
        }
        
        names.push_back(column_name);
        types.push_back(MSOLAPUtils::GetLogicalTypeFromDBTYPE(pColumnInfo[i].wType));
    }
    
    // Clean up
    CoTaskMemFree(pColumnInfo);
    CoTaskMemFree(pStringsBuffer);
    MSOLAPUtils::SafeRelease(&pIColumnsInfo);
    
    return true;
}

bool MSOLAPConnection::IsOpen() const {
    return pIDBInitialize != nullptr && pIDBCreateCommand != nullptr;
}

void MSOLAPConnection::Close() {
    if (pIDBCreateCommand) {
        MSOLAPUtils::SafeRelease(&pIDBCreateCommand);
    }
    
    if (pIDBInitialize) {
        pIDBInitialize->Uninitialize();
        MSOLAPUtils::SafeRelease(&pIDBInitialize);
    }
}

} // namespace duckdb