#include "msolap_db.hpp"
#include "msolap_stmt.hpp"
#include <comdef.h>

namespace duckdb {

MSOLAPDB::MSOLAPDB() 
    : com_initializer(nullptr),
      pIDBInitialize(nullptr),
      pIDBCreateSession(nullptr),
      pIDBCreateCommand(nullptr),
      pICommand(nullptr),
      connected(false),
      timeout_seconds(60) {
}

MSOLAPDB::~MSOLAPDB() {
    Close();
}

MSOLAPDB::MSOLAPDB(MSOLAPDB &&other) noexcept
    : com_initializer(std::move(other.com_initializer)),
      pIDBInitialize(other.pIDBInitialize),
      pIDBCreateSession(other.pIDBCreateSession),
      pIDBCreateCommand(other.pIDBCreateCommand),
      pICommand(other.pICommand),
      connected(other.connected),
      timeout_seconds(other.timeout_seconds) {
    
    // Clear the moved-from object's pointers
    other.pIDBInitialize = nullptr;
    other.pIDBCreateSession = nullptr;
    other.pIDBCreateCommand = nullptr;
    other.pICommand = nullptr;
    other.connected = false;
}

MSOLAPDB &MSOLAPDB::operator=(MSOLAPDB &&other) noexcept {
    if (this != &other) {
        // Clean up existing resources
        Close();
        
        // Move resources from other
        com_initializer = std::move(other.com_initializer);
        pIDBInitialize = other.pIDBInitialize;
        pIDBCreateSession = other.pIDBCreateSession;
        pIDBCreateCommand = other.pIDBCreateCommand;
        pICommand = other.pICommand;
        connected = other.connected;
        timeout_seconds = other.timeout_seconds;
        
        // Clear the moved-from object's pointers
        other.pIDBInitialize = nullptr;
        other.pIDBCreateSession = nullptr;
        other.pIDBCreateCommand = nullptr;
        other.pICommand = nullptr;
        other.connected = false;
    }
    return *this;
}

MSOLAPDB MSOLAPDB::Open(const std::string& connection_string, const MSOLAPOpenOptions &options) {
    MSOLAPDB db;
    db.Initialize(connection_string, options);
    return db;
}

void MSOLAPDB::Initialize(const std::string& connection_string, const MSOLAPOpenOptions &options) {
    // Store timeout setting
    this->timeout_seconds = options.timeout_seconds;
    
    // Create COM initializer
    com_initializer = std::make_unique<COMInitializer>();
    if (!com_initializer->IsInitialized()) {
        throw MSOLAPException("Failed to initialize COM");
    }
    
    HRESULT hr;
    
    // Create OLEDB Data Source Object
    IDBProperties* pIDBProperties = NULL;
    
    hr = ::CoCreateInstance(CLSID_MSOLAP, NULL, CLSCTX_INPROC_SERVER, 
                         IID_IDBInitialize, (void**)&pIDBInitialize);
    if (FAILED(hr)) {
        throw MSOLAPException(hr, "Failed to create MSOLAP instance");
    }
    
    // Get the IDBProperties interface
    hr = pIDBInitialize->QueryInterface(IID_IDBProperties, (void**)&pIDBProperties);
    if (FAILED(hr)) {
        ::SafeRelease(&pIDBInitialize);
        throw MSOLAPException(hr, "Failed to get IDBProperties interface");
    }
    
    // Set connection properties
    DBPROP rgProps[1];
    DBPROPSET rgPropSets[1];
    
    // Initialize the property structure
    rgProps[0].dwPropertyID = DBPROP_INIT_PROVIDERSTRING;
    rgProps[0].dwOptions = DBPROPOPTIONS_REQUIRED;
    rgProps[0].vValue.vt = VT_BSTR;
    rgProps[0].vValue.bstrVal = StringToBSTR(connection_string);
    rgProps[0].colid = DB_NULLID;
    
    // Initialize the property set structure
    rgPropSets[0].guidPropertySet = DBPROPSET_DBINIT;
    rgPropSets[0].cProperties = 1;
    rgPropSets[0].rgProperties = rgProps;
    
    // Set the properties
    hr = pIDBProperties->SetProperties(1, rgPropSets);
    
    // Free the BSTR
    ::SysFreeString(rgProps[0].vValue.bstrVal);
    
    // Release the IDBProperties interface
    ::SafeRelease(&pIDBProperties);
    
    if (FAILED(hr)) {
        ::SafeRelease(&pIDBInitialize);
        throw MSOLAPException(hr, "Failed to set connection properties");
    }
    
    // Initialize the data source
    hr = pIDBInitialize->Initialize();
    if (FAILED(hr)) {
        ::SafeRelease(&pIDBInitialize);
        throw MSOLAPException(hr, "Failed to initialize data source");
    }
    
    // Get the IDBCreateSession interface
    hr = pIDBInitialize->QueryInterface(IID_IDBCreateSession, (void**)&pIDBCreateSession);
    if (FAILED(hr)) {
        ::SafeRelease(&pIDBInitialize);
        throw MSOLAPException(hr, "Failed to get IDBCreateSession interface");
    }
    
    // Create a session
    IDBCreateCommand* pTempCreateCommand = NULL;
    hr = pIDBCreateSession->CreateSession(NULL, IID_IDBCreateCommand, (IUnknown**)&pTempCreateCommand);
    if (FAILED(hr)) {
        ::SafeRelease(&pIDBCreateSession);
        ::SafeRelease(&pIDBInitialize);
        throw MSOLAPException(hr, "Failed to create session");
    }
    
    pIDBCreateCommand = pTempCreateCommand;
    connected = true;
}

void MSOLAPDB::Close() {
    ::SafeRelease(&pICommand);
    ::SafeRelease(&pIDBCreateCommand);
    ::SafeRelease(&pIDBCreateSession);
    
    if (pIDBInitialize) {
        pIDBInitialize->Uninitialize();
        ::SafeRelease(&pIDBInitialize);
    }
    
    connected = false;
}

MSOLAPStatement MSOLAPDB::Prepare(const std::string& dax_query) {
    if (!connected) {
        throw MSOLAPException("Database not connected");
    }
    
    return MSOLAPStatement(*this, dax_query);
}

void MSOLAPDB::Execute(const std::string& dax_query) {
    auto stmt = Prepare(dax_query);
    stmt.Execute();
}

bool MSOLAPDB::IsConnected() const {
    return connected;
}

} // namespace duckdb