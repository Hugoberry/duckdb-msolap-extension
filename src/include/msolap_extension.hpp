#pragma once

#include "duckdb.hpp"

namespace duckdb {

class ExtensionLoader;

class MsolapExtension{
public:
    // Attach the extension to the database
    static void Load(ExtensionLoader &loader);
    // Return the name of the extension
    static std::string Name(){
        return "msolap";
    }
    static std::string Version(){
#ifdef EXT_VERSION_MSOLAP
        return EXT_VERSION_MSOLAP;
#else
        return "v0.1.3";
#endif
    }
};

} // namespace duckdb