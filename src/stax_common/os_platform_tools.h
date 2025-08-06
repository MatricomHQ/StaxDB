#pragma once


#if defined(__unix__) || defined(__APPLE__)

#elif defined(_WIN32)
#include <windows.h> 
#endif

#if defined(__unix__) || defined(__APPLE__)
using OsFileHandleType = int;
const OsFileHandleType INVALID_OS_FILE_HANDLE = -1;
#elif defined(_WIN32)
using OsFileHandleType = HANDLE;

const OsFileHandleType INVALID_OS_FILE_HANDLE = INVALID_HANDLE_VALUE; 
#else
#error "Unsupported platform: No OsFileHandleType defined"
#endif