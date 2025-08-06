#include "stax_common/os_file_extensions.h"
#include "stax_common/os_platform_tools.h"

#include <string>
#include <cstring>
#include <system_error>
#include <filesystem>
#include <vector> 


#if defined(__unix__) || defined(__APPLE__)
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/file.h> 
#elif defined(_WIN32)
#include <windows.h>
#include <winioctl.h> 
#include <psapi.h> 
#endif


#if defined(__APPLE__)
#include <mach/mach.h>
#endif


namespace OSFileExtensions {

#if defined(__unix__) || defined(__APPLE__)
    
    OsFileHandleType open_file_for_writing(const std::filesystem::path& path) {
        return open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    }
    OsFileHandleType open_file_for_reading_writing(const std::filesystem::path& path) {
        return open(path.c_str(), O_RDWR, 0644);
    }
    void close_file(OsFileHandleType handle) {
        if (handle != INVALID_OS_FILE_HANDLE) close(handle);
    }

    
    OsFileHandleType lock_file(const std::filesystem::path& lock_path) {
        int fd = open(lock_path.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd == -1) {
            return INVALID_OS_FILE_HANDLE;
        }
        if (flock(fd, LOCK_EX) == -1) {
            close(fd);
            return INVALID_OS_FILE_HANDLE;
        }
        return fd;
    }
    void unlock_file(OsFileHandleType lock_handle) {
        if (lock_handle != INVALID_OS_FILE_HANDLE) {
            flock(lock_handle, LOCK_UN);
            close(lock_handle);
        }
    }


    
    std::string extend_file_raw(OsFileHandleType handle, size_t new_size) {
        if (ftruncate(handle, new_size) == -1) {
            return "ftruncate failed: " + std::string(strerror(errno));
        }
        return "";
    }

    std::string write_to_file_raw(OsFileHandleType handle, const void* data, size_t size, size_t offset) {
        if (pwrite(handle, data, size, offset) == -1) {
             return "pwrite failed: " + std::string(strerror(errno));
        }
        return "";
    }

    
    std::pair<void*, std::string> map_file_raw(OsFileHandleType fd, size_t offset, size_t length, bool is_writeable) {
        if (length == 0) return {nullptr, ""};
        int prot = PROT_READ;
        if (is_writeable) prot |= PROT_WRITE;
        void* addr = mmap(nullptr, length, prot, MAP_SHARED, fd, offset);
        if (addr == MAP_FAILED) {
            return {nullptr, "mmap failed: " + std::string(strerror(errno))};
        }
        return {addr, ""};
    }

    std::string unmap_file_raw(void* addr, size_t length) {
        if (munmap(addr, length) == -1) {
            return "munmap failed: " + std::string(strerror(errno));
        }
        return "";
    }

    std::string flush_file_range_raw(void* addr, size_t length) {
        if (msync(addr, length, MS_SYNC) == -1) {
            return "msync failed: " + std::string(strerror(errno));
        }
        return "";
    }
    
    
    size_t get_resident_memory_for_range(void* start_addr, size_t length) {
        size_t page_size = sysconf(_SC_PAGESIZE);
        
        uintptr_t start_ptr = reinterpret_cast<uintptr_t>(start_addr);
        uintptr_t start_page_aligned = start_ptr & ~(page_size - 1);
        
        
        size_t adjusted_length = length + (start_ptr - start_page_aligned);
        size_t num_pages = (adjusted_length + page_size - 1) / page_size;

        
        // !!! BUG FOUND !!!!!
        // Root Cause: The `mincore` system call on Linux/Unix requires its third argument to be an `unsigned char*`.
        // The original code used `std::vector<char>`, whose `data()` method returns a `char*`.
        // This type mismatch is a strict error on the GCC compiler used in the Linux GitHub Actions runner.
        // Fix: Change the vector type to `std::vector<unsigned char>` to match the `mincore` signature.
        std::vector<unsigned char> vec(num_pages);
        
        
        
        if (mincore(reinterpret_cast<void*>(start_page_aligned), adjusted_length, vec.data()) != 0) {
            
            return 0;
        }

        size_t resident_pages = 0;
        for (size_t i = 0; i < num_pages; ++i) {
            
            if (vec[i] & 1) {
                resident_pages++;
            }
        }
        
        return resident_pages * page_size;
    }


#elif defined(_WIN32)
    
    OsFileHandleType open_file_for_writing(const std::filesystem::path& path) {
        HANDLE hFile = CreateFileA(path.string().c_str(), GENERIC_READ | GENERIC_WRITE, 0, NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
        if (hFile == INVALID_HANDLE_VALUE) {
            return INVALID_HANDLE_VALUE;
        }

        
        DWORD bytesReturned = 0;
        if (!DeviceIoControl(hFile, FSCTL_SET_SPARSE, NULL, 0, NULL, 0, &bytesReturned, NULL)) {
            
            
            CloseHandle(hFile);
            return INVALID_HANDLE_VALUE;
        }

        return hFile;
    }
    OsFileHandleType open_file_for_reading_writing(const std::filesystem::path& path) {
        return CreateFileA(path.string().c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
    }
    void close_file(OsFileHandleType handle) {
        if (handle != INVALID_OS_FILE_HANDLE) CloseHandle(handle);
    }

    
    OsFileHandleType lock_file(const std::filesystem::path& lock_path) {
        HANDLE hLockFile = CreateFileA(lock_path.string().c_str(), GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, NULL);
        if (hLockFile == INVALID_HANDLE_VALUE) {
            return INVALID_HANDLE_VALUE;
        }
        OVERLAPPED overlapped = {0};
        if (!LockFileEx(hLockFile, LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, &overlapped)) {
            CloseHandle(hLockFile);
            return INVALID_HANDLE_VALUE;
        }
        return hLockFile;
    }
    void unlock_file(OsFileHandleType lock_handle) {
        if (lock_handle != INVALID_OS_FILE_HANDLE) {
            OVERLAPPED overlapped = {0};
            UnlockFileEx(lock_handle, 0, 1, 0, &overlapped);
            CloseHandle(lock_handle);
        }
    }


    
    std::string extend_file_raw(OsFileHandleType handle, size_t new_size) {
        LARGE_INTEGER li;
        li.QuadPart = new_size;
        if (!SetFilePointerEx(handle, li, NULL, FILE_BEGIN)) {
            return "SetFilePointerEx failed: " + std::system_category().message(GetLastError());
        }
        if (!SetEndOfFile(handle)) {
            return "SetEndOfFile failed: " + std::system_category().message(GetLastError());
        }
        return "";
    }

    std::string write_to_file_raw(OsFileHandleType handle, const void* data, size_t size, size_t offset) {
        OVERLAPPED overlapped = {0};
        overlapped.Offset = static_cast<DWORD>(offset);
        overlapped.OffsetHigh = static_cast<DWORD>(offset >> 32);

        DWORD bytes_written = 0;
        if (!WriteFile(handle, data, static_cast<DWORD>(size), &bytes_written, &overlapped)) {
            if (GetLastError() != ERROR_IO_PENDING) {
                return "WriteFile failed: " + std::system_category().message(GetLastError());
            }
            if (!GetOverlappedResult(handle, &overlapped, &bytes_written, TRUE)) {
                 return "GetOverlappedResult for WriteFile failed: " + std::system_category().message(GetLastError());
            }
        }
        if (bytes_written != size) {
            return "WriteFile: Wrote fewer bytes than requested.";
        }
        return "";
    }

    
    std::pair<void*, std::string> map_file_raw(OsFileHandleType hFile, size_t offset, size_t length, bool is_writeable) {
        if (length == 0) return {nullptr, ""};
        
        DWORD protect = is_writeable ? (PAGE_READWRITE | SEC_RESERVE) : (PAGE_READONLY | SEC_RESERVE);
        LARGE_INTEGER li_max_size;
        li_max_size.QuadPart = length;

        HANDLE hMap = CreateFileMappingA(hFile, NULL, protect, li_max_size.HighPart, li_max_size.LowPart, NULL);
        if (hMap == NULL) {
            return {nullptr, "CreateFileMapping failed: " + std::system_category().message(GetLastError())};
        }

        DWORD desired_access = is_writeable ? FILE_MAP_WRITE : FILE_MAP_READ;
        LARGE_INTEGER li_offset;
        li_offset.QuadPart = offset;

        void* addr = MapViewOfFile(hMap, desired_access, li_offset.HighPart, li_offset.LowPart, length);
        
        CloseHandle(hMap);

        if (addr == NULL) {
            return {nullptr, "MapViewOfFile failed: " + std::system_category().message(GetLastError())};
        }
        return {addr, ""};
    }

    std::string unmap_file_raw(void* addr, size_t length) {
        if (!UnmapViewOfFile(addr)) {
            return "UnmapViewOfFile failed: " + std::system_category().message(GetLastError());
        }
        return "";
    }

    std::string flush_file_range_raw(void* addr, size_t length) {
        if (!FlushViewOfFile(addr, length)) {
            return "FlushViewOfFile failed: " + std::system_category().message(GetLastError());
        }
        return "";
    }
    
    
    size_t get_resident_memory_for_range(void* start_addr, size_t length) {
        SYSTEM_INFO sysInfo;
        GetSystemInfo(&sysInfo);
        size_t page_size = sysInfo.dwPageSize;
        
        uintptr_t start_ptr = reinterpret_cast<uintptr_t>(start_addr);
        size_t num_pages = (length + (start_ptr % page_size) + page_size - 1) / page_size;
        
        std::vector<PSAPI_WORKING_SET_EX_BLOCK> ws_blocks(num_pages);
        PSAPI_WORKING_SET_EX_INFORMATION ws_info = {0};
        ws_info.VirtualAddress = start_addr;
        ws_info.VirtualAttributes.Blocks = ws_blocks.data();

        size_t resident_pages = 0;
        if (QueryWorkingSetEx(GetCurrentProcess(), &ws_info, sizeof(ws_info))) {
            for (size_t i = 0; i < num_pages; ++i) {
                if (ws_blocks[i].Valid) {
                    resident_pages++;
                }
            }
        }
        return resident_pages * page_size;
    }
#endif
} 