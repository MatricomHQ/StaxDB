#pragma once

#include <string>
#include <utility>
#include <filesystem>
#include "stax_common/os_platform_tools.h" 

namespace OSFileExtensions {

    
    OsFileHandleType open_file_for_writing(const std::filesystem::path& path);
    OsFileHandleType open_file_for_reading_writing(const std::filesystem::path& path);
    void close_file(OsFileHandleType handle);

    
    OsFileHandleType lock_file(const std::filesystem::path& lock_path);
    void unlock_file(OsFileHandleType lock_handle);


    
    std::string extend_file_raw(OsFileHandleType handle, size_t new_size);
    std::string write_to_file_raw(OsFileHandleType handle, const void* data, size_t size, size_t offset);

    
    std::pair<void*, std::string> map_file_raw(OsFileHandleType fd, size_t offset, size_t length, bool is_writeable);
    std::string unmap_file_raw(void* addr, size_t length);
    std::string flush_file_range_raw(void* addr, size_t length);

    
    size_t get_resident_memory_for_range(void* start_addr, size_t length);

} 