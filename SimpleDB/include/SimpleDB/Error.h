#ifndef _SIMPLEDB_BASE_ERROR_H
#define _SIMPLEDB_BASE_ERROR_H

#include <exception>

namespace SimpleDB {

struct BaseError : std::exception {
    virtual const char* what() const noexcept { return "Base error"; }
};

namespace Error {

#define DECLARE_ERROR(ErrorType, description)                             \
    struct ErrorType##Error : BaseError {                                 \
        virtual const char* what() const noexcept { return description; } \
    };

// ==== Storage Error ====
DECLARE_ERROR(OpenFile, "Fail to open file");
DECLARE_ERROR(CreateFile, "Fail to create file");
DECLARE_ERROR(CloseFile, "Fail to close file");
DECLARE_ERROR(ReadFile, "Fail to read file");
DECLARE_ERROR(WriteFile, "Fail to write file");
DECLARE_ERROR(DeleteFile, "Fail to delete file");
DECLARE_ERROR(FileExists, "File already exists");
DECLARE_ERROR(InvalidDescriptor, "Invalid file descriptor");
DECLARE_ERROR(InvalidPageNumber, "Invalid file descriptor");
DECLARE_ERROR(OpenFileExceeded, "Number of opened files has exceeded");

#undef DECLARE_ERROR

}  // namespace Error

}  // namespace SimpleDB

#endif