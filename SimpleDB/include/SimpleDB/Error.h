#ifndef _SIMPLEDB_BASE_ERROR_H
#define _SIMPLEDB_BASE_ERROR_H

#include <exception>

namespace SimpleDB {

struct BaseError : std::exception {
    virtual const char* what() const noexcept { return "Base error"; }
};

namespace Error {
#define DECLARE_ERROR_CLASS(ErrorClass, BaseClass, description)           \
    struct ErrorClass##ErrorBase : BaseClass {                            \
        virtual const char* what() const noexcept { return description; } \
    };

#define DECLARE_ERROR(ErrorType, BaseClass, description)                  \
    struct ErrorType##Error : BaseClass {                                 \
        virtual const char* what() const noexcept { return description; } \
    };

// ==== I/O Error ====
DECLARE_ERROR_CLASS(IO, BaseError, "I/O error");

DECLARE_ERROR(OpenFile, IOErrorBase, "Fail to open file");
DECLARE_ERROR(CreateFile, IOErrorBase, "Fail to create file");
DECLARE_ERROR(CloseFile, IOErrorBase, "Fail to close file");
DECLARE_ERROR(ReadFile, IOErrorBase, "Fail to read file");
DECLARE_ERROR(WriteFile, IOErrorBase, "Fail to write file");
DECLARE_ERROR(DeleteFile, IOErrorBase, "Fail to delete file");
DECLARE_ERROR(FileExists, IOErrorBase, "File already exists");
DECLARE_ERROR(InvalidDescriptor, IOErrorBase, "Invalid file descriptor");
DECLARE_ERROR(InvalidPageNumber, IOErrorBase, "Invalid file descriptor");
DECLARE_ERROR(OpenFileExceeded, IOErrorBase,
              "Number of opened files has exceeded");
DECLARE_ERROR(InvalidPageHandle, IOErrorBase, "Invalid page handle");

// ==== Table Operation Error ====
DECLARE_ERROR_CLASS(Table, BaseError, "Table operation error");

DECLARE_ERROR(ReadTable, TableErrorBase, "Fail to read table");
DECLARE_ERROR(CreateTable, TableErrorBase, "Fail to create table");
DECLARE_ERROR(DuplicateColumnName, TableErrorBase,
              "Duplicate column name found");
DECLARE_ERROR(TableNotInitialized, TableErrorBase,
              "The table is not initialized before used");
DECLARE_ERROR(ColumnSerialization, TableErrorBase,
              "Fail to serialize column from byte stream");
DECLARE_ERROR(InvalidSlot, TableErrorBase, "Invalid page/slot number");
DECLARE_ERROR(InvalidColumnSize, TableErrorBase, "Invalid column size");
DECLARE_ERROR(InvalidColumnIndex, TableErrorBase, "Invalid column index");
DECLARE_ERROR(ColumnFull, TableErrorBase, "The column is full");
DECLARE_ERROR(ColumnExists, TableErrorBase, "The column already exists");
DECLARE_ERROR(InvalidPageMeta, TableErrorBase, "The page meta is invalid");
DECLARE_ERROR(TooManyColumns, TableErrorBase, "Too many columns");
DECLARE_ERROR(NullValueFoundInNotNullColumn, TableErrorBase,
              "Null value found in not null column");
DECLARE_ERROR(NullValueGivenForNotNullColumn, TableErrorBase,
              "Null value given for not null column");

#undef DECLARE_ERROR

}  // namespace Error

}  // namespace SimpleDB

#endif