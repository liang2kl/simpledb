#ifndef _SIMPLEDB_BASE_ERROR_H
#define _SIMPLEDB_BASE_ERROR_H

#include <exception>
#include <string>

namespace SimpleDB {

struct BaseError : std::exception {
    BaseError(std::string description) : description(description) {}
    virtual const char* what() const noexcept { return description.c_str(); }

private:
    std::string description;
};

#define DECLARE_ERROR_CLASS(ErrorClass, BaseClass, description) \
    struct ErrorClass##ErrorBase : BaseError {                  \
        ErrorClass##ErrorBase(std::string _description)         \
            : BaseError(_description) {}                        \
        ErrorClass##ErrorBase() : BaseError(description) {}     \
    };

#define DECLARE_ERROR(ErrorType, BaseClass, description)                   \
    struct ErrorType##Error : BaseClass {                                  \
        ErrorType##Error(std::string _description)                         \
            : BaseClass(std::string(description) + ": " + _description) {} \
        ErrorType##Error() : BaseClass(description) {}                     \
    };

// Internal errors.
namespace Internal {

DECLARE_ERROR_CLASS(Internal, BaseError, "Internal error");

// ==== I/O Error ====
DECLARE_ERROR_CLASS(IO, InternalErrorBase, "I/O error");

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
DECLARE_ERROR_CLASS(Table, InternalErrorBase, "Table operation error");

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

// ==== Iterator Error ====
DECLARE_ERROR_CLASS(Iterator, InternalErrorBase, "Iterator error");

DECLARE_ERROR(InvalidColumnName, IteratorErrorBase, "Invalid column name");
DECLARE_ERROR(UnexpedtedOperator, IteratorErrorBase, "Unexpected operator");
DECLARE_ERROR(InvalidOperator, IteratorErrorBase, "Invalid operator");
DECLARE_ERROR(InvalidRegex, IteratorErrorBase,
              "Invalid input regular expression");

// ==== Index Error ====
DECLARE_ERROR_CLASS(Index, InternalErrorBase, "Index error");

DECLARE_ERROR(InvalidIndexMeta, IndexErrorBase, "Invalid index meta");
DECLARE_ERROR(ReadIndex, IndexErrorBase, "Fail to read index");
DECLARE_ERROR(CreateIndex, IndexErrorBase, "Fail to create index");
DECLARE_ERROR(InvalidIndexType, IndexErrorBase, "Invalid index column type");
DECLARE_ERROR(IndexNotInitialized, IndexErrorBase,
              "The index is not initialized yet");
DECLARE_ERROR(IndexKeyExists, IndexErrorBase, "The index is already existed");
DECLARE_ERROR(IndexKeyNotExists, IndexErrorBase, "The index does not exist");

}  // namespace Internal

// Exposed errors.
namespace Error {

DECLARE_ERROR_CLASS(Execution, BaseError, "Fail to execute SQL");
DECLARE_ERROR(Syntax, ExecutionErrorBase, "Syntax error");
DECLARE_ERROR(Unknown, ExecutionErrorBase, "Unknown exception");
DECLARE_ERROR(Internal, ExecutionErrorBase, "Internal error");
DECLARE_ERROR(IncompatableValue, ExecutionErrorBase,
              "Incompatable value error");

}  // namespace Error

#undef DECLARE_ERROR
#undef DECLARE_ERROR_CLASS

}  // namespace SimpleDB

#endif