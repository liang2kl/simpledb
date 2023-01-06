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
DECLARE_ERROR(TooManyForeignKeys, TableErrorBase, "Too many foreign keys");
DECLARE_ERROR(InvalidPrimaryKey, TableErrorBase, "Invalid primary key");
DECLARE_ERROR(InvalidForeignKey, TableErrorBase, "Invalid foreign key");
DECLARE_ERROR(PrimaryKeyExists, TableErrorBase, "Primary key exists");
DECLARE_ERROR(PrimaryKeyNotExists, TableErrorBase,
              "Primary key does not exist");
DECLARE_ERROR(NullValueFoundInNotNullColumn, TableErrorBase,
              "Null value found in not null column");
DECLARE_ERROR(NullValueGivenForNotNullColumn, TableErrorBase,
              "Null value given for not null column");
DECLARE_ERROR(ValueNotGiven, TableErrorBase,
              "The value of a column without default value is not given");
DECLARE_ERROR(IncorrectColumnNum, TableErrorBase,
              "Incorrect number of columns are given");
DECLARE_ERROR(ForeignKeyViolation, TableErrorBase,
              "Violating foreign key constraints");

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
DECLARE_ERROR(IndexKeyExists, IndexErrorBase, "Duplicate index key found");
DECLARE_ERROR(IndexKeyNotExists, IndexErrorBase, "The index does not exist");
DECLARE_ERROR(WriteOnReadOnlyIndex, IndexErrorBase,
              "Internal: trying to write on a read-only index");

// ==== QueryBuilder Error ====
DECLARE_ERROR_CLASS(QueryBuilder, InternalErrorBase, "QueryBuilder error");
DECLARE_ERROR(MultipleScan, QueryBuilderErrorBase,
              "Multiple tables are provided for scan");
DECLARE_ERROR(InvalidLimit, QueryBuilderErrorBase, "Invalid limit given");
DECLARE_ERROR(NoScanDataSource, QueryBuilderErrorBase,
              "No scan data source provided");
DECLARE_ERROR(ColumnNotFound, QueryBuilderErrorBase,
              "Column not found in table");
DECLARE_ERROR(Aggregator, QueryBuilderErrorBase, "Invalid aggregator");
DECLARE_ERROR(AmbiguousColumn, QueryBuilderErrorBase, "Ambiguous column");
}  // namespace Internal

// Exposed errors.
namespace Error {

DECLARE_ERROR_CLASS(Execution, BaseError, "Fail to execute SQL");
DECLARE_ERROR(Syntax, ExecutionErrorBase, "Syntax error");
DECLARE_ERROR(Unknown, ExecutionErrorBase, "Unknown exception");
DECLARE_ERROR(Internal, ExecutionErrorBase, "Internal error");
DECLARE_ERROR(IncompatableValue, ExecutionErrorBase,
              "Incompatable value error");
DECLARE_ERROR(Uninitialized, ExecutionErrorBase, "DMBS is uninitialized");
DECLARE_ERROR(Initialization, ExecutionErrorBase, "Fail to initialize DMBS");
DECLARE_ERROR(InvalidDatabaseName, ExecutionErrorBase, "Invalid database name");
DECLARE_ERROR(DatabaseExists, ExecutionErrorBase,
              "The database already exists");
DECLARE_ERROR(CreateDatabase, ExecutionErrorBase, "Fail to create database");
DECLARE_ERROR(DatabaseNotExist, ExecutionErrorBase,
              "The database does not exists");
DECLARE_ERROR(DatabaseNotSelected, ExecutionErrorBase,
              "No database is selected");
DECLARE_ERROR(TableExists, ExecutionErrorBase, "The table already exists");
DECLARE_ERROR(InvalidTableName, ExecutionErrorBase, "Invalid table name");
DECLARE_ERROR(TableNotExists, ExecutionErrorBase, "The table does not exist");
DECLARE_ERROR(MultiplePrimaryKey, ExecutionErrorBase,
              "More than one primary key is given");
DECLARE_ERROR(CreateTable, ExecutionErrorBase, "Fail to create table");
DECLARE_ERROR(DropTable, ExecutionErrorBase, "Fail to drop table");
DECLARE_ERROR(AlterPrimaryKey, ExecutionErrorBase, "Fail to alter primary key");
DECLARE_ERROR(AlterForeignKey, ExecutionErrorBase, "Fail to alter foreign key");
DECLARE_ERROR(AlterIndex, ExecutionErrorBase, "Fail to alter index");
DECLARE_ERROR(Insert, ExecutionErrorBase, "INSERT statement failed");
DECLARE_ERROR(Select, ExecutionErrorBase, "SELECT statement failed");
DECLARE_ERROR(Update, ExecutionErrorBase, "UPDATE statement failed");
DECLARE_ERROR(Delete, ExecutionErrorBase, "DELETE statement failed");

}  // namespace Error

#undef DECLARE_ERROR
#undef DECLARE_ERROR_CLASS

}  // namespace SimpleDB

#endif