#include "DBMS.h"

using namespace SimpleDB::Internal;

namespace SimpleDB {
std::vector<Internal::ColumnMeta> DBMS::systemDatabaseTableColumns = {
    {.type = VARCHAR,
     .size = MAX_DATABASE_NAME_LEN,
     .nullable = false,
     .name = "name",
     .hasDefault = false}};

std::vector<Internal::ColumnMeta> DBMS::systemTablesTableColumns = {
    {.type = VARCHAR,
     .size = MAX_TABLE_NAME_LEN,
     .nullable = false,
     .name = "name",
     .hasDefault = false},
    {.type = VARCHAR,
     .size = MAX_DATABASE_NAME_LEN,
     .nullable = false,
     .name = "database",
     .hasDefault = false},
    {.type = INT,
     .nullable = true,
     .name = "primary_key",
     .hasDefault = false}};

std::vector<Internal::ColumnMeta> DBMS::systemIndexesTableColumns = {
    {.type = VARCHAR,
     .size = MAX_DATABASE_NAME_LEN,
     .nullable = false,
     .name = "database",
     .hasDefault = false},
    {.type = VARCHAR,
     .size = MAX_TABLE_NAME_LEN,
     .nullable = false,
     .name = "table",
     .hasDefault = false},
    {.type = VARCHAR,
     .size = MAX_COLUMN_NAME_LEN,
     .nullable = false,
     .name = "field",
     .hasDefault = false},
    /* type 0: primary key index; type 1: ordinary index; type 2: migrated from
       ordinary index to primary key index */
    {.type = INT, .nullable = false, .name = "type", .hasDefault = false}};

std::vector<Internal::ColumnMeta> DBMS::systemForeignKeyTableColumns = {
    {.type = VARCHAR,
     .size = MAX_DATABASE_NAME_LEN,
     .nullable = false,
     .name = "database",
     .hasDefault = false},
    {.type = VARCHAR,
     .size = MAX_TABLE_NAME_LEN,
     .nullable = false,
     .name = "table",
     .hasDefault = false},
    {.type = VARCHAR,
     .size = MAX_COLUMN_NAME_LEN,
     .nullable = false,
     .name = "column",
     .hasDefault = false},
    {.type = VARCHAR,
     .size = MAX_TABLE_NAME_LEN,
     .nullable = false,
     .name = "ref_table",
     .hasDefault = false},
    {.type = VARCHAR,
     .size = MAX_COLUMN_NAME_LEN,
     .nullable = false,
     .name = "ref_column",
     .hasDefault = false},
};

}  // namespace SimpleDB