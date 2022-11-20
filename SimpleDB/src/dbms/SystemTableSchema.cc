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
     .hasDefault = false}};

}  // namespace SimpleDB