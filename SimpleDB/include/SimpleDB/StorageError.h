#ifndef _SIMPLEDB_STORAGE_ERROR_H
#define _SIMPLEDB_STORAGE_ERROR_H

#include "BaseError.h"

namespace SimpleDB {

struct StorageError : BaseError {
    enum Type {
        FAIL_OPEN_FILE,
        FAIL_CREATE_FILE,
        FAIL_CLOSE_FILE,
        FAIL_READ_FILE,
        FAIL_WRITE_FILE,
        FAIL_DELETE_FILE,
        INVALID_DESCRIPTOR,
        OPEN_FILES_EXCEEDED,
    };
    Type type;
};

}  // namespace SimpleDB

#endif