#ifndef _SIMPLEDB_SIMPLEDB_H
#define _SIMPLEDB_SIMPLEDB_H

#include "Error.h"
#include "internal/Logger.h"

// The latter is just for code highliging.
#if TESTING
#include "internal/Index.h"
#include "internal/PageFile.h"
#endif

// Include at last, as it may include other headers that conflict with existing
// declarations.
#include "DBMS.h"

#endif