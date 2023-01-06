#include "SQLService.h"

#include <SimpleDB/DBMS.h>
#include <SimpleDB/Error.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include <chrono>
#include <exception>
#include <iostream>
#include <mutex>
#include <ratio>
#include <sstream>
#include <vector>

using grpc::ServerContext;
using grpc::Status;
using grpc::StatusCode;
using SimpleDB::Service::ExecutionBatchResponse;
using SimpleDB::Service::ExecutionRequest;
using SimpleDB::Service::ExecutionResponse;
using SimpleDB::Service::ExecutionResult;
using ErrorType = SimpleDB::Service::ExecutionError::Type;

using namespace SimpleDB::Error;

#define STOP_CLOCK()                                                          \
    end = steady_clock::now();                                                \
    duration = duration_cast<std::chrono::microseconds>(end - begin).count(); \
    response->mutable_stats()->set_elapse(duration);

SQLService::SQLService(SimpleDB::DBMS* dbms) : dbms(dbms) {}

Status SQLService::ExecuteSQLProgram(ServerContext* context,
                                     const ExecutionRequest* request,
                                     ExecutionBatchResponse* response) {
    // Lock the mutex to prevent concurrent execution.
    std::lock_guard<std::mutex> _(mutex);

    const std::string sql = request->sql();
    std::istringstream stream(sql);

    using std::chrono::duration_cast;
    using std::chrono::steady_clock;

    steady_clock::time_point begin = steady_clock::now();
    steady_clock::time_point end;
    long long duration;

    try {
        std::vector<ExecutionResult> results = dbms->executeSQL(stream);

        STOP_CLOCK();

        for (const auto& result : results) {
            ExecutionResponse* resp = response->add_responses();
            *resp->mutable_current_db() = dbms->getCurrentDatabase();
            resp->mutable_result()->CopyFrom(result);
        }

        return Status::OK;
    } catch (SyntaxError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_SYNTAX,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (IncompatableValueError& e) {
        makeError(response,
                  ErrorType::ExecutionError_Type_ERR_INCOMPATIBLE_VALUE,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (DatabaseExistsError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_DATABASE_EXIST,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (CreateDatabaseError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_CREATE_DATABASE,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (DatabaseNotExistError& e) {
        makeError(response,
                  ErrorType::ExecutionError_Type_ERR_DATABASE_NOT_EXIST,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (UninitializedError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_UNINITIALIZED,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (InitializationError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_INITIALIZATION,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (InvalidDatabaseNameError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_INVAL_DB_NAME,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (DatabaseNotSelectedError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_DB_NOT_SELECTED,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (TableExistsError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_TABLE_EXISTS,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (InvalidTableNameError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_INVAL_TABLE_NAME,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (TableNotExistsError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_TABLE_NOT_EXISTS,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (MultiplePrimaryKeyError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_MUL_PRIKEY,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (CreateTableError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_CREATE_TABLE,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (AlterPrimaryKeyError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_ALT_PRIKEY,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (AlterForeignKeyError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_ALT_FOREIGN_KEY,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (AlterIndexError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_ALT_INDEX,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (InsertError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_INSERT,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (SelectError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_SELECT,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (UpdateError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_UPDATE,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (DeleteError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_DELETE,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (InternalError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_INTERNAL,
                  e.what());
        STOP_CLOCK();
        return Status::OK;
    } catch (std::exception& e) {
        STOP_CLOCK();
        return Status(StatusCode::INTERNAL,
                      std::string("Unexpected exception occured: ") + e.what());
    }
}

void SQLService::makeError(ExecutionBatchResponse* response, ErrorType type,
                           const std::string& message) {
    ExecutionResponse resp;
    resp.mutable_error()->set_type(type);
    resp.mutable_error()->set_message(message);
    response->mutable_responses()->Add(std::move(resp));
}