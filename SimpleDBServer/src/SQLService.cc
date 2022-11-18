#include "SQLService.h"

#include <SimpleDB/DBMS.h>
#include <SimpleDB/Error.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/status.h>

#include <exception>
#include <iostream>
#include <mutex>
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

SQLService::SQLService(SimpleDB::DBMS* dbms) : dbms(dbms) {}

Status SQLService::ExecuteSQLProgram(ServerContext* context,
                                     const ExecutionRequest* request,
                                     ExecutionBatchResponse* response) {
    // Lock the mutex to prevent concurrent execution.
    std::lock_guard<std::mutex> _(mutex);

    const std::string sql = request->sql();
    std::istringstream stream(sql);

    try {
        std::vector<ExecutionResult> results = dbms->executeSQL(stream);
        for (const auto& result : results) {
            ExecutionResponse resp;
            resp.mutable_result()->CopyFrom(result);
            response->mutable_responses()->Add(std::move(resp));
        }
    } catch (SyntaxError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_SYNTAX,
                  e.what());
        return Status::OK;
    } catch (IncompatableValueError& e) {
        makeError(response,
                  ErrorType::ExecutionError_Type_ERR_INCOMPATIBLE_VALUE,
                  e.what());
        return Status::OK;
    } catch (DatabaseExistsError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_DATABASE_EXIST,
                  e.what());
        return Status::OK;
    } catch (CreateDatabaseError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_CREATE_DATABASE,
                  e.what());
        return Status::OK;
    } catch (DatabaseNotExistError& e) {
        makeError(response,
                  ErrorType::ExecutionError_Type_ERR_DATABASE_NOT_EXIST,
                  e.what());
        return Status::OK;
    } catch (InternalError& e) {
        makeError(response, ErrorType::ExecutionError_Type_ERR_INTERNAL,
                  e.what());
        return Status::OK;
    } catch (std::exception& e) {
        return Status(StatusCode::INTERNAL,
                      std::string("Unexpected exception occured: ") + e.what());
    }

    return Status::OK;
}

void SQLService::makeError(ExecutionBatchResponse* response, ErrorType type,
                           const std::string& message) {
    ExecutionResponse resp;
    resp.mutable_error()->set_type(type);
    resp.mutable_error()->set_message(message);
    response->mutable_responses()->Add(std::move(resp));
}