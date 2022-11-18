#pragma once

#include <SimpleDB/DBMS.h>
#include <SimpleDB/SimpleDB.h>
#include <SimpleDBService/query.grpc.pb.h>

#include <mutex>

class SQLService final : public SimpleDB::Service::Query::Service {
public:
    SQLService(SimpleDB::DBMS* dbms);
    virtual ::grpc::Status ExecuteSQLProgram(
        ::grpc::ServerContext* context,
        const ::SimpleDB::Service::ExecutionRequest* request,
        ::SimpleDB::Service::ExecutionBatchResponse* response) override;

private:
    SimpleDB::DBMS* dbms;
    void makeError(SimpleDB::Service::ExecutionBatchResponse* resp,
                   SimpleDB::Service::ExecutionError::Type type,
                   const std::string& message);
    std::mutex mutex;
};
