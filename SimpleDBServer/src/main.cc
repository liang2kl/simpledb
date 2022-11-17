#include <SimpleDB/DBMS.h>
#include <SimpleDB/Error.h>
#include <SimpleDB/SimpleDB.h>
#include <SimpleDBService/query.grpc.pb.h>
#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <string>

#include "SQLService.h"

void showUsage();
void runServer();
void sigintHandler(int);

SimpleDB::DBMS *dbms;
std::shared_ptr<grpc::Server> server;

int main(int argc, char *argv[]) {
    if (argc != 3) {
        showUsage();
    }

    std::string rootPath;

    if (argc == 3) {
        if (std::string(argv[1]).rfind("--dir") == 0) {
            rootPath = argv[2];
        } else {
            showUsage();
        }
    }

    // Register handlers
    signal(SIGINT, sigintHandler);

    // TODO: Read log level from commandline.
    SimpleDB::Internal::Logger::setLogLevel(
        SimpleDB::Internal::LogLevel::VERBOSE);

    dbms = new SimpleDB::DBMS(rootPath);

    try {
        dbms->init();
    } catch (SimpleDB::Error::InitializationError &e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }

    // Start grpc server.
    runServer();

    return 0;
}

void runServer() {
    SQLService service(dbms);
    grpc::ServerBuilder builder;

    const std::string serverAddress = "127.0.0.1:9100";

    builder.AddListeningPort(serverAddress, grpc::InsecureServerCredentials())
        .RegisterService(&service)
        .BuildAndStart();
    server = builder.BuildAndStart();
    std::cout << "Server listening on " << serverAddress << std::endl;
    server->Wait();
}

void showUsage() {
    std::cout << "Usage: <program> [--dir <data-directory>]\n";
    exit(1);
}

void sigintHandler(int) {
    // Exit gracefully...
    std::cout << "\nShutting down server..." << std::endl;
    server->Shutdown();
    delete dbms;
}
