#include <SimpleDB/DBMS.h>
#include <SimpleDB/Error.h>
#include <SimpleDB/SimpleDB.h>
#include <SimpleDBService/query.grpc.pb.h>
#include <gflags/gflags.h>
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

// Forward declarations
void runServer();
void sigintHandler(int);
void initFromCLIFlags(int argc, char *argv[]);

// CLI flags and validators
DEFINE_string(dir, "", "Root directory of the database data");
DEFINE_bool(verbose, false, "Output verbose logs");
DEFINE_bool(debug, false, "Output debug logs");
DEFINE_bool(silent, false, "Silence all logs");
DEFINE_string(addr, "127.0.0.1:9100", "Server bind address");
DEFINE_validator(dir, [](const char *flagName, const std::string &value) {
    if (value.empty()) {
        std::cerr << "ERROR: --" << flagName << " must be specified"
                  << std::endl;
        return false;
    }
    return true;
});

SimpleDB::DBMS *dbms;
std::shared_ptr<grpc::Server> server;

int main(int argc, char *argv[]) {
    // Parse CLI flags
    initFromCLIFlags(argc, argv);

    // Register handlers
    signal(SIGINT, sigintHandler);

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

void initFromCLIFlags(int argc, char *argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    SimpleDB::Internal::LogLevel logLevel =
        SimpleDB::Internal::LogLevel::NOTICE;

    if (FLAGS_silent) {
        logLevel = SimpleDB::Internal::LogLevel::SILENT;
    } else if (FLAGS_debug) {
        logLevel = SimpleDB::Internal::LogLevel::DEBUG_;
    } else if (FLAGS_verbose) {
        logLevel = SimpleDB::Internal::LogLevel::VERBOSE;
    }

    SimpleDB::Internal::Logger::setLogLevel(logLevel);

    dbms = new SimpleDB::DBMS(FLAGS_dir);
}

void runServer() {
    SQLService service(dbms);
    grpc::ServerBuilder builder;

    builder.AddListeningPort(FLAGS_addr, grpc::InsecureServerCredentials())
        .RegisterService(&service)
        .BuildAndStart();
    server = builder.BuildAndStart();
    std::cout << "Server listening on " << FLAGS_addr << std::endl;
    server->Wait();

    delete dbms;
}

void sigintHandler(int) {
    // Exit gracefully...
    std::cout << "\nShutting down server..." << std::endl;
    server->Shutdown();
}
