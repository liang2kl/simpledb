#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    SimpleDB::Logger::setLogLevel(SimpleDB::SILENT);
    return RUN_ALL_TESTS();
}
