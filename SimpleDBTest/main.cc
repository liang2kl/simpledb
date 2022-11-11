#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    SimpleDB::Internal::Logger::setLogLevel(SimpleDB::Internal::SILENT);
    return RUN_ALL_TESTS();
}
