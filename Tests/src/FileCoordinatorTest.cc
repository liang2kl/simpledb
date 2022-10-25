#define TESTING

#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

#include <filesystem>

using namespace SimpleDB;

class FileCoordinatorTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::create_directory("tmp"); }
    void TearDown() override { std::filesystem::remove_all("tmp"); }

    FileCoordinator coordinator;
};

// The same as FileManagerTest.TestWriteReadPage
TEST_F(FileCoordinatorTest, TestCoordinator) {
    // Initialize data.
    char buf[PAGE_SIZE] = {0x00, 0x00, 0x12, 0x24};
    buf[PAGE_SIZE - 2] = 0x36;

    const char filePath[] = "tmp/file-rw";
    EXPECT_NO_THROW(coordinator.createFile(filePath));
    ASSERT_TRUE(std::filesystem::exists(filePath))
        << "Fail to create file " << filePath;

    FileDescriptor fd = coordinator.openFile(filePath);

    EXPECT_NO_THROW(coordinator.writePage(fd, 2, buf));

    char readBuf[PAGE_SIZE];
    EXPECT_NO_THROW(coordinator.readPage(fd, 2, readBuf));

    EXPECT_EQ(memcmp(buf, readBuf, PAGE_SIZE), 0)
        << "Read data mismatch with written data";
    EXPECT_EQ(readBuf[PAGE_SIZE - 2], 0x36)
        << "Read data mismatch with written data";

    coordinator.closeFile(fd);

    // The CacheManager should have cleared the cache of this file.
    EXPECT_EQ(coordinator.cacheManager->activeCache.size(), 0);
    // The FileManager should have released the file descriptor.
    EXPECT_EQ(coordinator.fileManager->descriptorBitmap, 0);
}
