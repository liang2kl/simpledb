#ifndef TESTING
#define TESTING 1
#endif

#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

#include <filesystem>

using namespace SimpleDB;
using namespace SimpleDB::Internal;

#ifdef PAGE_SIZE
#undef PAGE_SIZE
#endif

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

    const char filePath[] = "tmp/file-rw";
    EXPECT_NO_THROW(coordinator.createFile(filePath));
    ASSERT_TRUE(std::filesystem::exists(filePath))
        << "Fail to create file " << filePath;

    FileDescriptor fd = coordinator.openFile(filePath);
    PageHandle handle = coordinator.getHandle(fd, 2);

    memcpy(coordinator.load(&handle), buf, PAGE_SIZE);
    ASSERT_NO_THROW(coordinator.markDirty(handle));

    ASSERT_NO_THROW(coordinator.closeFile(fd));
    ASSERT_NO_THROW(fd = coordinator.openFile(filePath));
    ASSERT_NO_THROW(handle = coordinator.getHandle(fd, 2));

    EXPECT_EQ(memcmp(buf, coordinator.load(&handle), PAGE_SIZE), 0)
        << "Read data mismatch with written data";

    coordinator.closeFile(fd);

    // The CacheManager should have cleared the cache of this file.
    EXPECT_EQ(coordinator.cacheManager->activeCache.size(), 0);
    // The FileManager should have released the file descriptor.
    EXPECT_EQ(coordinator.fileManager->descriptorBitmap, 0);
}

TEST_F(FileCoordinatorTest, TestRenewHandle) {
    const char filePath[] = "tmp/file-rw";
    ASSERT_NO_THROW(coordinator.createFile(filePath));
    ASSERT_TRUE(std::filesystem::exists(filePath))
        << "Fail to create file " << filePath;

    FileDescriptor fd = coordinator.openFile(filePath);
    PageHandle handle = coordinator.getHandle(fd, 2);

    EXPECT_NE(coordinator.load(&handle), nullptr);
    coordinator.cacheManager->writeBack(handle.cache);

    EXPECT_EQ(coordinator.cacheManager->load(handle), nullptr);
    // Renew happens here.
    EXPECT_NE(coordinator.load(&handle), nullptr);
    EXPECT_NE(coordinator.cacheManager->load(handle), nullptr);
}
