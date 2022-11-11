#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>
#include <stdio.h>

#include <filesystem>

#include "Util.h"

#ifdef PAGE_SIZE
#undef PAGE_SIZE
#endif

using namespace SimpleDB;
using namespace SimpleDB::Internal;

class FileManagerTest : public ::testing::Test {
protected:
    void SetUp() override { std::filesystem::create_directory("tmp"); }
    void TearDown() override { std::filesystem::remove_all("tmp"); }

    FileManager manager;
};

TEST_F(FileManagerTest, TestCreateRemoveFile) {
    DisableLogGuard guard;

    for (int i = 0; i < FileManager::MAX_OPEN_FILES; i++) {
        char filePath[50];
        snprintf(filePath, 10, "tmp/file-%d", i);
        // Remove files before testing.
        std::filesystem::remove(filePath);

        ASSERT_NO_THROW(manager.createFile(filePath));
        EXPECT_TRUE(std::filesystem::exists(filePath))
            << "Fail to create file " << filePath;

        FileDescriptor fd = manager.openFile(filePath);

        ASSERT_NO_THROW(manager.closeFile(fd));
        ASSERT_NO_THROW(manager.deleteFile(filePath));
        EXPECT_FALSE(std::filesystem::exists(filePath))
            << "Fail to delete file " << filePath;
        ;
    }
}

TEST_F(FileManagerTest, TestWriteReadPage) {
    // Initialize data.
    char buf[PAGE_SIZE] = {0x00, 0x00, 0x12, 0x24};
    buf[PAGE_SIZE - 2] = 0x36;

    const char filePath[] = "tmp/file-rw";
    ASSERT_NO_THROW(manager.createFile(filePath));
    ASSERT_TRUE(std::filesystem::exists(filePath))
        << "Fail to create file " << filePath;

    FileDescriptor fd = manager.openFile(filePath);

    ASSERT_NO_THROW(manager.writePage(fd, 2, buf));

    char readBuf[PAGE_SIZE];
    ASSERT_NO_THROW(manager.readPage(fd, 2, readBuf));

    EXPECT_EQ(memcmp(buf, readBuf, PAGE_SIZE), 0)
        << "Read data mismatch with written data";
    EXPECT_EQ(readBuf[PAGE_SIZE - 2], 0x36)
        << "Read data mismatch with written data";

    manager.closeFile(fd);
}

TEST_F(FileManagerTest, TestExceedFiles) {
    DisableLogGuard guard;

    for (int i = 0; i < FileManager::MAX_OPEN_FILES; i++) {
        char filePath[50];
        snprintf(filePath, 10, "tmp/file-%d", i);
        std::filesystem::remove(filePath);

        manager.createFile(filePath);
        manager.openFile(filePath);
    }

    char filePath[] = "tmp/file-overflow";

    ASSERT_NO_THROW(manager.createFile(filePath));
    EXPECT_THROW(manager.openFile(filePath), Internal::OpenFileExceededError)
        << "Open files exceeded but not thrown";
    ASSERT_NO_THROW(manager.deleteFile(filePath));
}

TEST_F(FileManagerTest, TestInvalidFileDescriptor) {
    DisableLogGuard guard;

    char buf[PAGE_SIZE];

    for (int fdValue = -1; fdValue <= FileManager::MAX_OPEN_FILES; fdValue++) {
        FileDescriptor fd(fdValue);
        EXPECT_THROW(manager.closeFile(fd), Internal::InvalidDescriptorError)
            << "Close file with invalid descriptor but not thrown";
        EXPECT_THROW(manager.readPage(fd, 0, buf),
                     Internal::InvalidDescriptorError)
            << "Read page with invalid descriptor but not thrown";
        EXPECT_THROW(manager.writePage(fd, 0, buf),
                     Internal::InvalidDescriptorError)
            << "Write page with invalid descriptor but not thrown";
    }
}

TEST_F(FileManagerTest, TestInvalidPageNumber) {
    const char filePath[] = "tmp/file";
    ASSERT_NO_THROW(manager.createFile(filePath));

    FileDescriptor fd;
    ASSERT_NO_THROW(fd = manager.openFile(filePath));

    char buf[PAGE_SIZE];

    EXPECT_THROW(manager.readPage(fd, -1, buf),
                 Internal::InvalidPageNumberError);
    ASSERT_NO_THROW(manager.deleteFile(filePath));
}
