// Simple hack to allow accessing private members.
#define private public

#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

#include <filesystem>

using namespace SimpleDB;

class CacheManagerTest : public ::testing::Test {
protected:
    CacheManagerTest() {
        fileManager = new FileManager();
        manager = new CacheManager(fileManager);
    }

    ~CacheManagerTest() {
        delete manager;
        delete fileManager;
    }

    void SetUp() override { std::filesystem::create_directory("tmp"); }
    void TearDown() override { std::filesystem::remove_all("tmp"); }

    FileManager *fileManager;
    CacheManager *manager;
};

TEST_F(CacheManagerTest, TestReadWritePage) {
    const char filePath[] = "tmp/file";
    char readBuf[PAGE_SIZE];
    char buf[PAGE_SIZE] = {0x01, 0x02};
    buf[PAGE_SIZE - 1] = 0x03;

    fileManager->createFile(filePath);
    FileDescriptor fd = fileManager->openFile(filePath);
    fileManager->writePage(fd, 1, buf);

    // Read page.
    EXPECT_NO_THROW(manager->readPage(fd, 1, readBuf));
    EXPECT_EQ(memcmp(buf, readBuf, PAGE_SIZE), 0);

    // Write cache.
    buf[0] = 0x04;
    buf[PAGE_SIZE - 1] = 0x05;

    EXPECT_NO_THROW(manager->writePage(fd, 1, buf));
    EXPECT_NO_THROW(manager->readPage(fd, 1, readBuf));
    EXPECT_EQ(memcmp(buf, readBuf, PAGE_SIZE), 0);

    // Write back.
    EXPECT_NO_THROW(manager->writeBack(fd, 1));
    memset(readBuf, 0, PAGE_SIZE);
    fileManager->readPage(fd, 1, readBuf);
    EXPECT_EQ(memcmp(buf, readBuf, PAGE_SIZE), 0);

    // Cleanup.
    EXPECT_NO_THROW(manager->onCloseFile(fd));
    fileManager->closeFile(fd);
}

TEST_F(CacheManagerTest, TestPageExchange) {
    const char filePath[] = "tmp/file";
    char buf[PAGE_SIZE];

    fileManager->createFile(filePath);
    FileDescriptor fd = fileManager->openFile(filePath);

    for (int i = 0; i < NUM_BUFFER_PAGE; i++) {
        ASSERT_NO_THROW(manager->writePage(fd, i, buf));
    }

    // Validate LRU algorithm.
    EXPECT_EQ(manager->activeCache.tail->next->data->id, 0);

    manager->readPage(fd, 5, buf);
    EXPECT_EQ(manager->activeCache.head->data->id, 5);

    manager->readPage(fd, NUM_BUFFER_PAGE, buf);
    EXPECT_EQ(manager->activeCache.tail->next->data->id, 1);

    EXPECT_NO_THROW(manager->onCloseFile(fd));
    fileManager->closeFile(fd);
}

TEST_F(CacheManagerTest, TestLeak) {
    const char filePath[] = "tmp/file";
    char buf[PAGE_SIZE];

    fileManager->createFile(filePath);
    FileDescriptor fd = fileManager->openFile(filePath);

    for (int i = 0; i < 20; i++) {
        ASSERT_NO_THROW(manager->writePage(fd, i, buf));
        EXPECT_EQ(manager->freeCache.size(), NUM_BUFFER_PAGE - i - 1);
        EXPECT_EQ(manager->activeCache.size(), i + 1);
    }

    EXPECT_NO_THROW(manager->onCloseFile(fd));
    EXPECT_EQ(manager->freeCache.size(), NUM_BUFFER_PAGE);
    EXPECT_EQ(manager->activeCache.size(), 0);

    EXPECT_NO_THROW(manager->onCloseFile(fd));
    fileManager->closeFile(fd);
}
