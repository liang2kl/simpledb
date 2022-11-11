#ifndef TESTING
#define TESTING 1
#endif
#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <random>

#include "Util.h"

using namespace SimpleDB;
using namespace SimpleDB::Internal;

#ifdef PAGE_SIZE
#undef PAGE_SIZE
#endif

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
    char buf[PAGE_SIZE];

    for (int i = 0; i < PAGE_SIZE; i++) {
        buf[i] = char(rand());
    }

    fileManager->createFile(filePath);
    FileDescriptor fd = fileManager->openFile(filePath);
    fileManager->writePage(fd, 1, buf);

    PageHandle handle;

    // Read page.
    EXPECT_NO_THROW(handle = manager->getHandle(fd, 1));
    EXPECT_TRUE(handle.validate());
    EXPECT_EQ(manager->load(handle), handle.cache->buf);

    // Write cache.
    char *cacheBuf = manager->load(handle);
    char c = cacheBuf[0];
    while (c == cacheBuf[0]) {
        c = char(rand());
    }
    cacheBuf[0] = c;

    EXPECT_FALSE(handle.cache->dirty);
    EXPECT_NO_THROW(manager->markDirty(handle));
    EXPECT_TRUE(handle.cache->dirty);

    EXPECT_EQ(handle.cache->buf[0], c);

    // Write back.
    EXPECT_NO_THROW(manager->writeBack(handle));
    memset(readBuf, 0, PAGE_SIZE);
    fileManager->readPage(fd, 1, readBuf);
    EXPECT_EQ(memcmp(cacheBuf, readBuf, PAGE_SIZE), 0);

    // Cleanup.
    EXPECT_NO_THROW(manager->onCloseFile(fd));
    fileManager->closeFile(fd);
}

TEST_F(CacheManagerTest, TestPageExchange) {
    DisableLogGuard guard;

    const char filePath[] = "tmp/file";

    fileManager->createFile(filePath);
    FileDescriptor fd = fileManager->openFile(filePath);

    std::vector<PageHandle> handles;

    for (int i = 0; i < NUM_BUFFER_PAGE; i++) {
        PageHandle handle;
        ASSERT_NO_THROW(handle = manager->getHandle(fd, i));
        ASSERT_NO_THROW(manager->markDirty(handle));
        handles.push_back(handle);
    }

    // Validate LRU algorithm.
    EXPECT_EQ(manager->activeCache.tail->next->data->id, 0);

    manager->getHandle(fd, 5);
    EXPECT_EQ(manager->activeCache.head->data->id, 5);
    EXPECT_EQ(manager->freeCache.size(), 0);

    manager->getHandle(fd, NUM_BUFFER_PAGE);
    EXPECT_EQ(manager->activeCache.tail->next->data->id, 1);
    EXPECT_EQ(manager->freeCache.size(), 0);

    // At this time, the cache of page 0 should be written back, thus
    // invalidating the handle.
    EXPECT_FALSE(handles[0].validate());

    EXPECT_NO_THROW(manager->discardAll(fd));
    fileManager->closeFile(fd);
}

TEST_F(CacheManagerTest, TestLeak) {
    DisableLogGuard guard;

    const char filePath[] = "tmp/file";

    fileManager->createFile(filePath);
    FileDescriptor fd = fileManager->openFile(filePath);

    for (int i = 0; i < 20; i++) {
        PageHandle handle;
        ASSERT_NO_THROW(handle = manager->getHandle(fd, i));
        EXPECT_EQ(manager->freeCache.size(), NUM_BUFFER_PAGE - i - 1);
        EXPECT_EQ(manager->activeCache.size(), i + 1);
    }

    EXPECT_NO_THROW(manager->onCloseFile(fd));
    EXPECT_EQ(manager->freeCache.size(), NUM_BUFFER_PAGE);
    EXPECT_EQ(manager->activeCache.size(), 0);

    fileManager->closeFile(fd);
}
