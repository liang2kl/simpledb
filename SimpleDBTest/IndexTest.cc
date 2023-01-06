#ifndef TESTING
#define TESTING 1
#endif
#ifndef DEBUG
#define DEBUG 1
#endif
#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <random>
#include <set>

#include "Util.h"

using namespace SimpleDB;
using namespace SimpleDB::Internal;

class IndexTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::filesystem::create_directory("tmp");
        srand(0);
    }
    void TearDown() override {
        index.close();
        std::filesystem::remove_all("tmp");
    }

    Index index;

    const char *indexFile = "tmp/index";

    void initIndex() { ASSERT_NO_THROW(index.create(indexFile)); }

    void reloadIndex() {
        index.close();
        ASSERT_NO_THROW(index.open(indexFile));
    }
};

TEST_F(IndexTest, TestUninitializeAccess) {
    int key = 1;
    EXPECT_THROW(index.insert(key, false, {0, 0}),
                 Internal::IndexNotInitializedError);
    EXPECT_THROW(index.remove(key, false, {0, 0}),
                 Internal::IndexNotInitializedError);
    EXPECT_THROW(index.findEq(key, false), Internal::IndexNotInitializedError);
}

TEST_F(IndexTest, TestCreateNewIndex) {
    initIndex();
    reloadIndex();

    EXPECT_EQ(index.meta.numNode, 1);
    EXPECT_EQ(index.meta.rootNode, 0);
}

TEST_F(IndexTest, TestInitFromInvalidFile) {
    const char *fileName = "tmp/invalid_file";
    PF::create(fileName);
    EXPECT_THROW(index.open(fileName), Internal::ReadIndexError);
}

TEST_F(IndexTest, TestInsertGet) {
    DisableLogGuard _;
    initIndex();

    // Create a set of random entries.
    std::set<int> keys;

    while (keys.size() < 1000 * MAX_NUM_ENTRY_PER_NODE) {
        keys.insert(rand());
    }

    std::vector<std::pair<int, RecordID>> entries;

    for (auto key : keys) {
        entries.push_back({key, {rand(), rand()}});
        entries.push_back({key, {rand(), rand()}});
    }

    std::shuffle(entries.begin(), entries.end(), std::mt19937(0));

    for (int i = 0; i < entries.size(); i++) {
        auto [key, rid] = entries[i];
        ASSERT_NO_THROW(index.insert(key, false, rid));
        EXPECT_EQ(index.meta.numEntry, i + 1);
    }

    reloadIndex();

    // Validate the entries.
    std::shuffle(entries.begin(), entries.end(), std::mt19937(0));

    for (auto entry : entries) {
        int key = entry.first;
        std::vector<RecordID> rids;
        ASSERT_NO_THROW(rids = index.findEq(key, false));
        ASSERT_EQ(rids.size(), 2);
        EXPECT_TRUE(rids[0] == entry.second || rids[1] == entry.second);
    }

    FileCoordinator::shared.cacheManager->discardAll(index.fd);
}

TEST_F(IndexTest, TestRemove) {
    DisableLogGuard _;
    initIndex();

    // Create a set of random entries.
    std::set<int> keys;

    while (keys.size() < 1000 * MAX_NUM_ENTRY_PER_NODE) {
        keys.insert(rand());
    }

    std::vector<std::pair<int, RecordID>> entries;

    for (auto key : keys) {
        entries.push_back({key, {rand(), rand()}});
    }

    std::shuffle(entries.begin(), entries.end(), std::mt19937(0));

    for (auto entry : entries) {
        int key = entry.first;
        RecordID rid = entry.second;
        ASSERT_NO_THROW(index.insert(key, false, rid));
    }

    // Remove the entries.
    std::shuffle(entries.begin(), entries.end(), std::mt19937(0));

    for (int i = 0; i < entries.size(); i++) {
        auto [key, rid] = entries[i];

        ASSERT_NO_THROW(index.remove(key, false, rid));
        EXPECT_EQ(index.meta.numEntry, entries.size() - i - 1);

        // Test remove non-exist key.
        ASSERT_THROW(index.remove(key, false, rid),
                     Internal::IndexKeyNotExistsError);
    }

    FileCoordinator::shared.cacheManager->discardAll(index.fd);
}