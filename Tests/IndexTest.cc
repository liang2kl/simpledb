#define TESTING
#define _DEBUG
#include <SimpleDB/SimpleDB.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <random>
#include <set>

#include "Util.h"

using namespace SimpleDB;

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

    ColumnMeta columnMetas[4] = {
        {.type = INT, .size = 4, .nullable = false, .name = "int_val"},
        {.type = FLOAT, .size = 4, .nullable = false, .name = "float_val"},
        {.type = VARCHAR,
         .size = 100,
         .nullable = false,
         .name = "varchar_val"},
        // A nullable column.
        {.type = INT, .size = 4, .nullable = true, .name = "int_val_nullable"},
    };

    const char *indexFile = "tmp/index";

    void initIndex() {
        ASSERT_NO_THROW(index.create(indexFile, columnMetas[0]));
    }

    void reloadIndex() {
        index.close();
        ASSERT_NO_THROW(index.open(indexFile));
    }
};

TEST_F(IndexTest, TestUninitializeAccess) {
    int key = 1;
    EXPECT_THROW(index.insert((const char *)&key, {0, 0}),
                 Internal::IndexNotInitializedError);
    EXPECT_THROW(index.remove((const char *)&key),
                 Internal::IndexNotInitializedError);
    EXPECT_THROW(index.find((const char *)&key),
                 Internal::IndexNotInitializedError);
}

TEST_F(IndexTest, TestCreateNewIndex) {
    initIndex();
    reloadIndex();

    EXPECT_EQ(index.meta.numNode, 1);
    EXPECT_EQ(index.meta.rootNode, 1);
    EXPECT_EQ(index.meta.size, 4);
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
    }

    std::shuffle(entries.begin(), entries.end(), std::mt19937(0));

    for (int i = 0; i < entries.size(); i++) {
        auto [key, rid] = entries[i];
        bool succeed;
        ASSERT_NO_THROW(index.insert((const char *)&key, rid));
        EXPECT_EQ(index.meta.numEntry, i + 1);

        // Test duplidate keys.
        ASSERT_THROW(index.insert((const char *)&key, rid),
                     Internal::IndexKeyExistsError);
        EXPECT_FALSE(succeed);
    }

    reloadIndex();

    // Validate the entries.
    std::shuffle(entries.begin(), entries.end(), std::mt19937(0));

    for (auto entry : entries) {
        int key = entry.first;
        RecordID rid;
        ASSERT_NO_THROW(rid = index.find((const char *)&key));
        EXPECT_EQ(rid, entry.second);
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
        ASSERT_NO_THROW(index.insert((const char *)&key, rid));
    }

    // Remove the entries.
    std::shuffle(entries.begin(), entries.end(), std::mt19937(0));

    for (int i = 0; i < entries.size(); i++) {
        auto [key, rid] = entries[i];

        ASSERT_NO_THROW(index.remove((const char *)&key));
        EXPECT_EQ(index.meta.numEntry, entries.size() - i - 1);

        // Test remove non-exist key.
        ASSERT_THROW(index.remove((const char *)&key),
                     Internal::IndexKeyNotExistsError);
    }

    FileCoordinator::shared.cacheManager->discardAll(index.fd);
}