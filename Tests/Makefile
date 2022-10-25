CC = g++
CXXFLAGS = -g -O2 -std=c++14

SRC_DIR = src
BIN_DIR = bin
TMP_DIR = tmp

INCLUDE = -I../SimpleDB/include -I../googletest/googletest/include

DB_LIB = ../SimpleDB/lib/libSimpleDB.a
GTEST_LIB = ../googletest/build/lib/libgtest.a

LIBS = -L../SimpleDB/lib -lSimpleDB -L../googletest/build/lib -lgtest

TEST_SRC = $(shell find $(SRC_DIR) -name '*Test.cc')
TEST_TARGETS = $(foreach file,$(TEST_SRC),$(BIN_DIR)/$(notdir $(basename $(file))))

all: $(TEST_TARGETS)

test-all: $(TEST_TARGETS)
	@sh scripts/testAll.sh

$(BIN_DIR)/%: $(SRC_DIR)/%.cc $(DB_LIB) $(GTEST_LIB) | $(BIN_DIR) $(TMP_DIR)
	@mkdir -p $(BIN_DIR)
	$(CC) $(INCLUDE) $(LIBS) $(CXXFLAGS) $< -o $@

$(DB_LIB):
	$(MAKE) -j -C ../SimpleDB

$(GTEST_LIB):
	mkdir -p ../googletest/build
	(cd ../googletest/build && cmake ..)
	$(MAKE) -j -C ../googletest/build

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(TMP_DIR):
	mkdir -p $(TMP_DIR)

.PHONY: clean
clean:
	- rm -r $(BIN_DIR)