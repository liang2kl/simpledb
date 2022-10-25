TEST_TARGET = SimpleDBTest

MAKE = make
CC = g++
CXXFLAGS = -g -O2 -std=c++17

SRC_DIR = src
BIN_DIR = bin
OBJ_DIR = obj

INCLUDE = -I../SimpleDB/include -I../googletest/googletest/include

DB_LIB = ../SimpleDB/lib/libSimpleDB.a
GTEST_LIB = ../googletest/build/lib/libgtest.a

LIBS = -L../SimpleDB/lib -lSimpleDB -L../googletest/build/lib -lgtest

TEST_SRC = $(shell find $(SRC_DIR) -name '*.cc')
TEST_OBJS = $(foreach file,$(TEST_SRC),$(OBJ_DIR)/$(notdir $(basename $(file))).o)

all: $(BIN_DIR)/$(TEST_TARGET)

test: $(BIN_DIR)/$(TEST_TARGET)
	@$(BIN_DIR)/$(TEST_TARGET)

$(BIN_DIR)/$(TEST_TARGET): $(TEST_OBJS) $(DB_LIB) $(GTEST_LIB) | $(BIN_DIR) $(OBJ_DIR)
	$(CC) $(CXXFLAGS) $(LIBS) $^ -o $@

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cc | $(OBJ_DIR)
	$(CC) $(INCLUDE) $(CXXFLAGS) -c $< -o $@

.PHONY: $(DB_LIB)
$(DB_LIB):
	$(MAKE) -j -C ../SimpleDB

$(GTEST_LIB):
	mkdir -p ../googletest/build
	(cd ../googletest/build && cmake ..)
	$(MAKE) -j -C ../googletest/build

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(OBJ_DIR):
	mkdir -p $(OBJ_DIR)

.PHONY: clean
clean:
	- rm -r $(BIN_DIR) $(OBJ_DIR)