TEST_TARGET = SimpleDBTest

MAKE = make
CC = g++
CXXFLAGS = -g -O0 -std=c++17

SRC_DIR = .
BIN_DIR = bin
OBJ_DIR = obj

INCLUDE = -I../SimpleDB/include -I../SimpleDB/include/SimpleDB \
	-I../googletest/googletest/include -I../SQLParser/include -I../SQLParser/include/antlr4-runtime

DB_LIB = ../SimpleDB/lib/libSimpleDB-Test.a
GTEST_LIB = ../googletest/build/lib/libgtest.a

LIBS = -L../SimpleDB/lib -lSimpleDB-Test -L../googletest/build/lib -lgtest

TEST_SRC = $(shell find $(SRC_DIR) -name '*.cc')
TEST_OBJS = $(foreach file,$(TEST_SRC),$(OBJ_DIR)/$(notdir $(basename $(file))).o)

HEADERS = $(shell find $(SRC_DIR) ../SimpleDB/include/SimpleDB -name '*.h')

export TESTING = true
export DEBUG = true

all: $(BIN_DIR)/$(TEST_TARGET)

test: $(BIN_DIR)/$(TEST_TARGET)
	@$(BIN_DIR)/$(TEST_TARGET)

$(BIN_DIR)/$(TEST_TARGET): $(GTEST_LIB) $(TEST_OBJS) | $(DB_LIB) $(BIN_DIR) $(OBJ_DIR)
	$(CC) $(CXXFLAGS) $(LIBS) $(TEST_OBJS) -o $@

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.cc $(HEADERS) | $(DB_LIB) $(OBJ_DIR)
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
	- $(MAKE) -C ../SimpleDB clean
	- rm -r $(BIN_DIR) $(OBJ_DIR)