TARGET = main
CC = g++
CXXFLAGS = -g -O2 -std=c++14

SRC_DIR = src
BIN_DIR = bin
SRC_FILES = $(shell find $(SRC_DIR) -type f -name "*.cc")
OBJS = $(foreach file,$(SRC_FILES),$(dir $(file))obj/$(notdir $(basename $(file))).o)
HEADERS = $(shell find $(SRC_DIR) -name '*.h')
INCLUDE = -I$(SRC_DIR)

SUB_DIRS = $(sort $(foreach src,$(SRC_FILES),$(dir $(src))))

$(TARGET): $(OBJS) | $(BIN_DIR)
	$(CC) $(CXXFLAGS) $(OBJS) -o $(BIN_DIR)/$(TARGET) $(INCLUDE)

define make-goal
$1obj/%.o: $1%.cc $(HEADERS)
	@mkdir -p $1/obj
	$(CC) $(INCLUDE) $(CXXFLAGS) -c $$< -o $$@
endef

$(foreach dir,$(SUB_DIRS),$(eval $(call make-goal,$(dir))))

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

.PHONY: clean
clean:
	- rm -rf $(OBJS) $(BIN_DIR)