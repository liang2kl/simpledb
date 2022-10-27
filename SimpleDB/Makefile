ifeq ($(TESTING), true)
	LIB_TARGET = libSimpleDB-Test.a
	FLAGS += -DTESTING
else
	LIB_TARGET = libSimpleDB.a
endif

ifeq ($(DEBUG), true)
	FLAGS += -D_DEBUG
	CXXFLAGS += -O0 -g
else
	CXXFLAGS += -O2
endif

CC = g++
CXXFLAGS += -std=c++17
AR = ar

SRC_DIR = src
LIB_DIR = lib
INCLUDE_DIR = include/SimpleDB

SRC_FILES = $(shell find $(SRC_DIR) -type f -name "*.cc")
OBJS = $(foreach file,$(SRC_FILES),$(dir $(file))obj/$(notdir $(basename $(file))).o)
HEADERS = $(shell find $(INCLUDE_DIR) -name '*.h')
INCLUDE = -I$(INCLUDE_DIR)

SUB_DIRS = $(sort $(foreach src,$(SRC_FILES),$(dir $(src))))

all: $(LIB_DIR)/$(LIB_TARGET)

$(LIB_DIR)/$(LIB_TARGET): $(OBJS) | $(LIB_DIR)
	$(AR) rcs $@ $^

define make-goal
$1obj/%.o: $1%.cc $(HEADERS)
	@mkdir -p $1/obj
	$(CC) $(INCLUDE) $(CXXFLAGS) $(FLAGS) -c $$< -o $$@
endef

$(foreach dir,$(SUB_DIRS),$(eval $(call make-goal,$(dir))))

$(LIB_DIR):
	mkdir -p $(LIB_DIR)

.PHONY: clean
clean:
	- rm -r $(OBJS) $(LIB_DIR)