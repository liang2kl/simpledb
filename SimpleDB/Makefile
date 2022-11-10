ifeq ($(TESTING), true)
	PARSER_LIB = ../SQLParser/lib/libSQLParser-Test.a
	LIB_TARGET = libSimpleDB-Test.a
	FLAGS += -DTESTING
	OBJ_SUFFIX = .test.o
else
	PARSER_LIB = ../SQLParser/lib/libSQLParser.a
	LIB_TARGET = libSimpleDB.a
	OBJ_SUFFIX = .o
endif

ifeq ($(DEBUG), true)
	FLAGS += -D_DEBUG
	CXXFLAGS += -O0 -g
else
	CXXFLAGS += -O2
endif

export TESTING

CC = g++
CXXFLAGS += -std=c++17
AR = ar
MAKE = make

SRC_DIR = src
LIB_DIR = lib
PARSER_DIR = ../SQLParser
INCLUDE_DIR = include/SimpleDB

PARSER_INCLUDE_DIR = $(PARSER_DIR)/include

SRC_FILES = $(shell find $(SRC_DIR) -type f -name "*.cc")
OBJS = $(foreach file,$(SRC_FILES),$(dir $(file))obj/$(notdir $(basename $(file)))$(OBJ_SUFFIX))
HEADERS = $(shell find $(INCLUDE_DIR) -name '*.h')
INCLUDE = -I$(INCLUDE_DIR) -I$(PARSER_INCLUDE_DIR) -I$(PARSER_INCLUDE_DIR)/antlr4-runtime

SUB_DIRS = $(sort $(foreach src,$(SRC_FILES),$(dir $(src))))

all: $(LIB_DIR)/$(LIB_TARGET)

$(LIB_DIR)/$(LIB_TARGET): $(OBJS) | $(PARSER_LIB) $(LIB_DIR)
	$(AR) rcs $@.tmp $(OBJS)
	libtool -static -o $@ $(PARSER_LIB) $@.tmp
	rm $@.tmp

.PHONY: $(PARSER_LIB)
$(PARSER_LIB):
	$(MAKE) -C $(PARSER_DIR) -j

define make-goal
$1obj/%$(OBJ_SUFFIX): $1%.cc $(HEADERS) | $(PARSER_LIB) 
	@mkdir -p $1/obj
	$(CC) $(INCLUDE) $(CXXFLAGS) $(FLAGS) -c $$< -o $$@
endef

$(foreach dir,$(SUB_DIRS),$(eval $(call make-goal,$(dir))))

$(LIB_DIR):
	mkdir -p $(LIB_DIR)

.PHONY: clean
clean:
	- rm -r $(OBJS) $(LIB_DIR)
	- $(MAKE) -C $(PARSER_DIR) clean