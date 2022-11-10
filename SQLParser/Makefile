ifeq ($(TESTING), true)
	LIB_TARGET = libSQLParser-Test.a
	FLAGS += -DTESTING
	OBJ_SUFFIX = .test.o
else
	LIB_TARGET = libSQLParser.a
	OBJ_SUFFIX = .o
endif

ANTLR = antlr4
CC = g++
AR = ar
MAKE = make

CXXFLAGS += -std=c++17
ANTLR_FLAGS = -Dlanguage=Cpp -visitor -no-listener -package SQLParser

LIB_DIR = lib
SRC_DIR = src
INCLUDE_DIR = include

# ANTLR runtime
ANTLR_BUILD_DIR = antlr4/runtime/Cpp/build
ANTLR_RUNTIME_LIB = $(ANTLR_BUILD_DIR)/runtime/libantlr4-runtime.a
ANTLR_INCLUDE_DIR = $(ANTLR_BUILD_DIR)/usr/local/include

ANTLR_SOURCES = $(SRC_DIR)/SQLLexer.cpp $(SRC_DIR)/SQLParser.cpp \
	$(SRC_DIR)/SQLVisitor.cpp $(SRC_DIR)/SQLBaseVisitor.cpp

ANTLR_HEADERS = $(SRC_DIR)/SQLLexer.h $(SRC_DIR)/SQLParser.h \
	$(SRC_DIR)/SQLVisitor.h $(SRC_DIR)/SQLBaseVisitor.h

# Sources
SRC_FILES = $(sort $(shell find $(SRC_DIR) -type f -name "*.cpp") $(ANTLR_SOURCES))
HDR_FILES = $(sort $(shell find $(SRC_DIR) -type f -name "*.h") $(ANTLR_HEADERS))
OBJS = $(foreach file,$(SRC_FILES),$(dir $(file))obj/$(notdir $(basename $(file)))$(OBJ_SUFFIX))
HEADERS = $(foreach file,$(HDR_FILES),$(INCLUDE_DIR)/SQLParser/$(notdir $(file)))
SUB_DIRS = $(sort $(foreach src,$(SRC_FILES),$(dir $(src))))
INCLUDE = -I$(INCLUDE_DIR) -I$(INCLUDE_DIR)/antlr4-runtime

all: $(LIB_DIR)/$(LIB_TARGET)

# Library target
$(LIB_DIR)/$(LIB_TARGET): $(ANTLR_RUNTIME_LIB) $(OBJS) | $(LIB_DIR)
	$(AR) rcs $@.tmp $(OBJS)
	libtool -static -o $@ $(ANTLR_RUNTIME_LIB) $@.tmp
	rm $@.tmp

# Copy headers
$(INCLUDE_DIR)/SQLParser/%.h: $(SRC_DIR)/%.h | $(INCLUDE_DIR)/SQLParser
	cp $< $@

# Building sources
define make-goal
$1obj/%$(OBJ_SUFFIX): $1%.cpp $(HEADERS) $(ANTLR_RUNTIME_LIB)
	@mkdir -p $1obj
	$(CC) $(INCLUDE) $(CXXFLAGS) $(FLAGS) -c $$< -o $$@
endef

$(foreach dir,$(SUB_DIRS),$(eval $(call make-goal,$(dir))))

# Special rules for antlr4 generated files
define make-antlr
$(SRC_DIR)/obj/$1$(OBJ_SUFFIX): $(SRC_DIR)/$1.cpp $(HEADERS) $(ANTLR_RUNTIME_LIB)
	@mkdir -p $(SRC_DIR)/obj
	$(CC) $(INCLUDE) $(CXXFLAGS) $(FLAGS) -c $$< -o $$@
endef

$(foreach file,$(ANTLR_SOURCES), $(eval $(call make-antlr,$(file))))

# Generate antlr sources
ANTLR_FIRST_SRC = $(word 1,$(ANTLR_SOURCES))
ANTLR_REST_SRC = $(filter-out $(ANTLR_FIRST_SRC),$(ANTLR_SOURCES))

$(ANTLR_REST_SRC) $(ANTLR_HEADERS): $(ANTLR_FIRST_SRC)

$(ANTLR_FIRST_SRC): $(SRC_DIR)/Sql.g4
	$(ANTLR) $(ANTLR_FLAGS) $<

# Build antlr runtime
$(ANTLR_RUNTIME_LIB): | $(INCLUDE_DIR)/antlr4-runtime
	mkdir -p $(ANTLR_BUILD_DIR)
	(cd $(ANTLR_BUILD_DIR) && cmake .. -Wno-dev)
	@echo Building antlr4 runtime...
	$(MAKE) -C $(ANTLR_BUILD_DIR) -j6
	DESTDIR=. $(MAKE) -C $(ANTLR_BUILD_DIR) install >/dev/null 2>&1
	cp -r $(ANTLR_INCLUDE_DIR)/antlr4-runtime $(INCLUDE_DIR)

$(LIB_DIR):
	mkdir -p $(LIB_DIR)

$(INCLUDE_DIR):
	mkdir -p $(INCLUDE_DIR)

$(INCLUDE_DIR)/SQLParser:
	mkdir -p $(INCLUDE_DIR)/SQLParser

$(INCLUDE_DIR)/antlr4-runtime:
	mkdir -p $(INCLUDE_DIR)/antlr4-runtime

.PHONY: clean
clean:
	-rm -f $(ANTLR_SOURCES) $(ANTLR_HEADERS)
	-rm -f $(ANTLR_RUNTIME_LIB)
	-rm -rf $(OBJS) $(LIB_DIR) $(INCLUDE_DIR)
