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

CXXFLAGS += -std=c++17
ANTLR_FLAGS = -Dlanguage=Cpp -visitor -package SQLParser

LIB_DIR = lib
SRC_DIR = src
INCLUDE_DIR = include
PARSER_SRC_DIR = $(SRC_DIR)/parser

# ANTLR runtime
ANTLR_BUILD_DIR = antlr4/runtime/Cpp/build
ANTLR_RUNTIME_LIB = $(ANTLR_BUILD_DIR)/runtime/libantlr4-runtime.a
ANTLR_INCLUDE_DIR = $(ANTLR_BUILD_DIR)/usr/local/include

ANTLR_SOURCES = $(PARSER_SRC_DIR)/SQLLexer.cpp $(PARSER_SRC_DIR)/SQLParser.cpp \
	$(PARSER_SRC_DIR)/SQLBaseListener.cpp $(PARSER_SRC_DIR)/SQLBaseVisitor.cpp \
	$(PARSER_SRC_DIR)/SQLListener.cpp $(PARSER_SRC_DIR)/SQLVisitor.cpp

ANTLR_HEADERS = $(PARSER_SRC_DIR)/SQLLexer.h $(PARSER_SRC_DIR)/SQLParser.h \
	$(PARSER_SRC_DIR)/SQLBaseListener.h $(PARSER_SRC_DIR)/SQLBaseVisitor.h \
	$(PARSER_SRC_DIR)/SQLListener.h $(PARSER_SRC_DIR)/SQLVisitor.h

# Sources
SRC_FILES = $(sort $(shell find $(SRC_DIR) -type f -name "*.cpp") $(ANTLR_SOURCES))
OBJS = $(foreach file,$(SRC_FILES),$(dir $(file))obj/$(notdir $(basename $(file)))$(OBJ_SUFFIX))
HEADERS = $(shell find $(INCLUDE_DIR) -name '*.h') $(ANTLR_HEADERS)
SUB_DIRS = $(sort $(foreach src,$(SRC_FILES),$(dir $(src))))
INCLUDE = -I$(INCLUDE_DIR) -I$(INCLUDE_DIR)/antlr4-runtime

all: $(LIB_DIR)/$(LIB_TARGET)

$(LIB_DIR)/$(LIB_TARGET): $(ANTLR_RUNTIME_LIB) $(OBJS) | $(LIB_DIR)
	$(AR) rcs $@-tmp $(OBJS)
	libtool -static -o $@ $(ANTLR_RUNTIME_LIB) $@-tmp
	@-rm $@-tmp

define make-goal
$1obj/%$(OBJ_SUFFIX): $1%.cpp $(HEADERS)
	@mkdir -p $1obj
	$(CC) $(INCLUDE) $(CXXFLAGS) $(FLAGS) -c $$< -o $$@
endef

$(foreach dir,$(SUB_DIRS),$(eval $(call make-goal,$(dir))))

# Special rules for antlr4 generated files
define make-antlr
$(PARSER_SRC_DIR)/obj/$1$(OBJ_SUFFIX): $(PARSER_SRC_DIR)/$1.cpp $(HEADERS)
	@mkdir -p $(PARSER_SRC_DIR)/obj
	$(CC) $(INCLUDE) $(CXXFLAGS) $(FLAGS) -c $$< -o $$@
endef

$(foreach file,$(ANTLR_SOURCES), $(eval $(call make-antlr,$(file))))

$(ANTLR_SOURCES) $(ANTLR_HEADERS): $(PARSER_SRC_DIR)/Sql.g4 $@
	$(ANTLR) $(ANTLR_FLAGS) $<

$(ANTLR_RUNTIME_LIB):
	mkdir -p $(ANTLR_BUILD_DIR)
	(cd $(ANTLR_BUILD_DIR) && cmake .. -Wno-dev)
	@echo Building antlr4 runtime...
	DESTDIR=$(ANTLR_BUILD_DIR)../run $(MAKE) -C $(ANTLR_BUILD_DIR) install -j6
	cp -r $(ANTLR_INCLUDE_DIR)/antlr4-runtime $(INCLUDE_DIR)

$(LIB_DIR):
	mkdir -p $(LIB_DIR)

.PHONY: clean
clean:
	-rm -f $(ANTLR_SOURCES) $(ANTLR_HEADERS)
	-rm -f $(ANTLR_RUNTIME_LIB)
	-rm -rf $(OBJS) $(LIB_DIR)
