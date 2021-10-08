.SUFFIXES: .cpp .o

CXX = g++

SRCDIR = src/
INC = include/
LIBS = lib/

# main source file
TARGET_SRC := main.cpp
TARGET_OBJ := main.o

# Include more files for library.
# SRCS_FOR_LIB := $(wildcard src/*.cpp)
SRCS_FOR_LIB := $(SRCDIR)disk_space_manager.cpp $(SRCDIR)buffer_manager.cpp $(SRCDIR)transaction_manager.cpp $(SRCDIR)index_manager.cpp $(SRCDIR)relational_set_operator.cpp $(SRCDIR)API.cpp
OBJS_FOR_LIB := $(SRCS_FOR_LIB:.cpp=.o)

CPPFLAGS += -pthread -g -fPIC -I $(INC)

TARGET = main

all: $(OBJS_FOR_LIB) $(TARGET)

$(SRCDIR)%.o: $(SRCDIR)%.c
	$(CXX) $(CPPFLAGS) -o $@ -c $< 

$(TARGET): $(TARGET_OBJ)
	make static_library
	$(CXX) $(CPPFLAGS) -o $@ $^ -L $(LIBS) -lbpt

clean:
	rm $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*

library:
	gcc -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

static_library:
	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)