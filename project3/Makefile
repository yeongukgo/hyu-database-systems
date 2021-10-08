.SUFFIXES: .c .o

CC=gcc

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(wildcard src/*.c)
SRCS:= $(SRCDIR)disk_space_manager.c $(SRCDIR)buffer_manager.c $(SRCDIR)index_manager.c $(SRCDIR)API.c
OBJS:=$(SRCS:.c=.o)

# main source file
TARGET_SRC:=$(SRCDIR)main.c
TARGET_OBJ:=$(SRCDIR)main.o

# Include more files if you write another source file.
SRCS_FOR_LIB1:=$(SRCDIR)disk_space_manager.c
OBJS_FOR_LIB1:=$(SRCS_FOR_LIB1:.c=.o)
SRCS_FOR_LIB2:=$(SRCDIR)buffer_manager.c
OBJS_FOR_LIB2:=$(SRCS_FOR_LIB2:.c=.o)
SRCS_FOR_LIB3:=$(SRCDIR)index_manager.c
OBJS_FOR_LIB3:=$(SRCS_FOR_LIB3:.c=.o)
SRCS_FOR_LIB4:=$(SRCDIR)API.c
OBJS_FOR_LIB4:=$(SRCS_FOR_LIB4:.c=.o)

OBJS_FOR_LIB:=$(OBJS)

CFLAGS+= -g -fPIC -I $(INC)

TARGET=main

all: $(TARGET)

$(TARGET): $(TARGET_OBJ)
	$(CC) $(CFLAGS) -g -o $(OBJS_FOR_LIB1) -c $(SRCS_FOR_LIB1)
	$(CC) $(CFLAGS) -g -o $(OBJS_FOR_LIB2) -c $(SRCS_FOR_LIB2)
	$(CC) $(CFLAGS) -g -o $(OBJS_FOR_LIB3) -c $(SRCS_FOR_LIB3)
	$(CC) $(CFLAGS) -g -o $(OBJS_FOR_LIB4) -c $(SRCS_FOR_LIB4)
	make static_library
	$(CC) $(CFLAGS) -g -o $@ $^ -L $(LIBS) -lbpt

clean:
	rm $(TARGET) $(TARGET_OBJ) $(OBJS_FOR_LIB) $(LIBS)*

library:
	gcc -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

static_library:
	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)
