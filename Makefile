CC      = gcc
CFLAGS  = -O2 -Wall -Wextra -pthread $(shell pkg-config --cflags libwebsockets libcjson)
LDLIBS  = $(shell pkg-config --libs libwebsockets libcjson) -lpthread -lrt
TARGET  = firehose

all: $(TARGET)

$(TARGET): firehose.c
	$(CC) $(CFLAGS) -o $@ $< $(LDLIBS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
