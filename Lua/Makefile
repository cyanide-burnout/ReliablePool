MODULE := ReliablePool.so

DIRECTORIES = \
	../Pool

LIBRARIES := \
	pthread

DEPENDENCIES = \
	uuid \
	lua5.1

OBJECTS = \
	../Pool/ReliablePool.o \
	Module.o

ARCHITECTURE ?= $(shell $(CC) -dumpmachine | grep -o -E '^[^-]+')
PKG_CONFIG_PATH = /usr/lib/$(ARCHITECTURE)-linux-gnu/pkgconfig/:/usr/lib/pkgconfig:/usr/share/pkgconfig

FLAGS += \
	-undefined -shared -g -rdynamic -fno-omit-frame-pointer -O2 -MMD -fPIC -Wno-discarded-qualifiers -z noexecstack \
	$(foreach directory, $(DIRECTORIES), -I$(directory)) \
	$(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config --cflags $(DEPENDENCIES))

LIBS  += \
	$(foreach library, $(LIBRARIES), -l$(library)) \
	$(shell PKG_CONFIG_PATH=$(PKG_CONFIG_PATH) pkg-config --libs $(DEPENDENCIES))

CFLAGS += $(FLAGS) -std=gnu11

all: build

module: build

build: $(PREREQUISITES) $(OBJECTS)
	$(CC) $(OBJECTS) $(FLAGS) $(LIBS) -o $(MODULE)

debug: $(PREREQUISITES) $(OBJECTS)
	$(CC) $(OBJECTS) $(FLAGS) $(LIBS) -gdwarf -o $(MODULE)

clean:
	rm -f $(MODULE) $(OBJECTS) $(wildcard $(filter %.d,$(OBJECTS:.o=.d)))

.PHONY: all build clean install

-include $(wildcard $(filter %.d,$(OBJECTS:.o=.d)))
