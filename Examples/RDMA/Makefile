EXECUTABLE := test

DIRECTORIES := \
	../../../FastRing/Ring \
	../../Tools \
	../../Pool

LIBRARIES := \
	pthread \
	ibverbs \
	rdmacm

DEPENDENCIES := \
	uuid \
	openssl \
	jemalloc \
	liburing \
	libsystemd \
	avahi-client

OBJECTS := \
	../../../FastRing/Ring/FastRing.o \
	../../../FastRing/Ring/FastAvahiPoll.o \
	../../Tools/CRC32C.o \
	../../Tools/HashMap.o \
	../../Tools/RedBlackTree.o \
	../../Pool/ReliablePool.o \
	../../Pool/ReliableTracker.o \
	../../Pool/ReliableIndexer.o \
	../../Pool/ReliableWaiter.o \
	../../Pool/InstantDiscovery.o \
	../../Pool/InstantReplicator.o \
	Test.o

FLAGS += \
	-Wno-unused-result -Wno-format-truncation -Wno-format-overflow -Wno-stringop-overflow \
	-rdynamic -fno-omit-frame-pointer -O2 -MMD -gdwarf \
	$(foreach directory, $(DIRECTORIES), -I$(directory)) \
	$(shell pkg-config --cflags $(DEPENDENCIES))

CFLAGS   += $(FLAGS)
CXXFLAGS += $(FLAGS)

LIBS := \
	$(foreach library, $(LIBRARIES), -l$(library)) \
	$(shell pkg-config --libs $(DEPENDENCIES))

all: build

build: $(PREREQUISITES) $(OBJECTS)
	$(CC) $(OBJECTS) $(FLAGS) $(LIBS) -o $(EXECUTABLE)

clean:
	rm -f $(EXECUTABLE) $(OBJECTS) $(wildcard $(filter %.d,$(OBJECTS:.o=.d)))

-include $(wildcard $(filter %.d,$(OBJECTS:.o=.d)))
