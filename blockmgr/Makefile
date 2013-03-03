CC      := gcc
#CFLAGS  := -fPIC -g -I../ -Werror=implicit-function-declaration -O2

LOCAL_CFLAGS := $(CFLAGS) -I../

all:libblockmgr.a

%.o: %.c Makefile
	$(CC) -c -o $@ $< $(LOCAL_CFLAGS) $(CLIBFLAGS)


libblockmgr.a:fileops.o blockmgr.o transact.o blockmgr.h blockmgrerror.h hash.o ../morphdb.o platformops.o
	echo $(LOCAL_CFLAGS)
	ar rcs libblockmgr.a fileops.o blockmgr.o transact.o ../libs/libskiplist.a hash.o ../morphdb.o platformops.o
	cp libblockmgr.a ../libs
	cp *.h ../include

clean:
	rm -f *.a
	rm -f *.o
	rm -f *~
	rm -f *.so
