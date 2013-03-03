CC      := gcc
CFLAGS  := -fPIC -g -I./include -Werror=implicit-function-declaration  -O2
LDFLAGS := -L./libs
export CFLAGS
all:libblockmgr.a libmorphdb.so libmorphdb.a btreetest testmorphbtree testblockmgr sl

%.o: %.c Makefile
	$(CC) -c -o $@ $< $(CFLAGS) $(CLIBFLAGS)


libblockmgr.a:morphdb.o libskiplist.a
	$(MAKE) -C ./blockmgr

libmorphdbskel.a:morphdb.o morphdb.h
	ar rcs libmorphdbskel.a morphdb.o

libmorphdb.a:morphdbbtree.o hash.o hash.h libblockmgr.a btreekeyops.o morphdb.o
	ar rcs libmorphdb.a morphdbbtree.o hash.o btreekeyops.o morphdb.o
	cp libmorphdb.a ./libs

libmorphdb.so:morphdbbtree.o hash.o hash.h libblockmgr.a btreekeyops.o morphdb.o
	gcc -shared -o libmorphdb.so morphdbbtree.o hash.o btreekeyops.o morphdb.o $(LDFLAGS) -lblockmgr
	cp libmorphdb.so ./libs

btreetest:btreetest.o libmorphdb.so libmorphdb.a morphdb.h
	gcc -o btreetest btreetest.o $(LDFLAGS) -lmorphdb -lskiplist -lpthread

#dbt:dbtree.o hash.o hash.h blockmgr.h libbson.a btreekeyops.o
#	gcc -g -o dbt dbtree.o mmdbkeyval.o hash.o fileops.o -L./ libbson.a -lblockmgr btreekeyops.o -lpthread

testmorphbtree:libmorphdb.so testMorphBTree.o randomKey.o
	gcc -o testmorphbtree randomKey.o testMorphBTree.o -L./libs -lmorphdb -lblockmgr -lskiplist -lpthread

libskiplist.a:skiplist.o btreekeyops.o skiplist.h
	ar rcs libskiplist.a skiplist.o btreekeyops.o morphdb.o
	cp libskiplist.a ./libs/

testblockmgr:libblockmgr.a testBMgr.o
	gcc -o testblockmgr testBMgr.o -L./libs -lblockmgr -lskiplist -lpthread

clean:
	rm -f dbt
	rm -f *.a
	rm -f *.o
	rm -f *~
	rm -f *.so
	rm -f ./libs/*
	$(MAKE) -C ./blockmgr clean
