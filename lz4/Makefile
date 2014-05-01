OS := $(shell uname)

ifeq ($(OS),Linux)
	SFX  = so
	JAVA_INCLUDE=-I/usr/java/default/include -I/usr/java/default/include/linux
else
	SFX = dylib
	JAVA_INCLUDE=-I/Developer/SDKs/MacOSX10.7.sdk/System/Library/Frameworks/JavaVM.framework/Versions/Current/Headers/
endif

all: lib 

lib: 
	gcc  -O3 -std=c99 -Wall -W -Wundef -Wno-implicit-function-declaration -funroll-loops -c -fPIC -I. \
$(JAVA_INCLUDE) lz4.c Lz4Impl.c lz4hc.c 
	gcc -shared -o lib/liblz4.$(SFX) lz4.o Lz4Impl.o lz4hc.o
clean:
	rm -f core *.o lib/*
