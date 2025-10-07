OS := $(shell uname)
ifeq ($(OS),Darwin)
        #CC      = /usr/local/opt/llvm/bin/clang++
        #CFLAGS  = -O3 -mavx -std=c++14 -w -march=native -I/usr/local/opt/llvm/include -fopenmp 
        #LDFLAGS = -L/usr/local/opt/llvm/lib
		CC      = /usr/local/bin/g++-7
        CFLAGS  = -O3 -mavx -std=c++14 -w -march=native -I/usr/local/opt/libomp/include -fopenmp
		#CFLAGS  = -mavx -std=c++14 -w -march=native -I/usr/local/opt/libomp/include -fopenmp
        LDFLAGS = -L/usr/local/opt/libomp/lib
else
        CC      = g++
        CFLAGS  = -O3 -std=c++14 -w -fopenmp 
        #CFLAGS  = -std=c++14 -w -fopenmp 
		LDFLAGS =
endif


SOURCES = containers/relation.cpp
OBJECTS = $(SOURCES:.cpp=.o)
	

all:main_two_layer main_two_layer_plus main_spatial_join_optimizations main_transformation_spatial_join# main_range main_range2  ## main_break main_stripes  main_adaptive

#main_range: $(OBJECTS)
#	$(CC) $(CFLAGS) $(LDFLAGS) $(OBJECTS) main_range.cpp -o sj_range $(LDADD)

#main_range2: $(OBJECTS)
#	$(CC) $(CFLAGS) $(LDFLAGS) $(OBJECTS) main_range2.cpp -o sj_range2 $(LDADD)

main_transformation_spatial_join: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(OBJECTS) main_transformation_spatial_join.cpp -o sj_transf $(LDADD)

main_spatial_join_optimizations: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(OBJECTS) main_spatial_join_optimizations.cpp -o sj_opt $(LDADD)

main_two_layer_plus: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(OBJECTS) main_two_layer_plus.cpp -o two_layer_plus $(LDADD)

main_two_layer: $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(OBJECTS) main_two_layer.cpp -o two_layer $(LDADD)

.cpp.o:
	$(CC) $(CFLAGS) -c $< -o $@

.cc.o:
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -rf containers/*.o
	rm -rf algorithms/*.o
	rm -rf partitioning/*.o
	rm -rf scheduling/*.o
	#rm -rf sj_range
	#rm -rf sj_range2
	rm -rf two_layer
	rm -rf two_layer_plus
	rm -rf sj_opt
	rm -rf sj_transf
