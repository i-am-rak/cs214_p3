HEADERS = Sorter.h

default: Sorter

Sorter.o: Sorter.c $(HEADERS)
	gcc -c Sorter.c Sorter.h

Sorter: Sorter.o
	gcc Sorter.o -o Sorter

clean:
	-rm -f Sorter.o
	-rm -f Sorter.h.gch
	-rm -f Sorter
	-find . -name "*-sorted-*" -delete
