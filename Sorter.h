/*****
*
*	Define structures and function prototypes for your sorter
*
*
*
******/
typedef struct _CSVRow{
	char * data;
	int point;
	char * string_row;
} CSVRow;

typedef struct _CSVFile{
	CSVRow * CSVRow;
} CSVFile;

//Suggestion: define a struct that mirrors a record (row) of the data set
//Suggestion: prototype a mergesort function

