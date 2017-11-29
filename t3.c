#include "Sorter.h"
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>


pthread_mutex_t running_mutex;
int numoffiles = 0;
pthread_t * threads;
int index_threads = 0;
static char * outdir_global;
static char * type_global ;

CSVFile all_files;


struct arg_struct {  //Struct to allow for multiple arguments in threads

	char * file_path;
	char * out_dir;
	char * sort_type;

} args;

int isCSV(const char* name) { //Check if the file is a csv
	char* temp=strdup(name);
	char* ext=strrchr(temp,'.');
	if(ext!=NULL&&strcmp(ext,".csv")==0)
	{
		free(temp);
		return 1;
	}
	free(temp);
	return 0;
} 


void file_test(char * filename, char * out_dir, char * sort_type) { //Test mutex locks using file output

	char * out_filename = malloc(100);
	
	sprintf(out_filename, "%s/AllFiles-sorted-%s.csv", out_dir, sort_type);
	
	FILE * pFile = fopen(filename, "r");
/*	
	char * test_string = malloc(101);
	if(pFile != NULL) {
		fgets(test_string, 100, pFile);
		fgets(test_string, 100, pFile);
	}
*/


	fclose(pFile);

	pthread_mutex_lock(&running_mutex);

	pFile = fopen (out_filename,"a");
	
	if (pFile!=NULL){
		//fprintf(pFile,"\n%s",test_string);
		fclose (pFile);
	}
	
	pthread_mutex_unlock(&running_mutex);
	
	//free(test_string);
	free(out_filename);
	free(filename);
	free(out_dir);
	free(sort_type);
}


void * display_info2(void * arguments) { //Test out if thread works by printing out the csv file name

	struct arg_struct *args = (struct arg_struct *)arguments;

	if( isCSV(args->file_path)){
		fprintf(stdout, "%s \n" , args->file_path);
		file_test(args->file_path, args->out_dir, args->sort_type);
	}

}


int display_info_threaded(const char *fpath, const struct stat *sb, int tflag) { //Function to be run by nftw

	struct arg_struct * args2 = malloc( sizeof(args2));
	args2->file_path = strdup(fpath);
	args2->out_dir = strdup(outdir_global);
	args2->sort_type = strdup(type_global);

	pthread_create(&threads[index_threads++], NULL, &display_info2,(void *) args2);
	return 0;
}


int count_files(const char *fpath, const struct stat *sb, int tflag) { //Check how many files there are to malloc

	++numoffiles;
	return 0;
}

	
int main(int argc, char *argv[]) {
	
	pthread_mutex_init(&running_mutex, NULL);
	char * in_dir = malloc(1000);
	outdir_global = malloc(1000);
	type_global  = malloc(1000);
	
	strcpy(in_dir, "./\0");
	strcpy(outdir_global, "./\0");
	

	if(argc != 3 && argc != 5 && argc != 7){
		fprintf(stderr, "<ERROR> : Incorrect number of arguments, for more information , please use  $ ./sorter -h \n");
		return 0;
	}

	short type_found = 0;

	int i;
	for(i = 1; i < argc; i++){
		
		if(strcmp(argv[i],"-c") == 0){
			if((i % 2) == 0){
				fprintf(stderr, "<ERROR> : Incorrect format, for more information , please use  $ ./sorter -h \n");
				return 0;
			}
			strcpy(type_global, argv[i+1]);
			type_found = 1;
		}

		if(strcmp(argv[i], "-d") == 0){
			if((i % 2) == 0){
				fprintf(stderr, "<ERROR> : Incorrect format, for more information , please use  $ ./sorter -h \n");
				return 0;
			}

			strcpy(in_dir, argv[i+1]);
		}

		if(strcmp(argv[i], "-o") == 0){
			if((i % 2) == 0){
				fprintf(stderr, "<ERROR> : Incorrect format, for more information , please use  $ ./sorter -h \n");
				return 0;
			}

			strcpy(outdir_global, argv[i+1]);
		}
	}
	
	char * out_filename = malloc(100);
	
	sprintf(out_filename, "%s/AllFiles-sorted-%s.csv", outdir_global, type_global);

	FILE * pFile;
	pFile = fopen (out_filename,"w");
	
	if (pFile!=NULL){
		fputs ("column1, column2 ",pFile);
		fclose (pFile);
	}

	free(out_filename);
	
	FILE *file;
	file = fopen(in_dir, "r");

	if (file == NULL){
		fprintf(stderr, "<ERROR> : Input directory not found or can't be accessed. \n");
		free(type_global);
		free(outdir_global);
		free(in_dir);
		return 0;
	}
	
	fclose(file);
	file = fopen(outdir_global, "r");

	if (file == NULL){
		fprintf(stderr, "<ERROR> : Output directory not found or can't be accessed. \n");
		free(type_global);
		free(outdir_global);
		free(in_dir);
		return 0;	
	}

	fclose(file);
					

	if (type_found == 0){
		fprintf(stderr, "<ERROR> : Incorrect format expected a -c flag: \n For more information , please use  $ ./sorter -h \n");
		free(type_global);
		free(outdir_global);
		free(in_dir);
		return 0;
	}
	
	int x = ftw(in_dir, count_files, 0);
    
	numoffiles++;
	threads = malloc(sizeof(pthread_t) * numoffiles);


	if(threads == NULL){
		fprintf(stderr,"<ERROR> : Too many expected threads, out of memory");
	}

	printf("type : %s \nin_dir : %s \noutdir : %s \n\n", type_global, in_dir, outdir_global);	//testing print

   	if (ftw(in_dir, display_info_threaded, 0) == -1) {
        fprintf(stderr, "<ERROR> : Error occured during the file tree walk");
		free(type_global);
		free(outdir_global);
		free(in_dir);
		exit(EXIT_FAILURE);
    }
  		
	for( index_threads = 0; index_threads <  numoffiles; index_threads++) {	
		pthread_join(threads[index_threads], NULL);	
	}
	
	pthread_mutex_destroy(&running_mutex);

  	printf("\n"); //extra new line for space 
	
	free(type_global);
	free(outdir_global);
	free(in_dir);
	exit(EXIT_SUCCESS);
}
