#define _XOPEN_SOURCE 500
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


struct arg_struct {

	char * file_path;
	char * out_dir;
	char * sort_type;

} args;

int isCSV(const char* name)
{
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


void 

file_test(char * filename, char * out_dir, char * sort_type){

	//printf(test);
	pthread_mutex_lock(&running_mutex);

	char * out_filename = malloc(100);
	
	//fprintf(stderr, "%s/AllFiles-sorted-%s.csv", out_dir, sort_type);
	sprintf(out_filename, "%s/AllFiles-sorted-%s.csv", out_dir, sort_type);

	FILE * pFile;
	pFile = fopen (out_filename,"a");
	
	//fprintf(stderr, "file pointer : %d\n", pFile);
	if (pFile!=NULL){
		fprintf(pFile,"\n%s",filename);
		fclose (pFile);
	}

	free(out_filename);

	pthread_mutex_unlock(&running_mutex);


}


void
* display_info2(void * arguments)
{

	//char * path = (char *) arguments;
	struct arg_struct *args = (struct arg_struct *)arguments;

	if( isCSV(args->file_path)){
		fprintf(stdout, "%s \n" , args->file_path);
		file_test(args->file_path, args->out_dir, args->sort_type);
	}
	
	//free(path);
//	pthread_mutex_unlock(&running_mutex);

}


int
display_info_threaded(const char *fpath, const struct stat *sb,
             int tflag, struct FTW *ftwbuf)
{
		
	//printf("testing\n");
	struct arg_struct * args2 = malloc( sizeof(args2));
	//printf("test3: %s , %s , %s \n" , fpath, outdir_global, type_global);
	args2->file_path = strdup(fpath);
	//strcpy(args2->file_path, fpath);
	args2->out_dir = strdup(outdir_global);
	//strcpy(args2->out_dir, outdir_global);
	args2->sort_type = strdup(type_global);
	//strcpy(args2->sort_type, type_global);

	//char * copy_of_path = strdup(fpath);
	//printf("testing malloc done\n");	
	pthread_create(&threads[index_threads++], NULL, &display_info2,(void *) args2);
	return 0;
}


int
count_files(const char *fpath, const struct stat *sb,
             int tflag, struct FTW *ftwbuf)
{

	++numoffiles;
	return 0;
}

	
int
main(int argc, char *argv[])
{
	
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
		
		//printf("%s\n", argv[i]);
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
		fputs ("test",pFile);
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
					
	//printf("type : %s \nin_dir : %s \noutdir : %s \n\n", type_global, in_dir, outdir_global);
	

	if (type_found == 0){
		fprintf(stderr, "<ERROR> : Incorrect format expected a -c flag: \n For more information , please use  $ ./sorter -h \n");
		free(type_global);
		free(outdir_global);
		free(in_dir);
		return 0;
	}

	
	int flags = 0;
	int x = nftw(in_dir, count_files, 20, flags);
    
	//fprintf(stdout, "num %d: \n" , numoffiles);
	numoffiles++;
	threads = malloc(sizeof(pthread_t) * numoffiles);


	if(threads == NULL){
		fprintf(stdout,"Too many expected threads, out of memory");
	}

	printf("type : %s \nin_dir : %s \noutdir : %s \n\n", type_global, in_dir, outdir_global);
	

	//printf("start1\n");
   	if (nftw(in_dir, display_info_threaded, 20, flags) == -1) {
        perror("nftw");
        exit(EXIT_FAILURE);
    }

  		
	for( index_threads = 0; index_threads <  numoffiles; index_threads++){
//		fprintf(stdout, "%d \n" , &threads[i]);
		pthread_join(threads[index_threads], NULL);	
	}
	pthread_mutex_destroy(&running_mutex);
/*   while (running_threads > 0){
		sleep(5);

   }

   */
  	printf("\n"); 
	exit(EXIT_SUCCESS);
}
