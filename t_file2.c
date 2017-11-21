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
char * outdir_global;
char * type_global ;


struct arg_struct {

	const char *fpath;
	const struct stat *sb;
	int tflag; 
	struct FTW *ftwbuf;

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
* display_info2(void * arguments)
{

	char * path = (char *) arguments;

	if( isCSV(path)){
		fprintf(stdout, "%s \n" , path);
	}
	
	free(path);
//	pthread_mutex_unlock(&running_mutex);

}


static int
display_info_threaded(const char *fpath, const struct stat *sb,
             int tflag, struct FTW *ftwbuf)
{
		
	//printf("start2\n");
	pthread_t tid;
	
	struct arg_struct args_local;
	args_local.fpath = fpath;
	args_local.sb = sb;
	args_local.tflag = tflag;
	args_local.ftwbuf = ftwbuf;	
	
	struct arg_struct * args = malloc(sizeof(*args));
	args->fpath = malloc(10000);
	args->sb = malloc(10000);
	args->ftwbuf = malloc(10000);
	*args = args_local;
	//filenames[i] = fpath;
	//flags[i] = tflag;
	//printf("%s\n:",args->fpath);
	//printf("mallocend\n");
	char * copy_of_path = strdup(fpath);
//	pthread_mutex_lock(&running_mutex);

//	running_threads++;
//	printf("%d",running_threads);
	
//	pthread_mutex_unlock(&running_mutex);
	//pthread_mutex_lock(&running_mutex);
	pthread_create(&threads[index_threads++], NULL, &display_info2,(void *) copy_of_path);
	//pthread_mutex_unlock(&running_mutex);

	//	fprintf(stdout,"%d \n" , &threads[i]);
	//pthread_join(threads[i++], NULL);
	return 0;
}


static int
count_files(const char *fpath, const struct stat *sb,
             int tflag, struct FTW *ftwbuf)
{

	++numoffiles;
	return 0;
}

	
int
main(int argc, char *argv[])
{
	char * in_dir = malloc(1000);
	char * outdir_global = malloc(1000);
	char * type_global  = malloc(1000);
	
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
					
	printf("type %s: in_dir: %s, outdir: %s \n", type_global, in_dir, outdir_global);
	

	if (type_found == 0){
		fprintf(stderr, "<ERROR> : Incorrect format expected a -c flag: \n For more information , please use  $ ./sorter -h \n");
		free(type_global);
		free(outdir_global);
		free(in_dir);
		return 0;
	}

	
	int flags = 0;
	pthread_mutex_init(&running_mutex, NULL);
	int x = nftw(in_dir, count_files, 20, flags);
    
	//fprintf(stdout, "num %d: \n" , numoffiles);
	numoffiles++;
	threads = malloc(sizeof(pthread_t) * numoffiles);


	if(threads == NULL){
		fprintf(stdout,"Too many expected threads, out of memory");
	}

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
