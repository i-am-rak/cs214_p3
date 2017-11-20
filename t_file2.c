#define _XOPEN_SOURCE 500
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>

volatile int running_threads = 0;
pthread_mutex_t running_mutex;
int numoffiles = 0;
pthread_t * threads;
int i = 0;
char ** filenames; 
//int * flags;

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
	sleep(1);
	//
	//struct arg_struct args2 = (struct arg_struct) arguments;
	//pthread_mutex_lock(&running_mutex);	
	//struct arg_struct * args = malloc(sizeof(*args));
	
//	args->fpath = malloc(10000);
//	args->sb = malloc(10000);
//	args->ftwbuf = malloc(10000);
//	
//	*args = *(struct arg_struct *) arguments;	
	//fprintf(stdout,"%s\n",  args->fpath);
//	pthread_mutex_unlock(&running_mutex);	
	
	char * path = (char *) arguments;


	fprintf(stdout,"%s\n",  path);
	
	/*
	pthread_mutex_lock(&running_mutex);


	//printf("%s\n", args->fpath);
    if( args->tflag == FTW_F && isCSV(args->fpath)){
		fprintf(stdout,"%s \n", args->fpath);
	
	}
*/
//	pthread_mutex_unlock(&running_mutex);
//	running_threads--;
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
	pthread_create(&threads[i++], NULL, &display_info2,(void *) copy_of_path);
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
	int flags = 0;
	pthread_mutex_init(&running_mutex, NULL);
	int x = nftw((argc < 2) ? "." : argv[1], count_files, 20, flags);
    
	//fprintf(stdout, "num %d: \n" , numoffiles);
	numoffiles++;
	threads = malloc(sizeof(pthread_t) * numoffiles);

	filenames = malloc(sizeof(char *) * numoffiles);
	//flags = malloc(sizeof(int) * numoffiles);

	if(threads == NULL){
		fprintf(stdout,"Too many expected threads, out of memory");
	}

	//printf("start1\n");
   	if (nftw((argc < 2) ? "." : argv[1], display_info_threaded, 20, flags) == -1) {
        perror("nftw");
        exit(EXIT_FAILURE);
    }

   printf("\n"); //cause of zsh
  		
	for( i = 1; i <  numoffiles; i++){
//		fprintf(stdout, "%d \n" , &threads[i]);
		pthread_join(threads[i], NULL);	
	}
	pthread_mutex_destroy(&running_mutex);
/*   while (running_threads > 0){
		sleep(5);

   }

   */
   exit(EXIT_SUCCESS);
}
