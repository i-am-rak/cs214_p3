#define _XOPEN_SOURCE 500
#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>

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
	//sleep(2);	

	struct arg_struct *args = (struct arg_struct *)arguments;	


    if( args->tflag == FTW_F && isCSV(args->fpath)){
		fprintf(stdout,"%s \n", args->fpath);
	
	}
}


static int
display_info(const char *fpath, const struct stat *sb,
             int tflag, struct FTW *ftwbuf)
{
    if( tflag == FTW_F && isCSV(fpath)){


	printf("%-3s %2d %7jd   %-40s %d %s\n",
        (tflag == FTW_D) ?   "d"   : (tflag == FTW_DNR) ? "dnr" :
        (tflag == FTW_DP) ?  "dp"  : (tflag == FTW_F) ?   "f" :
        (tflag == FTW_NS) ?  "ns"  : (tflag == FTW_SL) ?  "sl" :
        (tflag == FTW_SLN) ? "sln" : "???",
        ftwbuf->level, (intmax_t) sb->st_size,
        fpath, ftwbuf->base, fpath + ftwbuf->base);
	}
	return 0;           /* To tell nftw() to continue */
}



static int
display_info_threaded(const char *fpath, const struct stat *sb,
             int tflag, struct FTW *ftwbuf)
{
		
	pthread_t tid;
	static int i = 0;
		
	struct arg_struct args;

	args.fpath = fpath;
	args.sb = sb;
	args.tflag = tflag;
	args.ftwbuf = ftwbuf;

	pthread_create(&tid, NULL, &display_info2, (void *) &args);
	pthread_join(tid, NULL);

	return 0;
}


int
main(int argc, char *argv[])
{
	int flags = 0;
	int i = 0;

   if (nftw((argc < 2) ? "." : argv[1], display_info_threaded, 20, flags) == -1) {
        perror("nftw");
        exit(EXIT_FAILURE);
    }


   printf("\n"); //cause of zsh


   exit(EXIT_SUCCESS);
}
