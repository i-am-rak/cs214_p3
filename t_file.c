#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <stdio.h>
#include "Sorter.h"
#include <unistd.h>  
#include <dirent.h>  
#include <sys/stat.h>  
#include <sys/wait.h>
 
int p[2];
int depth=0;
int indexo;

int isCSV(char* name)
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








void printdir(char * token, char *dir, char * outdir, int idp)  
{  
	DIR *dp;  
	struct dirent *entry;  
	struct stat statbuf;  
	char* path=malloc(1000);
	if((dp=opendir(dir))==NULL)
	{  
        	kill(getpid(), SIGKILL); 
    	}  
    	chdir(dir);  
	int id;
	int csvid;
	int procs=0;
    	while((entry=readdir(dp))!=NULL)
	{
        	lstat(entry->d_name,&statbuf);  
        	if(S_ISDIR(statbuf.st_mode))
		{  
            		if(strcmp(entry->d_name, ".") == 0||strcmp(entry->d_name, "..") == 0)   
                	{
				continue;
			}
		
			id=fork();
			procs++;
			if(id==0)
			{
				procs=0;	
				write(p[1], &indexo, 1);
				depth+=1;
				//printf("%d, ", getpid());
				fflush(stdout);
				dp=opendir(entry->d_name);
				if(dp==NULL)
				{
					return;
				}
				strcat(path,entry->d_name);
				strcat(path,"/");
				chdir(entry->d_name);
			}
        	} 
		else if(isCSV(entry->d_name)) //fork on each csv to sort
		{
			if(strstr(entry->d_name, "-sorted-") != NULL) {
				    continue;
			}
			csvid=fork();
			procs++;
			if(csvid==0)
			{
				write(p[1], &indexo, 1);
				//printf("%d, ", getpid());
				fflush(stdout);
				strcat(path,entry->d_name);
				strcat(path,"\0");
				printf("%s\n",path);
				//sortCSVFile(entry->d_name, token , outdir);
				kill(getpid(), SIGKILL);
			}
		}
    	} 
	
		
	int i;
	for(i=0;i<procs;i++)
	{
		wait(NULL);
	}
	//if(getpid() != idp)
	//{
	//	kill(getpid(), SIGKILL);
	//}
	//close(p[1]);
    // where to kill something
	
	
	chdir("..");  
    closedir(dp);     
	free(path);
}  
 

int main(int argc, char ** argv){

	if(argc != 3 && argc != 5 && argc != 7){
		fprintf(stderr, "ERROR <Incorrect format> \n");
		return 0;	
	}

	
	//printf("%d\n", argc);	
	char * indir =  malloc(1000);
	char * tempop = malloc(1000);
	char * token = malloc(1000);

	strcpy(token, argv[2]);
	strcpy(indir, argv[4]);
	strcpy(tempop, argv[6]);
	//fprintf(stdout, "%s \n " , tempop);
	FILE *file;
	
	
	if(argc == 3 && argv[1][0] == '-' && argv[1][1] == 'c' && argv[2] != NULL) {
		strcpy(indir, "./");
		strcpy(tempop, "./");
		strcpy(token, argv[2]);
	}
	else if(argc == 5){
		strcpy(token, argv[2]);		
	if(argv[3][0] == '-' && argv[3][1] == 'd' && argv[4] != NULL){
		strcpy(indir, argv[4]);
		strcpy(tempop, "./");
		
	}
	else if(argv[3][0] == '-' && argv[3][1] == 'o' && argv[4] != NULL){
	
		file = fopen(argv[4], "r");
	if(file == NULL){
		fprintf(stderr, "ERROR: <No Output Dir Found>\n");
		return 0;
	}
	fclose(file);

		strcpy(tempop, argv[4]);
		strcpy(indir, "./");
	}
	else{
		fprintf(stderr,"Incorrect format\n");
		return 0;
	}
	}

	else if(argc == 7)
	{
		strcpy(token, argv[2]);
	if(argv[1] == NULL){
		fprintf(stderr, "\nERROR: <Expected -c \"item\">\n");	
		//free(token);
		//free(str_file);
		return 0;
	}

	if(argv[1][0] != '-' && argv[1][1] != 'c' && argv[2] == NULL){
		fprintf(stderr, "\nERROR: <Expected -c \"item\">\n");
		//free(token);
		//free(str_file);
		return 0;
	}
	
	
	
	if(argv[3][0] != '-' && argv[3][1] != 'd' && argv[4] == NULL){
		fprintf(stderr, "\nERROR: <Expected -d \"dir\">\n");
		//free(token);
		//free(str_file);
		return 0;
	}
	
	if(argv[5][0] != '-' && argv[5][1] != 'o' && argv[6] == NULL){
		fprintf(stderr, "\nERROR: <Expected -o \"output\">\n");
		//free(token);
		//free(str_file);
		return 0;
	}

		strcpy(tempop, argv[6]);
		strcpy(indir, argv[4]);
	
	file = fopen(argv[6], "r");
	//printf(argv[6]);
	if(file == NULL){
		fprintf(stderr, "\nERROR: <No Output Dir Found\n>");
		return 0;
	}
		fclose(file);

	}

	else{
		fprintf(stderr, "\nERROR: <Expected -c at least with -d or -o \"output\">\n");
		return 0;
	}

	//printf("hi");
	
	char * outdirnew = malloc(1000);
	realpath(tempop, outdirnew);
	//printf("%s\n" ,tempop);



	//pipe(p);
	int ipd = getpid();
	//int procNum=1;
	//printf("Initial PID: %d\nPIDS of all child processes: ", ipd);
	//fflush(stdout);
	
	//printf("%s, %s, %s", token, indir, temp90);	
	//sortCSVFile(argv[4], token,tempop);	
	printdir(token, indir , outdirnew, ipd);  
	//while(read(p[0], &depth, 1) != 0)
	//{
	//	procNum = procNum+1;
	//}
	//printf("\nTotal number of processes: %d\n", procNum);
	//fclose(file);	
	return 0;
}
