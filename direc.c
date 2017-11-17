#include <unistd.h>  
#include <stdio.h>  
#include <dirent.h>  
#include <string.h>  
#include <sys/stat.h>  
#include <sys/wait.h>
#include <stdlib.h>  
  
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
  
void printdir(char *dir)  
{  
	DIR *dp;  
	struct dirent *entry;  
	struct stat statbuf;  
	char* path=malloc(1000);
	if((dp=opendir(dir))==NULL)
	{  
        	return ;  
    	}  
    	chdir(dir);  
	int id;
	int idCSV;
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
			procs++;
			id=fork();
			if(id==0)
			{
				procs=0;
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
			if(strstr(entry->d_name,"-sorted-")!=NULL)
			{
				continue;
			}
			char* this=strdup(path);
			strcat(this,entry->d_name);
			strcat(this,"\0");
			printf("%s\n",this);
			//free(this);
	/*		
			//sortCSVFile(char* sortBy,char* fileName,char* outDir);
            		procs++;
			printf("forking on %s\n",entry->d_name);
			idCSV=fork();
			if(idCSV==0)
			{
				//sortCSVFile(path,sortBy,outDir);
			}
			printf("%s\n",entry->d_name);
	*/
		}
    	} 
	int i;
	for(i=0;i<procs;i++)
	{
		wait(NULL);
	}
    	chdir("..");  
    	closedir(dp);     
	free(path);
}  
  
  
int main(int argc, char *argv[])  
{ 
	char* topdir;
	if (argc >= 2)  
	{
        	topdir = argv[1];  
	}
	else
	{
		topdir=malloc(100);
		getcwd(topdir,sizeof(topdir));
		printf("%s\n",topdir);
	}
	printdir(topdir);  
	return 0; 
}  
