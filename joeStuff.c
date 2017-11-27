#include <unistd.h>  
#include <stdio.h>  
#include <dirent.h>  
#include <string.h>  
#include <sys/stat.h>  
#include <sys/wait.h>
#include <stdlib.h>  
#include <pthread.h>
#include <ctype.h>
#include "Sorter.h"

typedef struct params
{
	char* field;
	char* path;
	char* outDir;
}params;

pthread_mutex_t m;
pthread_mutex_t mrg;
pthread_t* allIDs;
CSVRow allFiles[5000];
char dataType;
int arrCounter=0;
int idCounter=0;
FILE* out;

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

void sortCSVFile(char * filename1,char * token, char * outdir){
	
	FILE* fp = fopen(filename1, "r");
	int file_count = 0;
	char c = 0;
	int i = 0;
	char* str_file = malloc(10);
	int row_position = 0;
	int j;
	//fprintf(stdout, "%s\n", token);		

	c = fgetc(fp);
	while (c != EOF)
	{
		//printf("%c\n",c);
		str_file = realloc(str_file, (i+1) * sizeof(char));	
		str_file[i] = c;
		if(c == '\n')
		{
			file_count++;
		}
		i++;
		c = fgetc(fp);
   	}
        
	fclose(fp);
	
	str_file[i] = '\0';
	
	
	CSVRow *movies = malloc(file_count * sizeof(CSVRow));
	//token = strtok(str_file, "\n");
	
	for(j = 0; j < file_count; j++)
	{
		movies[j].data = malloc(10000);
		movies[j].point = j;
		movies[j].string_row = malloc(10000);
	}

	CSVRow* help=malloc(sizeof(CSVRow)*file_count*2);    //array used for merging
    	for(j =0;j<file_count;j++)
    	{
		help[j].data=malloc(10000);
		help[j].point=j;
		help[j].string_row=malloc(10000);
    	}

	int temp = 0;
	int count = 0;
	int index = 0;
	int comma_position_max = 0;
	int p1 = 0;
	int p2 = 0;
	int char_found = 0;
	int comma_number = 0;
	char * check_token = malloc(1000);
	c = 0;

	for(j = 0; j < i; j++)
	{
		if(str_file[j] == '\n')
		{
			strncpy(movies[count].string_row, str_file+temp,j-temp+1);
			movies[count].string_row[j-temp+1] = '\0';
			if (count == 0)
			{
				c = movies[count].string_row[index];
				for(index = 0; index<strlen(movies[count].string_row) ; index++)\
				{
					c = movies[count].string_row[index];
					if(c == ',' || movies[count].string_row[index+1] == '\n')\
					{
						if( movies[count].string_row[index+1] == '\n')
						{
							index++;
						}
						comma_position_max++;
						if(index == p1 || index == p1+1)
						{
							check_token = "\0";
						}
						else
						{
							strncpy(check_token, movies[count].string_row+p1,index-p1);
							check_token[index-p1] =  '\0';
						}
						if(strcmp(check_token,token) == 0)
						{
							char_found = 1;
							break;
						}
						p1 = index+1;
						if( movies[count].string_row[index+1] == '\n')
						{
							index--;
						}

					}
				}
				if(char_found == 0)
				{
					free(check_token);
					free(movies);
					free(token);
					free(str_file);

					return;
				}
				strcpy(movies[count].data, token);
				movies[count].data[strlen(token)+1] = '\0';
			}
			else
			{
				comma_number = 0;
				index = 0;
				p1 = 0;
				c = movies[count].string_row[index];
				for(index = 0; index<strlen(movies[count].string_row); index++)
				{
					c = movies[count].string_row[index];
					if(c==','&&index+1!=strlen(movies[count].string_row)&&movies[count].string_row[index+1]=='"')
					{
							
						comma_number++;
						if((index == p1) && (comma_number == comma_position_max))
						{
							break;
						}
						else if(comma_number == comma_position_max)
						{
							strncpy(movies[count].data, movies[count].string_row+p1,index-p1);
							//trim(movies[count].data);
							break;
						}
						p1 = index+1;
						
						index = index+2;
						int x;
						for(x = 0; c != '"'; index++)
						{
							c = movies[count].string_row[index];
						}	
						
						c = movies[count].string_row[index];
						comma_number++;
						if((index == p1) && (comma_number == comma_position_max))
						{
							break;
						}
						else if(comma_number == comma_position_max)
						{
							strncpy(movies[count].data, movies[count].string_row+p1,index-p1);
							//trim(movies[count].data);
							break;
						}
						p1 = index+1;
						index++;
						c = movies[count].string_row[index];
	
					}
					if(c == ',' || movies[count].string_row[index+1] == '\n')
					{
						if( movies[count].string_row[index+1] == '\n')
						{
							index++;
						}

						comma_number++;
						if((index == p1) && (comma_number == comma_position_max))
						{
							break;
						}
						else if(comma_number == comma_position_max)
						{
							strncpy(movies[count].data, movies[count].string_row+p1,index-p1);
							//trim(movies[count].data);
							break;
						}
						p1 = index+1;
						if( movies[count].string_row[index+1] == '\n')
						{
							index--;
						}

					}
				}
			}
			
			temp = j+1;
			count++;
		}
	}
	
	dataType = 'i';
	int k;
	for(j = 1; j < file_count; j++)
	{
		for(k = 0; movies[j].data[k] != '\0'; k++)
		{
			if(!(isdigit(movies[j].data[k])))
			{
				if(movies[j].data[k] != '.' || movies[j].data[k] != '-')
				{
					dataType = 's';	
				}
			}
		}
	}

	//sort them first??
	//callMe(file_count,type,movies,help);
	
	//WRITE MOVIES INTO GLOBAL ARRAY
	for(i=0;i<file_count;i++)
	{
		pthread_mutex_lock(&mrg);
		int spot=arrCounter++;
		pthread_mutex_unlock(&mrg);
		allFiles[spot].data=strdup(movies[i].data);
		allFiles[spot].string_row=strdup(movies[i].string_row);
		allFiles[spot].point=movies[i].point;
	}

	for(j=0;j<file_count;j++)
	{
		free(movies[j].data);
		free(help[j].data);
		free(movies[j].string_row);
		free(help[j].string_row);
	}
	free(check_token);
	free(movies);
	free(help);
	free(token);
	free(str_file);

	return;

/*
	char * filename = malloc(1000);
	
	char * tempp = malloc(1000);
	strncpy(tempp,filename1,strlen(filename1)-4);
	tempp[strlen(filename1)-4] = '\0';
	sprintf(filename, "%s/%s-sorted-%s.csv",outdir,tempp,token);
	FILE* file_ptr = fopen(filename, "w");
	for(j = 0; j < file_count; j++)
	{
		fprintf(file_ptr, "%s", movies[j].string_row);
	}
	fclose(file_ptr);
	free(filename);
	free(tempp);
	

	for(j = 0; j < file_count; j++)
	{
		free(movies[j].data);
		free(help[j].data);
		//movies[j].point = j;
		free(movies[j].string_row);
		free(help[j].string_row);
	}

*/
}

/*
void* file(void* ps)	//give to thread if on file (to sort)
{
	params* ps2=(params*)ps;
	printf("%s\n",ps2->path);
//	sortCSVFile(ps2->path,ps2->field,ps2->outDir);
}
  
void* folder(void* ds)  
{  
	params* ds2=(params*)ds;
	char* dir=ds2->path;
	DIR* dp;  
	struct dirent* entry;  
	struct stat statbuf;  
	//chdir("./");
	if((dp=opendir(dir))==NULL)
	{  
		return "";
    	}  
    	//chdir(dir);  
	entry=readdir(dp);
	while((entry=readdir(dp))!=NULL)
	{
        	lstat(entry->d_name,&statbuf);  
		if(entry==NULL)
		{
			return "";
		}
        	else if(S_ISDIR(statbuf.st_mode))
		{  
        	    	if(strcmp(entry->d_name, ".") == 0||strcmp(entry->d_name, "..") == 0)
        	       	{
				continue;
			}
			params* new=malloc(sizeof(params*));
			new->field=strdup(ds2->field);
			
			
			new->path=strdup(ds2->path);
			strcat(new->path,entry->d_name);
			strcat(new->path,"/");


			new->outDir=strdup(ds2->outDir);
			pthread_mutex_lock(&m);
			pthread_t id=idCounter++;
			pthread_mutex_unlock(&m);
			pthread_create(&allIDs[id], NULL, folder, (void*)new);
       		} 
		else if(isCSV(entry->d_name)) 
		{
			params* new=malloc(sizeof(params*));
			new->field=strdup(ds2->field);
			new->path=strdup(ds2->path);
			strcat(new->path,entry->d_name);
			new->outDir=strdup(ds2->outDir);
			pthread_mutex_lock(&m);
			pthread_t id=idCounter++;
			pthread_mutex_unlock(&m);
			pthread_create(&allIDs[id],NULL, file,(void*)new);
		}
	}
	return "";
}  

void callMe(int size,char type,CSVRow* arr, CSVRow* b)
{
	return;
}
*/
int main(int argc, char* argv[])  
{ 
	out=fopen("final.csv","w");
/*
	allIDs=malloc(10000);
	char *topdir=malloc(1000);  
	if (argc >= 2)  
	{
        	strcpy(topdir,argv[1]);
	}
	else
	{
		strcpy(topdir,"./");
 	} 
	params* p=malloc(sizeof(params*));
	p->path="./";
	p->field="grass";
	p->outDir="chair3";
    	pthread_create(&allIDs[idCounter++],NULL,folder,(void*)p);
	int i=0;
	for(i=0;i<idCounter;i++)
	{
		pthread_join(allIDs[i],NULL);
	}
*/
	CSVRow* help; 
	memcpy(&help,&allFiles,arrCounter);
	printf("(%d)\n",idCounter-1);
	//callMe(arrCounter,dataType,allFiles,help);

	for(i=0;i<arrCounter;i++)
	{
		free(allFiles[i].data);
		free(help[i].data);
		free(allFiles[i].string_row);
		free(help[i].string_row);
	}
	free(topdir);
	fclose(out);
    	return 0; 
} 
