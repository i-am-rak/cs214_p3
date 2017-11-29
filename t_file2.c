#include <ftw.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h>
#include <ctype.h>
#include "Sorter.h"

pthread_mutex_t running_mutex;
pthread_mutex_t mrg;
int numoffiles = 0;
pthread_t * threads;
int index_threads = 0;
static char * outdir_global;
static char * type_global ;

char dataType;
CSVRow* allFiles;
int arrCounter=0;
char* header;

struct arg_struct {  //Struct to allow for multiple arguments in threads

	char* file_path;
	char* out_dir;
	char* sort_type;

} args;

void mergeStr(CSVRow* arr,CSVRow* help, int lptr,int rptr,int llimit,int rlimit,int num)
{
    int i=lptr,j=llimit,k=0;
    while(i<=rptr && j<=rlimit) 
    {
	if(strcmp(arr[i].data,arr[j].data)==0)
	{
		if(arr[i].point<arr[j].point)
		{
        	    strcpy(help[k].data,arr[i].data);
        	    help[k].point=arr[i].point;
        	    strcpy(help[k].string_row,arr[i].string_row);
        	    k++;
        	    i++;
		}
        	else
		{
        	    strcpy(help[k].data,arr[j].data);
        	    help[k].point=arr[j].point;
        	    strcpy(help[k].string_row,arr[j].string_row);
        	    k++;
        	    j++;
		}
	}
        else if(strcmp(arr[i].data,arr[j].data)<0)
	{
            strcpy(help[k].data,arr[i].data);
            help[k].point=arr[i].point;
            strcpy(help[k].string_row,arr[i].string_row);
            k++;
            i++;
	}
        else
	{
            strcpy(help[k].data,arr[j].data);
            help[k].point=arr[j].point;
            strcpy(help[k].string_row,arr[j].string_row);
            k++;
            j++;
	}
    }

    while(i<=rptr)
    {
            strcpy(help[k].data,arr[i].data);
            help[k].point=arr[i].point;
            strcpy(help[k].string_row,arr[i].string_row);
            k++;
            i++;
    }
    while(j<=rlimit) 
    {
            strcpy(help[k].data,arr[j].data);
            help[k].point=arr[j].point;
            strcpy(help[k].string_row,arr[j].string_row);
            k++;
            j++;
    }
    for(i=lptr,j=0;i<=rlimit;i++,j++)
    {
	        strcpy(arr[i].data,help[j].data);
		arr[i].point=help[j].point;
		strcpy(arr[i].string_row,help[j].string_row);
    }
}

void sortStr(CSVRow* a,CSVRow *b, int i,int j,int num)
{
    int mid;
    if(i<j)
    {
        mid=(i+j)/2;
        sortStr(a,b,i,mid,num); 
        sortStr(a,b,mid+1,j,num);
        mergeStr(a,b, i,mid,mid+1,j,num); 
    }
}

void mergeInt(CSVRow* arr,CSVRow* help, int lptr,int rptr,int llimit,int rlimit,int num)
{
    int i=lptr,j=llimit,k=0;
    while(i<=rptr && j<=rlimit) 
    {
	if(strtof(arr[i].data,NULL)==strtof(arr[j].data,NULL))
	{
		if(arr[i].point<arr[j].point)
		{
        	    strcpy(help[k].data,arr[i].data);
        	    help[k].point=arr[i].point;
        	    strcpy(help[k].string_row,arr[i].string_row);
        	    k++;
        	    i++;
		}
        	else
		{
        	    strcpy(help[k].data,arr[j].data);
        	    help[k].point=arr[j].point;
        	    strcpy(help[k].string_row,arr[j].string_row);
        	    k++;
        	    j++;
		}
	}
        else if(strtof(arr[i].data,NULL)<strtof(arr[j].data,NULL))
	{
			strcpy(help[k].data,arr[i].data);
            help[k].point=arr[i].point;
            strcpy(help[k].string_row,arr[i].string_row);
            k++;
            i++;
	}
        else
	{
            strcpy(help[k].data,arr[j].data);
            help[k].point=arr[j].point;
            strcpy(help[k].string_row,arr[j].string_row);
            k++;
            j++;
	}
    }
    while(i<=rptr) 
    {
            strcpy(help[k].data,arr[i].data);
            help[k].point=arr[i].point;
            strcpy(help[k].string_row,arr[i].string_row);
            k++;
            i++;
    }
    while(j<=rlimit) 
    {
            strcpy(help[k].data,arr[j].data);
            help[k].point=arr[j].point;
            strcpy(help[k].string_row,arr[j].string_row);
            k++;
            j++;
    }
    for(i=lptr,j=0;i<=rlimit;i++,j++)
    {
        strcpy(arr[i].data,help[j].data);
		arr[i].point=help[j].point;
		strcpy(arr[i].string_row,help[j].string_row);
    }
}

void sortInt(CSVRow* a,CSVRow* b, int i,int j,int num)
{
    int mid;
    if(i<j)
    {
        mid=(i+j)/2;
        sortInt(a,b, i,mid,num); 
        sortInt(a,b, mid+1,j,num); 
        mergeInt(a,b, i,mid,mid+1,j,num); 
    }
}

void callMe(int size,char type,CSVRow* arr, CSVRow* b)
{
	if(type=='i')
	{
		sortInt(arr,b, 1,size-1,size);
	}
	else
	{
		sortStr(arr,b, 1,size-1,size);
	}
	return;
}

void trim(char* str)
{
	char * t = malloc(strlen(str));
	int i =0;
	int j=0;
	for(i=0;i<strlen(str);i++)
	{
		if(isspace(str[i])==0)
		{
			t[j]=str[i];
			j++;
		}
	}
	t[j]='\0';
	strcpy(str,t);
	free(t);
}

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


void file_test(char* filename1, char* outdir,char* token) //the meat of joeStuff.c (missing main() stuff)
{
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
					//field not found
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
							trim(movies[count].data);
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
							trim(movies[count].data);
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
							trim(movies[count].data);
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

	//sort local array first
	callMe(file_count,dataType,movies,help);
	
	header=strdup(movies[0].string_row);
	//WRITE MOVIES INTO GLOBAL ARRAY
	for(i=1;i<file_count;i++)
	{
		printf("strcpy @ i=%d\n",i);
		pthread_mutex_lock(&mrg);
		int spot=arrCounter++;
		pthread_mutex_unlock(&mrg);
		strcpy(allFiles[spot].data,movies[i].data);
		strcpy(allFiles[spot].string_row,movies[i].string_row);
		allFiles[spot].point=movies[i].point;
		/*
		allFiles[spot].data=strdup(movies[i].data);
		allFiles[spot].string_row=strdup(movies[i].string_row);
		allFiles[spot].point=movies[i].point;
		*/
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
	//free(token);
	free(str_file);
	return;

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
	
	allFiles=malloc(sizeof(CSVRow*)*1000);
	int k;
	for(k=0;k<1000;k++)
	{
		allFiles[k].data=malloc(100);
		allFiles[k].string_row=malloc(1000);
	}
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

	printf("type : %s \nin_dir : %s \noutdir : %s \n\n", type_global, in_dir, outdir_global);	

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
	
	printf("before my added stuff in main() %d\n",arrCounter);
	fflush(stdout);
	CSVRow* help=malloc(sizeof(CSVRow*)*1000);
	for(i=0;i<100;i++)
	{
		help[i].data=malloc(100);
		help[i].string_row=malloc(1000);
	}
	printf("bef\n");
  	FILE* finalOut=fopen(out_filename,"w");		
	fprintf(finalOut,"%s",header);
	callMe(arrCounter,dataType,allFiles,help);
	printf("aft\n");
	fflush(stdout);
	for(i=0;i<arrCounter;i++)
	{
		printf(">%d: ",i);
		fflush(stdout);
		fprintf(finalOut,"%s",allFiles[i].string_row);
		printf("%s\n\n",allFiles[i].string_row);
	//	free(allFiles[i].data);
	//	free(allFiles[i].string_row);
	}
	pthread_mutex_destroy(&running_mutex);
  	printf("\n"); //extra new line for space 
	free(type_global);
	free(outdir_global);
	free(in_dir);
	free(out_filename);
	exit(EXIT_SUCCESS);
}
