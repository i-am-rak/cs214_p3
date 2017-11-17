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


void sortCSVFile(char * filename1,char * token, char * outdir){
	
	FILE * fp = fopen(filename1, "r");
	int file_count = 0;
	char c = 0;
	int i = 0;
	char * str_file = malloc(10);
	int row_position = 0;
	int j;
	//fprintf(stdout, "%s\n", token);		

	c = fgetc(fp);
	while (c != EOF) {
		//printf("%c\n",c);
		str_file = realloc(str_file, (i+1) * sizeof(char));	
		str_file[i] = c;
		if(c == '\n'){
			file_count++;
		}
		i++;
		c = fgetc(fp);
    }
        
	fclose(fp);
	
	str_file[i] = '\0';
	
	
	CSVRow *movies = malloc(file_count * sizeof(CSVRow));
	//token = strtok(str_file, "\n");
	
	for(j = 0; j < file_count; j++){
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

	//CSVRow *tempy = malloc(file_count * sizeof(CSVRow));
	//token = strtok(str_file, "\n");
	
	//for(int j = 0; j < file_count; j++){
	//	tempy[j].data = malloc(1000);
	//	tempy[j].point = j;
	//	tempy[j].string_row = malloc(1000);
	//}


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

	for(j = 0; j < i; j++){
		if(str_file[j] == '\n'){
			//printf("a\n");
			strncpy(movies[count].string_row, str_file+temp,j-temp+1);
			movies[count].string_row[j-temp+1] = '\0';
			if (count == 0){
				c = movies[count].string_row[index];
				for(index = 0; index<strlen(movies[count].string_row) ; index++){
					//fprintf(stdout, "%c\n", c);
					c = movies[count].string_row[index];
					if(c == ',' || movies[count].string_row[index+1] == '\n'){
						if( movies[count].string_row[index+1] == '\n'){
							index++;
						}
						comma_position_max++;
						if(index == p1 || index == p1+1){
							check_token = "\0";
						}
						else{
							strncpy(check_token, movies[count].string_row+p1,index-p1);
							check_token[index-p1] =  '\0';
							//fprintf(stdout, "[%s] , [%s]\n", check_token, token);
						}
						if(strcmp(check_token,token) == 0){
							char_found = 1;
							//fprintf(stdout, "[%s] , [%s]\n", check_token, token);
							break;
						}
						p1 = index+1;
						if( movies[count].string_row[index+1] == '\n'){
							index--;
						}

					}
					//printf("%d\n %c",char_found, c);
				}
				if(char_found == 0){
					//fprintf(stderr, "ERROR: <Selected item was not found in parameters>\n");
					free(check_token);
					free(movies);
					free(token);
					free(str_file);

					return;
				}
				//fprintf(stdout,"%d : %d\n",char_found , comma_position_max);
				//fprintf(stdout, "[%s] : [%s] \n",token, check_token);
				strcpy(movies[count].data, token);
				movies[count].data[strlen(token)+1] = '\0';
			}
			else{
				//fprintf(stdout, "%d \n ", count);
				comma_number = 0;
				index = 0;
				p1 = 0;
				c = movies[count].string_row[index];
				for(index = 0; index<strlen(movies[count].string_row); index++){
					//fprintf(stdout, "%c\n", c);
					c = movies[count].string_row[index];
					if(c == ',' && index+1 != strlen(movies[count].string_row) && movies[count].string_row[index+1] == '"'){
							
						comma_number++;
						if((index == p1) && (comma_number == comma_position_max)){
							//movies[count].data = "0\0";
							break;
						}
						else if(comma_number == comma_position_max){
							strncpy(movies[count].data, movies[count].string_row+p1,index-p1);
							//fprintf(stdout, "[%s] , [%s]\n", check_token, token);
							//movies[count].data[index-p1] = '\0';
							//fprintf(stdout, "%d: %s\n",count, movies[count].data);	
							trim(movies[count].data);
							break;
						}
						p1 = index+1;
						
						index = index+2;
						int x;
						for(x = 0; c != '"'; index++){
							c = movies[count].string_row[index];
						}	
						
						c = movies[count].string_row[index];
						//fprintf(stdout,"%c\n" , c);
						comma_number++;
						if((index == p1) && (comma_number == comma_position_max)){
							//movies[count].data = "0\0";
							break;
						}
						else if(comma_number == comma_position_max){
							strncpy(movies[count].data, movies[count].string_row+p1,index-p1);
							//fprintf(stdout, "[%s] , [%s]\n", check_token, token);
							//movies[count].data[index-p1] = '\0';
							//fprintf(stdout, "%d: %s\n",count, movies[count].data);	
							trim(movies[count].data);
							break;
						}
						p1 = index+1;
						index++;
						c = movies[count].string_row[index];
	
					}
					if(c == ',' || movies[count].string_row[index+1] == '\n'){

						if( movies[count].string_row[index+1] == '\n'){
							index++;
						}

						comma_number++;
						if((index == p1) && (comma_number == comma_position_max)){
							//movies[count].data = "NULL";
							break;
						}
						else if(comma_number == comma_position_max){
							strncpy(movies[count].data, movies[count].string_row+p1,index-p1);
							//fprintf(stdout, "[%s] , [%s]\n", check_token, token);
							//movies[count].data[index-p1] = '\0';
							//fprintf(stdout, "%d: %s\n",count, movies[count].data);	
							trim(movies[count].data);
							break;
						}
						p1 = index+1;
						if( movies[count].string_row[index+1] == '\n'){
							index--;
						}

					}
				}
			}
			
			temp = j+1;
			count++;
		}
	}
	
	char type = 'i';
	int k;
	for(j = 1; j < file_count; j++){
		for(k = 0; movies[j].data[k] != '\0'; k++){
			if(!(isdigit(movies[j].data[k]))){
				if(movies[j].data[k] != '.' || movies[j].data[k] != '-'){
					type = 's';	
				}
			}
		}
	}
	//printf("%d \n", type);
	//mergesort(movies,1,file_count-1,file_count);
	callMe(file_count,type,movies,help);
	//printf("heyo\n");
	//printf("\n");
	char * filename = malloc(1000);
	
	char * tempp = malloc(1000);
	strncpy(tempp,filename1,strlen(filename1)-4);
	tempp[strlen(filename1)-4] = '\0';
	sprintf(filename, "%s/%s-sorted-%s.csv",outdir,tempp,token);
	FILE* file_ptr = fopen(filename, "w");
	for(j = 0; j < file_count; j++){
		fprintf(file_ptr, "%s", movies[j].string_row);
		//printf("[%s]\n", movies[j].data);
	}
	fclose(file_ptr);
	//printf("\n\n");
	free(filename);
	free(tempp);
	

	for(j = 0; j < file_count; j++){
		free(movies[j].data);
		free(help[j].data);
		//movies[j].point = j;
		free(movies[j].string_row);
		free(help[j].string_row);
	}


	free(check_token);
	free(movies);
	free(help);
	free(token);
	free(str_file);

	return;
}

  
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
				printf("%d, ", getpid());
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
				printf("%d, ", getpid());
				fflush(stdout);
				strcat(path,entry->d_name);
				strcat(path,"\0");
				//printf("%s\n",path);
				sortCSVFile(entry->d_name, token , outdir);
				kill(getpid(), SIGKILL);
			}
		}
    	} 
	int i;
	for(i=0;i<procs;i++)
	{
		wait(NULL);
	}
	if(getpid() != idp)
	{
		kill(getpid(), SIGKILL);
	}
	close(p[1]);
    	chdir("..");  
    	closedir(dp);     
	free(path);
}  
 

int main(int argc, char ** argv){
/*
	if(stdin == NULL){
		fprintf(stderr, "ERROR: <No Input In STDIN>\n");	
		//free(token);
		//free(str_file);			
		return 0;
	}
*/	

	


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

	//printf("hi")
	
	char * outdirnew = malloc(1000);
	realpath(tempop, outdirnew);
	//printf("%s\n" ,tempop);



	pipe(p);
	int ipd = getpid();
	int procNum=1;
	printf("Initial PID: %d\nPIDS of all child processes: ", ipd);
	fflush(stdout);
	
	//printf("%s, %s, %s", token, indir, temp90);	
	//sortCSVFile(argv[4], token,tempop);	
	printdir(token, indir , outdirnew, ipd);  
	while(read(p[0], &depth, 1) != 0)
	{
		procNum = procNum+1;
	}
	printf("\nTotal number of processes: %d\n", procNum);
	//fclose(file);	
	return 0;
}
