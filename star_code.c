#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <mpi.h>

FILE *fd;
char **keyWord, **line;
int **final, **local_final;
int NUM_THREADS;
int numOfKeyWords = 500, numOfWikiLines = 1000;
int wikiLineLength = 2000;
int keyWordLength = 6;

void initArray(int numTasks) {
	int i, j;
	final = (int **) malloc (numOfKeyWords * sizeof(int *));
	for ( i = 0; i < numOfKeyWords; i++){
        for ( j = 0; j < numOfWikiLines; j++){
		    final[i] = (int *) malloc(numOfWikiLines * sizeof (int));
        }
	}

	local_final = (int **) malloc (numOfKeyWords * sizeof(int *));
	for ( i = 0; i < numOfKeyWords; i++){
		for ( j = 0; j < numOfWikiLines; j++){
		    local_final[i] = (int *) malloc(sizeof (int));
        }
	}
	
    keyWord = (char **) malloc (numOfKeyWords * sizeof(char *));
	for ( i = 0; i < numOfKeyWords; i++){
		keyWord[i] = (char *) malloc((keyWordLength + 1) * sizeof (char));
	}

    line = (char **) malloc (numOfWikiLines * sizeof(char *));
	for ( i = 0; i < numOfWikiLines; i++){
		line[i] = (char *) malloc((wikiLineLength + 1)* sizeof (char));
	}
}

void readFiles(){
   int err;
   int nwords, nlines;
   double nchars;

   // Read in the key words
   fd = fopen("/homes/dan/625/keywords.txt", "r");
   nwords = -1;
   do {
      err = fscanf( fd, "%[^\n]\n", keyWord[++nwords] );
   } while( err != EOF && nwords < numOfKeyWords );
   fclose( fd );

   printf( "Read in %d words\n", nwords);

   // Read in the lines from the WikiDump file
   fd = fopen("/homes/dan/625/wiki_dump.txt", "r");
   nlines = -1;
   do {
      err = fscanf( fd, "%[^\n]\n", line[++nlines] );
      if( line[nlines] != NULL ) nchars += (double) strlen( line[nlines] );
   } while( err != EOF && nlines < numOfWikiLines);
   fclose( fd );

   printf( "Read in %d lines averaging %.0lf chars/line\n", nlines, nchars / nlines);
}

void sortArray(){
    int i, j;
    char* temp;
    for(i = 0; i < numOfKeyWords; i++) {
		for(j = i + 1; j < numOfKeyWords; j++) {
			if(strcmp(keyWord[i], keyWord[j]) > 0){
				strcpy(temp, keyWord[i]);
				strcpy(keyWord[i], keyWord[j]);
				strcpy(keyWord[j], temp);		
			}
		}
    }
}

void main(int argc, char *argv){
    MPI_Status status;
    struct timeval t1, t2;
    double elapsedTime;
    int count = 0;
    int myVersion = 1, index = 0;
    int numTasks, rank, rc, i, j;
    MPI_Status Status;
    char *currentWord = (char*) malloc(keyWordLength * sizeof(char));

    //Check rc
    rc = MPI_Init(&argc, &argv);
    if(rc != MPI_SUCCESS){
        printf("Error starting MPI program. Terminating. \n");
        MPI_Abort(MPI_COMM_WORLD, rc);
    }

    gettimeofday(&t1, NULL);

    MPI_Comm_size(MPI_COMM_WORLD, &numTasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	initArray(numTasks);

    if(rank == 0){
		readFiles();
	}

    sortArray();

    NUM_THREADS = numTasks;
    printf("size = %d rank = %d\n", numTasks, rank);
    fflush(stdout);

    for(i = 0; i < numOfWikiLines; i++){
        MPI_Bcast(line[i], wikiLineLength, MPI_CHAR, 0, MPI_COMM_WORLD);
    }

    index = rank - 1;

    while(1){
        if(rank != 0){
            if(index >= numOfKeyWords){
                break;
            }
        printf("%d %d\n", index, rank);
            MPI_Recv(currentWord, keyWordLength, MPI_CHAR, 0, 10, MPI_COMM_WORLD, &status);

            int i;
            char* substr = (char *) malloc(keyWordLength * sizeof (char));

            for(i = 0; i < numOfWikiLines; i++){ 
                substr = strstr(line[i], currentWord);
                if(substr != NULL){ 
                    local_final[index][i] = 1; 
                }
                else{
                    local_final[index][i] = 0;
                }
            }

            index += (NUM_THREADS - 1); 
        }

        if(rank == 0){
    printf("%d %d\n", count, rank);
            MPI_Send(keyWord[count], keyWordLength, MPI_CHAR, (count % (NUM_THREADS-1) + 1), 10, MPI_COMM_WORLD);

            count++;
            if(count == numOfKeyWords){
                break;
            }
        }  
    }

    for ( i = 0; i < numOfKeyWords; i++){
        for ( j = 0; j < numOfWikiLines; j++){
            MPI_Reduce(&local_final[i][j], &final[i][j], 1, MPI_INT, MPI_MAX, 0, MPI_COMM_WORLD);
        }
    }

    int found = 0;
    if(rank == 0){
        for(i = 0; i < numOfKeyWords; i++){
            found = 0;
            for(j = 0; j < numOfWikiLines; j++){
                if(final[i][j] == 1){
                    if(found == 1) {
                        printf(" %d", j);
                    }
                    else{
                        printf("%s : %d", keyWord[i], j);
                        found = 1;
                    }
                }
            }
            if(found == 1){
                printf("\n");
            }
        }
    }
    MPI_Finalize();

    gettimeofday(&t2, NULL);
    elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000.0;
    elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000.0;

    if(rank == 0){
        printf("DATA: %d, %d, %d, %f\n", numOfKeyWords, numOfWikiLines, NUM_THREADS, elapsedTime);
    }

    return 0;
}