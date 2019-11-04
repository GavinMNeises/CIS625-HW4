
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include "sys/types.h"
#include "sys/sysinfo.h"
#include <sys/time.h>
#include <mpi.h>

int NUM_THREADS;

int compare(const void* a, const void* b) {
        const char **ia = (const char **)a;
        const char **ib = (const char **)b;
        return strcmp(*ia, *ib);
}

int main(int argc, char* argv[])
{
   int rc;          //apart of threading
    int numtasks, rank, length; // use for the threading, need to figure out how these work
	MPI_Status status;    // used for threading
    char mpi_name[MPI_MAX_PROCESSOR_NAME];

    int word_count = 0; int word_iterator = 0;
    int line_amount[100]; int words_gotten = 0;  // variables for communicating between threads when threading
   struct timeval t1, t2;       // values for beginnig and ending time stamps *
   double elapsedTime;                    // value for holding overall time *
   int nwords, maxwords = 50000;//50000;       //    Should only be used in the reading in data  *
   int nlines, maxlines = 100000;//1000000;   //Should only be used in the reading in data  *
   int i, k, n, err, *count, nthreads = 24;  //nthreads will be changed in script call 
   double nchars = 0;         //char count *
   FILE *fd;             //file reader *
   char **word, **line;    // holds the char strings *
   char *word_memory, *line_memory, *word_memory2;

    // may not need this chunk
   MPI_Init(&argc, &argv);
   MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
   MPI_Comm_rank(MPI_COMM_WORLD, &rank);

   gettimeofday(&t1, NULL);  // this can be here i think but can put around the threading section as well maybe
// Malloc space for the word list and lines *
   
   if(rank == 0){
   word = (char **) malloc( maxwords * sizeof( char * ) );
   word_memory = malloc(maxwords * 10 * sizeof(char));
   for(i=0; i < maxwords; i++){
       word[i] = (word_memory + i * 10);     //10 is max keyword length
   }
}
   count = (int *) malloc( maxwords * (maxlines * sizeof(int)) );  // not sure if needed
   
   line = (char **) malloc(maxlines * sizeof( char *));
   line_memory = malloc(maxlines * 2001 * sizeof(char));
   for(i=0; i <maxlines; i++){
       line[i] = (line_memory + i * 2001);     //2001 is max line length
   }
   

// Read in the keywords, should only need to be done by the rank 0 thread
if(rank == 0){
   fd = fopen( "/homes/dan/625/keywords.txt", "r" );
   nwords = -1;
   do {
      err = fscanf( fd, "%[^\n]\n", word[++nwords] );       //This read in should be fine *
   } while( err != EOF && nwords < maxwords );
   fclose( fd );

   printf( "Read in %d words\n", nwords);

   qsort(word, nwords, sizeof(char *), compare);
    word_memory2 = malloc(maxwords * 10 * sizeof(char));
    for(i = 0; i < maxwords; i++) {
        for(k = 0; k < 10; k++) {
                *(word_memory2 + i * 10 + k) = word[i][k];
            }
            word[i] = word_memory + i * 10;
        }
    memcpy(word_memory, word_memory2, maxwords * 10);
    free(word_memory2); word_memory2 = NULL;

}

MPI_Bcast(&nwords, 1, MPI_INT, 0, MPI_COMM_WORLD);        //first broadcast by rank 0 thread to give words to threads


// Read in the lines from the wiki dump

   fd = fopen( "/homes/dan/625/wiki_dump.txt", "r" );
   nlines = -1;
   do {
      err = fscanf( fd, "%[^\n]\n", line[++nlines] );
      if( line[nlines] != NULL ) nchars += (double) strlen( line[nlines] );    //This read in should be fine *
   } while( err != EOF && nlines < maxlines);
   fclose( fd );


// rank variables for ring
int before = (rank + numtasks - 1) % numtasks;
int after = (rank + 1) % numtasks;

//some chunking
int begin = rank * (nlines/numtasks);
int end = (rank + numtasks - 1) % numtasks; 
if(rank == (numtasks-1)){
    end = nlines;
}

//a bit more memory allocation for keyword being used
char *current_keyw = (char*) malloc((11) * sizeof(char));  //11 because max key word lenght (10) + 1
current_keyw[0] = '\0';


   /*
   // not sure what this section does, not sure if I need yet
   fd = fopen( "wiki.out", "w" );
   for( i = 0; i < nwords; i++ ) {
      fprintf( fd, "%d %s %d\n", i, word[i], count[i] );
   }
   fprintf( fd, "The run on %d cores took %lf seconds for %d words\n",
           nthreads, ttotal, nwords);
   fclose( fd );
    */

//mpi stuff, still not completly sure how it all works, but got a little help with this section
// varible names for reference int word_count = 0; int word_iterator = 0;
 //   int line_amount[100]; int words_gotten = 0;
while(1){  
    if(rank == 0){ //if rank 0 get the key word
        current_keyw = word[word_iterator];
        word_count = 0;
    }else{
        MPI_Recv(current_keyw, 10, MPI_CHAR, before, 1, MPI_COMM_WORLD, &status); //listen for current key word
        MPI_Recv(&word_count, 1, MPI_INT, before, 2, MPI_COMM_WORLD, &status);  //listen for current count
        MPI_Recv(&line_amount, 100, MPI_INT, before, 3, MPI_COMM_WORLD, &status);  //listen for line number amount
    }

    for(i = begin; i < end; i++){
        if((strstr(line[i], current_keyw) != NULL) && word_count < 100){
            line_amount[word_count] = i;
            word_count++;
        }
    }


    MPI_Ssend(current_keyw, strlen(current_keyw) + 1, MPI_CHAR, after, 1, MPI_COMM_WORLD);
    MPI_Ssend(&word_count, 1, MPI_INT, after, 2, MPI_COMM_WORLD);
    MPI_Ssend(&line_amount, 100, MPI_INT, after, 3, MPI_COMM_WORLD); 


    if(rank == 0 && ((word_iterator - words_gotten >= (numtasks -2)))){
        MPI_Recv(current_keyw, 10, MPI_CHAR, before, 1, MPI_COMM_WORLD, &status);
        MPI_Recv(&word_count, 1, MPI_INT, before, 2, MPI_COMM_WORLD, &status);  
        MPI_Recv(&line_amount, 100, MPI_INT, before, 3, MPI_COMM_WORLD, &status); 
        words_gotten++;

        if(word_count){ //print out if we have a word_count
            printf("%s: ", current_keyw);
            for(i=0; word_count > 0; i++){
                printf("%i, ", line_amount[i]);
                word_count--;
            }
        }
    }

    if(word_iterator >= nwords - 1){ //if there are no more key words after this 
        if (rank == 0){
            while (words_gotten < nwords){
                MPI_Recv(current_keyw, 10, MPI_CHAR, before, 1, MPI_COMM_WORLD, &status);
                MPI_Recv(&word_count, 1, MPI_INT, before, 2, MPI_COMM_WORLD, &status);  
                MPI_Recv(&line_amount, 100, MPI_INT, before, 3, MPI_COMM_WORLD, &status); 
                words_gotten++;

                if(word_count){ //print out if we have a word_count
                    printf("%s: ", current_keyw);
                    for(i=0; word_count > 0; i++){
                        printf("%i, ", line_amount[i]);
                        word_count--;
                    }

                }
            }
        }
        break;
    }

    word_iterator++;
    current_keyw[0] = '\0';
}


    if (rank == 0){
        free(word); 
        word = NULL;
        free(word_memory);
        word_memory = NULL;
    }
    free(line);
    line = NULL;
    free(line_memory);
    line_memory = NULL;

    if(rank == 0){
        sleep(1);
        fflush(stdout);
        
    }



   MPI_Finalize();
   gettimeofday(&t2, NULL);

   elapsedTime = (t2.tv_sec - t1.tv_sec) * 1000.0;    //sec to ms
   elapsedTime += (t2.tv_usec - t1.tv_usec) / 1000.0; // us to ms
   if(rank == 0){
   printf("DATA, %f seconds , %d cores, %d keywords, %d lines\n", elapsedTime, numtasks, maxwords, maxlines);

    }
	return 0;
}





