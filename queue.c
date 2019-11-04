#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <mpi.h>
#include "unrolled_int_linked_list.c"
#include <sys/time.h>

#define MAX_KEYWORD_LENGTH 10
#define MAX_LINE_LENGTH 2001
#define BATCH_SIZE 100

#define WIKI_FILE "/homes/dan/625/wiki_dump.txt"
#define KEYWORD_FILE "/homes/dan/625/keywords.txt"
#define OUTPUT_FILE "/homes/gmneises/wiki"

int compare(const void* a, const void* b) {
  const char **ia = (const char **)a;
  const char **ib = (const char **)b;
  return strcmp(*ia, *ib);
}

int main(int argc, char * argv[])
{
	int nWords, maxWords = 50000;
	int nLines, maxLines = 100000;
	struct Node** head;
	struct Node** tail;
	char *wordMem, **words, *lineMem, **lines, *tempWordMem;
	FILE *fd;

	int i, k;

	int numtasks, rank, len, rc;
	char hostname[MPI_MAX_PROCESSOR_NAME];

	struct timeval t1, t2;
	double timeElapsed;

	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Get_processor_name(hostname, &len);
	printf("Number of tasks= %d, My rank= %d, Running on %s\n", numtasks, rank, hostname);
	gettimeofday(&t1, NULL);

	//Allocate nodes 
	if(rank) {
		allocateNodePools();

		//Allocate the head and tail nodes 
		head = (struct Node**) malloc(BATCH_SIZE * sizeof(struct Node *));
		tail = (struct Node**) malloc(BATCH_SIZE * sizeof(struct Node *));
		for(i = 0; i < BATCH_SIZE; i++) {
			head[i] = tail[i] = node_alloc();
		}
	}

	//Set number of words depending on if worker or master
	int my_num_words;
	if(rank) {
		my_num_words = BATCH_SIZE;
	}
	else {
		my_num_words = maxWords;
	}

	//Allocate words 
	wordMem = malloc(my_num_words * MAX_KEYWORD_LENGTH * sizeof(char));
	words = (char **) malloc(maxWords * sizeof(char *));
	for(i = 0; i < my_num_words; i++){
		words[i] = wordMem + i * MAX_KEYWORD_LENGTH;
	}

	//Allocate lines
	lineMem = malloc(maxLines * MAX_LINE_LENGTH * sizeof(char));
	lines = (char **) malloc(maxLines * sizeof(char *));
	for(i = 0; i < maxLines; i++) {
		lines[i] = lineMem + i * MAX_LINE_LENGTH;
	}


	//Read in the words and sort them
	if(!rank) {
		int err;
		fd = fopen(KEYWORD_FILE, "r");
		if(!fd) {
			printf("Can't open keyword file\n");
			return 0;
		}
		nWords = -1;
		do {
			err = fscanf(fd, "%[^\n]\n", words[++nWords]);
		} while(err != EOF && nWords < maxWords);
		fclose(fd);
		printf("Read in %d words\n", nWords);

		qsort(words, nWords, sizeof(char *), compare);
		tempWordMem = malloc(maxWords * MAX_KEYWORD_LENGTH * sizeof(char));
		for(i = 0; i < maxWords; i++){
			for(k = 0; k < MAX_KEYWORD_LENGTH; k++){
				*(tempWordMem + i * MAX_KEYWORD_LENGTH + k) = words[i][k];
			}
			words[i] = wordMem + i * MAX_KEYWORD_LENGTH;
		}
		memcpy(wordMem, tempWordMem, maxWords * MAX_KEYWORD_LENGTH);
		free(tempWordMem); 
		tempWordMem = NULL;
	}
	MPI_Bcast(&nWords, 1, MPI_INT, 0, MPI_COMM_WORLD);

	//Read in the lines from the wiki file
	if(!rank) {
		int err;
		char *input_file = (char*) malloc(500 * sizeof(char));
		fd = fopen(WIKI_FILE, "r" );
		if(!fd) {
			printf("Can't open wiki file");
			return 0;
		}
		nLines = -1;
		do {
			err = fscanf( fd, "%[^\n]\n", lines[++nLines] );
		} while(err != EOF && nLines < maxLines);
		fclose(fd);
		free(input_file); 
		input_file = NULL;

		printf("Read in %d lines\n", nLines);
	} 


	//Broadcast lines
	MPI_Bcast(&nLines, 1, MPI_INT, 0, MPI_COMM_WORLD);
	MPI_Bcast(lineMem, nLines * MAX_LINE_LENGTH, MPI_CHAR, 0, MPI_COMM_WORLD);


	//Search for keywords in lines 

	//Workers
	if(rank) {
		MPI_Status stat;
		int someval = 1;
		int batches = nWords/BATCH_SIZE;
		int my_num_batches = 0;
		int batch_number = 0;

		int *result_size = (int *) malloc(nWords * sizeof(int));
		int *result_id = (int *) malloc(nWords * sizeof(int));
		int **result_arr = (int **) malloc(nWords * sizeof(int *));
		int num_results = 0;
		
		//While there is still work to do 
		while(1) {
			//Ask for more work
			MPI_Sendrecv(&someval, 1, MPI_INT, 0, 1, wordMem, BATCH_SIZE * MAX_KEYWORD_LENGTH, MPI_CHAR, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);
			
			//If there is no more work break 
			if(*wordMem == 0) {
				break;
			}

			//Receive more work
			my_num_batches++;
			MPI_Recv(&batch_number, 1, MPI_INT, 0, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);

			for(i = 0; i < nLines; i++) {
				for(k = 0; k < BATCH_SIZE; k++) {
					if(strstr(lines[i], words[k]) != NULL) {
						tail[k] = add(tail[k], i);
					}
				}
			}

			//Send results
			printf("Batch %d on Rank %d\n", batch_number, rank);
			for(i = 0; i < BATCH_SIZE; i++) {
				int *current_result;
				int len;
				
				//Malloc for line_numbers
				toArray(head[i], &current_result, &len);

				//Set result array, count, and id
				result_arr[num_results] = current_result;
				result_id[num_results] = batch_number * BATCH_SIZE + i;
				result_size[num_results] = len;
				num_results++;
			}

			//Reset linked lists for next batch
			for(i = 0; i < BATCH_SIZE; i++) {
				head[i] = tail[i] = node_alloc();
			}
		}

		MPI_Barrier(MPI_COMM_WORLD);

		printf("Rank %d had %d batches\n", rank, my_num_batches);

		// Send back to root
		for(i = 0; i < num_results; i++)
		{
			MPI_Ssend(result_size + i, 1, MPI_INT, 0, *(result_id + i), MPI_COMM_WORLD);
			if(*(result_size + i)) {
				MPI_Ssend(*(result_arr + i), *(result_size + i), MPI_INT, 0, *(result_id + i), MPI_COMM_WORLD);
			}
		}

		for(i = 0; i < num_results; i++)
		{
			free(*(result_arr + i));
		}
		free(result_id);
		free(result_arr);
		free(result_size);
	} 
	else { //Rank 0
		MPI_Status stat;
		int someval = 1;
		int batches = nWords / BATCH_SIZE;
		for(k = 0; k < batches; k++) {
			MPI_Recv(&someval, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);
			MPI_Ssend(wordMem + (k * MAX_KEYWORD_LENGTH * BATCH_SIZE), BATCH_SIZE * MAX_KEYWORD_LENGTH, MPI_CHAR, stat.MPI_SOURCE, someval, MPI_COMM_WORLD);
			MPI_Ssend(&k, 1, MPI_INT, stat.MPI_SOURCE, someval, MPI_COMM_WORLD);
		}
		*wordMem = 0;
		for(k = 0; k < numtasks - 1; k++) {
			MPI_Recv(&someval, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &stat);
			MPI_Ssend(wordMem, BATCH_SIZE * MAX_KEYWORD_LENGTH, MPI_CHAR, stat.MPI_SOURCE, someval, MPI_COMM_WORLD);
		}

		MPI_Barrier(MPI_COMM_WORLD);

		//Receive results
		int result_size;
		int *current_result;
		char *output_file = (char*) malloc(500 * sizeof(char));
		sprintf(output_file, "%s-%s.out", OUTPUT_FILE, argv[2]);
		fd = fopen(output_file, "w");
		for(i = 0; i < nWords; i++) {
			MPI_Recv(&result_size, 1, MPI_INT, MPI_ANY_SOURCE, i, MPI_COMM_WORLD, &stat);

			if(result_size) {
				current_result = malloc(result_size * sizeof(int));
				MPI_Recv(current_result, result_size, MPI_INT, stat.MPI_SOURCE, i, MPI_COMM_WORLD, &stat);

				// Output results
				fprintf(fd, "%s: ", words[i]);
				for(k = 0; k < result_size - 1; k++) {
					fprintf(fd, "%d, ", current_result[k]);
				}
				fprintf(fd, "%d\n", current_result[result_size - 1]);

				free(current_result);
			}
		}
		fclose(fd);

		free(output_file); 
		output_file = NULL;

		printf("All results printed!\n");
	}


	//Free memory

	//Lines
	free(lines);    
	lines = NULL;
	free(lineMem); 
	lineMem = NULL;

	//Words
	free(words);    
	words = NULL;
	free(wordMem); 
	wordMem = NULL;
	
	//Nodes
	if(rank)
	{
		destroyNodePools();
		free(head); 
		head = NULL;
		free(tail); 
		tail = NULL;
	}

	MPI_Finalize();
	
	//Print time
	gettimeofday(&t2, NULL);
	timeElapsed = (t2.tv_sec - t1.tv_sec) * 1000.0; //sec to ms
	timeElapsed += (t2.tv_usec - t1.tv_usec) / 1000.0; //us to ms
	if(!rank) {
		printf("DATA, %d, %d, %d, %f\n", nWords, nLines, numtasks, timeElapsed);
	}

	return 0;
}
