#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <ctype.h>
#include <time.h>
#include <errno.h>
#include <pthread.h> 



#define MAX_COMMAND_LENGTH 4
#define MAX_CLIENTS 32
#define MAX_HASH_TABLE_LENGTH 4096
#define MAX_STREAM_KEYS 2048


typedef char** listpack;
typedef struct __radix_node radix_node;
typedef struct __linked_str_list {
    radix_node* data_pointer; // Pointer to the radix node
    struct __linked_str_list* next; // Pointer to the next linked list node
} linked_str_list;
typedef struct __radix_node {
    char is_leaf; // Indicates if the node is a leaf
    char* id_key; // Key for the node
    listpack value; // Value associated with the key
    linked_str_list* children; // Pointer to linked list of child nodes
} radix_node;
typedef struct {
    char* stream_key;
    linked_str_list* stream_data;
} stream_entry;

typedef struct __linked_list {
	char expires;
	int milli_second; 
    char* key;
    char* value;
    struct __linked_list *next;
    struct __linked_list *before;
} linked_list;

struct hashTable {
    linked_list *buckets[MAX_HASH_TABLE_LENGTH];
};


struct hashTable myHashTable;
stream_entry* stream_array[MAX_STREAM_KEYS];


char* parse_redis_protocol(char* command);
	char **parse_command_args(int n_args, char* command, int * index, int c_type);
	char *parse_response(char **args, int command_type); 


// Hash table with linked-list interface for set and get commands
void init_Table();
void insert_new(char* key, char* value, char exp, int m_sec);
linked_list* lookup_table(char*key);
char remove_node(char* key);
unsigned long hash(const char *str);

// Radix tree interface for streams
void init_stream_table();
radix_node* create_new_stream_radix_node(char* id_key, linked_str_list* ch_pntr, char is_leaf, listpack k_v_data);
int lookup_up_stream_one_node(char* stream_entry_key, char* id_key);
void insert_new_stream(char* stream_key, char* id_key, listpack k_v_data);


int main() {
	// Disable output buffering
	setbuf(stdout, NULL);
	setbuf(stderr, NULL);
	
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	printf("Logs from your program will appear here!\n");

	// Uncomment this block to pass the first stage
	int server_fd, max_sd, sd;
	int client_array[MAX_CLIENTS];
	int valread;
	struct sockaddr_in client_addr;
	socklen_t client_addr_len;
	fd_set fd_setmask;


	server_fd = socket(AF_INET, SOCK_STREAM, 0);
	if (server_fd == -1) {
		printf("Socket creation failed: %s...\n", strerror(errno));
		return 1;
	}
	
	// Since the tester restarts your program quite often, setting SO_REUSEADDR
	// ensures that we don't run into 'Address already in use' errors
	int reuse = 1;
	if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
		printf("SO_REUSEADDR failed: %s \n", strerror(errno));
		return 1;
	}

	for(int i = 0; i < MAX_CLIENTS; i++) {
		client_array[i] = 0;
	}

	struct sockaddr_in serv_addr = { .sin_family = AF_INET ,
									 .sin_port = htons(6379),
									 .sin_addr = { htonl(INADDR_ANY) },
									};
	
	if (bind(server_fd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) != 0) {
		printf("Bind failed: %s \n", strerror(errno));
		return 1;
	}
	
	int connection_backlog = 5;
	if (listen(server_fd, connection_backlog) != 0) {
		printf("Listen failed: %s \n", strerror(errno));
		return 1;
	}
	
	init_Table();
	init_stream_table();
	printf("Waiting for a client to connect...\n");
	
	client_addr_len = sizeof(client_addr);

	while(1) {
		FD_ZERO(&fd_setmask);
		FD_SET(server_fd, &fd_setmask);

		max_sd = server_fd;

		for(int i = 0; i < MAX_CLIENTS; i++) {
			sd = client_array[i];
			if(sd > 0 ) FD_SET(sd, &fd_setmask);
			if(sd > max_sd) max_sd = sd;
		}

		select(max_sd+1, &fd_setmask, NULL, NULL, NULL);

		if(FD_ISSET(server_fd, &fd_setmask)) {
			int client_socket = accept(server_fd, (struct sockaddr *) &client_addr, &client_addr_len);
			if(client_socket < 0) {
				printf("Accept failed: %s \n", strerror(errno));
				close(server_fd);
				return 1;
			}
			for(int h = 0; h < MAX_CLIENTS; h++) {
				if(client_array[h] == 0) {
					client_array[h] = client_socket;
					break;
				}
			}
			printf("Client connected\n");
		}

		for(int i = 0; i < MAX_CLIENTS; i++) {
			sd = client_array[i];

			char buffer[4096];
			memset(&buffer, 0, sizeof(buffer));
			if(FD_ISSET(sd, &fd_setmask)) {
				if((valread = read(sd, buffer, sizeof(buffer))) == 0) {
					getpeername(sd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);
                    printf("Client disconnected: ip %s, port %d\n",
                           inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

                    close(sd);
                    client_array[i] = 0;
                } 
				else if(valread > 0) {
                    // Process the received data
					char *response = parse_redis_protocol(buffer);
					if(strcmp(response, "") == 0) {
						send(sd, "$-1\r\n", 5, 0);
						continue;
					}
					send(sd, response, strlen(response), 0);
					free(response);
                }
				 else {
					char response[7] = "+PONG\r\n";
                    send(sd, &response, sizeof(response), 0); // Echo message back to client
				}
			}
		}
	}
	close(server_fd);

	return 0;
}

char* parse_redis_protocol(char* command) {
    if (command[0] != '*') {
        printf("**ERROR: Invalid start of command\n");
        return "";
    }

    int number_of_elements = 0;
    int command_type = 0;
    char *sliced_string_holder;
    int index = 1;

    // #----- Get the number of commands ------# //
    while (command[index] != '\r' && command[index + 1] != '\n') {
        if (index == 1000) return "";
        if (!isdigit(command[index])) {
            printf("**ERROR: Expected digit for number of elements\n");
            return "";
        }
        index++;
    }

    sliced_string_holder = (char*) malloc(index * sizeof(char));
    strncpy(sliced_string_holder, &command[1], index - 1);
    sliced_string_holder[index - 1] = '\0';

    number_of_elements = atoi(sliced_string_holder);
    free(sliced_string_holder);
	// ------ ########################## ------- //

    index += 2;

	// #----- Get the command type ------------# //
	// Parse the length of the next element ($length\r\n)
	if (command[index] != '$') {
		printf("**ERROR: Expected '$' for bulk string length\n");
		return "";
	}
	index++;

	int length = 0;
	while (command[index] != '\r' && command[index + 1] != '\n') {
		if (!isdigit(command[index])) {
			printf("**ERROR: Expected digit for string length\n");
			return "";
		}
		length = length * 10 + (command[index] - '0');
		index++;
	}
	
	index += 2;  // Skip \r\n

	// Now we have the length, extract the actual command
	sliced_string_holder = (char*) malloc(length + 1);
	strncpy(sliced_string_holder, &command[index], length);
	sliced_string_holder[length] = '\0';
	
	index += length + 2;  // Skip the string and the following \r\n

	// Check if it's the command or the argument
	 // First element should be the command
	for(int g = 0; g < length; g++) 
		sliced_string_holder[g] = tolower(sliced_string_holder[g]);

	if (strcmp(sliced_string_holder, "echo") == 0) {
		command_type = 1;
		if(number_of_elements != 2) {
			printf("ERROR: ECHO accepts only one argument\n");
			return "";
		}
	} else if(strcmp(sliced_string_holder, "set") == 0) {
		if(number_of_elements != 3 && number_of_elements != 5) {
			printf("ERROR: Not proper SET command\n");
			return "";
		}
		command_type = 2;
		if(number_of_elements == 5) command_type = 4;
	} else if(strcmp(sliced_string_holder, "get") == 0) {
		command_type = 3;
		if(number_of_elements != 2) {
			printf("ERROR: GET accepts only one argument\n");
			return "";
		}
	} else if(strcmp(sliced_string_holder, "type") == 0) {
		if(number_of_elements != 2) {
			printf("ERROR: GET accepts only one argument\n");
			return "";
		}
		command_type = 5;
	} else if (strcmp(sliced_string_holder, "xadd") == 0) {
		if(number_of_elements != 5) {
			printf("ERROR: XADD should have at least 5 arguments\n");
			return "";
		}
		command_type = 6;
	} else if(strcmp(sliced_string_holder, "ping") == 0) {
		char* response = (char*) malloc(7 * sizeof(char));
		sprintf(response, "+PONG\r\n");
		free(sliced_string_holder);
		return  response;
	} else {
		printf("The command: %s, and strlen(%ld\n", sliced_string_holder, strlen(sliced_string_holder));
		printf("**ERROR: Unsupported command\n");
		free(sliced_string_holder);
		return "";
	}
	
	// #----- ################### ------------# //
	free(sliced_string_holder);

	index++;
	char **args = parse_command_args(number_of_elements-1, command, &index, command_type);
	if(args == NULL) {
		return "";
	}

	char *response = parse_response(args, command_type); 
	return response;

}

// starts from the beginning of the arguments
char ** parse_command_args(int n_args, char* command, int * index, int c_type) {

	int length = 0;
	char** arguments = malloc(n_args * sizeof(char*));

	for(int i = 0; i < n_args; i++) {
		while (command[*index] != '\r' && command[*index + 1] != '\n') {
			if (!isdigit(command[*index])) {
				printf("**ERROR: Expected digit for string length,%c\n", command[*index]);
				return NULL;
			}
			length = length * 10 + (command[*index] - '0');
			*index += 1;
		}
		*index += 2;
		char *arg = malloc(length+1 * sizeof(char));
		arg[length] = '\0';
		strncpy(arg, &command[*index], length);

		arguments[i] = arg;

		*index += length + 3; // len + 2 for \r\n and 1 for $
		length = 0;
	}	

	// Added this 
	if(n_args == 4 && c_type == 5) {
		for(int i = 0; i < 2; i++) 
			arguments[2][i] = tolower(arguments[2][i]);
		int len = (int) strlen(arguments[3]);
		for(int i = 0; i < len; i++) {
			if(isdigit(arguments[3][i]) == 0) {
				printf("ERROR: Fourth argument of SET should be an arguemnt to 'px' which should be a number\n");
				for(int i = 0; i < n_args; i++) 
					free(arguments[i]);
				free(arguments);
				return NULL;
			}
		}
		if(strcmp(arguments[2], "px") != 0) {
			printf("ERROR: Third argument of SET should be 'px'\n");
			for(int i = 0; i < n_args; i++) 
				free(arguments[i]);
			free(arguments);
			return NULL;
		}
	}
    
    return arguments;
}

// get argements, return response for the cleint
char *parse_response(char **args, int c_type) {

	char* response = NULL;
	char special = 0;
	switch (c_type)	{
		case 1:;
			// Calculate the length of the argument
			int arg_len = strlen(args[0]);
			// Allocate enough space for the response ($length\r\nstring\r\n)
			response = (char*) malloc((arg_len + 6 + 10) * sizeof(char));  // Max 10 digits for length + 6 for formatting
			sprintf(response, "$%d\r\n%s\r\n", arg_len, args[0]);

			free(args[0]);
			free(args);
			return response;
			break;
		
		case 4:;
			int mesec = atoi(args[3]);
			free(args[3]);
			insert_new(args[0], args[1], 1, mesec);
			free(args[2]);
			special = 1;
		case 2:
			if(special == 0) 
				insert_new(args[0], args[1], 0, 0);
			response = (char*) malloc(5 * sizeof(char));
			sprintf(response, "+OK\r\n");
			free(args);
			return response;
			break;

		case 3:;
			linked_list* node = lookup_table(args[0]);
			response = (char*) malloc(5 * sizeof(char));
			if(node == NULL) sprintf(response, "$-1\r\n");
			else sprintf(response, "$%ld\r\n%s\r\n", strlen(node->value), node->value);
			
			free(args[0]);
			free(args);
			return response;
			break;
		case 5:;
			linked_list* look_node = lookup_table(args[0]);
			if(look_node != NULL) {
				response = malloc(9 * sizeof(char));
				sprintf(response, "+string\r\n");
			} else if(lookup_up_stream_one_node(args[0], NULL) == 1) {
				response = malloc(7 * sizeof(char));
				sprintf(response, "+stream\r\n");
			}
			 else {
				response = malloc(7 * sizeof(char));
				sprintf(response, "+none\r\n");
			}
			free(args[0]);
			free(args);
			return response;
			break;
		case 6:;
			// $ redis-cli XADD stream_key 0-1 foo bar
			insert_new_stream(args[0], args[1], args);

			int len = strlen(args[1]);
			response = malloc(len+strlen(args[1])+5);
			sprintf(response, "$%d\r\n%s\r\n",len, args[1]);
			return response;
		default:
			break;
	}
	return "";
}

struct thread_argements {
	int m_sec;
	char *key;
};

void* delete_data(void *args) {
	struct thread_argements *myargs = (struct thread_argements*) args;
	struct timespec tc;
	int res;

	tc.tv_sec = myargs->m_sec / 1000;
	tc.tv_nsec = (myargs->m_sec % 1000) * 1000000;
	nanosleep(&tc, &tc);

	remove_node(myargs->key);
	free(args);
}
// HASH table data strucutre commands
void init_Table() {
    for(int i = 0; i < MAX_HASH_TABLE_LENGTH / 2; i+= 2) {
        myHashTable.buckets[i] = NULL;
		myHashTable.buckets[i+1] = NULL;
	}
}

void insert_new(char* key, char* value, char exp, int m_sec) {

	if(m_sec <= 0 && exp == 1) {
		free(key);
		free(value);
		return;
	};

    linked_list *new_list = malloc(sizeof(linked_list));
	if(new_list == NULL) {
		printf("ERROR allocating memory!\n");
		free(key);
		free(value);
		return;
	}
    new_list->key = key;
    new_list->value = value;
	
	new_list->expires = exp == 1 ? 1 : 0;
	new_list->milli_second = exp == 1 ? m_sec : 0;
	
    int index = hash(key);

    if(myHashTable.buckets[index] == NULL) {
        myHashTable.buckets[index] = new_list;
        new_list->next = NULL;
        new_list->before = NULL;
    } else {
        linked_list* old_node = myHashTable.buckets[index];
        new_list->before = NULL;
        old_node->before = new_list;
        new_list->next = old_node;
        myHashTable.buckets[index] = new_list;
    }

	if(exp == 1) {
		struct thread_argements* pass_args = malloc(sizeof(struct thread_argements));
		pass_args->key = key;
		pass_args->m_sec = m_sec;
		pthread_t thr;
		int ret = pthread_create(&thr, NULL, delete_data, (void *) pass_args);
		if(ret != 0) {
			free(key);
			free(value);
			return;
		}
		pthread_detach(thr);
	}
}

linked_list* lookup_table(char* key) {
    int index = hash(key);

    if(myHashTable.buckets[index] == NULL) return NULL;
    
    linked_list* node = myHashTable.buckets[index];
    while(node != NULL) {
        if(strcmp((node->key), key) == 0) {
            return node;
        } 
        node = node->next;
    }
    return NULL;
}

char remove_node(char* key) {
    linked_list* node = lookup_table(key);

    if(node == NULL) 
        return 1;

    // Handle the case where node is the first in the bucket
    if (node->before == NULL) {
        // Update the bucket to point to the next node
        int index = hash(key);
        myHashTable.buckets[index] = node->next;
    } else {
        // Link the previous node to the next node
        node->before->next = node->next;
    }

    // If the node is not the last, update the next node's before pointer
    if (node->next != NULL) {
        node->next->before = node->before;
    }

	free(node->key);
	free(node->value);
    free(node);
    return 0;
}

unsigned long hash(const char *str) {
    unsigned long hash = 5381;
    int c;

    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c;  // hash * 33 + c
    }

    return hash % MAX_HASH_TABLE_LENGTH;
}


void init_stream_table() {
    for(int i = 0; i < MAX_STREAM_KEYS; i++) 
        stream_array[i] = NULL;
}


radix_node* create_new_stream_radix_node(char* id_key, linked_str_list* ch_pntr, char is_leaf, listpack k_v_data) {
    radix_node* new_radix = malloc(sizeof(radix_node));

    new_radix->children = ch_pntr;
    new_radix->is_leaf = is_leaf;
    new_radix->value = k_v_data;
    new_radix->id_key = id_key;
    printf("This is a new node: %s", id_key);
    return new_radix;
}

int lookup_up_stream_one_node(char* stream_entry_key, char* id_key) {
    for(int i = 0; i < MAX_STREAM_KEYS; i++) {
		if(stream_array[i] == NULL) continue;
		

        if(strcmp(stream_array[i]->stream_key, stream_entry_key) == 0) {
			return 1;
            // linked_str_list* root_ent = stream_array[i]->stream_data;
            // while(root_ent->data_pointer != NULL) {
            //     radix_node* node = root_ent->data_pointer;
            //     // int index = 0;
            //     while(node != NULL) {
            //         if(node->is_leaf == 1) {
            //             printf("The node value is: %s with key: %s\n",node->value[1], node->id_key);
            //             if(strcmp(node->id_key, id_key) == 0) {
            //                 return 1;
            //             }
            //         }
            //     }
            //     root_ent = root_ent->next;
            // }
        }
    }
	printf("Couldn't findn\n");
    return 0;
}

void insert_new_stream(char* stream_key, char* id_key, listpack k_v_data) {
    // lookup for the stream key if it already exists, do their own logic

    // new stream key
    for(int i = 0; i < MAX_STREAM_KEYS; i++) {
        if(stream_array[i] == NULL) {
            // create the start data
            radix_node* new_data = create_new_stream_radix_node(id_key, NULL, 1, k_v_data);
            // link it with the root node
            linked_str_list* root_node = malloc(sizeof(linked_str_list));
            root_node->data_pointer = new_data;
            root_node->next = NULL;
            // link the root node with the stream entry;
            stream_entry* new_stream_entry = malloc(sizeof(stream_entry));
            new_stream_entry->stream_key = stream_key;
            new_stream_entry->stream_data = root_node;
            // root_node->data_pointer
            stream_array[i] = new_stream_entry;
			printf("Setting entry: %s", stream_array[i]->stream_key);
			break;
        }
    }
}

