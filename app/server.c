#ifndef SERVER_HEADERS
#define SERVER_HEADERS
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
#include <math.h>
#endif

#include "hash.h"
#include "redis-streams.h"


#define MAX_COMMAND_LENGTH 4
#define MAX_CLIENTS 32


extern stream_entry* stream_array[MAX_STREAM_KEYS];

extern struct hashTable myHashTable;
// extern char auto_seqence;
extern char *error_message;
extern char error_flag;
extern char name_stripped;
listpack key_value_data;


char* parse_redis_protocol(char* command);
	char **parse_command_args(int n_args, char* command, int * index, int c_type);
	char *parse_response(char **args, int c_type, int n_args); 



void slice(const char* str, char* result, size_t start, size_t end);

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
			printf("Client 	\n");
		}

		for(int i = 0; i < MAX_CLIENTS; i++) {
			sd = client_array[i];

			char buffer[4096];
			memset(&buffer, 0, sizeof(buffer));
			if(FD_ISSET(sd, &fd_setmask)) {
				if((valread = read(sd, buffer, sizeof(buffer))) == 0) {
					getpeername(sd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);
                    printf("Client disconnected: ip %s, port %d\n\n",
                           inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));

                    close(sd);
                    client_array[i] = 0;
                } 
				else if(valread > 0) {
                    // Process the received data
					char *response = parse_redis_protocol(buffer);
					if(error_flag == 1) {
						error_flag = 0;
						send(sd, error_message, strlen(error_message), 0);
						continue;
					}
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
        set_error("-ERR: Invalid start of command\r\n");
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
            set_error("-ERR: Expected digit for number of elements\r\n");
            return "";
        }
        index++;
    }

    sliced_string_holder = (char*) malloc(index * sizeof(char));
    strncpy(sliced_string_holder, &command[1], index - 1);
    sliced_string_holder[index - 1] = '\0';

    number_of_elements = atoi(sliced_string_holder);
    free(sliced_string_holder);

    index += 2;

	if (command[index] != '$') {
		set_error("-ERR: Expected '$' for bulk string length\r\n");
		return "";
	}
	index++;

	int length = 0;
	while (command[index] != '\r' && command[index + 1] != '\n') {
		if (!isdigit(command[index])) {
			set_error("-ERR: Expected digit for string length\r\n");
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

	for(int g = 0; g < length; g++) 
		sliced_string_holder[g] = tolower(sliced_string_holder[g]);

	if (strcmp(sliced_string_holder, "echo") == 0) {
		command_type = 1;
		if(number_of_elements != 2) {
			set_error("-ERR: ECHO accepts only one argument\r\n");
			free(sliced_string_holder);
			return "";
		}
	} else if(strcmp(sliced_string_holder, "set") == 0) {
		if(number_of_elements != 3 && number_of_elements != 5) {
			set_error("-ERR: Not proper SET command\r\n");
			free(sliced_string_holder);
			return "";
		}
		command_type = 2;
		if(number_of_elements == 5) command_type = 4;
	} else if(strcmp(sliced_string_holder, "get") == 0) {
		if(number_of_elements != 2) {
			set_error("-ERR: GET accepts only one argument\r\n");
			free(sliced_string_holder);
			return "";
		}
		command_type = 3;
	} else if(strcmp(sliced_string_holder, "type") == 0) {
		if(number_of_elements != 2) {
			set_error("-ERR: GET accepts only one argument\r\n");
			free(sliced_string_holder);
			return "";
		}
		command_type = 5;
	} else if (strcmp(sliced_string_holder, "xadd") == 0) {
		if(number_of_elements < 5) {
			set_error("-ERR: XADD should have at least 5 arguments\r\n");
			free(sliced_string_holder);
			return "";
		}
		command_type = 6;
	} else if(strcmp(sliced_string_holder, "xrange") == 0) {
		if(number_of_elements != 2 && number_of_elements != 4) {
			set_error("-ERR: XRANGE acceepts a stream key, and an optional range values of [MIN MAX]\r\n");
			free(sliced_string_holder);
			return "";
		}
		command_type = 7;
	} else if(strcmp(sliced_string_holder, "xread") == 0) {
		if(number_of_elements < 3) {
			set_error("-ERR: XREAD should have at least 3 arguments\r\n");
            free(sliced_string_holder);
            return "";
		}
		command_type = 8;
	} else if(strcmp(sliced_string_holder, "ping") == 0) {
		char* response = (char*) malloc(7 * sizeof(char));
		sprintf(response, "+PONG\r\n");
		free(sliced_string_holder);
		return  response;
	} else {
		set_error("-ERR: Unsupported command\r\n");
		free(sliced_string_holder);
		return "";
	}
	
	free(sliced_string_holder);

	index++;
	char **args = parse_command_args(number_of_elements-1, command, &index, command_type);
	if(args == NULL) {
		return "";
	}

	char *response = parse_response(args, command_type, number_of_elements-1); 
	return response;
}

// starts from the beginning of the arguments
char ** parse_command_args(int n_args, char* command, int * index, int c_type) {

	int length = 0;
	listpack arguments;
	
	if(c_type == 6) {
		if((n_args-2) % 2 != 0) {
			set_error("-ERR XADD command requires a stream key, id, and multiple of 2 K-V pairs\r\n");
			return NULL;
		}
		arguments = malloc((n_args - (n_args-2)) * sizeof(char*));
		key_value_data = malloc((n_args-1) * sizeof(char*));
	} else arguments = malloc(n_args * sizeof(char*));
	
	int index_k_v = 0;
	for(int i = 0; i < n_args; i++) {
		while (command[*index] != '\r' && command[*index + 1] != '\n') {
			if (!isdigit(command[*index])) {
				set_error("-ERR Expected digit for string length\r\n");
				return NULL;
			}
			length = length * 10 + (command[*index] - '0');
			*index += 1;
		}
		
		*index += 2;

		if(i == 0 && c_type == 8) {
			*index += length + 3;
			length = 0;
			continue;
		} 
		
		char *arg = malloc(length+1 * sizeof(char));
		arg[length] = '\0';
		strncpy(arg, &command[*index], length);
		if(i > 1 && c_type == 6) {
			key_value_data[index_k_v] = arg;
			index_k_v++;
		} else arguments[c_type == 8 ? i-1 : i] = arg;

		*index += length + 3;
		length = 0;
	}	

	if(c_type == 6) key_value_data[index_k_v] = NULL;

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
char *parse_response(char **args, int c_type, int n_args) {
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
			} else if(lookup_up_stream(args[0]) != NULL) {
				response = malloc(7 * sizeof(char));
				sprintf(response, "+stream\r\n");
			} else {
				response = malloc(7 * sizeof(char));
				sprintf(response, "+none\r\n");
			}
			free(args[0]);
			free(args);
			return response;
			break;
		case 6:;
			// $ redis-cli XADD stream_key 0-1 foo bar
			char* id = insert_to_radix_tree(args, key_value_data, 0, 0);
			if(id == NULL) {
				free(args[0]);
				for(int i = 0; key_value_data[i] != NULL; i++)
					free(key_value_data[i]);
				return "";
			}
			
			int len = strlen(id);
			response = malloc(len+strlen(id)+5);
			sprintf(response, "$%d\r\n%s\r\n",len, id);
			return response;
		case 7:;
			char* id_arr = insert_to_radix_tree(args, NULL, 1, n_args);
			if(id_arr == NULL) {
                free(args[0]);
                return "";
            }
			return id_arr;

		case 8:;
			// check			
			if((n_args-1) % 2 != 0) return "";	
			
			char* id_arrs = insert_to_radix_tree(args, NULL, 2, n_args);
			if(id_arrs == NULL) { 
				free(args[0]);
                return "";
            }
			return id_arrs;
		default:
			break;
	}
	return "";
}


void slice(const char* str, char* result, size_t start, size_t end) {
    strncpy(result, str + start, end - start);
}
