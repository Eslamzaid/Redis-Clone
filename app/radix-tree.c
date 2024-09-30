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
#include "radix-tree.h"


stream_entry* stream_array[MAX_STREAM_KEYS];
listpack key_value_data = NULL;

int lookup_node_entry_index;
char auto_seqence = 0;
char *error_message;
char error_flag = 0;
char name_stripped = 0;


void init_stream_table() {
    for(int i = 0; i < MAX_STREAM_KEYS; i++) 
        stream_array[i] = NULL;
}


radix_node* create_new_stream_radix_node(char* id_key, linked_str_list* ch_pntr, char is_leaf, listpack k_v_data) {
    radix_node* new_radix = malloc(sizeof(radix_node));
	if(new_radix == NULL) {
		printf("Couln't initilize data\n");
		exit(1);
	}
    new_radix->next_list = ch_pntr;
    new_radix->is_leaf = is_leaf;
    new_radix->value = k_v_data;
    new_radix->id_key = id_key;
    return new_radix;
}


// need refactoring
linked_str_list* lookup_up_stream(char* stream_entry_key) {
    for(int i = 0; i < MAX_STREAM_KEYS; i++) {
		if(stream_array[i] == NULL) continue;
		
        if(strcmp(stream_array[i]->stream_key, stream_entry_key) == 0) {
			lookup_node_entry_index = i;
            linked_str_list* root_node = stream_array[i]->root_header;
			return root_node;
		}
    }
    return NULL;
}

// What I want to do?:
/**
 * I want to traverse the tree to find the node with the nearest key-id, if not find, create a new one
 * 
 * FIRST-LEVEL: linked-str_list
 * SECOND-LEVEL & Third-Level: radix_node
 * 
 * Example: 
 * 
 *    FIRST-LEVEL	   		      (Root-list)
 * 							 (1)/ 			 (2)\
 * 							   V				 V
 * 	  Second-Level   	  (3)(radix_node)		(NULL)
 * 						 ID: 12345-0
 * 						 NEXT: |
 * 							   V
 * 	  Third-Level   		 (NULL)
 * 
 * 1) Loop through the FIRST-LEVEL to the radix-nodes it have (1) (2)...., if non return NULL
 * 2) We will go to (3) check the id for it, if it starts with the same or is the same as the key, we will continue
 * 		2.1) if the current node (3) is a leaf, we will stop and return this;
 * 		2.2) Else we will loop through the next_list it have which in type of a linked list, until we find the last one
 */

radix_node* lookup_up_node(linked_str_list* starter_list, char* id_key, char depth_insert) {
	// radix_node* deepest_node = NULL;
    if(starter_list == NULL) return NULL;
	if(starter_list->node_pointer == NULL) return NULL;
	// FIrst without seq, next with seq
	
	// ? IF a key is provided
	int index = 0;
	linked_str_list* current_list = starter_list; 
	while(current_list != NULL) { 
		radix_node* node = current_list->node_pointer;
		printf("The node: %s", node->id_key);
		if(node->id_key[0] != id_key[index]) {	
			current_list = current_list->next;
			printf("\nlook_up_node: NOT FOND? node: %s, key: %s, depth: %d\n", node->id_key, id_key, depth_insert);
			continue;
		}
		
		int len = strlen(node->id_key);

		printf("CHECKING: %s\n", node->id_key);	
		//! THE PROBLEM IS DOWN
		// It is a leaf node
		if(node->is_leaf == 1) {
			printf("FOUNDDDDDDDDDDDDDDDDDDdd LEAF: %s, and %d\n", node->id_key, node->is_leaf);
			return node;
		};

		for(int i = 0; i < len && id_key[index] != '\0'; i++) {	
			if(node->id_key[i] != id_key[index]) 
				break;
			index++;
		}

		// if(depth_insert == 1) {
		// 	if(index == (int) strlen(node->id_key)) return node;
		// 	printf("node id, :%s, and the id: %s, with index: %d,\n", node->id_key, id_key, index);
		// }
		
		// if(depth_insert == 1) return node;
		linked_str_list* child_list = node->next_list;
		while(child_list != NULL) {
			if(child_list->node_pointer->is_leaf == 1) {
				while(child_list != NULL) {
					if(child_list->next == NULL) break;
					child_list = child_list->next;
				}
				return child_list->node_pointer;
			}
			else if(child_list->node_pointer->id_key[0] != id_key[index]) { // !INDEX index-1
				printf("***********z********EVEN ENTERED HERE, %s, and %s and index: %d\n", child_list->node_pointer->id_key, id_key, index);
				child_list = child_list->next;
				continue;
			}
			printf("the values: %s, %s, %d", child_list->node_pointer->id_key, id_key, index);

			int child_len = strlen(child_list->node_pointer->id_key);
			for(int i = 0; i < child_len && id_key[index] != '\0'; i++) {
				if(child_list->node_pointer->id_key[i] != id_key[index])
					break;
				index++;
			}
			child_list = child_list->next;
		}
		
		current_list = current_list->next;
		index=0;
	}
	printf("\nlookup_up_node: FOUND NOTHING\n");
	return NULL;
}


int insert_new_stream(char* stream_key, char* id_key, listpack k_v_data) {
	for(int i = 0; i < MAX_STREAM_KEYS; i++) {
		if(stream_array[i] == NULL) {
			// create the start data	
			unsigned long ms_num;
			int seq_num;
			if(convert_to_int(id_key, &ms_num, &seq_num, NULL) == NULL) 
				return 1;

			if(ms_num == 0 && seq_num < 1) {
				error_message = "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
				return 1;
			}

			radix_node* new_starter = create_new_stream_radix_node(id_key, NULL, 1, k_v_data);
			// link it with the root node
			linked_str_list* root_list = malloc(sizeof(linked_str_list));
			root_list->node_pointer = new_starter;
			root_list->next = NULL;
			// link the root node with the stream entry;
			stream_entry* new_stream_entry = malloc(sizeof(stream_entry));
			new_stream_entry->stream_key = stream_key;
			new_stream_entry->root_header = root_list;
			// root_list->node_pointer
			stream_array[i] = new_stream_entry;
			return 0;
		}
	}
	return 1;
}

/**
 * if(auto_seqence == 1) {
	auto_seqence = 0;
	free(id_key);
	
	int new_len;
	if(ms_num == 0) new_len = 2;
	else new_len = (int)((ceil(log10(ms_num))+1)*sizeof(char));
	if(seq_num == 0) new_len += 1;
	else new_len += (int)((ceil(log10(seq_num))+1)*sizeof(char));

	id_key = malloc(new_len*sizeof(char));
	if(id_key == NULL) {
		printf("ERRORRER MALLOC\n");
		exit(1);
	}
	id_key[new_len] = '\0';
	sprintf(id_key, "%ld-%d", ms_num, seq_num);
	new_starter = create_new_stream_radix_node(id_key, NULL, 1, k_v_data);
}
 */

int find_prefix_length(char* id1, char* id2) {
    int len = 0;
    while (id1[len] != '\0' && id2[len] != '\0' && id1[len] == id2[len]) {
        len++;
    }
    return len;
}

// before writing code, what I want to do, in this function
/**
 * 1: I want to insert a new stream, to an already existing stream, but with checking
 *    and stripping 
 * 
 * A stream is a radix_node, with data on it, assoistated with a stream key, and will get in
 * funnels of starters, due to radix tree nature of storing releavnt data together, thus saving memory
 * In order to check for a stream, I need to check every stream is nodes, if we found the start of the stream equal, then we will go there and 
 * search it's next_list, until we found the latest child with same id_key, in this case return that child.
 * 
 * TODO: crate an interface function called insert_to_radix_tree, and pass args (DONE)
 * TODO: create a search function that returns the latest child, or NULL
 * TODO: create the inserting function.
 */

int insert_new_to_stream(linked_str_list* starter_list, char* id_key, unsigned long ms_new, int seq_new, listpack k_v_data) {

	//^ ASSUME it returns the thing we want
	radix_node* deepest_node = lookup_up_node(starter_list, id_key, 1);
	
	// if deepest_node == NULL, (when this happens?): when for example the first letter in the path we are in is not the same as the id_key
	if(deepest_node == NULL) { 
		radix_node* new_node = create_new_stream_radix_node(id_key, NULL, 1, k_v_data);  
		linked_str_list* new_list = malloc(sizeof(linked_str_list));
		new_list->node_pointer = new_node;
		new_list->next = NULL;
		while(starter_list != NULL) {
			if(starter_list->next == NULL) {
				starter_list->next = new_list;
				break;
			}
			starter_list = starter_list->next;
		}
		return 0;
	}
	
	unsigned long node_ms;
	int node_sequence;
	if(convert_to_int(deepest_node->id_key, &node_ms, &node_sequence, NULL) == NULL) return 1; 
	
	printf("\tSTOP: %s with %ld and %d\n", deepest_node->id_key, node_ms, node_sequence);
	printf("\t\tCOMPARED WITH: %s, %ld, %d\n",id_key, ms_new, seq_new);

	int prefix_len = find_prefix_length(deepest_node->id_key, id_key);

	//* CHECKING the ID is valid
	if (prefix_len == (int) strlen(id_key)) {
		set_error("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
		return 1;
	}
	if (node_ms == ms_new && node_sequence >= seq_new) {
		set_error("-ERR THE ID IS lesssssss\r\n");
		return 1;
	}
	if (node_ms > ms_new ) {
		printf("###### %s and the insert: %s\n", deepest_node->id_key, id_key);
		set_error("-ERR The ID is smaller\r\n");
		return 1;
	}

	if(prefix_len > 0) {
		// ! now we got the deepest node, add to it.
		printf("THE DEEPEST NDOE: %s\n", deepest_node->id_key);

		char* new_old_name_parent = malloc((prefix_len) * sizeof(char));
		slice(deepest_node->id_key, new_old_name_parent, 0, prefix_len);
		printf("THE len: %ld, and prefix: %d\n", strlen(deepest_node->id_key), prefix_len);
		char* new_old_name_child = malloc((strlen(deepest_node->id_key)-prefix_len) * sizeof(char));
		slice(deepest_node->id_key, new_old_name_child, prefix_len, strlen(deepest_node->id_key));
		printf("OKAY WHAT ABOUT THE PARENT: %s, and the new name of the parent: %s\n", new_old_name_parent, new_old_name_child);

		char* new_new_id = malloc((strlen(id_key) - prefix_len) * sizeof(char));
		slice(id_key, new_new_id, prefix_len, strlen(id_key));

		printf("\t THE node new name: %s, and teh new key name: %s, what type: %d\n", new_old_name_child, new_new_id, deepest_node->is_leaf);
		
		// Create the new node

		radix_node* new_node = create_new_stream_radix_node(new_new_id, NULL, 1, k_v_data);
		linked_str_list* new_list = malloc(sizeof(linked_str_list));
		new_list->node_pointer = new_node;
		new_list->next = NULL;

		radix_node* old_node = create_new_stream_radix_node(new_old_name_child, NULL, 1, deepest_node->value);
		linked_str_list* old_node_list = malloc(sizeof(linked_str_list));
		old_node_list->node_pointer = old_node;
		old_node_list->next = new_list;

		if(deepest_node->is_leaf == 1) {
			deepest_node->is_leaf = 0;
			// deepest_node->next_list = new_list;
			deepest_node->next_list = old_node_list;
			free(deepest_node->id_key);
			deepest_node->id_key = new_old_name_parent;
		} 
		else {
			linked_str_list* child = deepest_node->next_list;
			while(child != NULL) {
				if(child->next == NULL) {
					child->next = old_node_list;
					break;
				}
				child = child->next;
			}
		}
		
		name_stripped = 1;
	}

	return 0;
}

// ^ INTERFACE
char* insert_to_radix_tree(listpack args_meta, listpack k_v_d) {
	unsigned long ms_new = 0;
	int seq_new = 0;
	linked_str_list* starter_list = lookup_up_stream(args_meta[0]);
	// Handle the id auto generate
	if(args_meta[1][0] == '*') {
		char* new_id = convert_to_int(NULL, &ms_new, &seq_new, starter_list);
		printf("**************, every id: %s\n", new_id);
		if(starter_list == NULL) {
			if(insert_new_stream(args_meta[0], new_id, k_v_d) == 1) return NULL;
		} else if(insert_new_to_stream(starter_list, new_id, ms_new, seq_new, k_v_d) == 1) {
			free(new_id);
			return NULL;
		}

		free(args_meta[1]);
		args_meta[1] = new_id;
		return args_meta[1];
	}

	return args_meta[1];
}

char* convert_to_int(char* id, unsigned long* mille_seconds, int* seq_number, linked_str_list* starter_node) {
	if(id == NULL) {
		unsigned long ms = get_current_time();
		*mille_seconds = ms;
		int new_len;

		if(ms <= 9) new_len = 2;
		else new_len = (int)((ceil(log10(ms))+1)*sizeof(char));
		char* ms_string = malloc((new_len+1) * sizeof(char));
		ms_string[new_len] = '\0';
		sprintf(ms_string, "%ld-", ms);

		radix_node* same_key_node = lookup_up_node(starter_node, ms_string, 0); 
		//~ only for leaf ^
		
		if(same_key_node == NULL) {
			*seq_number = ms == 0 ? 1 : 0;
		} else {
			int same_seq;
			if(convert_to_int(same_key_node->id_key, NULL, &same_seq, NULL) == NULL) {
				printf("ERRORR OCCURED\n");
				return NULL;
			}
			*seq_number = same_seq + 1;
			printf("\n Convert_to_int: key: %s, seq: %d\n", same_key_node->id_key, *seq_number);
		}
		free(ms_string);

		int seq_len;
		if(*seq_number <= 9) seq_len = 1;
		else seq_len = (int)((ceil(log10(*seq_number)))*sizeof(char));

		ms_string = malloc((new_len+seq_len) * sizeof(char));
		ms_string[new_len+1] = '\0';
		sprintf(ms_string, "%ld-%d", ms, *seq_number);

		printf("Convert_to_int: The new string: %s\n", ms_string);
		return ms_string;
	}
	
	int len = strlen(id);

	for(int i = 0; i < len; i++) {
		if(id[i] == '-') {
			if(mille_seconds != NULL) {
				char* m_seconds = malloc((i+1) * sizeof(char));
				m_seconds[i+1] = '\0';
				slice(id, m_seconds, 0, i);
				*mille_seconds = atoi(m_seconds);
				free(m_seconds);
			} 
			
			int seq_ind = ((len-(i+1))+1);
			char* seq_num = malloc(seq_ind * sizeof(char));
			if(seq_num == NULL) {
				printf("ERORR MALLOC \n");
				exit(EXIT_FAILURE);
			}
			seq_num[seq_ind-1] = '\0';
			slice(id, seq_num, i, i+seq_ind);
			*seq_number = atoi(seq_num);
			free(seq_num);			
			
			if(mille_seconds != NULL) 
				if(*mille_seconds == 0 && *seq_number < 1) {
					set_error("-ERR The ID specified in XADD must be greater than 0-0\r\n");
					return NULL;
				}	
			
			return "";
		} 
		if(isdigit(id[i]) == 0) {
			error_message = "-ERROR Id should only have integer values\n";
			return NULL;
		}
	}
	*seq_number = atoi(id); 
	return "";
}

unsigned long get_current_time(void) {
    long            ms; // Milliseconds
    time_t          s;  // Seconds
    struct timespec spec;

    clock_gettime(CLOCK_REALTIME, &spec);

    s  = spec.tv_sec;
    ms = round(spec.tv_nsec / 1.0e6); // Convert nanoseconds to milliseconds
    if (ms > 999) {
        s++;
        ms = 0;
    }
    
    return s;
}

void set_error(char* message) {	
	error_flag = 1;
	error_message = message;
}
