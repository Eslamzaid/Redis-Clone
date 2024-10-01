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
#include "redis-streams.h"

stream_entry* stream_array[MAX_STREAM_KEYS];
char error_flag = 0;
char* error_message;
int empty = 0;


static unsigned long long get_current_time(void);
static char* generate_id(linked_str_list* starter_list, unsigned long long *ms, int *seq_num);
static char* parse_id(linked_str_list* starter_list, char* id, unsigned long long *ms, int *seq_num, char get_upd);

void init_stream_table() {
    for(int i = 0; i < MAX_STREAM_KEYS; i++) 
        stream_array[i] = NULL;
}

linked_str_list* create_new_list(char* id, listpack kv_data) {
    linked_str_list* new_list = malloc(sizeof(linked_str_list));
    if(new_list == NULL) {
        printf("Error creating new list\n");
        exit(EXIT_FAILURE);
    }
    new_list->key = id;
    new_list->value = kv_data;
    new_list->next = NULL;
    return new_list;
} 

stream_entry* lookup_up_stream(char* stream_entry_key) {
    for(int i = 0; i < MAX_STREAM_KEYS; i++) {
        if(stream_array[i] == NULL) continue;
        if(strcmp(stream_array[i]->stream_key, stream_entry_key) == 0)
            return stream_array[i];
    }
    return NULL;
}


// I am gonna have a function for setting the entries in the stack_group data structure
    // this function if passed nothing, return NULL
    // if passed only min, && max is + return every list starting from min to end
    // if passed only max, && min is - return every list from the start to the max.

// the data is stored in a reverse order MAX-MIN, now I want to retrieve data as MIN-MAX
// Okay I will store the data in a reverse order, and will let the parser deal with it.
// then I will have another function that will transform that data structure into hte redis-array
linked_str_list* set_group(linked_str_list* starter_ent, listpack args, stack_group* elements_group) {
    linked_str_list* current_list = starter_ent;
    linked_str_list* current_new_list;
    char top_set = 0;
    unsigned long long curr_ms;
    int curr_seq;


    unsigned long long min; // = strtoul(args[1], NULL, 10);
    int min_seq;
    unsigned long long max; // = strtoul(args[2], NULL, 10);
    int max_seq;

    char* byebye = parse_id(NULL, args[1], &min, &min_seq, 0);
    if(byebye != NULL) free(byebye);
    byebye = parse_id(NULL, args[2], &max, &max_seq, 0);
    if(byebye != NULL) free(byebye);

    while(current_list != NULL) {
        char* junk = parse_id(current_list, current_list->key, &curr_ms, &curr_seq, 0);
        if(junk == NULL) {
            return NULL;
        }
        free(junk);
        if(curr_ms >= min && curr_ms <= max ) {
            if(curr_seq >= min_seq && curr_seq <= max_seq) {
                linked_str_list* new_list = create_new_list(current_list->key, current_list->value);
                if(top_set == 0) {
                    // elements_group->top = new_list;
                    elements_group->num_elems = 1;
                    elements_group->num_of_chars = 0;
                    current_new_list = new_list;
                    top_set = 1;
                } else {
                    // [cur]-> NULL
                    linked_str_list* loop_list = current_new_list;
                    while(loop_list != NULL) {
                        if(loop_list->next == NULL) {
                            loop_list->next = new_list;
                            break;
                        }
                        loop_list = loop_list->next;
                    }
                    elements_group->num_elems++;
                }
                int len = 4; //*2\r\n
                len+= 1 + (int)((ceil(log10(strlen(current_list->key))))*sizeof(char)) + 2; // ${LEN_KEY}\r\n
                len+= strlen(current_list->key) + 2; // ID\r\n
                int i = 0;
                while(current_list->value[i] != NULL) {
                    len += 1 + (int)((ceil(log10(strlen(current_list->value[i]))))*sizeof(char)) + 2; // ${LEN_VAL[i]}\r\n
                    len += strlen(current_list->value[i]) + 2; // {VAL[0]}\r\n
                    i++;
                }
                len += 1 + (int)((ceil(log10(i)))*sizeof(char)) + 2; // *{NUM_VALS}\r\n
                elements_group->num_of_chars += len;
            }
        }
        current_list = current_list->next;
    }

    if(top_set == 0) {
        empty = 1;
        return NULL;
    }
    return current_new_list;
}

linked_str_list* reverse_linked_list(linked_str_list* head) {
    linked_str_list* prev = NULL;
    linked_str_list* current = head;
    linked_str_list* next = NULL;

    while (current != NULL) {
        next = current->next;  // Save next node
        current->next = prev;  // Reverse the current node's next pointer
        prev = current;        // Move prev one step forward
        current = next;        // Move current one step forward
    }

    return prev; // New head of the reversed list
}

char* get_elements(stream_entry* starter_ent, listpack args, int n_args) {
    // ^ args[1] == is the min 
    // ^ args[2] == is the max
    int args_len = n_args-1;
    stack_group* elems_group = malloc(sizeof(stack_group));
    if(args_len == 0) {
        return NULL;
    } 
    
    linked_str_list* the_list = set_group(starter_ent->linked_list, args, elems_group);
    if(the_list == NULL) {
        if(empty == 1) {
            printf("EMPTY\n");
            empty = 0;
            return NULL;
        } return NULL;
    }
    
    linked_str_list* output = reverse_linked_list(the_list);

    int len = 3; // Start with "*\r\n"
    len += (elems_group->num_elems <= 9 ? 
        1 : (int)((ceil(log10(elems_group->num_elems))))); // Account for the number digits
    
    elems_group->num_of_chars += len;

    // Allocate enough space for the response (including null-terminator)
    char* response = malloc((elems_group->num_of_chars + 1) * sizeof(char));
    if (!response) {
        set_error("Failed to allocate memory\n");
        return NULL;
    }

    // Construct the Redis protocol string in the response
    sprintf(response, "*%d\r\n", elems_group->num_elems); // Format response as "*{NUM_ELE}\r\n"
    
    // Null-terminate the response (sprintf already null-terminates it)
    response[elems_group->num_of_chars] = '\0';

    int i = 0;
    linked_str_list* current_list = output;
    while(i < elems_group->num_elems) {
        // adding the num of elements in an index of the array;
        strcat(response, "*2\r\n");
        // adding the id
        int key_len = strlen(current_list->key);
        char num_of_nums = (int)((ceil(log10(key_len)))*sizeof(char));
        int char_id_len = 3+num_of_nums+key_len+2;
        char* id_string = malloc(char_id_len*sizeof(char));
        // id_string[char_id_len] = '\0';
        sprintf(id_string, "$%d\r\n%s\r\n", key_len, current_list->key);
        strcat(response, id_string);
        free(id_string);

        // adding the N of values
        int j = 0; 
        while(current_list->value[j] != NULL) 
            j++;
        int num_of_values_len = 3 + (j <= 9 ? 1 : (int)((ceil(log10(j)))*sizeof(char)));
        char* num_of_values_string = malloc(num_of_values_len * sizeof(char));
        // num_of_values_string[num_of_values_len] = '\0';
        sprintf(num_of_values_string, "*%d\r\n", j);
        strcat(response, num_of_values_string);
        free(num_of_values_string);
        // adding the values

        j = 0;
        while (current_list->value[j] != NULL) {
            int value_len = strlen(current_list->value[j]);
            int value_string_len = snprintf(NULL, 0, "$%d\r\n%s\r\n", value_len, current_list->value[j]);
            char* value_string = malloc((value_string_len+1) * sizeof(char));
            if(value_string == NULL) {
                printf("Memory error\n");
                exit(EXIT_FAILURE);
            }
            sprintf(value_string, "$%d\r\n%s\r\n", value_len, current_list->value[j]);
            strcat(response, value_string);
            // free(value_string);
            j++;
        }

        current_list = current_list->next;
        i++;
    }
    
    free(args[2]);
    while (output != NULL) {
        linked_str_list* next = output->next;
        free(output);
        output = next;
    }

    return response;
}


linked_str_list* lookup_up_node_from_stream(linked_str_list* stream_list, unsigned long long ms_num) {
    while(stream_list != NULL) {
        unsigned long long current_ms;
        char* junk = parse_id(stream_list, stream_list->key, &current_ms, NULL, 0);
        free(junk);
        
        if(current_ms > ms_num) {
            set_error("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
            return NULL;
        } else if(current_ms == ms_num) return stream_list;
        
        stream_list = stream_list->next;
    }
    return NULL;
}

char insert_new_stream(char* stream_key, char* id, listpack kv_data) {
    for(int i = 0; i < MAX_STREAM_KEYS; i++) {
        if(stream_array[i] == NULL) {
            // printf("REACHED HERE?\n");
            linked_str_list* new_list = create_new_list(id, kv_data);
            stream_entry* new_entry = malloc(sizeof(stream_entry));
            new_entry->linked_list = new_list;
            new_entry->stream_key = stream_key;
            stream_array[i] = new_entry;
            return 0;
        }
    }
    return 1;
}

char* insert_new_to_stream(stream_entry* current_stream_entry, char* id, listpack kv_data) { 

    // now the data structure is not a tree, it is a linked list:
    unsigned long long ms;
    int seq_num;

    char* junk = parse_id(current_stream_entry->linked_list, id, &ms, &seq_num, 0);
    if(junk == NULL) return NULL;

    linked_str_list* current_list = current_stream_entry->linked_list;
    while(current_list != NULL) {
        unsigned long long current_ms; 
        int current_seq_num; 

        char* current_junk = parse_id(current_list, current_list->key, &current_ms, &current_seq_num, 0);
        if(current_junk == NULL) {
            free(junk);
            return NULL;
        }
        
        if ((current_ms == ms && (current_seq_num == seq_num || current_seq_num >= seq_num)) || 
            current_ms > ms) {
            set_error("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
            free(junk);
            free(current_junk);
            return NULL;
        } 
        current_list = current_list->next;
    }

    linked_str_list* new_list = create_new_list(junk, kv_data);
    new_list->next = current_stream_entry->linked_list;

    current_stream_entry->linked_list = new_list;
    free(id);
    

    return junk;
}


char* insert_to_radix_tree(listpack args_meta, listpack k_v_d, char xadd_xrange, int n_args) {
	stream_entry* stream_entry = lookup_up_stream(args_meta[0]);
    char* new_id;
    if(xadd_xrange == 0) {
        if(stream_entry == NULL) { 
            new_id = parse_id(NULL, args_meta[1], NULL, NULL, 1);
            if(new_id == NULL) return NULL;
            if(insert_new_stream(args_meta[0], new_id, k_v_d) == 1) return NULL;
        } else {
            new_id = parse_id(stream_entry->linked_list, args_meta[1], NULL, NULL, 1);
            if(new_id == NULL) return NULL;
            new_id = insert_new_to_stream(stream_entry, new_id, k_v_d);
            if(new_id == NULL) return NULL;
        }
    } else {
        if(stream_entry == NULL) return NULL;
        else new_id = get_elements(stream_entry, args_meta, n_args);
    }

    free(args_meta[1]);
    return new_id;
}

char* parse_id(linked_str_list* linked_list, char* id, unsigned long long *ms, int *seq_num, char get_upd) {
    int len = strlen(id);
    if(id[0] == '*' && len == 1) { 
        return generate_id(linked_list, ms, seq_num);
    } else {
        for(int i = 0; i < len; i++) {
            if(id[i] == '-') {
                char* ms_str = malloc((i+1) * sizeof(char));
                slice(id, ms_str, 0, i);
                unsigned long long ms_number = strtoull(ms_str, NULL, 10);
                if(ms != NULL) *ms = ms_number;
                free(ms_str);

                if(i+1 == len) {
                    set_error("-ERR Missing Sequence number\n");
                    return NULL;
                } 

                int seq_value;
                if(id[i+1] == '*') {
                    linked_str_list* list = lookup_up_node_from_stream(linked_list, ms_number);
                    if(list == NULL) {  
                        seq_value = ms_number == 0 ? 1 : 0;
                        if(seq_num != NULL) *seq_num = seq_value;
                     } else {
                        unsigned long long temp_ms;
                        int temp_seq;
                        if(parse_id(linked_list, list->key, &temp_ms, &temp_seq, 0) == NULL) return NULL;

                        if(temp_ms == ms_number) {
                            seq_value = get_upd == 0 ? temp_seq : temp_seq+1;
                            if(seq_num != NULL) seq_value = temp_seq;
                        } else {
                            seq_value = ms_number == 0 ? 1 : 0;
                        }
                    }
                } else {
                    int j;
                    for(j = i+1; j < len; j++)
                        if(!isdigit(id[j])) return NULL;
                    char* temp_string = malloc((j-i) * sizeof(char));
                    temp_string[(j-i)-1] = '\0';
                    slice(id, temp_string, i+1, j);
                    seq_value = strtoull(temp_string, NULL, 10);
                    if(seq_num != NULL) *seq_num = seq_value;
                    free(temp_string);
                }


                if(ms_number == 0 && seq_value == 0) {
                    set_error("-ERR The ID specified in XADD must be greater than 0-0\r\n");
                    return NULL;
                }
                int seq_len = seq_value <= 9 ? 1 : (int)((ceil(log10(seq_value)))*sizeof(char));
                int ms_len = ms_number <= 9 ? 1 : (int)((ceil(log10(ms_number)))*sizeof(char));

                int final_len = ms_len + seq_len;
                char* response = malloc(final_len+1 * sizeof(char));

                response[final_len] = '\0';
                
                sprintf(response, "%lld-%d", ms_number, seq_value);
                return response;
            }
            if(!isdigit(id[i])) return NULL;
        }
    }
    return NULL;
}

static char* generate_id(linked_str_list* linked_list, unsigned long long *ms_num, int *seq_n) {
    unsigned long long ms = get_current_time();
    int seq;
    int seq_ind = 1;
    
    linked_str_list* get_list = lookup_up_node_from_stream(linked_list, ms);
    
    if(get_list == NULL) {
        seq = ms == 0 ? 1 : 0;
    } else {
        char* junk = parse_id(get_list, get_list->key, NULL, &seq, 0);
        if(junk == NULL) return NULL;
        free(junk);
        seq++;
    }
    
    int new_len = (int)((ceil(log10(ms)))*sizeof(char));
    char* response = malloc((new_len + seq_ind) * sizeof(char));
    response[new_len+seq_ind] = '\0';
    if(ms_num != NULL) *ms_num = ms;
    if(seq_n != NULL) *seq_n = seq;
    sprintf(response, "%lld-%d", ms, seq);
    return response;
}

void set_error(char* message) {	
	error_flag = 1;
	error_message = message;
}

static unsigned long long get_current_time(void) {
    time_t seconds = time(NULL);
    long long milliseconds = seconds * 1000LL;
    
    return milliseconds;
}
