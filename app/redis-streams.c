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

static unsigned long get_current_time(void);
static char* generate_id(linked_str_list* starter_list, unsigned long *ms, int *seq_num);
static char* parse_id(linked_str_list* starter_list, char* id, unsigned long *ms, int *seq_num, char get_upd);

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

// char* get_elements(stream_entry starter_ent, char* id_min, char* id_max) {

// }

linked_str_list* lookup_up_node_from_stream(linked_str_list* stream_list, unsigned long ms_num) {
    while(stream_list != NULL) {
        unsigned long current_ms;
        char* junk = parse_id(stream_list, stream_list->key, &current_ms, NULL, 0);
        free(junk);
        
        if(current_ms > ms_num) {
            set_error("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
            return NULL;
        } else if(current_ms <= ms_num) return stream_list;
        
        printf("THE stream key is: %s, and the id is: %ld\n", stream_list->key, ms_num);
        stream_list = stream_list->next;
    }
    return NULL;
}

char insert_new_stream(char* stream_key, char* id, listpack kv_data) {
    for(int i = 0; i < MAX_STREAM_KEYS; i++) {
        if(stream_array[i] == NULL) {
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
    unsigned long ms;
    int seq_num;

    char* junk = parse_id(current_stream_entry->linked_list, id, &ms, &seq_num, 0);
    if(junk == NULL) return NULL;

    linked_str_list* current_list = current_stream_entry->linked_list;
    while(current_list != NULL) {
        unsigned long current_ms; 
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

char* insert_to_radix_tree(listpack args_meta, listpack k_v_d, char xadd_xrange) {
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
        else {
            // new_id = get_elements(stream_entry, )
        }
    }

    free(args_meta[1]);
    return new_id;
}

char* parse_id(linked_str_list* linked_list, char* id, unsigned long *ms, int *seq_num, char get_upd) {
    int len = strlen(id);
    if(id[0] == '*' && len == 1) { 
        return generate_id(linked_list, ms, seq_num);
    } else {
        for(int i = 0; i < len; i++) {
            if(id[i] == '-') {
                char* ms_str = malloc((i+1) * sizeof(char));
                slice(id, ms_str, 0, i);
                unsigned long ms_number = atoi(ms_str);
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
                        unsigned long temp_ms;
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
                    seq_value = atoi(temp_string);
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
                
                sprintf(response, "%ld-%d", ms_number, seq_value);
                return response;
            }
            if(!isdigit(id[i])) return NULL;
        }
    }
    return NULL;
}

static char* generate_id(linked_str_list* linked_list, unsigned long *ms_num, int *seq_n) {
    unsigned long ms = get_current_time();
    int seq;
    int seq_ind = 1;
    
    linked_str_list* get_list = lookup_up_node_from_stream(linked_list, ms);
    
    if(get_list == NULL) {
        seq = ms == 0 ? 1 : 0;
    } else {
        int get_len = strlen(get_list->key);
        for(int i = 0; i < get_len; i++) {
            if(get_list->key[i] == '-') {
                seq_ind = ((get_len-(i+1))+1);
                char* seq_str = malloc(seq_ind * sizeof(char));
                if(seq_str == NULL) {
                    printf("ERORR MALLOC \n");
                    exit(EXIT_FAILURE);
                }
                seq_str[seq_ind-1] = '\0';
                slice(get_list->key, seq_str, i+1, strlen(get_list->key));

                char* ms_str = malloc((i+1) * sizeof(char));
                slice(get_list->key, ms_str, 0, i);
                unsigned long ms_number = atoi(ms_str);
                if(ms == ms_number) {
                    seq = atof(seq_str);
                    seq++;
                } else  seq = ms == 0 ? 1 : 0;
                free(ms_str);
                free(seq_str);		
            }
        }
    }
    
    int new_len = (int)((ceil(log10(ms)))*sizeof(char));
    char* response = malloc((new_len + seq_ind) * sizeof(char));
    response[new_len+seq_ind] = '\0';
    if(ms_num != NULL) *ms_num = ms;
    if(seq_n != NULL) *seq_n = seq;
    sprintf(response, "%ld-%d", ms, seq);
    return response;
}

void set_error(char* message) {	
	error_flag = 1;
	error_message = message;
}

static unsigned long get_current_time(void) {
    // long            ms; // Milliseconds
    // time_t          s;  // Seconds
    // struct timespec spec;

    // clock_gettime(CLOCK_REALTIME, &spec);

    // s  = spec.tv_sec;
    // ms = round(spec.tv_nsec / 1.0e6); // Convert nanoseconds to milliseconds
    // if (ms > 999) {
    //     s++;
    //     ms = 0;
    // }
        // Get the current time in seconds
    time_t seconds = time(NULL);
    
    // Convert seconds to milliseconds
    long long milliseconds = seconds * 1000LL;
    
    return milliseconds;
}
