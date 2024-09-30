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
#include <math.h>
#include <pthread.h> 
#include <inttypes.h>

#define MAX_STREAM_KEYS 2048

typedef char** listpack;
typedef struct __radix_node radix_node;
typedef struct __linked_str_list {
    radix_node* node_pointer; // Pointer to the radix node
    struct __linked_str_list* next; // Pointer to the next linked list node
} linked_str_list;
typedef struct __radix_node {
    char is_leaf; // Indicates if the node is a leaf
    char* id_key; // Key for the node
    listpack value; // Value associated with the key
    linked_str_list* next_list; // Pointer to linked list of child nodes
} radix_node;

typedef struct {
    char* stream_key;
    linked_str_list* root_header;
} stream_entry;

// Radix tree interface for streams
void init_stream_table();
radix_node* create_new_stream_radix_node(char* id_key, linked_str_list* ch_pntr, char is_leaf, listpack k_v_data);

linked_str_list* lookup_up_stream(char* stream_entry_key);
radix_node* lookup_up_node(linked_str_list* starter_list, char* id_key, char depth_insert);

char* insert_to_radix_tree(listpack args_meta, listpack k_v_d);
int insert_new_stream(char* stream_key, char* id_key, listpack k_v_data);
int insert_new_to_stream(linked_str_list* r_note, char* id_key, unsigned long ms_new, int seq_new, listpack k_v_data);

char* convert_to_int(char* id, unsigned long* mille_seconds, int* seq_number, linked_str_list* starter_node);
void slice(const char* str, char* result, size_t start, size_t end);
unsigned long get_current_time(void);
void set_error(char* message);

