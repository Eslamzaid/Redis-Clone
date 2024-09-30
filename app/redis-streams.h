#define MAX_STREAM_KEYS 2048

typedef char** listpack;

typedef struct __linked_str_list {
    char* key;
    listpack value;
    struct __linked_str_list *next;
} linked_str_list;

typedef struct {
    char* stream_key;
    linked_str_list* linked_list;
} stream_entry;

char* insert_to_radix_tree(listpack args_meta, listpack k_v_d, char xadd_xrange);
void init_stream_table();
void set_error(char* message);
stream_entry* lookup_up_stream(char* stream_entry_key);
void slice(const char* str, char* result, size_t start, size_t end);
