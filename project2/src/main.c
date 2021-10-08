// #include "bpt.h"
#include "disk_space_manager.h"
#include "index_manager.h"
#include <string.h>

// MAIN

int main( int argc, char ** argv ) {

    char instruction;

    int64_t key;
    int temp;
    char value[120];
    int result;

    int count_i = 0;
    int count_f = 0;
    int count_d = 0;

    printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction) {
        case 'o':
            scanf("%s", value);
            open_table(value);
            break;
        case 'i':
            key = 0;
            memset(value, 0, 120);
            // scanf("%ld %s", &key, value);
            scanf("%d %ld %s", &temp, &key, value);
            result = db_insert(key, value);
            if(result == 0) {
                count_i++;
            }
            printf("count_i : %d\n", count_i);
            break;
        case 'f':
            key = 0;
            memset(value, 0, 120);
            scanf("%d %ld", &temp, &key);
            result = db_find(key, value);
            printf("find result : %d\n", result);
            // printf("key : %ld, value : %s\n", key , value);
            printf("key : %ld, value : %s\n", key , value);

            if(result == 0) count_f++;
            printf("count_f : %d\n", count_f);
            break;
        case 'd':
            key = 0;
            scanf("%d %ld", &temp, &key);
            result = db_delete(key);
            printf("delete result : %d\n", result);
            if(result == 0) count_d++;
            printf("count_d : %d\n", count_d);
            break;
        case 'q':
            while (getchar() != (int)'\n');
            return EXIT_SUCCESS;
            break;
        default:
            break;
        }
        
        while (getchar() != (int)'\n');
        printf("> ");
    }
    printf("\n");

    return EXIT_SUCCESS;
}
