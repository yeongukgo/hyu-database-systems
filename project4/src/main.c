// #include "bpt.h"
#include "API.h"
#include <string.h>

// MAIN

int main( int argc, char ** argv ) {

    char instruction;

    int num, num2;
    int64_t key;
    char value[120];
    int result;

    int count_i = 0;
    int count_f = 0;
    int count_d = 0;

    printf("> ");
    while (scanf("%c", &instruction) != EOF) {
        switch (instruction) {
        case 'n': // init db
            scanf("%d", &num);
            result = init_db(num);
            printf("init_db result : %d\n", result);
            break;
        case 's': // shutdown db
            result = shutdown_db();
            printf("shutdown_db result : %d\n", result);
            break;
        case 'o':
            scanf("%s", value);
            result = open_table(value);
            printf("open_table result : %d\n", result);
            break;
        case 'c':
            scanf("%d", &num);
            result = close_table(num);
            printf("close_table result : %d\n", result);
            break;
        case 'i':
            key = 0;
            memset(value, 0, 120);
            // scanf("%ld %s", &key, value);
            scanf("%d %ld %s", &num, &key, value);

            result = db_insert(num, key, value);
            printf("insert result : %d\n", result);
            if(result == 0) {
                count_i++;
            }
            printf("count_i : %d\n", count_i);
            break;
        case 'f':
            key = 0;
            memset(value, 0, 120);
            scanf("%d %ld", &num, &key);
            printf("input_f : %d %ld\n", num, key);
            result = db_find(num, key, value);
            printf("find result : %d\n", result);
            // printf("key : %ld, value : %s\n", key , value);
            printf("key : %ld, value : %s\n", key , value);

            if(result == 0) count_f++;
            printf("count_f : %d\n", count_f);
            break;
        case 'd':
            key = 0;
            scanf("%d %ld", &num, &key);
            result = db_delete(num, key);
            printf("delete result : %d\n", result);
            if(result == 0) count_d++;
            printf("count_d : %d\n", count_d);
            break;
        case 'j':
            scanf("%d %d %s", &num, &num2, value);
            result = join_table(num, num2, value);
            printf("join result : %d\n", result);
            break;
        case 'q':
            shutdown_db();
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
