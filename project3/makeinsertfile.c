#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define NUM 100000

FILE *insert;
FILE *insert1;
FILE *insert2;
FILE *insert3;
FILE *insert4;
FILE *find;
FILE *delete;
FILE *delete1;
FILE *delete2;
FILE *delete3;
FILE *delete4;

void main() {

    // sequntial input. tree height 2
    // header 1
    // root 1
    // leaf 249
    // record : 16*248+31 = 3999
    // all : 1 + 1 + 249 = 251

    // sequntial input. tree height 3
    // header 1
    // root 1
    // internal 249
    // leaf 125*248 + 249 = 31249
    // record : 16*125*248 + 16*248+31 = 499999
    // all : 1 + 1 + 249 + 31249 = 31500

    //

    // sequntial input. tree height 4
    // root
    // internal 249
    // internal 125*248 + 249 = 31249
    // leaf ((125*248 + 249) - 1)*125 + 249 = 3906249
    // record : (((125*248 + 249) - 1)*125 + 249 - 1)*16 + 31 = 62499999
    // page : 1 + 1 + 249 + 31249 + 3906249 = 3937749

    int buffer_size = 1000;

    insert = fopen("insert.txt", "w");
    insert1 = fopen("insert1.txt", "w");
    insert2 = fopen("insert2.txt", "w");
    insert3 = fopen("insert3.txt", "w");
    insert4 = fopen("insert4.txt", "w");
    find = fopen("find.txt", "w");
    delete = fopen("delete.txt", "w");
    delete1 = fopen("delete1.txt", "w");
    delete2 = fopen("delete2.txt", "w");
    delete3 = fopen("delete3.txt", "w");
    delete4 = fopen("delete4.txt", "w");
    int i;
    
    fprintf(insert, "n %d\n", buffer_size);
    fprintf(insert, "o t1\n");
    fprintf(insert1, "n %d\n", buffer_size);
    fprintf(insert1, "o t1\n");
    fprintf(insert2, "n %d\n", buffer_size);
    fprintf(insert2, "o t1\n");
    fprintf(insert3, "n %d\n", buffer_size);
    fprintf(insert3, "o t1\n");
    fprintf(insert4, "n %d\n", buffer_size);
    fprintf(insert4, "o t1\n");
    fprintf(find, "n %d\n", buffer_size);
    fprintf(find, "o t1\n");
    fprintf(delete, "n %d\n", buffer_size);
    fprintf(delete, "o t1\n");
    fprintf(delete1, "n %d\n", buffer_size);
    fprintf(delete1, "o t1\n");
    fprintf(delete2, "n %d\n", buffer_size);
    fprintf(delete2, "o t1\n");
    fprintf(delete3, "n %d\n", buffer_size);
    fprintf(delete3, "o t1\n");
    fprintf(delete4, "n %d\n", buffer_size);
    fprintf(delete4, "o t1\n");

    printf("test1\n");

    __int32_t array[NUM];

    for(i = 0; i < NUM; i++) { // sequential
        array[i] = i+1;
    }
  
    srand(time(NULL));
    __int32_t random1 = rand();
    __int32_t random2 = rand();
    __int32_t temp;

    printf("test2\n");
    for(i = 0; i < NUM; i++) { // swap
        //printf("test2-1\n");
        random1 = (rand() % NUM);
        temp = array[random1];
        random2 = (rand() % NUM);
        array[random1] = array[random2];
        array[random2] = temp;
    }

    printf("test3\n");
    for(i = 0; i < NUM/4; i++) { // print
        //printf("test4\n");
        printf("i 4 %d thisis%d\n", array[i], array[i]);
        fprintf(insert, "i 4 %d thisis%d\n", array[i], array[i]);
        fprintf(insert1, "i 4 %d thisis%d\n", array[i], array[i]);
        fprintf(find, "f 4 %d\n", array[i]);
        fprintf(delete, "d 4 %d\n", array[i]);
        fprintf(delete1, "d 4 %d\n", array[i]);
    }

    printf("test3\n");
    for(i = NUM/4; i < 2*(NUM/4); i++) { // print
        //printf("test4\n");
        printf("i 4 %d thisis%d\n", array[i], array[i]);
        fprintf(insert, "i 4 %d thisis%d\n", array[i], array[i]);
        fprintf(insert2, "i 4 %d thisis%d\n", array[i], array[i]);
        fprintf(find, "f 4 %d\n", array[i]);
        fprintf(delete, "d 4 %d\n", array[i]);
        fprintf(delete2, "d 4 %d\n", array[i]);
    }

    printf("test3\n");
    for(i = 2*(NUM/4); i < 3*(NUM/4); i++) { // print
        //printf("test4\n");
        printf("i 4 %d thisis%d\n", array[i], array[i]);
        fprintf(insert, "i 4 %d thisis%d\n", array[i], array[i]);
        fprintf(insert3, "i 4 %d thisis%d\n", array[i], array[i]);
        fprintf(find, "f 4 %d\n", array[i]);
        fprintf(delete, "d 4 %d\n", array[i]);
        fprintf(delete3, "d 4 %d\n", array[i]);
    }

    printf("test4\n");
    for(i = 3*(NUM/4); i < NUM; i++) { // print
        //printf("test4\n");
        printf("i 4 %d thisis%d\n", array[i], array[i]);
        fprintf(insert, "i 4 %d thisis%d\n", array[i], array[i]);
        fprintf(insert4, "i 4 %d thisis%d\n", array[i], array[i]);
        fprintf(find, "f 4 %d\n", array[i]);
        fprintf(delete, "d 4 %d\n", array[i]);
        fprintf(delete4, "d 4 %d\n", array[i]);
    }

    fprintf(insert, "s\nq\n");
    fprintf(insert1, "s\nq\n");
    fprintf(insert2, "s\nq\n");
    fprintf(insert3, "s\nq\n");
    fprintf(insert4, "s\nq\n");
    fprintf(find, "s\nq\n");
    fprintf(delete, "s\nq\n");
    fprintf(delete1, "s\nq\n");
    fprintf(delete2, "s\nq\n");
    fprintf(delete3, "s\nq\n");
    fprintf(delete4, "s\nq\n");



    fclose(insert);
    fclose(insert1);
    fclose(insert2);
    fclose(insert3);
    fclose(insert4);
    fclose(find);
    fclose(delete);
    fclose(delete1);
    fclose(delete2);
    fclose(delete3);
    fclose(delete4);
}// sequntial input. tree height 3
    // header 1
    // root 1
    // internal 249
    // leaf 125*248 + 249 = 31249
    // record : 16*125*248 + 16*248+31 = 499999
    // all : 1 + 1 + 249 + 31249 = 31500
