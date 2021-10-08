#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define NUM 1000000

FILE *insert;
FILE *insert1;
FILE *insert2;
FILE *insert3;
FILE *insert4;
FILE *find;
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

    insert = fopen("insert.txt", "w");
    insert1 = fopen("insert1.txt", "w");
    insert2 = fopen("insert2.txt", "w");
    insert3 = fopen("insert3.txt", "w");
    insert4 = fopen("insert4.txt", "w");
    find = fopen("find.txt", "w");
    delete1 = fopen("delete1.txt", "w");
    delete2 = fopen("delete2.txt", "w");
    delete3 = fopen("delete3.txt", "w");
    delete4 = fopen("delete4.txt", "w");
    int i;
    
    fprintf(insert, "o data.dat\n");
    fprintf(insert1, "o data.dat\n");
    fprintf(insert2, "o data.dat\n");
    fprintf(insert3, "o data.dat\n");
    fprintf(insert4, "o data.dat\n");
    fprintf(find, "o data.dat\n");
    fprintf(delete1, "o data.dat\n");
    fprintf(delete2, "o data.dat\n");
    fprintf(delete3, "o data.dat\n");
    fprintf(delete4, "o data.dat\n");

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
        printf("i %d thisis%d\n", array[i], array[i]);
        fprintf(insert, "i %d thisis%d\n", array[i], array[i]);
        fprintf(insert1, "i %d thisis%d\n", array[i], array[i]);
        fprintf(find, "f %d\n", array[i]);
        fprintf(delete1, "d %d\n", array[i]);
    }

    printf("test3\n");
    for(i = NUM/4; i < 2*(NUM/4); i++) { // print
        //printf("test4\n");
        printf("i %d thisis%d\n", array[i], array[i]);
        fprintf(insert, "i %d thisis%d\n", array[i], array[i]);
        fprintf(insert2, "i %d thisis%d\n", array[i], array[i]);
        fprintf(find, "f %d\n", array[i]);
        fprintf(delete2, "d %d\n", array[i]);
    }

    printf("test3\n");
    for(i = 2*(NUM/4); i < 3*(NUM/4); i++) { // print
        //printf("test4\n");
        printf("i %d thisis%d\n", array[i], array[i]);
        fprintf(insert, "i %d thisis%d\n", array[i], array[i]);
        fprintf(insert3, "i %d thisis%d\n", array[i], array[i]);
        fprintf(find, "f %d\n", array[i]);
        fprintf(delete3, "d %d\n", array[i]);
    }

    printf("test4\n");
    for(i = 3*(NUM/4); i < NUM; i++) { // print
        //printf("test4\n");
        printf("i %d thisis%d\n", array[i], array[i]);
        fprintf(insert, "i %d thisis%d\n", array[i], array[i]);
        fprintf(insert4, "i %d thisis%d\n", array[i], array[i]);
        fprintf(find, "f %d\n", array[i]);
        fprintf(delete4, "d %d\n", array[i]);
    }



    fclose(insert);
    fclose(insert1);
    fclose(insert2);
    fclose(insert3);
    fclose(insert4);
    fclose(find);
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
