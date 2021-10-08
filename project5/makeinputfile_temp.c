#include <stdio.h>
#include <stdlib.h>

FILE *input;

int main(void) {
    input = fopen("input.txt", "w");

    int i;

    for(i = 0; i < __INT_MAX__; i++) {
        fprintf(input, "b\n");
    }

    for(i = 0; i < 10; i++) {
        fprintf(input, "b\n");
    }

    fclose(input);

    return 0;
}