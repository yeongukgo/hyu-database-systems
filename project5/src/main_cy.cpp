#include "API.h"


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

int table_id = 0;

void* thread1(void* args) {
    int trx_id;
    char values1[120] = "update1";
    char values2[120] = "update2";
    int64_t key1, key2;

    key1 = 8;
    key2 = 6;

    trx_id = begin_trx();
    printf("update_result : %d", db_update(table_id,key1, values1,trx_id));
    sleep(3);
    printf("update_result : %d", db_update(table_id,key1, values1,trx_id));

    end_trx(trx_id);

    return NULL;
}

void* thread2(void* args) {
    int trx_id;
    char values1[120] = "update1";
    char values2[120] = "update2";
    int64_t key1, key2;

    key1 = 6;
    key2 = 7;

    sleep(1);
    trx_id = begin_trx();
    printf("update_result : %d", db_update(table_id,key1, values1,trx_id));
    sleep(3);
    printf("update_result : %d", db_update(table_id,key1, values1,trx_id));

    end_trx(trx_id);

    return NULL;
}


void* thread3(void* args) {
    int trx_id;
    char values1[120] = "update1";
    char values2[120] = "update2";
    int64_t key1, key2;

    key1 = 7;
    key2 = 8;

    sleep(2);
    trx_id = begin_trx();
    printf("update_result : %d", db_update(table_id,key1, values1,trx_id));
    sleep(3);
    printf("update_result : %d", db_update(table_id,key1, values1,trx_id));

    end_trx(trx_id);

    return NULL;
}


int main() {
    pthread_t td1,td2,td3;
    char value1[120] = "thisisi6";
    char value2[120] = "thisisi7";
    char value3[120] = "thisisi8";
    
    init_db(100);

    table_id = open_table("test1.dat");

    db_insert(table_id, 6, value1);
    db_insert(table_id, 7, value2);
    db_insert(table_id, 8, value3);

    printf("log1\n");

    pthread_create(&td1,NULL,thread1,NULL);
    pthread_create(&td2,NULL,thread2,NULL);
    pthread_create(&td3,NULL,thread3,NULL);
    
    pthread_join(td1,NULL);
    pthread_join(td2,NULL);
    pthread_join(td3,NULL);

    shutdown_db();
    return 0;
}