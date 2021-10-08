#ifndef __DEFINITION_H__
#define __DEFINITION_H__

#include <stdbool.h>

// SIZE.
#define SIZE_PAGE 4096
#define SIZE_HEADER_PAGE_RESERVED 4072
#define SIZE_FREE_PAGE_RESERVED 4088
#define SIZE_PAGE_HEADER_RESERVED 104
#define SIZE_RECORD 128
#define SIZE_ENTRY 16
#define SIZE_RECORD_VALUE 120

// ~
#define HEADER_PAGE_NUM 0

// MACRO CONVERTER.
#define OFFSET_TO_PAGENUM( offset ) ( (offset) / SIZE_PAGE )
#define PAGENUM_TO_OFFSET( pagenum ) ( (pagenum) * SIZE_PAGE )

// ORDER.
#define ORDER_LEAF 32
#define ORDER_INTERNAL 249

// RETURN VALUE.
#define SUCCESS 0

#endif // __DEFINITION_H__