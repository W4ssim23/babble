#ifndef __BABBLE_THREAD_H__
#define __BABBLE_THREAD_H__

#include "babble_types.h"
#include "babble_server_answer.h"

void init_cmd_buff(void);
void *exec_thread_core(void *);
void *com_thread_core(void *);
void clean_cmd_buff(void);

#endif