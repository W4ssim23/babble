
#include <stdio.h>
#include <string.h>

#include "babble_server.h"
#include "babble_types.h"
#include "babble_server_answer.h"

#include "babble_thread.h"
#include "fastrand.h"
#include <pthread.h>
#include <threads.h>

#include <semaphore.h>

// will be moved to its own file "babble_cmd_buffer.*", maybe

typedef struct cmd_buff_elem
{
    command_t cmd;
    answer_t *answer;
} cmd_buff_elem_t;

cmd_buff_elem_t cmd_buffer[CMD_BUFF_SIZE];
int next, curr_ticket, next_ticket;

pthread_mutex_t buff = PTHREAD_MUTEX_INITIALIZER;
sem_t sem_exec;
sem_t sem_com;

void init_cmd_buff(void)
{
    next = 0;
    curr_ticket = 0;
    next_ticket = 1;
    memset(cmd_buffer, 0, CMD_BUFF_SIZE * sizeof(cmd_buff_elem_t *));

    sem_init(&sem_exec, 0, 0);
    sem_init(&sem_com, 0, CMD_BUFF_SIZE);
}

// get proceed function as an argument
void *exec_thread_core(void *arg)
{
    (void)arg;

    fastRandomSetSeed(time(NULL) + pthread_self() * 100);

    while (1)
    {

        sem_wait(&sem_exec);

        pthread_mutex_lock(&buff);

        if (process_command(&(cmd_buffer[next].cmd), &(cmd_buffer[next].answer)) == -1)
        {
            fprintf(stderr, "Warning: unable to process command from client %lu\n", cmd_buffer[next].cmd.key);
        }

        next = (next + 1) % CMD_BUFF_SIZE;
        curr_ticket++;

        pthread_mutex_unlock(&buff);

        sem_post(&sem_com);
    }
}

void *com_thread_core(void *arg)
{
    (void)arg;

    while ((recv_size = network_recv(newsockfd, (void **)&recv_buff)) > 0)
    {

        cmd = new_command(client_key);

        sem_wait(&sem_com);

        pthread_mutex_lock(&buff);

        if (send_answer_to_client(answer) == -1)
        {
            fprintf(stderr, "Warning: unable to answer command from client %lu\n", answer->key);
        }
        free_answer(answer);

        next = (next + 1) % CMD_BUFF_SIZE;
        curr_ticket++;

        pthread_mutex_unlock(&buff);

        sem_post(&sem_exec);
    }

    if (client_name[0] != 0)
    {
        cmd = new_command(client_key);
        cmd->cid = UNREGISTER;

        if (process_command(cmd, &answer) == -1)
        {
            fprintf(stderr, "Warning -- failed to unregister client %s\n", client_name);
        }
        free(cmd);
    }
}