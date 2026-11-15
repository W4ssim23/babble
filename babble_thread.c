#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "babble_server.h"
#include "babble_types.h"
#include "babble_server_answer.h"
#include "babble_communication.h"
#include "babble_thread.h"
#include "fastrand.h"

#include <pthread.h>
#include <threads.h>
#include <semaphore.h>
#include <unistd.h>

// will be moved to its own file "babble_cmd_buffer.*", maybe

// typedef struct cmd_buff_elem
// {
//     command_t cmd;
//     // answer_t *answer;
// } cmd_buff_elem_t;

// cmd_buff_elem_t cmd_buffer[CMD_BUFF_SIZE];
// int next, curr_ticket, next_ticket;

typedef struct cmd_buff_struct
{
    command_t **cmd;
    uint next;
    uint curr;
} cmd_buff_t;

cmd_buff_t cmd_buff;
pthread_mutex_t buff_mutex = PTHREAD_MUTEX_INITIALIZER;
sem_t sem_exec;
sem_t sem_com;

void init_cmd_buff(void)
{
    // next = 0;
    // curr_ticket = 0;
    // next_ticket = 1;
    // memset(cmd_buffer, 0, CMD_BUFF_SIZE * sizeof(cmd_buff_elem_t *));

    cmd_buff.cmd = calloc(CMD_BUFF_SIZE, sizeof(command_t *)); // TODO: free before termination
    cmd_buff.next = 0;
    cmd_buff.curr = 0;

    sem_init(&sem_exec, 0, 0);
    sem_init(&sem_com, 0, CMD_BUFF_SIZE);
}

void clean_cmd_buff(void)
{
    free(cmd_buff.cmd);
}

// get proceed function as an argument
void *exec_thread_core(void *arg)
{
    (void)arg;

    fastRandomSetSeed(time(NULL) + pthread_self() * 100);

    while (1)
    {

        sem_wait(&sem_exec);

        pthread_mutex_lock(&buff_mutex);

        // process cmd and send enswear
        answer_t *answer = NULL;

        if (process_command(cmd_buff.cmd[cmd_buff.curr], &answer) == -1)
        {
            fprintf(stderr, "Warning: unable to process command from client %lu\n", cmd_buff.cmd[cmd_buff.curr]->key);
        }

        // free(cmd);

        if (send_answer_to_client(answer) == -1)
        {
            fprintf(stderr, "Warning: unable to answer command from client %lu\n", answer->key);
        }
        free_answer(answer);

        cmd_buff.curr = (cmd_buff.curr + 1) % CMD_BUFF_SIZE;

        pthread_mutex_unlock(&buff_mutex);

        sem_post(&sem_com);
    }
    return NULL;
}

void *com_thread_core(void *arg)
{
    int newsockfd = *(int *)arg;
    free(arg);

    char client_name[BABBLE_ID_MSIZE + 1];
    char *recv_buff = NULL;
    int recv_size = 0;
    command_t *cmd;
    answer_t *answer = NULL;
    unsigned long client_key = 0;

    memset(client_name, 0, BABBLE_ID_MSIZE + 1);
    if ((recv_size = network_recv(newsockfd, (void **)&recv_buff)) < 0)
    {
        fprintf(stderr, "Error -- recv from client\n");
        close(newsockfd);
        return NULL;
    }

    cmd = new_command(0);

    if (parse_command(recv_buff, cmd) == -1 || cmd->cid != LOGIN)
    {
        fprintf(stderr, "Error -- in LOGIN message\n");
        close(newsockfd);
        free(cmd);
        return NULL;
    }

    /* before processing the command, we should register the
     * socket associated with the new client; this is to be done only
     * for the LOGIN command */
    cmd->sockfd = newsockfd;

    if (process_command(cmd, &answer) == -1)
    {
        fprintf(stderr, "Error -- in LOGIN\n");
        close(newsockfd);
        free(cmd);
        return NULL;
    }

    /* notify client of registration */
    if (send_answer_to_client(answer) == -1)
    {
        fprintf(stderr, "Error -- in LOGIN ack\n");
        close(newsockfd);
        free(cmd);
        free_answer(answer);
        return NULL;
    }
    else
    {
        free_answer(answer);
    }

    /* let's store the key locally */
    client_key = cmd->key;

    strncpy(client_name, cmd->msg, BABBLE_ID_MSIZE);
    free(recv_buff);
    free(cmd);

    /* looping on client commands */
    while ((recv_size = network_recv(newsockfd, (void **)&recv_buff)) > 0)
    {
        cmd = new_command(client_key);

        if (parse_command(recv_buff, cmd) == -1)
        {
            fprintf(stderr, "Warning: unable to parse message from client %s\n", client_name);
            notify_parse_error(cmd, recv_buff, &answer);
            send_answer_to_client(answer);
            free_answer(answer);
            free(cmd);
        }
        else
        {
            sem_wait(&sem_com);

            pthread_mutex_lock(&buff_mutex);

            cmd_buff.cmd[cmd_buff.next] = cmd;
            cmd_buff.next = (cmd_buff.next + 1) % CMD_BUFF_SIZE;

            pthread_mutex_unlock(&buff_mutex);

            sem_post(&sem_exec);
        }
        free(recv_buff);
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
    return NULL;
}