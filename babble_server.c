#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <assert.h>
#include <pthread.h>

#include "babble_server.h"
#include "babble_types.h"
#include "babble_utils.h"
#include "babble_server_answer.h"
#include "fastrand.h"

#include "babble_thread.h"

/* to activate random delays in the processing of messages */
int random_delay_activated;

static void display_help(char *exec)
{
    printf("Usage: %s -p port_number -r [activate_random_delays]\n", exec);
}

int parse_command(char *str, command_t *cmd)
{
    char *name = NULL;

    /* start by cleaning the input */
    str_clean(str);

    /* get command id */
    cmd->cid = str_to_command(str, &cmd->answer_expected);

    switch (cmd->cid)
    {
    case LOGIN:
        if (str_to_payload(str, cmd->msg, BABBLE_ID_MSIZE))
        {
            name = get_name_from_key(cmd->key);
            fprintf(stderr, "Error from [%s]-- invalid LOGIN -> %s\n", name, str);
            free(name);
            return -1;
        }
        break;
    case PUBLISH:
        if (str_to_payload(str, cmd->msg, BABBLE_PUBLICATION_MSIZE))
        {
            name = get_name_from_key(cmd->key);
            fprintf(stderr, "Warning from [%s]-- invalid PUBLISH -> %s\n", name, str);
            free(name);
            return -1;
        }
        break;
    case FOLLOW:
        if (str_to_payload(str, cmd->msg, BABBLE_ID_MSIZE))
        {
            name = get_name_from_key(cmd->key);
            fprintf(stderr, "Warning from [%s]-- invalid FOLLOW -> %s\n", name, str);
            free(name);
            return -1;
        }
        break;
    case TIMELINE:
        cmd->msg[0] = '\0';
        break;
    case FOLLOW_COUNT:
        cmd->msg[0] = '\0';
        break;
    case RDV:
        cmd->msg[0] = '\0';
        break;
    default:
        name = get_name_from_key(cmd->key);
        fprintf(stderr, "Error from [%s]-- invalid client command -> %s\n", name, str);
        free(name);
        return -1;
    }

    return 0;
}

/* processes the command and eventually generates an answer */
int process_command(command_t *cmd, answer_t **answer)
{
    int res = 0;

    switch (cmd->cid)
    {
    case LOGIN:
        res = run_login_command(cmd, answer);
        break;
    case PUBLISH:
        random_delay(random_delay_activated);
        res = run_publish_command(cmd, answer);
        break;
    case FOLLOW:
        random_delay(random_delay_activated);
        res = run_follow_command(cmd, answer);
        break;
    case TIMELINE:
        random_delay(random_delay_activated);
        res = run_timeline_command(cmd, answer);
        break;
    case FOLLOW_COUNT:
        res = run_fcount_command(cmd, answer);
        break;
    case RDV:
        res = run_rdv_command(cmd, answer);
        break;
    case UNREGISTER:
        res = unregisted_client(cmd);
        *answer = NULL;
        break;
    default:
        fprintf(stderr, "Error -- Unknown command id\n");
        return -1;
    }

    if (res)
    {
        fprintf(stderr, "Error -- Failed to run command ");
        display_command(cmd, stderr);
    }

    return res;
}

int main(int argc, char *argv[])
{
    int sockfd, newsockfd;
    int portno = BABBLE_PORT;

    int opt;
    int nb_args = 1;

    while ((opt = getopt(argc, argv, "+hp:r")) != -1)
    {
        switch (opt)
        {
        case 'p':
            portno = atoi(optarg);
            nb_args += 2;
            break;
        case 'r':
            random_delay_activated = 1;
            nb_args += 1;
            break;
        case 'h':
        case '?':
        default:
            display_help(argv[0]);
            return -1;
        }
    }

    if (nb_args != argc)
    {
        display_help(argv[0]);
        return -1;
    }

    server_data_init();

    //
    init_cmd_buff();

    pthread_t exec_thread;
    for (int i = 0; i < MAX_EXEC_TH; i++)
    {
        if (pthread_create(&exec_thread, NULL, exec_thread_core, NULL) != 0)
        {
            perror("Failed to create exec_thread");
            continue;
        }
        pthread_detach(exec_thread);
    }

    //

    if ((sockfd = server_connection_init(portno)) == -1)
    {
        return -1;
    }

    printf("Babble server bound to port %d\n", portno);

    /* seed for the per-thread random number generator */
    fastRandomSetSeed(time(NULL) + pthread_self() * 100);

    /* main server loop */
    while (1)
    {

        if ((newsockfd = server_connection_accept(sockfd)) == -1)
        {
            return -1;
        }

        int *arg = malloc(sizeof(int));
        *arg = newsockfd;
        pthread_t com_thread;
        if (pthread_create(&com_thread, NULL, com_thread_core, arg) != 0)
        {
            perror("Failed to create com_thread");
            close(*arg);
            free(arg);
            continue;
        }
        pthread_detach(com_thread);
    }

    // dead code... TODO: find a better solution
    clean_cmd_buff();

    close(sockfd);
    return 0;
}
