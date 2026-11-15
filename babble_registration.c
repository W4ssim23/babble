#include <stdio.h>
#include <string.h>

#include "babble_registration.h"

#include <pthread.h>

pthread_mutex_t reg_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
int writers, readers;

client_bundle_t *registration_table[MAX_CLIENT];
int nb_registered_clients;

void registration_init(void)
{
    nb_registered_clients = 0;

    memset(registration_table, 0, MAX_CLIENT * sizeof(client_bundle_t *));

    writers = 0;
    readers = 0;
}

client_bundle_t *registration_lookup(unsigned long key)
{

    // reader sync
    pthread_mutex_lock(&reg_mutex);
    while (writers > 0)
        pthread_cond_wait(&cond, &reg_mutex);

    readers++;
    pthread_mutex_unlock(&reg_mutex);

    int i = 0;
    client_bundle_t *c = NULL;

    for (i = 0; i < nb_registered_clients; i++)
    {
        if (registration_table[i]->key == key)
        {
            c = registration_table[i];
            break;
        }
    }

    pthread_mutex_lock(&reg_mutex);
    readers--;
    if (readers == 0)
        pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&reg_mutex);

    return c;
}

int registration_insert(client_bundle_t *cl)
{

    // writer sync

    pthread_mutex_lock(&reg_mutex);
    writers++;
    while (readers > 0)
        pthread_cond_wait(&cond, &reg_mutex);

    if (nb_registered_clients == MAX_CLIENT)
    {
        fprintf(stderr, "ERROR: MAX NUMBER OF CLIENTS REACHED\n");

        writers--;
        pthread_cond_broadcast(&cond);
        pthread_mutex_unlock(&reg_mutex);

        return -1;
    }

    /* lookup to find if key already exists */
    int i = 0;

    for (i = 0; i < nb_registered_clients; i++)
    {
        if (registration_table[i]->key == cl->key)
        {
            break;
        }
    }

    if (i != nb_registered_clients)
    {
        fprintf(stderr, "Error -- id % ld already in use\n", cl->key);

        writers--;
        pthread_cond_broadcast(&cond);
        pthread_mutex_unlock(&reg_mutex);

        return -1;
    }

    /* insert cl */
    registration_table[nb_registered_clients] = cl;
    nb_registered_clients++;

    writers--;
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&reg_mutex);

    return 0;
}

client_bundle_t *registration_remove(unsigned long key)
{

    // writer sync

    pthread_mutex_lock(&reg_mutex);
    writers++;
    while (readers > 0)
        pthread_cond_wait(&cond, &reg_mutex);

    int i = 0;

    for (i = 0; i < nb_registered_clients; i++)
    {
        if (registration_table[i]->key == key)
        {
            break;
        }
    }

    if (i == nb_registered_clients)
    {
        fprintf(stderr, "Error -- no client found\n");

        writers--;
        pthread_cond_broadcast(&cond);
        pthread_mutex_unlock(&reg_mutex);

        return NULL;
    }

    client_bundle_t *cl = registration_table[i];

    nb_registered_clients--;
    registration_table[i] = registration_table[nb_registered_clients];

    writers--;
    pthread_cond_broadcast(&cond);
    pthread_mutex_unlock(&reg_mutex);

    return cl;
}
