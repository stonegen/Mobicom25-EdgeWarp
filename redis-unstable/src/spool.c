#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>  // Include this header for usleep
#include "spool.h"


void* dedicated_thread_worker(void *arg) {
    dedicated_thread_t *dt = (dedicated_thread_t *)arg;

    while (1) {
        pthread_mutex_lock(&(dt->mutex));
        
        // Wait for a task or stop signal
        while (!dt->task_ready && !dt->stop) {
            pthread_cond_wait(&(dt->cond), &(dt->mutex));
        }

        if (dt->stop) {
            pthread_mutex_unlock(&(dt->mutex));
            break;
        }

        // Execute the task
        if (dt->task_ready && dt->func != NULL) {
            dt->func(dt->arg);
        }

        // Mark the task as complete
        dt->task_ready = false;
        dt->task_complete = true;
        pthread_cond_signal(&(dt->cond));  // Notify the main thread that the task is complete

        pthread_mutex_unlock(&(dt->mutex));
    }

    return NULL;
}

dedicated_thread_t* create_dedicated_thread() {
    dedicated_thread_t *dt = malloc(sizeof(dedicated_thread_t));

    pthread_mutex_init(&(dt->mutex), NULL);
    pthread_cond_init(&(dt->cond), NULL);
    dt->stop = false;
    dt->task_ready = false;
    dt->task_complete = false;
    dt->func = NULL;
    dt->arg = NULL;

    pthread_t thread;
    pthread_create(&thread, NULL, dedicated_thread_worker, dt);
    pthread_detach(thread);

    return dt;
}

void assign_single_key_task(dedicated_thread_t *dt, thread_func_t func, void *arg) {
    pthread_mutex_lock(&(dt->mutex));

    dt->func = func;
    dt->arg = arg;
    dt->task_ready = true;
    dt->task_complete = false;

    pthread_cond_signal(&(dt->cond));
    pthread_mutex_unlock(&(dt->mutex));
}

void wait_for_task_completion(dedicated_thread_t *dt) {
    pthread_mutex_lock(&(dt->mutex));
    
    while (!dt->task_complete) {
        pthread_cond_wait(&(dt->cond), &(dt->mutex));
    }

    pthread_mutex_unlock(&(dt->mutex));
}

void stop_dedicated_thread(dedicated_thread_t *dt) {
    pthread_mutex_lock(&(dt->mutex));
    dt->stop = true;
    pthread_cond_signal(&(dt->cond));
    pthread_mutex_unlock(&(dt->mutex));

    // Give the thread some time to exit
    usleep(1000);  // Sleep for 1ms (adjust as needed)

    // Clean up resources
    pthread_mutex_destroy(&(dt->mutex));
    pthread_cond_destroy(&(dt->cond));
    free(dt);
}