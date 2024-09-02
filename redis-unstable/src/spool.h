#ifndef SPOOL_H
#define SPOOL_H

#include <pthread.h>
#include <stdbool.h>

typedef void (*thread_func_t)(void*);

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool stop;
    bool task_ready;
    bool task_complete;
    thread_func_t func;
    void *arg;
} dedicated_thread_t;

void* dedicated_thread_worker(void *arg);
dedicated_thread_t* create_dedicated_thread(void);
void assign_single_key_task(dedicated_thread_t *dt, thread_func_t func, void *arg);
void stop_dedicated_thread(dedicated_thread_t *dt);
void wait_for_task_completion(dedicated_thread_t *dt);

#endif // SPOOL_H
