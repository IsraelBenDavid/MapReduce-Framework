#include "Barrier.h"
#include <pthread.h>
#include <cstdio>

#define MT_LEVEL 50

struct ThreadContext {
    int threadID;
    Barrier *barrier;
};

void *foo (void *arg)
{
  ThreadContext *tc = (ThreadContext *) arg;
  printf ("Before barriers: %d\n", tc->threadID);
  int c = 0;
  for (int i = 0; i < 1000000000; ++i)
    {
      c++;
    }

  tc->barrier->barrier ();

  printf ("Between barriers: %d\n", tc->threadID);

  tc->barrier->barrier ();

  printf ("After barriers: %d\n", tc->threadID);

  return 0;
}

int main (int argc, char **argv)
{
  pthread_t threads[MT_LEVEL];
  ThreadContext contexts[MT_LEVEL];
  Barrier barrier (MT_LEVEL);

  for (int i = 0; i < MT_LEVEL; ++i)
    {
      contexts[i] = {i, &barrier};
    }

  for (int i = 0; i < MT_LEVEL; ++i)
    {
      pthread_create (threads + i, NULL, foo, contexts + i);
    }

//  for (int i = 0; i < MT_LEVEL; ++i)
//    {
//      pthread_join (threads[i], NULL);
//    }
  printf ("hiiiiiii\n");

  return 0;
}
