#include <algorithm>
#include <iostream>
#include <atomic>
#include "Barrier.h"
#include <pthread.h>
#include <cstdio>
#include "MapReduceFramework.h"
#include <semaphore.h>

/***** Messages *****/

/***** Constants *****/

#define PERCENT_COEF 100

/***** Typedefs *****/


typedef struct Job {
    pthread_t *workers;
    Barrier *barrier;
    sem_t map_mutex, reduce_mutex, emit2_mutex, emit3_mutex, sort_mutex;
    std::vector<IntermediateVec> intermediate_vecs;
    std::vector<IntermediateVec> shuffled_intermediate_vecs;
    JobState job_state;
    std::atomic<uint32_t> atomic_counter;
    int multiThreadLevel;
    const MapReduceClient *client;
    const InputVec *inputVec;
    OutputVec *outputVec;
    float shuffle_percentage;
} Job;

typedef struct ThreadContext {
    int threadID;
    IntermediateVec intermediate_vec;
    Job *job;
} ThreadContext;

typedef struct JobContext {
    ThreadContext *contexts;
    Job *job;
} JobContext;

/***** Global Variables *****/




/***** Internal Interface *****/

/*
 * Returns true if first equals second and false otherwise
 */
bool is_equal (const K2 *first, const K2 *second)
{
  return !(*first < *second || *second < *first);
}

/*
 * Returns the max key that is in intermediate_vecs. if intermediate_vecs is empty the function
 * returns nullptr.
 */
K2 *max_key (Job *job)
{
  // check if intermediate_vecs is empty
  bool is_empty = true;
  for (const auto &vec: job->intermediate_vecs)
    {
      is_empty = is_empty && vec.empty ();
    }
  if (is_empty)
    {
      return nullptr;
    }

  // find the max key
  // get the first max key
  K2 *max_value;
  for (const auto &vec: job->intermediate_vecs)
    {
      if (!vec.empty ())
        {
          max_value = vec.back ().first;
          break;
        }
    }
  // look for a greater key
  for (const auto &vec: job->intermediate_vecs)
    {
      if (!vec.empty ()
      && *max_value <
      *(vec.back ().first))
        {
          max_value = vec.back ().first;
        }
    }
  return max_value;
}

/*
 * each thread will sort its intermediate vector according to the keys within
 */
void sort_phase (ThreadContext *thread_context)
{
  auto* curr_vec = &thread_context->intermediate_vec;
  if(!curr_vec->empty())
    {
      std::sort (curr_vec->begin (), curr_vec->end (),
                 [] (const IntermediatePair &a, const IntermediatePair &b)
                 {
                     return *a.first < *b.first;
                 });
      // todo: mutex?
      sem_wait (&(thread_context->job->sort_mutex));
      thread_context->job->intermediate_vecs.push_back (*curr_vec);
      sem_post (&(thread_context->job->sort_mutex));
    }
}

/* In this phase each thread reads pairs of (k1, v1) from the input vector and calls the map function
 * on each of them. The map function in turn will produce (k2, v2) and will call the emit2 function to
 * update the framework databases.
 */
void map_phase (ThreadContext *thread_context)
{
  auto* curr_job = thread_context->job;

  while ((curr_job->atomic_counter.load() & (0xffff)) < curr_job->inputVec->size ())
    {
//      sem_wait (&curr_job->map_mutex);
//      auto old_value = curr_job->atomic_counter.load () & (0xffff);
//      curr_job->atomic_counter++;
//      sem_post (&curr_job->map_mutex);
      // todo: change to 64 bit
      auto old_value = (curr_job->atomic_counter++) & (0xffff);
      if (old_value < curr_job->inputVec->size ())
        {
          thread_context->intermediate_vec.clear ();
          InputPair pair = curr_job->inputVec->at (old_value);
          // call map
          curr_job->client->map (pair.first, pair.second, thread_context);
          sort_phase (thread_context);
        }
    }
}




float count_elements(std::vector<IntermediateVec> vec){
  float counter = 0;
  for (auto &v: vec){
      counter += (float) v.size();
    }
  return counter;
}

/* Thread number 0 will combine the vectors into a single data structure.
 * The function uses an atomic counter for counting the number of vectors in it.
 */
void shuffle_phase (ThreadContext *thread_context)
{
  K2 *max_key_element = max_key (thread_context->job);
  float total_elements = count_elements(thread_context->job->intermediate_vecs);
  float elem_counter = 0;

  int i = 0;

  while (max_key_element)
    {
      i++;
      IntermediateVec pairs_vec;
      for (auto &intermediate_vec: thread_context->job->intermediate_vecs)
        {
          if (intermediate_vec.empty()){
              continue;
            }
          if (is_equal (intermediate_vec.back ().first, max_key_element))
            {

              pairs_vec.push_back (intermediate_vec.back ());
              intermediate_vec.pop_back ();
              elem_counter++;
              thread_context->job->shuffle_percentage =
                  PERCENT_COEF * elem_counter / total_elements;
            }
        }
      thread_context->job->shuffled_intermediate_vecs.push_back (pairs_vec);
      max_key_element = max_key (thread_context->job);
    }
}

/*
 * the function pops a vector from the back of the queue and run reduce on it.
 */
void reduce_phase (ThreadContext *thread_context)
{

  while (((thread_context->job->atomic_counter.load() & (0x3fff0000)) >> 16)
         < thread_context->job->shuffled_intermediate_vecs.size ())
    {
      sem_wait (&(thread_context->job->reduce_mutex));
      auto old_value =
      ((thread_context->job->atomic_counter.load() & (0x3fff0000)) >> 16);
      thread_context->job->atomic_counter+= 1 << 16;
      sem_post (&(thread_context->job->reduce_mutex));
      if (old_value < thread_context->job->shuffled_intermediate_vecs.size ())
        {
          IntermediateVec shuffled_vec = thread_context->job->shuffled_intermediate_vecs.at (old_value);
          // call reduce
          thread_context->job->client->reduce (&shuffled_vec, thread_context);
        }
    }
}


/***** External Interface *****/


/*
 * The function receives as input intermediary element (K2, V2) and context.
 * The function saves the intermediary element in the context data structures and
 * updates the number of intermediary elements using atomic counter.
 */
void emit2 (K2 *key, V2 *value, void *context)
{
  auto *thread_context = (ThreadContext *) context; // todo: static cast?
  sem_wait (&thread_context->job->emit2_mutex);
  thread_context->intermediate_vec.push_back ({key, value});
  sem_post (&(thread_context->job->emit2_mutex));
}

/*
 * The function receives as input intermediary element (K3, V3) and context.
 * The function saves the output element in the context data structures (output vector) and updates
 * the number of output elements using atomic counter.
 */
void emit3 (K3 *key, V3 *value, void *context)
{
  auto *thread_context = (ThreadContext *) context; // todo: static cast?
  sem_wait (&thread_context->job->emit3_mutex);
  thread_context->job->outputVec->push_back ({key, value});
  sem_post (&thread_context->job->emit3_mutex);
}



void *worker (void *arg)
{
  auto *thread_context = (ThreadContext *) arg;
  /* Map Phase */

//  thread_context->job->atomic_counter = 1 << 30;
  map_phase (thread_context);

  /* Sort Phase */
//  sort_phase (thread_context);
  thread_context->job->barrier->barrier ();

  /* Shuffle Phase */
  if (thread_context->threadID == 0)
    { // only thread 0
      thread_context->job->atomic_counter += 1 << 30;
      shuffle_phase (thread_context);
      thread_context->job->atomic_counter += 1 << 30;
    }

  thread_context->job->barrier->barrier ();

  /* Reduce Phase */
  reduce_phase (thread_context);

}

JobContext *job_init (const MapReduceClient *client,
                      const InputVec *inputVec,
                      OutputVec *outputVec,
                      int multiThreadLevel)
{
  auto *job_context = new JobContext;
  job_context->job = new Job;
  job_context->job->workers = new pthread_t[multiThreadLevel];
  job_context->job->barrier = new Barrier (multiThreadLevel);

  job_context->job->job_state = {UNDEFINED_STAGE, 0};

  job_context->job->atomic_counter = 0;

  job_context->job->multiThreadLevel = multiThreadLevel;
  job_context->job->client = client;
  job_context->job->inputVec = inputVec;
  job_context->job->outputVec = outputVec;

  sem_init (&job_context->job->map_mutex, 0, 1);
  sem_init (&job_context->job->reduce_mutex, 0, 1);
  sem_init (&job_context->job->emit2_mutex, 0, 1);
  sem_init (&job_context->job->emit3_mutex, 0, 1);
  sem_init (&job_context->job->sort_mutex, 0, 1);

  job_context->contexts = new ThreadContext[multiThreadLevel];

  for (int i = 0; i < multiThreadLevel; ++i)
    {
      job_context->contexts[i].threadID = i;
      job_context->contexts[i].job = job_context->job;
    }
  job_context->job->atomic_counter = 1 << 30;
  job_context->job->shuffle_percentage = 0;

  return job_context;
}

JobHandle startMapReduceJob (const MapReduceClient &client,
                             const InputVec &inputVec, OutputVec &outputVec,
                             int multiThreadLevel)
{
  // job init
  auto *job_context = job_init (&client, &inputVec, &outputVec, multiThreadLevel);

  for (int i = 0; i < multiThreadLevel; ++i)
    {
      pthread_create (job_context->job->workers + i, NULL, worker, job_context->contexts + i);
    }

  return (JobHandle) job_context;

}

void waitForJob (JobHandle job)
{
  auto *curr_job = ((JobContext *) job)->job;
  for (int i = 0; i < curr_job->multiThreadLevel; ++i)
    {
      pthread_join (curr_job->workers[i], NULL);
    }
}
void getJobState (JobHandle job, JobState *state)
{
  auto* curr_job = ((JobContext *) job)->job;
  curr_job->job_state.stage = static_cast<stage_t>(curr_job->atomic_counter.load () >> 30);
  float counter;
  switch (curr_job->job_state.stage)
    {
      case UNDEFINED_STAGE:
        curr_job->job_state.percentage = 0;
        break;
      case MAP_STAGE:
        counter = (float)(curr_job->atomic_counter.load() & (0xffff));
        curr_job->job_state.percentage = PERCENT_COEF*counter / (float)curr_job->inputVec->size();
        break;
      case SHUFFLE_STAGE:
        curr_job->job_state.percentage = curr_job->shuffle_percentage;
        break;
      case REDUCE_STAGE:
        counter = (float)((curr_job->atomic_counter.load() & (0x3fff0000)) >> 16);
        curr_job->job_state.percentage = PERCENT_COEF*counter / (float)curr_job->shuffled_intermediate_vecs.size();
        break;
    }
  JobState last_state = *state;

  if (curr_job->job_state.percentage > 100){
      curr_job->job_state.percentage = 100;
  }
  *state = curr_job->job_state;
}
void closeJobHandle (JobHandle job)
{
  waitForJob (job);
  auto *curr_job= (JobContext *) job;
  sem_destroy (&curr_job->job->map_mutex);
  sem_destroy (&curr_job->job->reduce_mutex);
  sem_destroy (&curr_job->job->emit2_mutex);
  sem_destroy (&curr_job->job->emit3_mutex);
  sem_destroy (&curr_job->job->sort_mutex);

  delete[] curr_job->contexts;

  delete[] curr_job->job->workers;
  delete curr_job->job->barrier;
  delete curr_job->job;

  delete curr_job;
}