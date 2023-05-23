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
#define STAGE_LOC << 62
#define TOTAL_LOC << 31
#define GET_STAGE_BITS >> 62
#define GET_TOTAL_BITS >> 31 & (0x7fffffff)
#define GET_COUNT_BITS & (0x7fffffff)

/***** Typedefs *****/


typedef struct Job {
    pthread_t *workers;
    Barrier *barrier;
    sem_t map_mutex, reduce_mutex, emit2_mutex, emit3_mutex, sort_mutex;
    std::vector<IntermediateVec> intermediate_vecs;
    std::vector<IntermediateVec> shuffled_intermediate_vecs;
    JobState job_state;
    std::atomic<uint64_t> new_atomic_counter;
    int multiThreadLevel;
    const MapReduceClient *client;
    const InputVec *inputVec;
    OutputVec *outputVec;
    bool called_join;
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
      if (!vec.empty () && *max_value < *(vec.back ().first))
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


  while ((curr_job->new_atomic_counter.load() GET_COUNT_BITS) < (curr_job->new_atomic_counter GET_TOTAL_BITS))
    {
//      sem_wait (&curr_job->map_mutex); todo: is needed??
      auto old_value = curr_job->new_atomic_counter++ GET_COUNT_BITS;
//      sem_post (&curr_job->map_mutex);
      if (old_value < curr_job->inputVec->size ())
        {
//          thread_context->intermediate_vec.clear ();
          InputPair pair = curr_job->inputVec->at (old_value);
          // call map
          curr_job->client->map (pair.first, pair.second, thread_context);

        }
    }
    if(!thread_context->intermediate_vec.empty()) sort_phase (thread_context);
}




size_t count_elements(std::vector<IntermediateVec> vec){
  size_t counter = 0;
  for (auto &v: vec){
      counter += v.size();
    }
  return counter;
}

/* Thread number 0 will combine the vectors into a single data structure.
 * The function uses an atomic counter for counting the number of vectors in it.
 */
void shuffle_phase (ThreadContext *thread_context)
{
  auto* curr_job = thread_context->job;
  K2 *max_key_element = max_key (thread_context->job);



  while (max_key_element)
    {
      IntermediateVec pairs_vec;
      for (auto &intermediate_vec: thread_context->job->intermediate_vecs)
        {
          if (intermediate_vec.empty()) continue;
          while (!intermediate_vec.empty() &&
              is_equal (intermediate_vec.back ().first, max_key_element)){
              pairs_vec.push_back (intermediate_vec.back ());
              intermediate_vec.pop_back ();
              curr_job->new_atomic_counter++;
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
  auto* curr_job = thread_context->job;
  while ((curr_job->new_atomic_counter.load() GET_COUNT_BITS) < (curr_job->new_atomic_counter.load() GET_TOTAL_BITS))
    {
//      sem_wait (&(thread_context->job->reduce_mutex));
//      std::cout << "counter " << get_atomic_values (curr_job->new_atomic_counter.load()).counter << std::endl;
//      std::cout << "total " << get_atomic_values (curr_job->new_atomic_counter.load()).total << std::endl;
//      sem_post (&(thread_context->job->reduce_mutex));
      auto old_value = curr_job->new_atomic_counter++ GET_COUNT_BITS;
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
  auto *thread_context = (ThreadContext *) context;
//  sem_wait (&thread_context->job->emit2_mutex); // todo: maybe we dont need mutex
  thread_context->intermediate_vec.push_back ({key, value});
//  sem_post (&(thread_context->job->emit2_mutex));
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

void reset_counter(Job* job, stage_t stage, uint64_t total){
  job->new_atomic_counter = ((uint64_t) stage STAGE_LOC) |
                                         (total TOTAL_LOC);
}

void *worker (void *arg)
{
  auto *thread_context = (ThreadContext *) arg;
  /* Map Phase */

//  thread_context->job->atomic_counter = 1 << 30;

  map_phase (thread_context);

  thread_context->job->barrier->barrier ();

  /* Shuffle Phase */
  if (thread_context->threadID == 0)
    { // only thread 0

      reset_counter (thread_context->job, SHUFFLE_STAGE,
                     count_elements(thread_context->job->intermediate_vecs));
      shuffle_phase (thread_context);
      reset_counter (thread_context->job, REDUCE_STAGE,
                     thread_context->job->shuffled_intermediate_vecs.size ());
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

  job_context->job->called_join = false;

  reset_counter (job_context->job, MAP_STAGE, job_context->job->inputVec->size ());

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
  if(curr_job->called_join) return;
  for (int i = 0; i < curr_job->multiThreadLevel; ++i)
    {
      pthread_join (curr_job->workers[i], NULL);
    }
  curr_job->called_join = true;
}
void getJobState (JobHandle job, JobState *state)
{
  auto* curr_job = ((JobContext *) job)->job;
  auto values = get_atomic_values (curr_job->new_atomic_counter.load ());
  curr_job->job_state.stage = static_cast<stage_t>(values.stage);
  curr_job->job_state.percentage = PERCENT_COEF * (float)values.counter / (float) values.total;

  if (curr_job->job_state.stage == UNDEFINED_STAGE){
      curr_job->job_state.percentage = 0;
  }
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