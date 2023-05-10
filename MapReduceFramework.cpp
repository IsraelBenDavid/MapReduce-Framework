#include <algorithm>
#include <iostream>
#include <atomic>
#include "Barrier.h"
#include <pthread.h>
#include <cstdio>
#include "MapReduceFramework.h"

/***** Messages *****/

/***** Constants *****/

/***** Typedefs *****/
struct ThreadContext {
    int threadID;
    Barrier* barrier;
    const MapReduceClient* client;
    const InputVec* inputVec;
    OutputVec* outputVec;
} ;

/***** Global Variables *****/
std::vector<IntermediateVec> intermediate_vecs;
JobState job_state;

/***** Internal Interface *****/

/*
 * Returns true if first equals second and false otherwise
 */
bool is_equal(const K2 *first, const K2 *second){
  return !(*first<*second || *second<*first);
}

/*
 * Returns the max key that is in intermediate_vecs. if intermediate_vecs is empty the function
 * returns nullptr.
 */
K2* max_key(){
  // check if intermediate_vecs is empty
  bool is_empty = true;
  for (const auto& vec : intermediate_vecs) {
      is_empty = is_empty && vec.empty();
    }
  if (is_empty){
      return nullptr;
    }

  // find the max key
  // get the first max key
  K2* max_value;
  for (const auto& vec : intermediate_vecs) {
      if(!vec.empty()){
          max_value = vec.back().first;
          break;
      }
    }
  // look for a greater key
  for (const auto& vec : intermediate_vecs) {
      if (!vec.empty() && *max_value < *(vec.back().first)) {
          max_value = vec.back().first;
        }
    }
  return max_value;
}


/***** External Interface *****/



void emit2 (K2* key, V2* value, void* context){
  ((IntermediateVec*)context)->push_back ({key, value}); // todo: static cast?
}

void emit3 (K3* key, V3* value, void* context){
  ((OutputVec *)context)->push_back ({key, value}); // todo: static cast?
}


void* worker(void* arg){

  auto* thread_context = (ThreadContext*) arg;
  /* Map Phase */
  job_state.stage = MAP_STAGE;
  std::atomic<int> counter(0);
  for (auto pair: *(thread_context->inputVec))
    {
      // call map
      IntermediateVec intermediate_vec;
      thread_context->client->map (pair.first, pair.second, &intermediate_vec);
      intermediate_vecs.push_back (intermediate_vec);
//      counter++;
//      job_state.percentage = (float)counter*100/(float)thread_context->inputVec->size();
    }
    job_state.percentage = 30;
  // -- barrier -- ???

  /* Sort Phase */
  for (auto intermediate_vec: intermediate_vecs)
    {
      std::sort(intermediate_vec.begin(), intermediate_vec.end(),
                [](const IntermediatePair& a, const IntermediatePair& b) {
                    return *a.first < *b.first;
                });
    }
  job_state.percentage = 60;

  // -- barrier -- !!

  /* Shuffle Phase */
  job_state.stage = SHUFFLE_STAGE;
  std::vector<IntermediateVec> shuffled_intermediate_vecs;
  K2* max_key_element = max_key();
  while (max_key_element){
    IntermediateVec pairs_vec;
      for (auto& intermediate_vec: intermediate_vecs)
        {
          if (is_equal (intermediate_vec.back().first, max_key_element)){
              pairs_vec.push_back(intermediate_vec.back());
              intermediate_vec.pop_back();
          }
        }
      shuffled_intermediate_vecs.push_back(pairs_vec);
      max_key_element = max_key ();

  }
  job_state.percentage = 80;


  // -- barrier -- ?

  /* Reduce Phase */
  job_state.stage = REDUCE_STAGE;
  for (auto& shuffled_vec: shuffled_intermediate_vecs)
    {
      // call reduce
      thread_context->client->reduce(&shuffled_vec, thread_context->outputVec);
    }
  job_state.percentage = 100;
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
  // job init
  job_state = {UNDEFINED_STAGE, 0};

  pthread_t threads[multiThreadLevel];
  ThreadContext contexts[multiThreadLevel];
  Barrier barrier(multiThreadLevel);

  for (int i = 0; i < multiThreadLevel; ++i) {
      contexts[i] = {i, &barrier, &client, &inputVec, &outputVec};
    }


  for (int i = 0; i < 1; ++i) {
      pthread_create(threads + i, NULL, worker, contexts + i);
    }

  for (int i = 0; i < 1; ++i) {
      pthread_join(threads[i], NULL);
    }
  return (JobHandle) &job_state;

}


void waitForJob(JobHandle job);
void getJobState(JobHandle job, JobState* state){
  *state = *((JobState*) job);
}
void closeJobHandle(JobHandle job){
  int x = 1;
  return;
}