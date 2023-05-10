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
OutputVec output_vec_p;
InputVec input_vec_p;
std::atomic<int> map_counter;
std::atomic<int> reduce_counter;
std::vector<IntermediateVec> shuffled_intermediate_vecs;


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

/* In this phase each thread reads pairs of (k1, v1) from the input vector and calls the map function
 * on each of them. The map function in turn will produce (k2, v2) and will call the emit2 function to
 * update the framework databases.
 */
void map_phase(ThreadContext* thread_context){
  job_state.stage = MAP_STAGE;
  while (map_counter < thread_context->inputVec->size()){
    int old_value = ++map_counter-1; // todo: is it ok?
    IntermediateVec intermediate_vec;
    InputPair pair = thread_context->inputVec->at(old_value);
    // call map
    thread_context->client->map (pair.first, pair.second, &intermediate_vec);
    intermediate_vecs.push_back (intermediate_vec);
  }

  job_state.percentage = 30;
}

/*  each thread will sort its intermediate vector according to the keys within
 */
void sort_phase(ThreadContext* thread_context){
  for (auto intermediate_vec: intermediate_vecs)
    {
      std::sort(intermediate_vec.begin(), intermediate_vec.end(),
                [](const IntermediatePair& a, const IntermediatePair& b) {
                    return *a.first < *b.first;
                });
    }
  job_state.percentage = 60;

}

/* Thread number 0 will combine the vectors into a single data structure.
 * The function uses an atomic counter for counting the number of vectors in it.
 */
void shuffle_phase(ThreadContext* thread_context){
  job_state.stage = SHUFFLE_STAGE;
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
}

/*
 * the function pops a vector from the back of the queue and run reduce on it.
 */
void reduce_phase(ThreadContext* thread_context){
  job_state.stage = REDUCE_STAGE;
  while (reduce_counter < shuffled_intermediate_vecs.size()){
      int old_value = ++reduce_counter-1; // todo: is it ok?
      IntermediateVec shuffled_vec = shuffled_intermediate_vecs.at(old_value);
      // call reduce
      thread_context->client->reduce(&shuffled_vec, thread_context->outputVec);
    }
  job_state.percentage = 100;
}

void* worker(void* arg){

  auto* thread_context = (ThreadContext*) arg;
  /* Map Phase */
  map_phase (thread_context);

  /* Sort Phase */
  sort_phase (thread_context);
  thread_context->barrier->barrier();

  /* Shuffle Phase */
  if (thread_context->threadID == 0){ // only thread 0
    shuffle_phase (thread_context);
  }


  thread_context->barrier->barrier();

  /* Reduce Phase */
  reduce_phase (thread_context);

}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
  // job init
  job_state = {UNDEFINED_STAGE, 0};
  map_counter = 0;
  reduce_counter = 0;


  output_vec_p = outputVec;
  input_vec_p = inputVec;
  pthread_t threads[multiThreadLevel];
  ThreadContext contexts[multiThreadLevel];

//  Barrier barrier(multiThreadLevel);
  auto* barrier = new Barrier(multiThreadLevel);

  for (int i = 0; i < multiThreadLevel; ++i) {
      contexts[i] = {i, barrier, &client, &input_vec_p, &output_vec_p};
    }


  for (int i = 0; i < multiThreadLevel; ++i) {
      pthread_create(threads + i, NULL, worker, contexts + i);
    }
printf("it's not going to end today");
  printf("go home");
//  for (int i = 0; i < multiThreadLevel; ++i)
//    {
//      pthread_join (threads[i], NULL);
//    }

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