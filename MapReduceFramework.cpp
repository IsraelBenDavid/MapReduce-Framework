#include <algorithm>
#include <iostream>
#include "MapReduceFramework.h"

std::vector<IntermediateVec> intermediate_vecs;
OutputVec output_vec;

void emit2 (K2* key, V2* value, void* context){
  ((IntermediateVec*)context)->push_back ({key, value}); // todo: static cast?
}

void emit3 (K3* key, V3* value, void* context){
  ((OutputVec *)context)->push_back ({key, value}); // todo: static cast?
}

bool is_equal(const K2 *first, const K2 *second){
  return !(*first<*second || *second<*first);
}

K2* max_key(){
  bool is_empty = true;
  for (const auto& vec : intermediate_vecs) {
    is_empty = is_empty && vec.empty();
  }
  if (is_empty){
      return nullptr;
  }

  K2* max_value = intermediate_vecs.begin()->back().first;
  for (const auto& vec : intermediate_vecs) {
    if (!vec.empty() && *max_value < *(vec.back().first)) {
            max_value = vec.back().first;
    }
  }
  return max_value;
}



void worker(const MapReduceClient& client,
            const InputVec& inputVec, OutputVec& outputVec){

  // Map Phase

  for (auto pair: inputVec)
    {
      // call map
      IntermediateVec intermediate_vec;
      client.map (pair.first, pair.second, &intermediate_vec);
      intermediate_vecs.push_back (intermediate_vec);

    }
  // -- barrier -- ???

  // Sort Phase
  for (auto intermediate_vec: intermediate_vecs)
    {
      std::sort(intermediate_vec.begin(), intermediate_vec.end(),
                [](const IntermediatePair& a, const IntermediatePair& b) {
                    return *a.first < *b.first;
                });
    }

  // -- barrier --

  // Shuffle Phase

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


  // -- barrier -- ?

  // Reduce Phase
  for (auto& shuffled_vec: shuffled_intermediate_vecs)
    {
      // call reduce
      client.reduce(&shuffled_vec, &outputVec);
    }

}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
  worker(client, inputVec, outputVec);
}
