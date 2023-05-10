#include "MapReduceFramework.h"

std::vector<IntermediateVec> intermediate_vecs;
OutputVec output_vec;

void emit2 (K2* key, V2* value, void* context){
  ((IntermediateVec*)context)->push_back ({key, value});
}

void emit3 (K3* key, V3* value, void* context){
  output_vec.push_back ({key, value});
}

void thread(const MapReduceClient& client,
            const InputVec& inputVec, OutputVec& outputVec){

  // Map Phase
  for (auto pair: inputVec)
    {
      // call map
      IntermediateVec intermediate_vec;
      client.map (pair.first, pair.second, &intermediate_vec);
      intermediate_vecs.push_back (intermediate_vec);
    }
  // -- barrier --

  // Sort Phase
  

  // Shuffle Phase

  // -- barrier --

  // Reduce Phase

}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel)
{
  thread (client, inputVec, outputVec);
}
