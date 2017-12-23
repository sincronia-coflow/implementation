#include "coflow.h"
//#include <climits>
#include <vector>

Coflow::Coflow(int coflow_id, double release_date, std::vector<Flow> flows, int no_senders, int no_receivers){
  this->coflow_id = coflow_id;
  this->release_date = release_date;
  this->flows = flows;
  this->deadline = 10000000;
  this->priority = -1;
  this->processing_time = -1;
  this->start_time = -1;
  this->completion_time = -1;
  double size = 0;
  for(int i=0;i<this->flows.size();i++){
    size += this->flows[i].size;
  }
  this->size = size;
  this->size_left = size;
  double length = -1;
  for(int i=0;i<flows.size();i++){
    if(flows[i].size > length){
      length = flows[i].size;
    }
  }
  this->width = std::min(no_senders,no_receivers);
  this->length = length;
  this->weight = 1;
  this->oracle_time = -1;
}
