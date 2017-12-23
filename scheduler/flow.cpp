#include "flow.h"

Flow::Flow(int flow_id, int coflow_id, double size, int sender_id, int receiver_id){
  this->flow_id = flow_id;
  this->coflow_id = coflow_id;
  this->size = size;
  this->size_left = size;
  this->sender_id = sender_id;
  this->receiver_id = receiver_id;
  this->start_time = -1;
  this->completion_time = -1;
  this->processing_time = -1;
  this->scheduled = 0;
  this->bw_allocated = 0;

}
