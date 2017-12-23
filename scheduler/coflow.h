#ifndef COFLOW_H
#define COFLOW_H

#include "flow.h"

#include <vector>

class Coflow{
public:
  int coflow_id;
  // int release_date;
  double release_date;
  std::vector<Flow> flows;
  int deadline;
  int priority;
  double processing_time;
  double start_time;
  double completion_time;
  double size;
  double size_left;
  int width;
  int length;
  int weight;
  double oracle_time;
  Coflow(int coflow_id, double release_date, std::vector<Flow> flows, int no_senders, int no_receivers);

  /*bool operator< (const Coflow & coflow_object) const{     //overloading < operator for set comparison
    return (this->coflow_id < coflow_object.coflow_id);
  }*/


};




#endif
