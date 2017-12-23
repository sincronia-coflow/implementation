#ifndef PRIMAL_DUAL_H
#define PRIMAL_DUAL_H

#include <set>
#include "bigswitch.h"

struct bigswitch{
  int portsize;  //No of inp/out ports
};

struct coflow_2{
  int **D_k;  //matrix holding flow sizes from s to r
  int *v_k;   //vector holding row sums and column sums for D matrix
  int m_k;    //port which has the maximum flow size for the coflow
  int scheduled;   //the order after being scheduled, scheduled == -1 denotes the job/coflow is unscheduled
  int w_k;    // weight of the coflow
};



std::vector<Coflow> primal_dual_ordering(std::vector<Coflow> coflows, Bigswitch bigs);


#endif
