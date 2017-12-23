#ifndef  BIGSWITCH_H
#define BIGSWITCH_H

#include <set>
#include "coflow.h"


class Bigswitch{
public:
  int no_ports;
  std::vector<Coflow> coflows;
  std::vector<Coflow> unscheduled_coflows;
  std::vector<Coflow> scheduled_coflows;
  std::vector<Coflow> finished_coflows;
};



#endif
