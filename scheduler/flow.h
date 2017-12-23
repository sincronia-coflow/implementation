#ifndef FLOW_H
#define FLOW_H

class Flow{
public:
  int flow_id;
  int coflow_id;
  double size;
  int sender_id;
  int receiver_id;
  double bw_allocated;
  double processing_time;
  double start_time;
  double completion_time;
  double size_left;
  int scheduled;

  Flow(int flow_id, int coflow_id, double size, int sender_id, int receiver_id);
};


#endif
