#ifndef SINCRONIA_SCHEDULER_H
#define SINCRONIA_SCHEDULER_H

#include <iostream>
#include <map>
#include <vector>
#include <ctime>
#include <iomanip>
#include <sys/time.h>

#include "coflow.h"
#include "onlinescheduler.h"
#include "bigswitch.h"
#include "primal_dual.h"
#include <math.h>
#include <algorithm>
#include <kj/time.h>
#include "sincronia-coflow.hpp"

Bigswitch B;
long getMicrotime(){
	struct timeval currentTime;
	gettimeofday(&currentTime, NULL);
	return currentTime.tv_sec * (int)1e6 + currentTime.tv_usec;
}
std::vector<Coflow> inadmissible_coflows;

bool heuristic(const Coflow & c1, const Coflow & c2){
  return c1.size_left < c2.size_left;
}

bool heuristic_comp(const Coflow & c1, const Coflow & c2)
{
  double counter = (double) getMicrotime();
  double x = 128*100000;
  if(((((double)counter)+x - ((double)c1.release_date))/c1.oracle_time) == ((((double)counter) + x- ((double)c2.release_date))/c2.oracle_time)){
    return c1.release_date < c2.release_date;
  }
  else{
    return (c1.oracle_time/(((double)counter) + x - ((double)c1.release_date))) < (c2.oracle_time/(((double)counter) +x - ((double)c2.release_date)));
  }

}


// ticks every second
struct coflowTimeUnit{
	long epoch_start;
	long timeStep;
}; // wall clock seconds since epoch start

// TODO a Phase contains scheduling Epochs
struct coflowTimeUnit sincronia_epoch_reset() {
    return {
	    .epoch_start = getMicrotime(),
        .timeStep = 0,
    };
}

void sincronia_update_time(struct coflowTimeUnit *now) { //update time updates the timeStep to the microseconds till start of the epoch
	long wall = getMicrotime();
	long microseconds = wall - now->epoch_start;
	now->timeStep = microseconds;
};

kj::Duration sincronia_duration(uint32_t time_steps) {
	return time_steps * 100 * kj::MILLISECONDS;
}

class CoflowScheduler {
public:
    coflowTimeUnit current_time;

    CoflowScheduler() {
        current_time = sincronia_epoch_reset();
    };

    // Decide whether to run the scheduler now
    virtual kj::Duration time_to_schedule() = 0;

    // Schedule the coflows by shuffling the order within the vector.
    virtual void schedule(std::vector<coflow*> *to_schedule) = 0;


};

class DummyScheduler : public CoflowScheduler {
public:
    coflowTimeUnit next_phase_time;
    DummyScheduler() : CoflowScheduler() {
        this->next_phase_time = this->current_time;
    };

    // The dummy scheduler runs every 5 seconds
    virtual kj::Duration time_to_schedule() {
        this->next_phase_time = this->current_time;
        this->next_phase_time.timeStep += 5;
        return sincronia_duration(5);
    };

    // The dummy scheduler picks an arbitrary order - the one it was handed.
    virtual void schedule(std::vector<coflow*> *to_schedule) {
        sincronia_update_time(&current_time);
        if (current_time.timeStep < next_phase_time.timeStep) {
            std::cout
                << "[scheduler] DummyScheduler, work conservation schedule: "
                << "scheduling " << to_schedule->size() << " coflows\n";
        } else {
            std::cout
                << "[scheduler] DummyScheduler, phased schedule: "
                << "scheduling " << to_schedule->size() << " coflows\n";
        }

        return;
    };
};

class OnlineScheduler : public CoflowScheduler {
public:
    int epoch_no;
    int MAX_RESET_COUNTER ;
    double abs_wall_start;
    double ACCESS_LINK_BANDWIDTH;
    std::vector<Coflow> scheduled_coflows; //scheduled coflows, priorities not to be disturbed for these coflows, if present in the ready queue, will only be updated if the first if condition is true
    OnlineScheduler() : CoflowScheduler() {
      this->epoch_no = -2;  //default epoch counter = -1
      this->abs_wall_start = (this->current_time).epoch_start; //wall start time in microseconds
      this->MAX_RESET_COUNTER = 8; //8 means one big epoch is of size 2^8 time units (1 time unit here is 100millisecond)
      this->ACCESS_LINK_BANDWIDTH = 1; //Gbps
      B.no_ports = 20;
      std::cout << std::setprecision(20) << "recorded abs_wall_start_time: " << abs_wall_start << '\n';
    };



    void convert_schedule(std::vector<Coflow> vec_cof, std::vector<coflow*> *new_vec_cof){
      //convert the coflow vector into appropriate ordering for the API
      //job_id of both the vectors should be in the same order
      for(int i=0;i<vec_cof.size();i++){
        int job_id_to_search = vec_cof[i].coflow_id;
        //search the index of the job_id in the second vector
        for(int j=i;j<vec_cof.size();j++){
          if(vec_cof[i].coflow_id==(*((*new_vec_cof)[j])).job_id){
            coflow* tempcoflow = (*new_vec_cof)[i];
            (*new_vec_cof)[i] = (*new_vec_cof)[j];
            (*new_vec_cof)[j] = tempcoflow;
          }
        }
      }

    }
    std::vector<Coflow> convert_coflow_structure(std::vector<coflow*> *to_schedule){
      //create coflows to format supported by functions, from all the ready coflows at the moment
      std::vector<Coflow> input_coflows;
      input_coflows.clear();

      for(int i=0;i<to_schedule->size();i++){
        coflow *C = ((*to_schedule)[i]);
        std::map<uint32_t, flow> *ready_flows = C->ready_flows;
        std::map<uint32_t, flow> :: iterator itr;
        std::vector<Flow> flows;
        for(itr = (*ready_flows).begin(); itr!=(*ready_flows).end();itr++){
          flow F = itr->second;
          Flow f = Flow(itr->first,C->job_id,F.info.size,F.from,F.to);
          flows.push_back(f);
        }

        int no_senders = 0, no_receivers = 0;
        std::vector<int> senders, receivers;
        for(int j=0;j<B.no_ports;j++){
          senders.push_back(0);
          receivers.push_back(0);
        }
        for(int j=0;j<flows.size();j++){
          senders[flows[j].sender_id]++;
          receivers[flows[j].receiver_id]++;
        }
        for(int j=0;j<senders.size();j++){
          if(senders[j]){
            no_senders++;
          }
        }
        for(int j=0;j<receivers.size();j++){
          if(receivers[j]){
            no_receivers++;
          }
        }
        Coflow C_converted = Coflow(C->job_id, C->wall_start, flows, no_senders, no_receivers);
        C_converted.oracle_time = C->bottleneck_size;
        input_coflows.push_back(C_converted);
      }

      return input_coflows;
    }

    int find_epoch_end_time_steps(int epoch_no){
      //time to let the scheduler know whether we are at the start of the epoch, or just doing work conservation shceduling
      //epoch_no is the current epoch
      if(epoch_no==-1){
        return 0;
      }
      if(epoch_no==0){
        return 2;
      }
      return pow(2,epoch_no);
    }

    int find_deadline_time_steps_for_epoch(){
      if(epoch_no==0){
        return 1;
      }
      else{
        return pow(2,(epoch_no-1));
      }
    }

    void display_coflows(std::vector<Coflow> vec_cof){
      if(vec_cof.size()==0){
        std::cout << "no coflow " << '\n';
      }
      for(int i=0;i<vec_cof.size();i++){
        std::cout << " ID: " <<  vec_cof[i].coflow_id;
      }
      std::cout << " " << '\n';
    }


    virtual kj::Duration time_to_schedule() {
	std::cout << std::setprecision(20) << "time to schedule called at time: " << getMicrotime() << '\n';
      epoch_no = epoch_no + 1;
      if(epoch_no == MAX_RESET_COUNTER){
        epoch_no = 0;
      }
      std::cout << "Starting epoch: " << epoch_no << '\n';
      current_time = sincronia_epoch_reset();
      return sincronia_duration(find_epoch_end_time_steps(epoch_no));
    };

    // The scheduler orders the ready coflows
    virtual void schedule(std::vector<coflow*> *to_schedule) {
        std::cout << std::setprecision(20) << "time of calling schedule(): " << getMicrotime() << '\n';
        sincronia_update_time(&current_time); //this will give access to the current time wrt the current spoch start time

        std::vector<Coflow> vector_coflows, work_cons_coflows;
        vector_coflows.clear();
        work_cons_coflows.clear();
        std::vector<Coflow> input_coflows = convert_coflow_structure(to_schedule);
        int epoch_end_time_steps = find_epoch_end_time_steps(epoch_no);
        std::cout << "current epoch: "  << epoch_no << '\n';
        std::cout << std::setprecision(20)<< "epoch end time steps (microsecond): " <<  epoch_end_time_steps*100000 << '\n';
        std::cout << std::setprecision(20)<< "current time steps elapsed (microsecond): " <<  current_time.timeStep << '\n';
        //gives the end time of the current epoch given by epoch_no

        if(current_time.timeStep <200){ //start of the epoch
          std::cout << "In loop 1:" << '\n';
          //find admissible coflows for the current epoch
          //update both scheduled coflows and work conserving coflows
          //generate admissible coflows
          double cutoff_time = (current_time.epoch_start - abs_wall_start)/2 + abs_wall_start;
          std::cout << std::setprecision(20) << "Cutoff time: " << cutoff_time << '\n';
          std::vector<Coflow> candidate_coflows = find_candidate_coflow_set(input_coflows,cutoff_time);
          std::cout << "Candidate coflows: " << '\n';
          for(int i=0;i<candidate_coflows.size();i++){
            std::cout << " ID: " << candidate_coflows[i].coflow_id;
          }
          std::cout << " " << '\n';
	  std::vector<Coflow> left_out_coflows;
	  left_out_coflows.clear();
	  for(int i=0;i<input_coflows.size();i++){
		int flag = 1;
	  	for(int j=0;j<candidate_coflows.size();j++){
			if(candidate_coflows[j].coflow_id == input_coflows[i].coflow_id){
				flag = 0;
			}
		}
		if(flag == 1){
		left_out_coflows.push_back(input_coflows[i]);
		}
	  }
          long deadline = find_deadline_time_steps_for_epoch()*100000; //deadline in microseconds
          std::cout << "deadline: " << deadline << '\n';
          std::vector<Coflow> admitted_coflows = generate_online_admissible_set_coflows(candidate_coflows,deadline);
          std::vector<Coflow> vector_coflows = primal_dual_ordering(admitted_coflows,B);
          scheduled_coflows = vector_coflows; //these priorities won't change until the next epoch
          std::vector<Coflow> work_cons_coflows = inadmissible_coflows;
	  for(int i=0;i<left_out_coflows.size();i++){
		work_cons_coflows.push_back(left_out_coflows[i]);
	  }
          std::stable_sort(work_cons_coflows.begin(),work_cons_coflows.end(),heuristic_comp);
          for(int i=0;i<work_cons_coflows.size();i++){
            vector_coflows.push_back(work_cons_coflows[i]);
          }
        }
        else{
          std::cout << "loop 2" << '\n';
          //update only work conserving coflow priorities
          //first remove the coflows which are part of the scheduled coflows
          std::cout << "Scheduled coflows" << '\n';
          display_coflows(scheduled_coflows);
          std::cout << "input_coflows" << '\n';
          display_coflows(input_coflows);
          std::cout << "vector_coflows" << '\n';
          display_coflows(vector_coflows);
          for(int i=0;i<scheduled_coflows.size();i++){
            for(int j=input_coflows.size()-1;j>0;j--){
              if(scheduled_coflows[i].coflow_id == input_coflows[j].coflow_id){
                vector_coflows.push_back(input_coflows[j]);
                input_coflows.erase(input_coflows.begin()+j);
              }
            }
          }

          work_cons_coflows = input_coflows; //only work conserving coflows left in input coflows now
          std::stable_sort(work_cons_coflows.begin(),work_cons_coflows.end(),heuristic_comp);
          for(int i=0;i<work_cons_coflows.size();i++){
            vector_coflows.push_back(work_cons_coflows[i]);
          }

        }
        //convert the ordering into shuffling the required vector of coflow pointers
        //use the coflow IDs for the same
        convert_schedule(vector_coflows,to_schedule);

        return;
    };
  };


#endif
