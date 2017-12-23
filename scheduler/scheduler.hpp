#ifndef SINCRONIA_SCHEDULER_H
#define SINCRONIA_SCHEDULER_H

#include <iostream>
#include <map>
#include <vector>
#include <ctime>
#include <iomanip>

#include "coflow.h"
#include "onlinescheduler.h"
#include "bigswitch.h"
#include "primal_dual.h"
#include <math.h>
#include <algorithm>
#include <kj/time.h>
#include "sincronia-coflow.hpp"

Bigswitch B;
std::vector<Coflow> inadmissible_coflows;
// bool heuristic(const Coflow & c1, const Coflow & c2){
//   double counter = current_time.epoch_start + current_time.timeStep;
//   if(((((double)counter)+1.0 - ((double)c1.release_date))/c1.oracle_time) == ((((double)counter) + 1.0- ((double)c2.release_date))/c2.oracle_time)){
//     return c1.release_date < c2.release_date;
//   }
//   else{
//     return (c1.oracle_time/(((double)counter) + 1.0 - ((double)c1.release_date))) < (c2.oracle_time/(((double)counter) +1.0 - ((double)c2.release_date)));
//   }
// }

bool heuristic(const Coflow & c1, const Coflow & c2){
  return c1.size_left < c2.size_left;
}

bool heuristic_comp(const Coflow & c1, const Coflow & c2)
{
  // std::cout << "x: " << x << '\n';
  double counter = time(NULL);
  double x = 32;
  if(((((double)counter)+x - ((double)c1.release_date))/c1.oracle_time) == ((((double)counter) + x- ((double)c2.release_date))/c2.oracle_time)){
    return c1.release_date < c2.release_date;
  }
  else{
    return (c1.oracle_time/(((double)counter) + x - ((double)c1.release_date))) < (c2.oracle_time/(((double)counter) +x - ((double)c2.release_date)));
  }

}


// ticks every second
struct coflowTimeUnit{
    time_t epoch_start;
    uint32_t timeStep;
}; // wall clock seconds since epoch start

// TODO a Phase contains scheduling Epochs
struct coflowTimeUnit sincronia_epoch_reset() {
    return {
        .epoch_start = time(NULL),
        .timeStep = 0,
    };
}

void sincronia_update_time(struct coflowTimeUnit *now) {
    time_t wall = time(NULL);
    double seconds = difftime(wall, now->epoch_start);
    now->timeStep = (uint32_t) seconds;
};

kj::Duration sincronia_duration(uint32_t time_steps) {
    return time_steps * kj::SECONDS;
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
    // B.no_ports = 150 //TODO enter the number of machines here
    std::vector<Coflow> scheduled_coflows; //scheduled coflows, priorities not to be disturbed for these coflows, if present in the ready queue, will only be updated if the first if condition is true
    OnlineScheduler() : CoflowScheduler() {
      this->epoch_no = -2;  //default epoch counter = -1
      this->abs_wall_start = (this->current_time).epoch_start;
      this->MAX_RESET_COUNTER = 8;
      this->ACCESS_LINK_BANDWIDTH = 1; //Gbps
      B.no_ports = 20;
      std::cout << "recorded abs_wall_start_time: " << abs_wall_start << '\n';
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
          // std::cout << "here: " << F.info.size << " " << '\n';
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
      std::cout << "time to schedule called at time: " << time(NULL) << '\n';
      epoch_no = epoch_no + 1;
      if(epoch_no == MAX_RESET_COUNTER){
        epoch_no = 0;
      }
      std::cout << "Starting epoch: " << epoch_no << '\n';
      current_time = sincronia_epoch_reset();
      return sincronia_duration(find_epoch_end_time_steps(epoch_no));
      // std::cout << "time to schedule called at time: " << time(NULL) << '\n';
      // std::cout << "currently at the end of epoch: " << epoch_no<< '\n';
      // if(epoch_no+1 == MAX_RESET_COUNTER){
      //   std::cout << "duration returned is duration of epoch(will be reset): " << 0 << " duration: "<<  find_epoch_end_time_steps(0) << '\n';
      // }
      // else{
      //   std::cout << "duration returned is duration of epoch: " << epoch_no + 1 << " duration: "<<  find_epoch_end_time_steps(epoch_no+1) << '\n';
      // }
      //
      // if(epoch_no==-1){
      //   epoch_no = epoch_no + 1;
      // }
      // return sincronia_duration(find_epoch_end_time_steps(epoch_no+1));
      //   // return time_to_schedule * kj::SECONDS;
    };

    // The scheduler orders the ready coflows
    virtual void schedule(std::vector<coflow*> *to_schedule) {
        std::cout << "time of calling schedule(): " << time(NULL) << '\n';
        sincronia_update_time(&current_time); //this will give access to the current time wrt the current spoch start time

        std::vector<Coflow> vector_coflows, work_cons_coflows;
        vector_coflows.clear();
        work_cons_coflows.clear();
        // std::cout << "Input scheduler" << '\n';
        std::vector<Coflow> input_coflows = convert_coflow_structure(to_schedule);
        std::cout << "input coflow characteristics: " << '\n';
        for(int i=0;i<input_coflows.size();i++){
          std::cout << "Coflow ID: " << input_coflows[i].coflow_id << " start time: " << input_coflows[i].release_date << " oracle_time: " << input_coflows[i].oracle_time;
          std::cout << " flows: ";
          for(int j=0;j<input_coflows[i].flows.size();j++){
            std::cout << " flow " << j+1 << " : from: " <<  input_coflows[i].flows[j].sender_id << " size left: " << input_coflows[i].flows[j].size_left << " to: " << input_coflows[i].flows[j].receiver_id;
          }
          std::cout << '\n';
        }
        // for(int i=0;i<input_coflows.size();i++){
        //   std::cout << "ID: " << input_coflows[i].coflow_id << ' ';
        // }
        // std::cout << " " << '\n';
        int epoch_end_time_steps = find_epoch_end_time_steps(epoch_no);
        std::cout << "current epoch: "  << epoch_no << '\n';
        std::cout << "epoch end time steps: " <<  epoch_end_time_steps << '\n';
        std::cout << "current time steps elapsed: " <<  current_time.timeStep << '\n';
        //gives the end time of the current epoch given by epoch_no

        // if(current_time.timeStep >= epoch_end_time_steps){
        //   std::cout << "In loop 1:" << '\n';
        //   epoch_no = epoch_no + 1;
        //   current_time = sincronia_epoch_reset();
        //   if(epoch_no==MAX_RESET_COUNTER){
        //     epoch_no = 0;
        //   }
        if(current_time.timeStep == 0){ //start of the epoch
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
          int deadline = find_deadline_time_steps_for_epoch();
          std::cout << "deadline: " << deadline << '\n';
		std::cout << "a" << '\n';
          std::vector<Coflow> admitted_coflows = generate_online_admissible_set_coflows(candidate_coflows,deadline);
          //TODO change constraint to match access link BW, unit of coflow size and time unit
		std::cout << "b" << '\n';
		display_coflows(admitted_coflows);
          std::vector<Coflow> vector_coflows = primal_dual_ordering(admitted_coflows,B);
		std::cout << "c" << '\n';
          scheduled_coflows = vector_coflows; //these priorities won't change until the next epoch
          //inadmissible coflows will be work conserving coflows
		std::cout << "d" << '\n';
          std::vector<Coflow> work_cons_coflows = inadmissible_coflows;
	  for(int i=0;i<left_out_coflows.size();i++){
		work_cons_coflows.push_back(left_out_coflows[i]);
	  }
		std::cout << "e" << '\n';
          std::stable_sort(work_cons_coflows.begin(),work_cons_coflows.end(),heuristic_comp);
          for(int i=0;i<work_cons_coflows.size();i++){
            vector_coflows.push_back(work_cons_coflows[i]);
          }
		std::cout << "f" << '\n';
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
          std::cout << "input_coflows after removing scheduled coflows" << '\n';
          display_coflows(input_coflows);
          // std::cout << "vector_coflows after removing scheduled_coflows" << '\n';
          // display_coflows(vector_coflows);

          work_cons_coflows = input_coflows; //only work conserving coflows left in input coflows now
          std::stable_sort(work_cons_coflows.begin(),work_cons_coflows.end(),heuristic_comp);
          for(int i=0;i<work_cons_coflows.size();i++){
            vector_coflows.push_back(work_cons_coflows[i]);
          }
          std::cout << "sorted work_cons_coflows" << '\n';
          std::cout << "final vector_coflows with scheduled + work_cons_coflows" << '\n';
          display_coflows(vector_coflows);

        }
        //convert the ordering into shuffling the required vector of coflow pointers
        //use the coflow IDs for the same
        // std::cout << "Output scheduler" << '\n';
        // for(int i=0;i<vector_coflows.size();i++){
        //   std::cout << "ID: " << vector_coflows[i].coflow_id << ' ';
        // }
        // std::cout << " " << '\n';
        convert_schedule(vector_coflows,to_schedule);

        //
        return;
    };
  };


#endif
