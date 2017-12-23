#include "onlinescheduler.h"
#include "gurobi_c++.h"
#include <iostream>
#include "common.h"

int CONVERSION_FACTOR = 134217728; //assuming 1 Gbps Access link bandwidth, coflow size in Bytes, deadline in seconds, data sent in 1 sec = 1 gigabit = 1024*1024*1024/8 bytes
std::vector<Coflow> generate_online_admissible_set_coflows(std::vector<Coflow> unscheduled_coflows,double deadline){
  std::vector<Coflow> selected_coflow_vector;
  std::vector<Coflow> unselected_coflow_vector;
  try{
  GRBEnv env = GRBEnv();

  GRBModel model = GRBModel(env);
  // env.set("OutputFlag",0);

  //Create variables
  std::vector<GRBVar> x;
  for(int i=0;i<unscheduled_coflows.size();i++){
    x.push_back(model.addVar(0.0, 1.0, 0.0, GRB_CONTINUOUS));
  }

  //set objective

  GRBLinExpr expr = 0;
  for(int i=0;i<unscheduled_coflows.size();i++){
    GRBLinExpr term = 1 - x[i];
    expr += unscheduled_coflows[i].weight*term;
  }

  model.setObjective(expr, GRB_MINIMIZE);
  std::vector<GRBLinExpr> constraint_vector;
  for(int l=0;l<B.no_ports;l++){
    GRBLinExpr constraint = 0;
    for(int i=0;i<unscheduled_coflows.size();i++){
      double p_li = 0;
      for(int j=0;j<unscheduled_coflows[i].flows.size();j++){
        if(unscheduled_coflows[i].flows[j].sender_id == l){
          p_li += unscheduled_coflows[i].flows[j].size_left;
        }
      }
      constraint += p_li*x[i];
    }
    constraint_vector.push_back(constraint);
  }
  for(int l=0;l<B.no_ports;l++){
    GRBLinExpr constraint = 0;
    for(int i=0;i<unscheduled_coflows.size();i++){
      double p_li = 0;
      for(int j=0;j<unscheduled_coflows[i].flows.size();j++){
        if(unscheduled_coflows[i].flows[j].receiver_id == l){
          p_li += unscheduled_coflows[i].flows[j].size_left;
        }
      }
      constraint += p_li*x[i];
    }
    constraint_vector.push_back(constraint);
  }
  for(int l=0;l<2*B.no_ports;l++){
    model.addConstr(constraint_vector[l],GRB_LESS_EQUAL,deadline*CONVERSION_FACTOR);
  }
  model.optimize();

  //get the solution vector
  std::vector<double> vector_solution;
  std::vector<int> vector_select;

  for(int i=0;i<unscheduled_coflows.size();i++){
    vector_solution.push_back(-1);
    vector_select.push_back(-1);
  }
  std::vector<int> v;
  for(int i=0;i<unscheduled_coflows.size();i++){
    vector_solution[i] = x[i].get(GRB_DoubleAttr_X);
    // std::cout << "vector_solution: " << vector_solution[i] << '\n';
    if(vector_solution[i]<0.5){
      vector_select[i] = 0;
    }
    else{
      vector_select[i] = 1;
    }
  }

  for(int i=0;i<unscheduled_coflows.size();i++){
    if(vector_select[i]==1){
      selected_coflow_vector.push_back(unscheduled_coflows[i]);
      // B.scheduled_coflows.push_back(unscheduled_coflows[i]);
    }
    else{
      unselected_coflow_vector.push_back(unscheduled_coflows[i]);
      // B.unscheduled_coflows.push_back(unscheduled_coflows[i]);
      // work_conservation_coflows.push_back(unscheduled_coflows[i]);
    }
  }
  }catch(GRBException e) {
    std::cout << "Error code = " << e.getErrorCode() << std::endl;
    std::cout << e.getMessage() << std::endl;
  } catch(...) {
    std::cout << "Exception during optimization" << std::endl;
  }

  inadmissible_coflows = unselected_coflow_vector;
  return selected_coflow_vector;
}

std::vector<Coflow> find_candidate_coflow_set(std::vector<Coflow> C, double deadline){
  if(deadline==-1){
    B.unscheduled_coflows.clear();
    return C;
  }

  std::vector<Coflow> R_k;
  std::vector<Coflow> new_unscheduled_vector;
  for(int i=0;i<C.size();i++){
    // std::cout << "Cid: " << C[i].coflow_id << " release_date: " << C[i].release_date << " deadline: " << deadline << '\n';
    if(C[i].release_date <= deadline){
      R_k.push_back(C[i]);
      // B.scheduled_coflows.push_back(C[i]);
    }
    else{
      new_unscheduled_vector.push_back(C[i]);
    }
  }
  B.unscheduled_coflows = new_unscheduled_vector;
  return R_k;
}
