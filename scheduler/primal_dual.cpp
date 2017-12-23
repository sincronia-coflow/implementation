
#include <stdlib.h>
#include <stdio.h>

#include <set>
#include <vector>

#include "bigswitch.h"
#include "coflow.h"
#include "flow.h"
#include "primal_dual.h"



std::vector<Coflow> primal_dual_ordering(std::vector<Coflow> coflows, Bigswitch bigs) {


  bigswitch B;
  B.portsize = bigs.no_ports;


  //FILE *F;
  //F = fopen("input.txt","r");         //Create and open file for input

  int numCoflows;
  //fscanf(F,"%d",&numCoflows);          //First line in file contains the number of Coflows
  numCoflows = coflows.size();

  struct coflow_2 *J = (struct coflow_2*) malloc(numCoflows * sizeof(struct coflow_2));   //allocate memory for array of coflows

  int i;
  for (i = 0; i < numCoflows; i++) {

    //fscanf(F,"%d",&J[i].w_k);
    J[i].w_k = coflows[i].weight;
    J[i].scheduled = -1;
    J[i].D_k = (int**) malloc(B.portsize * sizeof(int*));     //allocate memory for 2D array D_k of size m*m
    int j;
    for (j=0;j<B.portsize;j++){
      J[i].D_k[j] = (int*) malloc(B.portsize * sizeof(int));
    }


    int k,l;
    for(k=0;k<B.portsize;k++){
      for(l=0;l<B.portsize;l++){
        //fscanf(F,"%d",&J[i].D_k[k][l]);    //input the flow sizes in the newly allocated D_k matrix
        J[i].D_k[k][l] = 0;
      }
    }
    int f;
    for(f=0;f<coflows[i].flows.size();f++){
      J[i].D_k[coflows[i].flows[f].sender_id][coflows[i].flows[f].receiver_id] = coflows[i].flows[f].size;
    }


    J[i].v_k = (int*)malloc(2*B.portsize*sizeof(int));   //v_k is a vector of size #ports which hold total inflow/outflow for that port

    for(j=0;j<2*B.portsize;j++){

      if(j<=B.portsize-1){
        J[i].v_k[j] = 0;
        for(k=0;k<B.portsize;k++){
          J[i].v_k[j] += J[i].D_k[j][k];
        }
      }

      else{
        J[i].v_k[j] = 0;
        for(k=0;k<B.portsize;k++){
          J[i].v_k[j] += J[i].D_k[k][j-B.portsize];  //check the index
        }
      }

    }

    int max=-1;    //Now find the port with maximum flow input/output
    for(j=0;j<2*B.portsize;j++){
      if(J[i].v_k[j]>max){
        max = J[i].v_k[j];
        J[i].m_k = j;
      }
    }
  }    //This completely inputs the data to coflow sturctures and calculates the required v_k and m_k terms

  // Now we have coflows ready
  int k=0,l=0;
  float* beta = (float*)malloc(numCoflows*sizeof(float));  //initialize beta array for the algorithm
  for(k=0;k<numCoflows;k++)
    beta[k] = 0;


  int *p_k = (int*)malloc(numCoflows*sizeof(int));  //initialize p_k for the algorithm
  int *temp_v_k = (int*)malloc(2*B.portsize*sizeof(int));


  for(k=1;k<=numCoflows;k++){    //main loop of the algorithm

    int q1;
    for(q1=0;q1<2*B.portsize;q1++){
      temp_v_k[q1] = 0.0;
    }  //IMPORTANT!! added this line! (check if this makes any change in the prev code)


    for(l=0;l<numCoflows;l++){
      if(J[l].scheduled == -1){
        int q;
        for(q=0;q<2*B.portsize;q++){
          temp_v_k[q] += J[l].v_k[q];
        }



        //p_k[k-1] = J[l].m_k;       //we allocate p_k to be the port number with max flow size across all unscheduled jobs/coflows
      }
      int x;
      int max_val = -1;
      for(x=0;x<2*B.portsize;x++){
        if(max_val < temp_v_k[x]){
          max_val = temp_v_k[x];
          p_k[k-1] = x;
        }
      }
    }
    //printf("k is %d, p_k is %d\n",k,p_k[k-1] );
    float min_beta = 1000000000000000.0;
    //printf("min_beta is %f\n",min_beta);
    int min_j;
    int j;
    for(j=0;j<numCoflows;j++){  //we need to find the min value of term over all the jobs/coflows
      if(J[j].scheduled!=-1)    //if coflow is already scheduled, we do not consider the coflow
        continue;
      //printf("%d coflow considered and its weight is %d\n",j+1,J[j].w_k );
      int n;
      float sum_v_beta = 0.0;
      for(n=1;n<=k-1;n++){
        sum_v_beta += J[j].v_k[p_k[n-1]]*beta[n-1];
      }
      float term;
      term = (float)(J[j].w_k - sum_v_beta) / (float)J[j].v_k[p_k[k-1]];
      //printf("beta for coflow considered %f\n",term );
      if(term < min_beta){
        min_beta = term;
        min_j = j;
      }
    }
    beta[k-1] = min_beta;

    //printf("min_j now is %d\n",min_j );
    J[min_j].scheduled = numCoflows - k + 1;   //note that the final numbering goes from 1 to numCoflows


  }

  //this the algo
    //1)find the bottleneck port in the set of jobs
    //2)find the minimum beta and the job which minimizes beta
    //3)remove the job from the set of jobs and repeat

  //now print the final output
  int p;
  for(p=0;p<numCoflows;p++){
    // printf("Coflow %d : Scheduled at number %d\n",p+1,J[p].scheduled );
  }

  std::vector<Coflow> v = coflows;
  /*Coflow C;  //temp coflow for initializing
  int v_size;
  for(v_size=0;v_size<numCoflows;v_size++){
    v.push_back(C);
  }*/
  for(int v_size=0;v_size<numCoflows;v_size++){
    v[J[v_size].scheduled-1] = coflows[v_size];
  }

  return v;
}
