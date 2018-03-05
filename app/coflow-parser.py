#coflow parser to generate text file corresponding to each node to create corresponding map

import sys

tracefile_loc = sys.argv[1];
node_id = int(sys.argv[2]);
outputfile_loc = sys.argv[3];

tracefile = open(tracefile_loc,'r');
outputfile = open(outputfile_loc,'w');

num_coflows = int(tracefile.readline().split()[1]);

for i in range(num_coflows):
    flow_list = [];
    coflow_info = tracefile.readline().split();
    coflow_info_int = [int(x) for x in coflow_info];
    coflow_id = coflow_info_int[0];
    release_date = coflow_info_int[1]; #release_date in ms
    num_flows = coflow_info_int[2];
    for j in range(num_flows):
        if(coflow_info_int[5+3*(j)]==node_id):
            flow_list.append(j);
    flow_list_str = [str(x) for x in flow_list];
    if(flow_list_str == []):
        outputfile.write(str(coflow_id)+' '+str(release_date)+'\n');
    else:
        linetowrite = ' '.join(flow_list_str);
        outputfile.write(str(coflow_id)+' '+str(release_date)+' '+linetowrite+'\n');
