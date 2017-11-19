@0xe064a1d4489394b5;

# a Scheduler schedules coflows.
interface Scheduler {
    # A piece of data to send.
    struct Data {
        dataID @0 :UInt32;
        size @1 :UInt32; # during registration, if size is unknown, = 0.
    }

    regCoflow @0 ( coflows :List(Coflow) ) -> ();

    # A sender and recipient for a Data.
    struct Flow {
        from @0 :UInt32;
        to @1 :UInt32;
        data @2 :Data; 
    }

    # A coflow
    struct Coflow {
        jobID @0 :UInt32;
        flows @1 :List(Flow);
    }

    sendCoflow @1 ( coflowSlice :CoflowSlice ) -> ();

    # One port's view of a single Coflow.
    struct CoflowSlice {
        nodeID @0 :UInt32;
        jobID @1 :UInt32;
        sending @2 :List(Data);
    }

    # Return scheduled coflow to node.
    getSchedule @2 ( jobId :UInt32, nodeId :UInt32 ) -> (schedule :CoflowSchedule);


    struct Scheduled {
        jobID @0 :UInt32;
        priority @1 :UInt32;
        receiving @2 :List(Data);
    }

    # A coflow's schedule.
    struct CoflowSchedule {
        union {
            schedule @0   :Scheduled; # a scheduled coflow slice, with which data_id's to receive
            noSchedule @1 :Void; # if that port has no scheduled coflows currently
        }
    }
}
