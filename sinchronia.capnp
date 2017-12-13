@0xe064a1d4489394b5;

using Go = import "/go.capnp";
$Go.package("scheduler");
$Go.import("github.com/akshayknarayan/sinchronia/client/scheduler");

# a Scheduler schedules coflows.
interface Scheduler {
    # A piece of data to send.
    struct SchInfo {
        dataID @0 :UInt32;
        size @1 :UInt32; # during registration, if size is unknown, = 0.
    }

    regCoflow @0 ( coflows :List(Coflow) ) -> ();

    # A sender and recipient for a Data.
    struct Flow {
        from @0 :UInt32;
        to @1 :UInt32;
        data @2 :SchInfo; 
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
        sending @2 :List(SchInfo);
    }

    # Return scheduled coflow to node.
    getSchedule @2 ( jobId :UInt32, nodeId :UInt32 ) -> (schedule :CoflowSchedule);


    struct Scheduled {
        jobID @0 :UInt32;
        priority @1 :UInt32;
        receiving @2 :List(SchInfo);
    }

    # A coflow's schedule.
    struct CoflowSchedule {
        union {
            schedule @0   :Scheduled; # a scheduled coflow slice, with which data_id's to receive
            noSchedule @1 :Void; # if that port has no scheduled coflows currently
        }
    }

    coflowDone @3 ( jobId :UInt32, nodeId :UInt32, finished :List(UInt32) ) -> ();
}

# a Receiver receives Data
interface Receiver {
    # The data to send
    struct SndInfo {
        jobID @0 :UInt32;
        dataID @3 :UInt32;
        from @1 :UInt32;
        to @2 :UInt32;
        blob @4 :Data;
    }

    send @0 (data :SndInfo)  -> ();
}
