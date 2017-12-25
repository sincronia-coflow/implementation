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

    # A sender and recipient for a piece of data (SchInfo).
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

    # sendCoflow is called once per coflow per node, when all its sending flows are ready.
    # It returns once the coflow is ready to be sent.
    sendCoflow @1 ( nodeID :UInt32, jobID :UInt32, sending :List(SchInfo) ) -> ( receiving :List(SchInfo) );

    # Return scheduled coflows for node.
    getSchedule @2 ( nodeId :UInt32 ) -> ( schedule :List(CoflowSchedule) );

    struct CoflowSchedule {
        jobID @0 :UInt32;
        priority @1 :UInt32;
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
