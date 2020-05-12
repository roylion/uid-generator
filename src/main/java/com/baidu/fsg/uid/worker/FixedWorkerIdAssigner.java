package com.baidu.fsg.uid.worker;

public class FixedWorkerIdAssigner implements WorkerIdAssigner {

    private long workerId;

    public FixedWorkerIdAssigner(long workerId) {
        this.workerId = workerId;
    }

    @Override
    public long assignWorkerId() {
        return workerId;
    }
}
