// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import com.starrocks.catalog.MaterializedView;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Long-running job responsible for MV incremental maintenance
 */
public class MVMaintenanceJob implements Writable {
    private static final Logger LOG = LogManager.getLogger(MVMaintenanceJob.class);

    private final MaterializedView view;
    private ExecPlan plan;
    private final JobState state;

    /**
     * The incremental maintenance of MV consists of epochs, whose lifetime is defined as:
     * 1. Triggered by transaction publish
     * 2. Acquire the last-committed binlog LSN and latest binlog LSN
     * 3. Start a transaction for incremental maintaining the MV
     * 4. Schedule task executor to consume binlog since last-committed, and apply these changes to MV
     * 5. Commit the transaction to make is visible to user
     * 6. Commit the binlog consumption LSN(be atomic with transaction commitment to make)
     */
    public static class Epoch {
        public long transactionId;

        public Epoch() {

        }
    }

    public enum JobState {
        // Just initialized
        INIT,

        // Preparing for the job
        PREPARING,

        // Pause the job, waiting for the continue event
        PAUSED,

        // Wait for epoch start
        WAIT_EPOCH,

        // Running the epoch
        RUN_EPOCH,

        // Failed the whole job, needs reconstruction (unsupported environment change would cause job failure
        FAILED
    }


    public MVMaintenanceJob(MaterializedView view) {
        this.view = view;
        this.state = JobState.INIT;
    }

    /**
     * Main entrance of the job:
     * 0. Generate the physical job structure, including fragment distribution, parallelism
     * 1. Deliver tasks to executors on BE
     * 2. Trigger the epoch
     */
    public void start() {
    }

    /**
     * Trigger the incremental maintenance by transaction publish
     */
    public void triggerByTxn() {
        if (state.equals(JobState.WAIT_EPOCH)) {
            runEpoch();
        } else {
            throw UnsupportedException.unsupportedException("TODO: implement ");
        }
    }

    public void pauseJob() {
        throw UnsupportedException.unsupportedException("TODO: implement pause action");
    }

    public void continueJob() {
        throw UnsupportedException.unsupportedException("TODO: implement continue action");
    }

    public void run() {
        throw UnsupportedException.unsupportedException("TODO: implement the daemon runner");
    }

    private void prepareJob() {

    }

    /**
     * TODO(murphy) abstract it to support other kinds of EpochCoordinator
     */
    private static class TxnBasedEpochCoordinator {
        private Epoch epoch;

        public TxnBasedEpochCoordinator() {
        }

        public void run() {
            beginEpoch();
            commitEpoch();
        }

        private void beginEpoch() {
            throw UnsupportedException.unsupportedException("TODO: implement");
        }


        private void commitEpoch() {
            throw UnsupportedException.unsupportedException("TODO: implement");
        }
    }

    private void runEpoch() {
        TxnBasedEpochCoordinator epochCoordinator = new TxnBasedEpochCoordinator();
        try {
            epochCoordinator.run();
        } catch (Exception e) {
            LOG.warn("job {} run epoch failed: {}", this, e);
            throw e;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        throw UnsupportedException.unsupportedException("TODO");
    }

}
