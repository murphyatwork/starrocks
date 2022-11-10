// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// TODO(Murphy) refactor all MV management code into here
public class MVManager {
    private static final Logger LOG = LogManager.getLogger(LocalMetastore.class);
    private static final MVManager INSTANCE = new MVManager();

    private LocalMetastore localMetastore;

    private MVManager() {
    }

    public static MVManager getInstance() {
        return INSTANCE;
    }

    public void setLocalMetastore(LocalMetastore localMetastore) {
        this.localMetastore = localMetastore;
    }

    /**
     * Prepare maintenance work for materialized view
     * 1. Create Intermediate Materialized Table(IMT)
     * 2. Create Sink table for MV (A little different from normal MV)
     * 3. Store metadata of MV in metastore
     * 4. Register the job of MV maintenance
     */
    public void prepareMaintenanceWork(CreateMaterializedViewStatement stmt, MaterializedView view) {
        if (!view.getRefreshScheme().isIncremental()) {
            return;
        }
        ExecPlan plan = Preconditions.checkNotNull(stmt.getMaintenancePlan());


        LOG.info("Prepare maintenance of MV: {}({})", view.getName(), view.getMvId());
    }

    /**
     * Start the maintenance job for MV after created
     * 1. Schedule the maintenance task in executor
     * 2. Coordinate the epoch/transaction progress: reading changelog of base table and incremental refresh the MV
     * 3. Maintain the visibility of MV and job metadata
     */
    public void startMaintainMV(MaterializedView view) {

        LOG.info("Start maintenance of MV: {}({})", view.getName(), view.getMvId());
    }

    /**
     * Stop the maintenance job for MV after dropped
     */
    public void stopMaintainMV(MvId mvId) {
        throw UnsupportedException.unsupportedException("drop mv job is not supported");
    }

    /**
     * Re-Schedule the MV maintenance job
     * 1. Tablet migration
     * 2. Cluster rescale
     * 3. Temporary failure happens(FE or BE)
     */
    public void reScheduleMaintainMV(MvId mvId) {
        throw UnsupportedException.unsupportedException("re-schedule mv job is not supported");
    }

    /**
     * Rebuild the maintenance job for critical changes
     * 1. Schema change of MV (should be optimized later)
     * 2. Binlog lost
     * 3. Base table schema change (should be optimized later)
     */
    public void rebuildMaintainMV(MvId mvId) {
        throw UnsupportedException.unsupportedException("rebuild mv job is not supported");
    }
}
