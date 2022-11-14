// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.mv.KeyInference;
import com.starrocks.sql.optimizer.rule.mv.MVOperatorProperty;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        createIMT(stmt);

        LOG.info("Prepare maintenance of MV: {}({})", view.getName(), view.getMvId());
    }

    /*
     * TODO(murphy) partial duplicated with LocalMetaStore::createMaterializedView
     * Create sink table with
     * 1. Columns
     * 2. Distribute by key buckets
     * 3. Duplicate Key/Primary Key
     *
     * Prefer user specified key, otherwise inferred key from plan
     */
    public MaterializedView createSinkTable(CreateMaterializedViewStatement stmt, long mvId, long dbId) throws DdlException {
        ExecPlan plan = Preconditions.checkNotNull(stmt.getMaintenancePlan());
        OptExpression physicalPlan = plan.getPhysicalPlan();
        MVOperatorProperty property =
                Preconditions.checkNotNull(physicalPlan.getMvOperatorProperty(), "incremental mv must have property");
        KeyInference.KeyPropertySet keys = property.getKeySet();
        if (keys.empty()) {
            throw UnsupportedException.unsupportedException("mv could not infer keys");
        }
        keys.sortKeys();
        KeyInference.KeyProperty key = keys.getKeys().get(0);

        // Columns
        List<Column> columns = stmt.getMvColumnItems();

        // Duplicate/Primary Key
        Map<Integer, String> columnNames = plan.getOutputColumns().stream().collect(
                Collectors.toMap(ColumnRefOperator::getId, ColumnRefOperator::getName));
        Set<String> keyColumns = key.columns.getStream().mapToObj(columnNames::get).collect(Collectors.toSet());
        for (Column col : columns) {
            col.setIsKey(keyColumns.contains(col.getName()));
        }
        if (!property.getModify().isInsertOnly()) {
            stmt.setKeysType(KeysType.PRIMARY_KEYS);
        }

        // Partition scheme
        PartitionDesc partitionDesc = stmt.getPartitionExpDesc();
        PartitionInfo partitionInfo;
        if (partitionDesc != null) {
            partitionInfo = partitionDesc.toPartitionInfo(Collections.singletonList(stmt.getPartitionColumn()),
                    Maps.newHashMap(), false);
        } else {
            partitionInfo = new SinglePartitionInfo();
        }

        // Distribute Key, already set in MVAnalyzer
        DistributionDesc distributionDesc = stmt.getDistributionDesc();
        Preconditions.checkNotNull(distributionDesc);
        DistributionInfo distributionInfo = distributionDesc.toDistributionInfo(columns);
        if (distributionInfo.getBucketNum() == 0) {
            int numBucket = calBucketNumAccordingToBackends();
            distributionInfo.setBucketNum(numBucket);
        }

        // Refresh
        MaterializedView.MvRefreshScheme mvRefreshScheme = new MaterializedView.MvRefreshScheme();
        mvRefreshScheme.setType(MaterializedView.RefreshType.INCREMENTAL);

        String mvName = stmt.getTableName().getTbl();
        return new MaterializedView(mvId, dbId, mvName, columns, stmt.getKeysType(), partitionInfo,
                distributionInfo, mvRefreshScheme);
    }

    private void createIMT(CreateMaterializedViewStatement stmt) {

    }

    // Copy from LocalMetaStore
    private int calBucketNumAccordingToBackends() {
        int backendNum = GlobalStateMgr.getCurrentSystemInfo().getBackendIds().size();
        // When POC, the backends is not greater than three most of the time.
        // The bucketNum will be given a small multiplier factor for small backends.
        int bucketNum = 0;
        if (backendNum <= 3) {
            bucketNum = 2 * backendNum;
        } else if (backendNum <= 6) {
            bucketNum = backendNum;
        } else if (backendNum <= 12) {
            bucketNum = 12;
        } else {
            bucketNum = Math.min(backendNum, 48);
        }
        return bucketNum;
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
