// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.analysis.ColumnDef;
import com.starrocks.analysis.KeysDesc;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TypeDef;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvId;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.common.DdlException;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.sql.analyzer.CreateTableAnalyzer;
import com.starrocks.sql.ast.CreateMaterializedViewStatement;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.common.UnsupportedException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.physical.stream.PhysicalStreamOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.rule.mv.KeyInference;
import com.starrocks.sql.optimizer.rule.mv.MVOperatorProperty;
import com.starrocks.sql.plan.ExecPlan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
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
     * 2. Store metadata of MV in metastore
     * 3. Register the job of MV maintenance
     */
    public void prepareMaintenanceWork(CreateMaterializedViewStatement stmt, MaterializedView view) throws DdlException {
        if (!view.getRefreshScheme().isIncremental()) {
            return;
        }
        LOG.info("creating IMT for MV {}", view.getName());
        IMTCreator.createIMT(stmt, view);

        // TODO(murphy) persist the meta of MV (IMT, MaintenancePlan)
        // TODO(murphy) register the job of MV
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

    // Copy from LocalMetaStore
    private static int calBucketNumAccordingToBackends() {
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


    /**
     * Manage job of materialized-view maintenance
     */
    static class JobManager {

    }

    /**
     * Create Intermediate-Materialized-Table for MV
     */
    static class IMTCreator {
        private static final Logger LOG = LogManager.getLogger(IMTCreator.class);

        private static final IMTCreator INSTANCE = new IMTCreator();

        public static void createIMT(CreateMaterializedViewStatement stmt, MaterializedView view) throws DdlException {
            ExecPlan maintenancePlan = stmt.getMaintenancePlan();
            ColumnRefFactory columnRefFactory = stmt.getColumnRefFactory();
            OptExpression optExpr = maintenancePlan.getPhysicalPlan();
            List<OptExpression> streamOperators = new ArrayList<>();
            collectStreamOperators(optExpr, streamOperators);

            List<CreateTableStmt> createTables =
                    streamOperators.stream().map(op -> createIMTForOperator(stmt, op, view)).collect(Collectors.toList());
            for (CreateTableStmt create : createTables) {
                LOG.info("creating IMT {} for MV {}", create.getTableName(), view.getName());
                try {
                    GlobalStateMgr.getCurrentState().createTable(create);
                } catch (DdlException e) {
                    // TODO(murphy) cleanup created IMT, or make it atomic
                    LOG.info("create IMT {} failed due to ", create.getTableName(), e);
                    throw e;
                }
            }
        }

        private static CreateTableStmt createIMTForOperator(CreateMaterializedViewStatement stmt, OptExpression opt,
                                                            MaterializedView view) {
            PhysicalStreamOperator streamOp = (PhysicalStreamOperator) opt.getOp();
            MVOperatorProperty property = opt.getMvOperatorProperty();
            ColumnRefFactory columnRefFactory = stmt.getColumnRefFactory();
            Map<String, String> properties = view.getProperties();

            // Duplicate/Primary Key
            KeyInference.KeyProperty bestKey = property.getKeySet().getBestKey();

            // Columns
            List<Column> columns = new ArrayList<>();
            List<Column> keyColumns = new ArrayList<>();
            List<ColumnDef> columnDefs = new ArrayList<>();
            ColumnRefSet columnRefSet = opt.getOutputColumns();
            for (int columnId : columnRefSet.getColumnIds()) {
                ColumnRefOperator refOp = columnRefFactory.getColumnRef(columnId);

                Column newColumn = new Column(refOp.getName(), refOp.getType());
                boolean isKey = bestKey.columns.contains(refOp);
                newColumn.setIsKey(isKey);
                newColumn.setIsAllowNull(false);
                columns.add(newColumn);
                if (isKey) {
                    keyColumns.add(newColumn);
                }

                TypeDef typeDef = TypeDef.create(refOp.getType().getPrimitiveType());
                ColumnDef columnDef = new ColumnDef(refOp.getName(), typeDef);
                columnDef.setIsKey(isKey);
                columnDef.setAllowNull(!isKey);
                columnDefs.add(columnDef);
            }

            // Key type
            KeysType keyType = property.getModify().isInsertOnly() ? KeysType.DUP_KEYS : KeysType.PRIMARY_KEYS;
            KeysDesc keyDesc = new KeysDesc(keyType, keyColumns.stream().map(x -> x.getName()).collect(Collectors.toList()));

            // Partition scheme
            // TODO(murphy) support partition the IMT, current we don't support it
            PartitionDesc partitionDesc = new PartitionDesc();

            // Distribute Key
            DistributionDesc distributionDesc = new DistributionDesc();
            Preconditions.checkNotNull(distributionDesc);
            HashDistributionInfo distributionInfo = new HashDistributionInfo();
            distributionInfo.setBucketNum(calBucketNumAccordingToBackends());
            distributionInfo.setDistributionColumns(keyColumns);

            // Table name: imt_mvname_operatorid_seq;
            // TODO(murphy) refine it
            String mvName = stmt.getTableName().getTbl();
            long seq = GlobalStateMgr.getCurrentState().getNextId();
            String tableName = "imt_" + mvName + seq;
            TableName canonicalName = new TableName(stmt.getTableName().getDb(), tableName);

            // Properties
            Map<String, String> extProperties = Maps.newTreeMap();
            String comment = "IMT for MV";

            return new CreateTableStmt(false, false, canonicalName, columnDefs,
                    CreateTableAnalyzer.EngineType.OLAP.name(), keyDesc, partitionDesc, distributionDesc, properties,
                    extProperties, comment);
        }

        private static void collectStreamOperators(OptExpression root, List<OptExpression> output) {
            if (root.getOp() instanceof PhysicalStreamOperator) {
                output.add(root);
            }
            root.getInputs().forEach(child -> collectStreamOperators(child, output));
        }
    }
}
