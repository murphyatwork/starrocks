// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.sql.optimizer.rule.tree.lowcardinality;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.physical.PhysicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.tree.TreeRewriteRule;
import com.starrocks.sql.optimizer.task.TaskContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LowCardinalityRewriteRule implements TreeRewriteRule {

    private static final Logger LOG = LogManager.getLogger(LowCardinalityRewriteRule.class);

    @Override
    public OptExpression rewrite(OptExpression root, TaskContext taskContext) {
        SessionVariable session = taskContext.getOptimizerContext().getSessionVariable();
        boolean isQuery = taskContext.getOptimizerContext().getConnectContext().getState().isQuery();
        if (!session.isEnableLowCardinalityOptimize() || !session.isUseLowCardinalityOptimizeV2()) {
            return root;
        }

        ColumnRefFactory factory = taskContext.getOptimizerContext().getColumnRefFactory();
        DecodeContext context = new DecodeContext(factory);

        if (!session.isCboRewriteJsonPathDict()) {
            {
                DecodeCollector collector = new DecodeCollector(session, isQuery);
                collector.collect(root, context);
                if (!collector.isValidMatchChildren()) {
                    return root;
                }
            }

            DecodeRewriter rewriter = new DecodeRewriter(factory, context);
            return rewriter.rewrite(root);
        }

        // --- Begin: Rewrite get_json_string to ColumnRefOperator ---
        Map<ColumnRefOperator, CallOperator> rewrittenMap = new HashMap<>();
        root = new GetJsonStringRewriter(factory, rewrittenMap).rewriteOptTree(root);
        // --- End: Rewrite get_json_string to ColumnRefOperator ---

        for (ColumnRefOperator columnRef : rewrittenMap.keySet()) {
            context.allStringColumns.add(columnRef.getId());
        }

        {
            DecodeCollector collector = new DecodeCollector(session, isQuery);
            collector.collect(root, context);
            if (!collector.isValidMatchChildren()) {
                return root;
            }
        }

        System.err.printf("rewrite get_json_string function: %s\n", rewrittenMap);

        DecodeRewriter rewriter = new DecodeRewriter(factory, context);
        OptExpression decodedRoot = rewriter.rewrite(root);

        // --- Begin: Restore ColumnRefOperator back to get_json_string ---
        OptExpression restoredRoot = new GetJsonStringRestorer(rewrittenMap).rewriteOptTree(decodedRoot);
        // --- End: Restore ColumnRefOperator back to get_json_string ---

        return restoredRoot;
    }

    // Helper to rewrite get_json_string to ColumnRefOperator
    private static class GetJsonStringRewriter extends OptExpressionVisitor<OptExpression, Void> {
        private final ColumnRefFactory factory;
        private final Map<ColumnRefOperator, CallOperator> rewrittenMap;
        private final Map<ColumnRefOperator, Column> rewrittenColumn = Maps.newHashMap();
        private final Map<String, ColumnRefOperator> jsonPathRewrittenMap = Maps.newHashMap();

        public GetJsonStringRewriter(ColumnRefFactory factory, Map<ColumnRefOperator, CallOperator> rewrittenMap) {
            this.factory = factory;
            this.rewrittenMap = rewrittenMap;
        }

        public OptExpression rewriteOptTree(OptExpression root) {
            return root.getOp().accept(this, root, null);
        }

        @Override
        public OptExpression visit(OptExpression opt, Void context) {
            Operator op = opt.getOp();
            Projection proj = op.getProjection();
            Map<ColumnRefOperator, Column> columnRefOperatorColumnMap = Maps.newHashMap();
            if (proj != null) {
                Map<ColumnRefOperator, ScalarOperator> newColRefMap = new HashMap<>();
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : proj.getColumnRefMap().entrySet()) {
                    newColRefMap.put(entry.getKey(), rewriteScalar(entry.getValue()));
                }
                Projection newProj = new Projection(newColRefMap);
                op.setProjection(newProj);
            }
            if (op.getPredicate() != null) {
                op.setPredicate(rewriteScalar(op.getPredicate()));
            }
            List<OptExpression> newInputs = opt.getInputs().stream()
                    .map(child -> child.getOp().accept(this, child, null))
                    .collect(Collectors.toList());
            for (int i = 0; i < newInputs.size(); ++i) {
                opt.setChild(i, newInputs.get(i));
            }

            if (op instanceof PhysicalOlapScanOperator scanOperator) {
                for (Column col : rewrittenColumn.values()) {
                    scanOperator.getTable().addColumn(col);
                }
                rewrittenColumn.putAll(scanOperator.getColRefToColumnMetaMap());

                PhysicalScanOperator newOp =
                        PhysicalOlapScanOperator.builder().withOperator(scanOperator)
                                .setColRefToColumnMetaMap(rewrittenColumn)
                                .build();
                return OptExpression.builder()
                        .with(opt)
                        .setOp(newOp)
                        .build();
            }
            return opt;
        }

        private ScalarOperator rewriteScalar(ScalarOperator scalar) {
            if (scalar instanceof CallOperator) {
                CallOperator call = (CallOperator) scalar;
                if ("get_json_string".equalsIgnoreCase(call.getFnName()) && call.getChildren().size() == 2) {
                    ScalarOperator jsonPathArg = call.getChild(1);
                    if (jsonPathArg instanceof ConstantOperator) {
                        String jsonPath = ((ConstantOperator) jsonPathArg).getVarchar();
                        // Use the type and nullability of the call for the new column
                        if (jsonPathRewrittenMap.containsKey(jsonPath)) {
                            ColumnRefOperator ref = jsonPathRewrittenMap.get(jsonPath);
                            rewrittenMap.put(ref, call);
                            return ref;
                        }
                        ColumnRefOperator colRef = factory.create(jsonPath, call.getType(), call.isNullable());
                        rewrittenMap.put(colRef, call);
                        jsonPathRewrittenMap.put(jsonPath, colRef);
                        Column column = new Column(jsonPath, call.getType(), call.isNullable());
                        rewrittenColumn.put(colRef, column);
                        return colRef;
                    }
                }
            }
            List<ScalarOperator> children = scalar.getChildren();
            if (children == null || children.isEmpty()) {
                return scalar;
            }
            boolean changed = false;
            List<ScalarOperator> newChildren = children.stream()
                    .map(this::rewriteScalar)
                    .collect(Collectors.toList());
            for (int i = 0; i < children.size(); ++i) {
                if (children.get(i) != newChildren.get(i)) {
                    changed = true;
                    break;
                }
            }
            if (!changed) {
                return scalar;
            }
            ScalarOperator copy = scalar.clone();
            for (int i = 0; i < newChildren.size(); ++i) {
                copy.setChild(i, newChildren.get(i));
            }
            return copy;
        }
    }

    // Helper to restore ColumnRefOperator back to get_json_string
    private static class GetJsonStringRestorer extends OptExpressionVisitor<OptExpression, Void> {
        private final Map<ColumnRefOperator, CallOperator> rewrittenMap;

        public GetJsonStringRestorer(Map<ColumnRefOperator, CallOperator> rewrittenMap) {
            this.rewrittenMap = rewrittenMap;
        }

        public OptExpression rewriteOptTree(OptExpression root) {
            return root.getOp().accept(this, root, null);
        }

        @Override
        public OptExpression visit(OptExpression opt, Void context) {
            Operator op = opt.getOp();
            Projection proj = op.getProjection();
            if (proj != null) {
                Map<ColumnRefOperator, ScalarOperator> newColRefMap = new HashMap<>();
                for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : proj.getColumnRefMap().entrySet()) {
                    newColRefMap.put(entry.getKey(), restoreScalar(entry.getValue()));
                }
                Projection newProj = new Projection(newColRefMap);
                op.setProjection(newProj);
            }
            if (op.getPredicate() != null) {
                op.setPredicate(restoreScalar(op.getPredicate()));
            }
            List<OptExpression> newInputs = opt.getInputs().stream()
                    .map(child -> child.getOp().accept(this, child, null))
                    .collect(Collectors.toList());
            for (int i = 0; i < newInputs.size(); ++i) {
                opt.setChild(i, newInputs.get(i));
            }
            return opt;
        }

        private ScalarOperator restoreScalar(ScalarOperator scalar) {
            if (scalar instanceof ColumnRefOperator) {
                ColumnRefOperator colRef = (ColumnRefOperator) scalar;
                if (rewrittenMap.containsKey(colRef)) {
                    return rewrittenMap.get(colRef);
                }
            }
            List<ScalarOperator> children = scalar.getChildren();
            if (children == null || children.isEmpty()) {
                return scalar;
            }
            boolean changed = false;
            List<ScalarOperator> newChildren = children.stream()
                    .map(this::restoreScalar)
                    .collect(Collectors.toList());
            for (int i = 0; i < children.size(); ++i) {
                if (children.get(i) != newChildren.get(i)) {
                    changed = true;
                    break;
                }
            }
            if (!changed) {
                return scalar;
            }
            ScalarOperator copy = scalar.clone();
            for (int i = 0; i < newChildren.size(); ++i) {
                copy.setChild(i, newChildren.get(i));
            }
            return copy;
        }
    }
}