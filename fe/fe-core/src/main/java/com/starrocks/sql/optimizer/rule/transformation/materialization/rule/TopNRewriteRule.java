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

package com.starrocks.sql.optimizer.rule.transformation.materialization.rule;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Table;
import com.starrocks.sql.optimizer.MvRewriteContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MaterializedViewRewriter;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MvUtils;
import com.starrocks.sql.optimizer.rule.transformation.materialization.RewriteContext;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

/**
 * Rewrite MV with TopN node
 */
public class TopNRewriteRule extends BaseMaterializedViewRewriteRule {
    private static final TopNRewriteRule INSTANCE = new TopNRewriteRule();

    public TopNRewriteRule() {
        super(RuleType.TF_MV_TOPN_REWRITE_RULE,
                Pattern.create(OperatorType.LOGICAL_TOPN)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_AGGR)));
    }

    public static TopNRewriteRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!MvUtils.isValidMVPlan(input)) {
            return false;
        }
        return super.check(input, context);
    }

    @Override
    public MaterializedViewRewriter getMaterializedViewRewrite(MvRewriteContext mvContext) {
        return new TopNMVRewritter(mvContext);
    }

    public static class TopNMVRewritter extends MaterializedViewRewriter {

        public TopNMVRewritter(MvRewriteContext mvRewriteContext) {
            super(mvRewriteContext);
        }

        @Override
        protected boolean isMVApplicable(OptExpression mvExpression,
                                         List<Table> queryTables,
                                         List<Table> mvTables,
                                         MatchMode matchMode,
                                         OptExpression queryExpression) {
            LogicalTopNOperator queryTopN = (LogicalTopNOperator) queryExpression.getOp();
            LogicalTopNOperator mvTopN =
                    (LogicalTopNOperator) mvRewriteContext.getMaterializationContext().getRawMvExpr().getOp();

            if (queryTopN.equals(mvTopN)) {
                return true;
            }
            if (!checkOrderPrefix(queryTopN, mvTopN)) {
                return false;
            }
            if (!checkLimit(queryTopN, mvTopN)) {
                return false;
            }
            return true;
        }

        @Override
        protected OptExpression viewBasedRewrite(RewriteContext rewriteContext,
                                                 OptExpression mvScanExpr) {
            OptExpression mvExpr = mvRewriteContext.getMaterializationContext().getRawMvExpr();
            if (mvExpr.getOp().getOpType() != OperatorType.LOGICAL_TOPN) {
                return super.viewBasedRewrite(rewriteContext, mvScanExpr);
            }

            OptExpression query = rewriteContext.getQueryExpression();
            LogicalTopNOperator queryTopN = (LogicalTopNOperator) query.getOp();

            // Apply the TopN node on top of rewritten MVScan
            mvScanExpr = baseRewrite(rewriteContext, mvScanExpr);
            if (mvScanExpr == null) {
                return null;
            }
            LogicalTopNOperator newTopN = LogicalTopNOperator.builder().withOperator(queryTopN).build();
            OptExpression rewriteOp = OptExpression.create(newTopN, mvScanExpr);
            //            deriveLogicalProperty(rewriteOp);
            return rewriteOp;
        }

        private OptExpression baseRewrite(RewriteContext rewriteContext, OptExpression mvScanExpr) {
            OptExpression exprWithoutTopN = rewriteContext.getQueryExpression().inputAt(0);
            Preconditions.checkState(AggregateScanRule.getInstance().check(exprWithoutTopN, optimizerContext), "todo");

            List<OptExpression> res = AggregateScanRule.getInstance().transform(exprWithoutTopN, optimizerContext);
            if (CollectionUtils.isEmpty(res)) {
                return null;
            }
            return res.get(0);
        }

        private boolean checkOrderPrefix(LogicalTopNOperator query, LogicalTopNOperator mv) {
            if (mv.getOrderByElements().isEmpty()) {
                return true;
            }
            if (query.getOrderByElements().size() < mv.getOrderByElements().size()) {
                return false;
            }
            List<Ordering> querySubOrder = query.getOrderByElements().subList(0, mv.getOrderByElements().size());

            for (int i = 0; i < mv.getOrderByElements().size(); i++) {
                Ordering queryOrder = query.getOrderByElements().get(i);
                Ordering mvOrder = query.getOrderByElements().get(i);
                if (!queryOrder.isEquivalent(mvOrder)) {
                    return false;
                }
            }
            return true;
        }

        private boolean checkLimit(LogicalTopNOperator query, LogicalTopNOperator mv) {
            if (mv.getOffset() == Operator.DEFAULT_OFFSET && mv.getLimit() == Operator.DEFAULT_LIMIT) {
                return true;
            }
            return mv.getOffset() <= query.getOffset() &&
                    mv.getOffset() + mv.getLimit() >= query.getOffset() + query.getLimit();
        }
    }
}
