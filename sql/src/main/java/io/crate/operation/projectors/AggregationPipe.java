/*
 * Licensed to Crate.io Inc. (Crate) under one or more contributor license
 * agreements.  See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file to
 * you under the Apache License, Version 2.0 (the "License");  you may not
 * use this file except in compliance with the License.  You may obtain a
 * copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, to use any modules in this file marked as "Enterprise Features",
 * Crate must have given you permission to enable and use such Enterprise
 * Features and you must have a valid Enterprise or Subscription Agreement
 * with Crate.  If you enable or use the Enterprise Features, you represent
 * and warrant that you have a valid Enterprise or Subscription Agreement
 * with Crate.  Your use of the Enterprise Features if governed by the terms
 * and conditions of your Enterprise or Subscription Agreement with Crate.
 */

package io.crate.operation.projectors;

import io.crate.breaker.RamAccountingContext;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.operation.AggregationContext;
import io.crate.operation.aggregation.Aggregator;
import io.crate.operation.collect.CollectExpression;

public class AggregationPipe extends AbstractProjector {

    private final Aggregator[] aggregators;
    private final Iterable<CollectExpression<Row, ?>> collectExpressions;
    private final Object[] cells;
    private final Row row;
    private final Object[] states;

    public AggregationPipe(Iterable<CollectExpression<Row, ?>> collectExpressions,
                           AggregationContext[] aggregations,
                           RamAccountingContext ramAccountingContext) {
        cells = new Object[aggregations.length];
        row = new RowN(cells);
        states = new Object[aggregations.length];
        this.collectExpressions = collectExpressions;
        aggregators = new Aggregator[aggregations.length];
        for (int i = 0; i < aggregators.length; i++) {
            aggregators[i] = new Aggregator(
                ramAccountingContext,
                aggregations[i].symbol(),
                aggregations[i].function(),
                aggregations[i].inputs()
            );
            // prepareState creates the aggregationState. In case of the AggregationProjector
            // we only want to have 1 global state not 1 state per node/shard or even document.
            states[i] = aggregators[i].prepareState();
        }
    }

    @Override
    public Result setNextRow(Row row) {
        for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        for (int i = 0; i < aggregators.length; i++) {
            Aggregator aggregator = aggregators[i];
            states[i] = aggregator.processRow(states[i]);
        }
        return Result.CONTINUE;
    }

    @Override
    public void fail(Throwable t) {
        downstream.fail(t);
    }

    @Override
    public void finish(RepeatHandle repeatHandle) {
        for (int i = 0; i < aggregators.length; i++) {
            cells[i] = aggregators[i].finishCollect(states[i]);
        }
        downstream.setNextRow(row);
        downstream.finish(RepeatHandle.UNSUPPORTED);
    }
}
