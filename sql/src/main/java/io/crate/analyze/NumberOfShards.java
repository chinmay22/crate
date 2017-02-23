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

package io.crate.analyze;

import io.crate.analyze.expressions.ExpressionToNumberVisitor;
import io.crate.data.Row;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.Expression;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Optional;

@Singleton
public class NumberOfShards {

    private static final Integer MIN_NUM_SHARDS = 4;

    private final ClusterService clusterService;

    @Inject
    public NumberOfShards(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    int fromClusteredByClause(ClusteredBy clusteredBy, Row parameters) {
        Optional<Expression> numberOfShards = clusteredBy.numberOfShards();
        if (numberOfShards.isPresent()) {
            int numShards = ExpressionToNumberVisitor.convert(numberOfShards.get(), parameters).intValue();
            if (numShards < 1) {
                throw new IllegalArgumentException("num_shards in CLUSTERED clause must be greater than 0");
            }
            return numShards;
        }
        return defaultNumberOfShards();
    }

    int defaultNumberOfShards() {
        int numDataNodes = clusterService.state().nodes().dataNodes().size();
        assert numDataNodes > 0 : "number of data nodes cannot be less than 0";
        return Math.max(MIN_NUM_SHARDS, numDataNodes * 2);
    }

}
