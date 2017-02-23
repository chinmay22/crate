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

package io.crate.planner.consumer;


import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.where.DocKeys;
import io.crate.exceptions.VersionInvalidException;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.Limits;
import io.crate.planner.NoopPlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.node.dql.ESGet;

import java.util.Optional;

public class ESGetStatementPlanner {

    public static Plan convert(QueriedDocTable table, Planner.Context context) {
        QuerySpec querySpec = table.querySpec();
        Optional<DocKeys> optKeys = querySpec.where().docKeys();
        assert !querySpec.hasAggregates() : "Can't create ESGet plan for queries with aggregates";
        assert !querySpec.groupBy().isPresent() : "Can't create ESGet plan for queries with group by";
        assert optKeys.isPresent() : "Can't create ESGet without docKeys";

        DocTableInfo tableInfo = table.tableRelation().tableInfo();
        DocKeys docKeys = optKeys.get();
        if (docKeys.withVersions()){
            throw new VersionInvalidException();
        }
        Limits limits = context.getLimits(querySpec);
        if (limits.hasLimit() && limits.finalLimit() == 0) {
            return new NoopPlan(context.jobId());
        }
        table.tableRelation().validateOrderBy(querySpec.orderBy());
        return new ESGet(
            context.nextExecutionPhaseId(),
            tableInfo,
            querySpec.outputs(),
            optKeys.get(),
            querySpec.orderBy(),
            limits.finalLimit(),
            limits.offset(),
            context.jobId());
    }
}
