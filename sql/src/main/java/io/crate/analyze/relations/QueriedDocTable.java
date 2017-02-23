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

package io.crate.analyze.relations;

import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.metadata.Functions;
import io.crate.metadata.Path;
import io.crate.metadata.TransactionContext;

import java.util.Collection;

public class QueriedDocTable extends QueriedTableRelation<DocTableRelation> {

    public QueriedDocTable(DocTableRelation tableRelation, Collection<? extends Path> paths, QuerySpec querySpec) {
        super(tableRelation, paths, querySpec);
    }

    public QueriedDocTable(DocTableRelation tableRelation, QuerySpec querySpec) {
        super(tableRelation, querySpec);
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitQueriedDocTable(this, context);
    }

    void analyzeWhereClause(Functions functions, TransactionContext transactionContext) {
        WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(functions, tableRelation());
        querySpec().where(whereClauseAnalyzer.analyze(querySpec().where(), transactionContext));
    }
}
