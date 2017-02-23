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

import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CreateBlobTable;
import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.util.Optional;

class CreateBlobTableAnalyzer {

    private final Schemas schemas;
    private final NumberOfShards numberOfShards;

    CreateBlobTableAnalyzer(Schemas schemas, NumberOfShards numberOfShards) {
        this.schemas = schemas;
        this.numberOfShards = numberOfShards;
    }

    public CreateBlobTableAnalyzedStatement analyze(CreateBlobTable node, ParameterContext parameterContext) {
        CreateBlobTableAnalyzedStatement statement = new CreateBlobTableAnalyzedStatement();
        TableIdent tableIdent = BlobTableAnalyzer.tableToIdent(node.name());
        statement.table(tableIdent, schemas);

        int numShards;
        Optional<ClusteredBy> clusteredBy = node.clusteredBy();
        if (clusteredBy.isPresent()) {
            numShards = numberOfShards.fromClusteredByClause(clusteredBy.get(), parameterContext.parameters());
        } else {
            numShards = numberOfShards.defaultNumberOfShards();
        }
        statement.tableParameter().settingsBuilder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, numShards);

        // apply default in case it is not specified in the genericProperties,
        // if it is it will get overwritten afterwards.
        TablePropertiesAnalyzer.analyze(
            statement.tableParameter(), new BlobTableParameterInfo(),
            node.genericProperties(), parameterContext.parameters(), true);

        return statement;
    }
}
