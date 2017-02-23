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

import io.crate.data.Row;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.sql.tree.AlterBlobTable;

import static io.crate.analyze.BlobTableAnalyzer.tableToIdent;

class AlterBlobTableAnalyzer {

    private final Schemas schemas;

    AlterBlobTableAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public AlterBlobTableAnalyzedStatement analyze(AlterBlobTable node, Row parameters) {
        TableIdent tableIdent = tableToIdent(node.table());
        assert BlobSchemaInfo.NAME.equals(tableIdent.schema()) : "schema name must be 'blob'";
        BlobTableInfo tableInfo = (BlobTableInfo) schemas.getTableInfo(tableIdent);

        TableParameter tableParameter = new TableParameter();
        if (node.genericProperties().isPresent()) {
            TablePropertiesAnalyzer.analyze(
                tableParameter,
                tableInfo.tableParameterInfo(),
                node.genericProperties(),
                parameters
            );
        } else if (!node.resetProperties().isEmpty()) {
            TablePropertiesAnalyzer.analyze(tableParameter, tableInfo.tableParameterInfo(), node.resetProperties());
        }
        return new AlterBlobTableAnalyzedStatement(tableInfo, tableParameter);
    }
}
