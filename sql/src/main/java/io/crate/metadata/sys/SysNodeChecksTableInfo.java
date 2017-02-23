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

package io.crate.metadata.sys;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.WhereClause;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.table.Operation;
import io.crate.metadata.table.StaticTableInfo;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.EnumSet;
import java.util.Set;

@Singleton
public class SysNodeChecksTableInfo extends StaticTableInfo {

    public static final TableIdent IDENT = new TableIdent(SysSchemaInfo.NAME, "node_checks");
    private static final ImmutableList<ColumnIdent> PRIMARY_KEYS = ImmutableList.of(SysNodeChecksTableInfo.Columns.ID);
    private static final RowGranularity GRANULARITY = RowGranularity.DOC;

    private final ClusterService clusterService;
    private final static Set<Operation> SUPPORTED_OPERATIONS = EnumSet.of(Operation.READ, Operation.UPDATE);

    public static class Columns {
        public static final ColumnIdent ID = new ColumnIdent("id");
        public static final ColumnIdent NODE_ID = new ColumnIdent("node_id");
        public static final ColumnIdent SEVERITY = new ColumnIdent("severity");
        public static final ColumnIdent DESCRIPTION = new ColumnIdent("description");
        public static final ColumnIdent PASSED = new ColumnIdent("passed");
        public static final ColumnIdent ACKNOWLEDGED = new ColumnIdent("acknowledged");
    }

    @Inject
    protected SysNodeChecksTableInfo(ClusterService clusterService) {
        super(IDENT, new ColumnRegistrar(IDENT, GRANULARITY)
                .register(SysNodeChecksTableInfo.Columns.ID, DataTypes.INTEGER)
                .register(SysNodeChecksTableInfo.Columns.NODE_ID, DataTypes.STRING)
                .register(SysNodeChecksTableInfo.Columns.SEVERITY, DataTypes.INTEGER)
                .register(SysNodeChecksTableInfo.Columns.DESCRIPTION, DataTypes.STRING)
                .register(SysNodeChecksTableInfo.Columns.PASSED, DataTypes.BOOLEAN)
                .register(SysNodeChecksTableInfo.Columns.ACKNOWLEDGED, DataTypes.BOOLEAN),
            PRIMARY_KEYS);
        this.clusterService = clusterService;
    }

    @Override
    public RowGranularity rowGranularity() {
        return GRANULARITY;
    }

    @Override
    public Routing getRouting(WhereClause whereClause, @Nullable String preference) {
        return Routing.forTableOnAllNodes(IDENT, clusterService.state().nodes());
    }

    @Override
    public Set<Operation> supportedOperations() {
        return SUPPORTED_OPERATIONS;
    }
}
