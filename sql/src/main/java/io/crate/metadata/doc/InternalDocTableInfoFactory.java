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

package io.crate.metadata.doc;

import com.google.common.annotations.VisibleForTesting;
import io.crate.metadata.Functions;
import io.crate.metadata.TableIdent;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ExecutorService;

@Singleton
public class InternalDocTableInfoFactory implements DocTableInfoFactory {

    private final Functions functions;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Provider<TransportPutIndexTemplateAction> putIndexTemplateActionProvider;
    private final ExecutorService executorService;

    @Inject
    public InternalDocTableInfoFactory(Functions functions,
                                       IndexNameExpressionResolver indexNameExpressionResolver,
                                       Provider<TransportPutIndexTemplateAction> putIndexTemplateActionProvider,
                                       ThreadPool threadPool) {
        this(functions,
            indexNameExpressionResolver,
            putIndexTemplateActionProvider,
            (ExecutorService) threadPool.executor(ThreadPool.Names.SUGGEST));
    }

    @VisibleForTesting
    InternalDocTableInfoFactory(Functions functions,
                                IndexNameExpressionResolver indexNameExpressionResolver,
                                Provider<TransportPutIndexTemplateAction> transportPutIndexTemplateActionProvider,
                                ExecutorService executorService) {
        this.functions = functions;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        putIndexTemplateActionProvider = transportPutIndexTemplateActionProvider;
        this.executorService = executorService;
    }

    @Override
    public DocTableInfo create(TableIdent ident, ClusterService clusterService) {
        boolean checkAliasSchema = clusterService.state().metaData().settings().getAsBoolean("crate.table_alias.schema_check", true);
        DocTableInfoBuilder builder = new DocTableInfoBuilder(
            functions,
            ident,
            clusterService,
            indexNameExpressionResolver,
            putIndexTemplateActionProvider.get(),
            executorService,
            checkAliasSchema
        );
        return builder.build();
    }
}
