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

package io.crate.integrationtests;

import com.google.common.base.Throwables;
import io.crate.blob.v2.BlobIndex;
import io.crate.blob.v2.BlobIndicesService;
import io.crate.blob.v2.BlobShard;
import io.crate.plugin.BlobPlugin;
import io.crate.rest.CrateRestFilter;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.core.Is.is;

public abstract class BlobIntegrationTestBase extends ESIntegTestCase {

    private Field indicesField;
    private Field shardsField;

    @Before
    public void initFields() throws Exception {
        indicesField = BlobIndicesService.class.getDeclaredField("indices");
        indicesField.setAccessible(true);
        shardsField = BlobIndex.class.getDeclaredField("shards");
        shardsField.setAccessible(true);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(Node.HTTP_ENABLED, true)
            .put(CrateRestFilter.ES_API_ENABLED_SETTING, true)
            .build();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(BlobPlugin.class);
    }

    @After
    public void assertNoTmpFilesAndNoIndicesRemaining() throws Exception {
        assertBusy(() -> forEachIndicesMap(i -> {
            for (BlobIndex blobIndex : i.values()) {
                try {
                    Map<Integer, BlobShard> o = (Map<Integer, BlobShard>) shardsField.get(blobIndex);
                    for (BlobShard blobShard : o.values()) {
                        Path tmpDir = blobShard.blobContainer().getTmpDirectory();
                        try (Stream<Path> files = Files.list(tmpDir)) {
                            assertThat(files.count(), is(0L));
                        }
                    }
                } catch (IOException | IllegalAccessException e) {
                    throw Throwables.propagate(e);
                }
            }
        }));

        internalCluster().wipeIndices("_all");
        assertBusy(() -> forEachIndicesMap(i -> assertThat(i.keySet(), empty())));
    }

    private void forEachIndicesMap(Consumer<Map<String, BlobIndex>> consumer) {
        Iterable<BlobIndicesService> blobIndicesServices = internalCluster().getInstances(BlobIndicesService.class);
        for (BlobIndicesService blobIndicesService : blobIndicesServices) {
            try {
                Map<String, BlobIndex> indices = (Map<String, BlobIndex>) indicesField.get(blobIndicesService);
                consumer.accept(indices);
            } catch (IllegalAccessException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
