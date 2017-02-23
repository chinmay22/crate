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

package io.crate.operation.collect;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.operation.collect.sources.ShardCollectSource;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.index.shard.ShardId;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class ShardCollectorProviderTest extends SQLTransportIntegrationTest {

    public void assertNoShardEntriesLeftInShardCollectSource() throws Exception {
        final Field shards = ShardCollectSource.class.getDeclaredField("shards");
        shards.setAccessible(true);
        final List<ShardCollectSource> shardCollectSources = Lists.newArrayList((internalCluster().getInstances(ShardCollectSource.class)));
        for (ShardCollectSource shardCollectSource : shardCollectSources) {
            try {
                //noinspection unchecked
                Map<ShardId, ShardCollectorProvider> shardMap = (Map<ShardId, ShardCollectorProvider>) shards.get(shardCollectSource);
                assertThat(shardMap.size(), is(0));
            } catch (IllegalAccessException e) {
                Throwables.propagate(e);
            }
        }
    }

    @Test
    public void testClosedIndicesHaveNoShardEntries() throws Exception {
        execute("create table t(i int) with (number_of_replicas='0-all')");
        ensureGreen();
        client().admin().indices().close(new CloseIndexRequest("t")).actionGet();
        waitUntilShardOperationsFinished();
        assertNoShardEntriesLeftInShardCollectSource();
    }

    @Test
    public void testDeletedIndicesHaveNoShardEntries() throws Exception {
        execute("create table tt(i int) with (number_of_replicas='0-all')");
        ensureGreen();
        execute("drop table tt");
        waitUntilShardOperationsFinished();
        assertNoShardEntriesLeftInShardCollectSource();
    }
}
