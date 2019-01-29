/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.integrationtests;

import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.carrotsearch.randomizedtesting.RandomizedTest.$;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, supportsDedicatedMasters = false, numClientNodes = 0)
public class DecommissionNodeStatementITest extends SQLTransportIntegrationTest {


    @Test
    public void testDecommisssionLocalNode() throws Exception {
        String localNodeId = internalCluster().clusterService().localNode().getId();

        execute("select id from sys.nodes");
        assertThat(response.rowCount(), is(2L));

        try {
            execute("alter cluster decommission ?", $(localNodeId));
            ensureYellow();
            execute("select id from sys.nodes");
            assertBusy(() -> assertThat(response.rowCount(), is(1L)), 60, TimeUnit.SECONDS);

        } finally {
            // satisfy min_master_nodes again; otherwise the teardown blocks
            internalCluster().startNode();
        }
    }

    @Test
    public void testDecommisssionRemoteNode() throws Exception {
        String localNodeId = internalCluster().clusterService().localNode().getId();

        execute("select id from sys.nodes where id != ?", new Object[] {localNodeId});
        assertThat(response.rowCount(), is(1L));

        String remoteNodeId = (String) response.rows()[0][0];

        try {
            execute("alter cluster decommission ?", $(remoteNodeId));
            ensureYellow();

            execute("select id from sys.nodes");
            assertBusy(() -> assertThat(response.rowCount(), is(1L)), 60, TimeUnit.SECONDS);

        } finally {
            // satisfy min_master_nodes again; otherwise the teardown blocks
            internalCluster().startNode();
        }
    }
}
