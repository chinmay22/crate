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

package io.crate.exceptions;

import io.crate.test.integration.CrateUnitTest;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.core.Is.is;

public class FailedShardsExceptionTest extends CrateUnitTest {

    @Test
    public void testThatGenMessageDoesNotRaiseNPEIfShardOperationFailedExceptionIsNull() throws Exception {
        //noinspection ThrowableInstanceNeverThrown
        FailedShardsException exception = new FailedShardsException(new ShardOperationFailedException[]{null});
        assertThat(exception.getMessage(), is("query failed on unknown shard / table"));
    }

    @Test
    public void testShardFailureReasonIsNull() throws Exception {
        FailedShardsException exception = new FailedShardsException(new ShardOperationFailedException[]{new ShardOperationFailedException() {
            @Override
            public String index() {
                return null;
            }

            @Override
            public int shardId() {
                return 0;
            }

            @Override
            public String reason() {
                return null;
            }

            @Override
            public RestStatus status() {
                return null;
            }

            @Override
            public void readFrom(StreamInput in) throws IOException {
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
            }

            @Override
            public Throwable getCause() {
                return null;
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                return null;
            }
        }, null});
        assertThat(exception.getMessage(), is("query failed on shards 0 ( null )"));
    }
}
