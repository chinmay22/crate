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

package io.crate.operation.projectors.fetch;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectMap;
import com.google.common.base.Function;
import io.crate.Streamer;
import io.crate.action.FutureActionListener;
import io.crate.data.Bucket;
import io.crate.executor.transport.NodeFetchRequest;
import io.crate.executor.transport.NodeFetchResponse;
import io.crate.executor.transport.TransportFetchNodeAction;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class TransportFetchOperation implements FetchOperation {

    private static final Function<NodeFetchResponse, IntObjectMap<? extends Bucket>> GET_FETCHED =
        new Function<NodeFetchResponse, IntObjectMap<? extends Bucket>>() {

            @Nullable
            @Override
            public IntObjectMap<? extends Bucket> apply(@Nullable NodeFetchResponse input) {
                return input == null ? null : input.fetched();
            }
        };
    private final TransportFetchNodeAction transportFetchNodeAction;
    private final Map<String, ? extends IntObjectMap<Streamer[]>> nodeIdToReaderIdToStreamers;
    private final UUID jobId;
    private final int executionPhaseId;

    public TransportFetchOperation(TransportFetchNodeAction transportFetchNodeAction,
                                   Map<String, ? extends IntObjectMap<Streamer[]>> nodeIdToReaderIdToStreamers,
                                   UUID jobId,
                                   int executionPhaseId) {
        this.transportFetchNodeAction = transportFetchNodeAction;
        this.nodeIdToReaderIdToStreamers = nodeIdToReaderIdToStreamers;
        this.jobId = jobId;
        this.executionPhaseId = executionPhaseId;
    }

    @Override
    public CompletableFuture<IntObjectMap<? extends Bucket>> fetch(String nodeId,
                                                                   IntObjectMap<? extends IntContainer> toFetch,
                                                                   boolean closeContext) {
        FutureActionListener<NodeFetchResponse, IntObjectMap<? extends Bucket>> listener = new FutureActionListener<>(GET_FETCHED);
        transportFetchNodeAction.execute(
            nodeId,
            nodeIdToReaderIdToStreamers.get(nodeId),
            new NodeFetchRequest(jobId, executionPhaseId, closeContext, toFetch),
            listener);
        return listener;
    }
}
