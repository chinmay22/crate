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

package io.crate.executor.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.transport.*;

import java.util.Locale;


@Singleton
public class Transports {

    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public Transports(ClusterService clusterService, TransportService transportService) {
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    public <TRequest extends TransportRequest, TResponse extends TransportResponse> void sendRequest(
        String action,
        String node,
        TRequest request,
        ActionListener<TResponse> listener,
        TransportResponseHandler<TResponse> handler,
        TransportRequestOptions options) {

        DiscoveryNode discoveryNode = clusterService.state().nodes().get(node);
        if (discoveryNode == null) {
            listener.onFailure(new IllegalArgumentException(
                String.format(Locale.ENGLISH, "node \"%s\" not found in cluster state!", node)));
            return;
        }
        transportService.sendRequest(discoveryNode, action, request, options, handler);
    }

    public <TRequest extends TransportRequest, TResponse extends TransportResponse> void sendRequest(
        String action,
        String node,
        TRequest request,
        ActionListener<TResponse> listener,
        TransportResponseHandler<TResponse> handler) {
        sendRequest(action, node, request, listener, handler, TransportRequestOptions.EMPTY);
    }
}
