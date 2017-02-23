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

package io.crate.planner.node;

import io.crate.planner.distribution.UpstreamPhase;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;

public class ExecutionPhases {

    /**
     * @return true if the executionNodes indicate a execution on the handler node.
     *
     * size == 0 is also true
     * (this currently is the case on MergePhases if there is a direct-response from previous phase to MergePhase)
     */
    public static boolean executesOnHandler(String handlerNode, Collection<String> executionNodes) {
        switch (executionNodes.size()) {
            case 0:
                return true;
            case 1:
                return executionNodes.iterator().next().equals(handlerNode);
            default:
                return false;
        }
    }

    public static ExecutionPhase fromStream(StreamInput in) throws IOException {
        ExecutionPhase.Type type = ExecutionPhase.Type.values()[in.readVInt()];
        ExecutionPhase node = type.factory().create();
        node.readFrom(in);
        return node;
    }

    public static void toStream(StreamOutput out, ExecutionPhase node) throws IOException {
        out.writeVInt(node.type().ordinal());
        node.writeTo(out);
    }

    public static boolean hasDirectResponseDownstream(Collection<String> downstreamNodes) {
        for (String nodeId : downstreamNodes) {
            if (nodeId.equals(ExecutionPhase.DIRECT_RETURN_DOWNSTREAM_NODE)) {
                return true;
            }
        }
        return false;
    }

    public static String debugPrint(ExecutionPhase phase) {
        StringBuilder sb = new StringBuilder("phase{id=");
        sb.append(phase.phaseId());
        sb.append("/");
        sb.append(phase.name());
        sb.append(", ");
        sb.append("nodes=");
        sb.append(phase.nodeIds());
        if (phase instanceof UpstreamPhase) {
            UpstreamPhase uPhase = (UpstreamPhase) phase;
            sb.append(", dist=");
            sb.append(uPhase.distributionInfo().distributionType());
        }
        sb.append("}");
        return sb.toString();
    }
}
