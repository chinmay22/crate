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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.UUID;

public class NodeFetchRequest extends TransportRequest {

    private UUID jobId;
    private int fetchPhaseId;
    private boolean closeContext;

    @Nullable
    private IntObjectMap<? extends IntContainer> toFetch;

    public NodeFetchRequest() {
    }

    public NodeFetchRequest(UUID jobId,
                            int fetchPhaseId,
                            boolean closeContext,
                            IntObjectMap<? extends IntContainer> toFetch) {
        this.jobId = jobId;
        this.fetchPhaseId = fetchPhaseId;
        this.closeContext = closeContext;
        if (!toFetch.isEmpty()) {
            this.toFetch = toFetch;
        }
    }

    public UUID jobId() {
        return jobId;
    }

    public int fetchPhaseId() {
        return fetchPhaseId;
    }

    public boolean isCloseContext() {
        return closeContext;
    }

    @Nullable
    public IntObjectMap<? extends IntContainer> toFetch() {
        return toFetch;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        jobId = new UUID(in.readLong(), in.readLong());
        fetchPhaseId = in.readVInt();
        closeContext = in.readBoolean();
        int numReaders = in.readVInt();
        if (numReaders > 0) {
            IntObjectHashMap<IntArrayList> toFetch = new IntObjectHashMap<>(numReaders);
            for (int i = 0; i < numReaders; i++) {
                int readerId = in.readVInt();
                int numDocs = in.readVInt();
                IntArrayList docs = new IntArrayList(numDocs);
                toFetch.put(readerId, docs);
                for (int j = 0; j < numDocs; j++) {
                    docs.add(in.readInt());
                }
                this.toFetch = toFetch;
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(jobId.getMostSignificantBits());
        out.writeLong(jobId.getLeastSignificantBits());
        out.writeVInt(fetchPhaseId);
        out.writeBoolean(closeContext);
        if (toFetch == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(toFetch.size());
            for (IntObjectCursor<? extends IntContainer> toFetchCursor : toFetch) {
                out.writeVInt(toFetchCursor.key);
                out.writeVInt(toFetchCursor.value.size());
                for (IntCursor docCursor : toFetchCursor.value) {
                    out.writeInt(docCursor.value);
                }
            }
        }
    }
}
