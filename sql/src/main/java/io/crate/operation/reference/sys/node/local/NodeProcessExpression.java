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

package io.crate.operation.reference.sys.node.local;

import io.crate.metadata.ReferenceImplementation;
import io.crate.monitor.ExtendedProcessCpuStats;
import io.crate.operation.reference.NestedObjectExpression;
import org.elasticsearch.monitor.process.ProcessStats;

class NodeProcessExpression extends NestedObjectExpression {

    private static final String OPEN_FILE_DESCRIPTORS = "open_file_descriptors";
    private static final String MAX_OPEN_FILE_DESCRIPTORS = "max_open_file_descriptors";
    private static final String PROBE_TIMESTAMP = "probe_timestamp";
    private static final String CPU = "cpu";

    NodeProcessExpression(ProcessStats processStats, ExtendedProcessCpuStats cpuStats) {
        addChildImplementations(processStats, cpuStats);
    }

    private void addChildImplementations(final ProcessStats processStats, ExtendedProcessCpuStats cpuStats) {
        childImplementations.put(OPEN_FILE_DESCRIPTORS, new ReferenceImplementation<Long>() {
            @Override
            public Long value() {
                if (processStats != null) {
                    return processStats.getOpenFileDescriptors();
                } else {
                    return -1L;
                }
            }
        });
        childImplementations.put(MAX_OPEN_FILE_DESCRIPTORS, new ReferenceImplementation<Long>() {
            @Override
            public Long value() {
                if (processStats != null) {
                    return processStats.getMaxFileDescriptors();
                } else {
                    return -1L;
                }
            }
        });
        childImplementations.put(PROBE_TIMESTAMP, new ReferenceImplementation<Long>() {
            @Override
            public Long value() {
                if (processStats != null) {
                    return processStats.getTimestamp();
                } else {
                    return -1L;
                }
            }
        });
        childImplementations.put(CPU, new NodeProcessCpuExpression(cpuStats));
    }
}
