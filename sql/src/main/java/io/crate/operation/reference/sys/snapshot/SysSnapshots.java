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

package io.crate.operation.reference.sys.snapshot;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.crate.operation.reference.sys.repositories.SysRepositoriesService;
import io.crate.operation.reference.sys.repositories.SysRepository;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotsService;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class SysSnapshots {

    private final SysRepositoriesService sysRepositoriesService;
    private final SnapshotsService snapshotsService;
    private static final ESLogger LOGGER = Loggers.getLogger(SysSnapshots.class);

    @Inject
    public SysSnapshots(SysRepositoriesService sysRepositoriesService, SnapshotsService snapshotsService) {
        this.sysRepositoriesService = sysRepositoriesService;
        this.snapshotsService = snapshotsService;
    }

    public Iterable<SysSnapshot> snapshotsGetter() {
        List<SysSnapshot> sysSnapshots = new ArrayList<>();
        for (Object entry : sysRepositoriesService.repositoriesGetter()) {
            final String repositoryName = ((SysRepository) entry).name();

            List<Snapshot> snapshots;
            try {
                snapshots = snapshotsService.snapshots(repositoryName, true);
            } catch (Throwable t) {
                // TODO: catch can probably be removed due to ignore unavailable flag?
                LOGGER.warn("Error occurred listing snapshots of repository {}", t, repositoryName);
                continue;
            }
            sysSnapshots.addAll(Lists.transform(snapshots, new Function<Snapshot, SysSnapshot>() {
                @Nullable
                @Override
                public SysSnapshot apply(@Nullable Snapshot snapshot) {
                    if (snapshot == null) {
                        return null;
                    }
                    return new SysSnapshot(
                        snapshot.name(),
                        repositoryName,
                        snapshot.indices(),
                        snapshot.startTime(),
                        snapshot.endTime(),
                        snapshot.version().toString(),
                        snapshot.state().name()
                    );
                }
            }));
        }
        return sysSnapshots;
    }
}
