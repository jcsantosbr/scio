/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.transforms;

import com.google.common.collect.Maps;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public abstract class DoFnWithExternalResource<InputT, OutputT, ResourceT>
    extends DoFn<InputT, OutputT> {

  public enum Parallelism {
    /** One copy per sub-class. */
    CLASS,
    /** One copy per instance. */
    INSTANCE,
    /** One copy per thread. */
    THREAD,
  }

  private final UUID instanceId = UUID.randomUUID();
  private String resourceId;
  private transient ResourceT resource;

  public abstract Parallelism getParallelism();
  public abstract ResourceT setupResource();
  public abstract void teardownResource(ResourceT resource);

  private String getResourceId() {
    String className = this.getClass().getName();
    switch (getParallelism()) {
      case CLASS:
        return className;
      case INSTANCE:
        return className + "/" + instanceId.toString();
      case THREAD:
        return className + "/" + instanceId.toString() + "/" + UUID.randomUUID().toString();
    }
  }

  @Setup
  public void setup() {
    resourceId = getResourceId();
    resource = ResourceManager.get(resourceId, (id) -> setupResource());
  }

  @Teardown
  public void teardown() {
    // ref counting?
    teardownResource(resource);
  }

  private static class ResourceManager {

    private static ConcurrentMap<String, Object> resources = Maps.newConcurrentMap();

    @SuppressWarnings("unchecked")
    public static <T> T get(String resourceId, Function<String, T> fn) {
      return (T) resources.computeIfAbsent(resourceId, fn);
    }

  }

}
