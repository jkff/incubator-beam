/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.datastore;

import org.apache.beam.sdk.annotations.Experimental;

/**
 * <p>{@link DatastoreIO} provides an API for reading from and writing to
 * <a href="https://developers.google.com/datastore/">Google Cloud Datastore</a> over different
 * versions of the Datastore Client libraries.
 *
 * <p>To use the v1beta3 version see {@link V1Beta3}.
 */
@Experimental(Experimental.Kind.SOURCE_SINK)
public class DatastoreIO {

  private DatastoreIO() {}

  /**
   * Returns a {@link V1Beta3} that provides an API for accessing Datastore through v1beta3 version
   * of Datastore Client library.
   */
  public static V1Beta3 v1beta3() {
    return new V1Beta3();
  }
}
