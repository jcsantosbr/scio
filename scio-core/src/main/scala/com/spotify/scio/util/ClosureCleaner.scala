/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.util

import java.io.NotSerializableException

import org.apache.beam.sdk.util.SerializableUtils

private[scio] object ClosureCleaner {
  def apply[T <: AnyRef](obj: T): T = {
    try {
      SerializableUtils.serializeToByteArray(obj.asInstanceOf[Serializable])
    } catch {
      case e: IllegalArgumentException if e.getCause.isInstanceOf[NotSerializableException] =>
        ChillClosureCleaner(obj)
      case e: ClassCastException =>
        ChillClosureCleaner(obj)
    }
    obj
  }
}
