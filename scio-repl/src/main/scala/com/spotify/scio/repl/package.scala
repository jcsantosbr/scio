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

package com.spotify.scio

import com.spotify.scio.values.SCollection

import scala.reflect.ClassTag

package object repl {

  // TODO: scala 2.11
  // implicit class ReplSCollection[T: ClassTag](private val self: SCollection[T]) extends AnyVal {
  implicit class ReplSCollection[T: ClassTag](val self: SCollection[T]) {

    /** Convenience method to close the current [[ScioContext]] and collect elements. */
    def closeAndCollect(): Iterator[T] = {
      val f = self.materialize
      self.context.close()
      f.waitForResult().value
    }
  }

}
