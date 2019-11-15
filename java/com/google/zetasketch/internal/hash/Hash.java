/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.zetasketch.internal.hash;

/**
 * Library to compute standard 64 bit hashes for values.
 *
 * <p>This library is designed to return specific hashes that are compatible with other programming
 * languages, in particular C++ and Go. This is important so that aggregators that use hashes
 * produce {@code AggregatorStateProto}s that are exchangeable between different implementations.
 */
public interface Hash {
  /** Returns the 64 bit hash of the byte array value. */
  public long of(byte[] value); 

  /** Returns the 64 bit hash of the integer value. */
  public long of(int value);

  /** Returns the 64 bit hash of the long value. */
  public long of(long value); 

  /** Returns the 64 bit hash of the String value. */
  public long of(String value);
}

