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
package org.apache.beam.sdk.testing;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.Instant;

/**
 * An immutable triple of value, timestamp, and window.
 *
 * @param <T> the type of the value
 */
@AutoValue
public abstract class ValueInSingleWindow<T> {
  /** Returns the value of this {@code ValueInSingleWindow}. */
  public abstract T getValue();

  /** Returns the timestamp of this {@code ValueInSingleWindow}. */
  public abstract Instant getTimestamp();

  /** Returns the window of this {@code ValueInSingleWindow}. */
  public abstract BoundedWindow getWindow();

  /** Returns the pane of this {@code ValueInSingleWindow} in its window. */
  public abstract PaneInfo getPane();

  @Override
  public abstract String toString();

  public static <T> ValueInSingleWindow<T> of(
      T value, Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
    return new AutoValue_ValueInSingleWindow<>(value, timestamp, window, paneInfo);
  }

  public static class FullCoder<T> extends AtomicCoder<ValueInSingleWindow<T>> {
    private final Coder<T> valueCoder;
    private final Coder<BoundedWindow> windowCoder;

    public static <T> FullCoder<T> of(
        Coder<T> valueCoder, Coder<? extends BoundedWindow> windowCoder) {
      return new FullCoder<>(valueCoder, windowCoder);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    FullCoder(Coder<T> valueCoder, Coder<? extends BoundedWindow> windowCoder) {
      this.valueCoder = valueCoder;
      this.windowCoder = (Coder) windowCoder;
    }

    @Override
    public void encode(ValueInSingleWindow<T> windowedElem, OutputStream outStream, Context context)
        throws IOException {
      Context nestedContext = context.nested();
      valueCoder.encode(windowedElem.getValue(), outStream, nestedContext);
      InstantCoder.of().encode(windowedElem.getTimestamp(), outStream, nestedContext);
      windowCoder.encode(windowedElem.getWindow(), outStream, nestedContext);
      PaneInfo.PaneInfoCoder.INSTANCE.encode(windowedElem.getPane(), outStream, context);
    }

    @Override
    public ValueInSingleWindow<T> decode(InputStream inStream, Context context) throws IOException {
      Context nestedContext = context.nested();
      T value = valueCoder.decode(inStream, nestedContext);
      Instant timestamp = InstantCoder.of().decode(inStream, nestedContext);
      BoundedWindow window = windowCoder.decode(inStream, nestedContext);
      PaneInfo pane = PaneInfo.PaneInfoCoder.INSTANCE.decode(inStream, nestedContext);
      return new AutoValue_ValueInSingleWindow<>(value, timestamp, window, pane);
    }
  }
}
