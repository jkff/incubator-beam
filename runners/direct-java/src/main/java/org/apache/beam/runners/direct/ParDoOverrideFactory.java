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
package org.apache.beam.runners.direct;

import org.apache.beam.runners.core.GroupByKeyIntoKeyedWorkItems;
import org.apache.beam.runners.core.SplittableParDo;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFnAdapters;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature;
import org.apache.beam.sdk.transforms.reflect.DoFnSignatures;
import org.apache.beam.sdk.util.KeyedWorkItem;
import org.apache.beam.sdk.util.KeyedWorkItemCoder;
import org.apache.beam.sdk.util.ReifyTimestampsAndWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

/**
 * A {@link PTransformOverrideFactory} that provides overrides for applications of a {@link ParDo}
 * in the direct runner. Currently overrides applications of <a
 * href="https://s.apache.org/splittable-do-fn">Splittable DoFn</a>.
 */
class ParDoOverrideFactory implements PTransformOverrideFactory {
  @Override
  @SuppressWarnings("unchecked")
  public <InputT extends PInput, OutputT extends POutput> PTransform<InputT, OutputT> override(
      PTransform<InputT, OutputT> transform) {
    if (!(transform instanceof ParDo.Bound)) {
      return transform;
    }
    ParDo.Bound<InputT, OutputT> that = (ParDo.Bound<InputT, OutputT>) transform;
    DoFn<InputT, OutputT> fn = DoFnAdapters.getDoFn(that.getFn());
    if (fn == null) {
      // This is an OldDoFn, hence not splittable.
      return transform;
    }
    DoFnSignature signature = DoFnSignatures.INSTANCE.getOrParseSignature(fn.getClass());
    if (!signature.processElement().isSplittable()) {
      return transform;
    }
    return new SplittableParDo(that.getName(), fn, new DirectGroupByKeyIntoKeyedWorkItems());
  }

  /** The Direct Runner specific implementation of {@link GroupByKeyIntoKeyedWorkItems}. */
  private static class DirectGroupByKeyIntoKeyedWorkItems<InputT>
      implements GroupByKeyIntoKeyedWorkItems<String, InputT> {
    @Override
    public PTransform<PCollection<KV<String, InputT>>, PCollection<KeyedWorkItem<String, InputT>>>
        forInputCoder(Coder<InputT> inputCoder) {
      return new Transform<>(inputCoder);
    }

    static class Transform<InputT>
        extends PTransform<
            PCollection<KV<String, InputT>>, PCollection<KeyedWorkItem<String, InputT>>> {
      private final Coder<InputT> inputCoder;

      private Transform(Coder<InputT> inputCoder) {
        this.inputCoder = inputCoder;
      }

      @Override
      public PCollection<KeyedWorkItem<String, InputT>> apply(
          PCollection<KV<String, InputT>> input) {
        return input
            .apply(new ReifyTimestampsAndWindows<String, InputT>())
            .apply(new DirectGroupByKey.DirectGroupByKeyOnly<String, InputT>())
            .setCoder(
                KeyedWorkItemCoder.of(
                    StringUtf8Coder.of(),
                    inputCoder,
                    input.getWindowingStrategy().getWindowFn().windowCoder()));
      }
    }
  }
}
