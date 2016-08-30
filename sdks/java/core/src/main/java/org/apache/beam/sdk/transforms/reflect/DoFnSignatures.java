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
package org.apache.beam.sdk.transforms.reflect;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeParameter;
import com.google.common.reflect.TypeToken;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.common.ReflectHelpers;
import org.apache.beam.sdk.values.PCollection;

/**
 * Parses a {@link DoFn} and computes its {@link DoFnSignature}. See {@link #getOrParseSignature}.
 */
public class DoFnSignatures {
  public static final DoFnSignatures INSTANCE = new DoFnSignatures();

  private DoFnSignatures() {}

  private final Map<Class<?>, DoFnSignature> signatureCache = new LinkedHashMap<>();

  /** @return the {@link DoFnSignature} for the given {@link DoFn}. */
  public synchronized DoFnSignature getOrParseSignature(
      @SuppressWarnings("rawtypes") Class<? extends DoFn> fn) {
    DoFnSignature signature = signatureCache.get(fn);
    if (signature == null) {
      signatureCache.put(fn, signature = parseSignature(fn));
    }
    return signature;
  }

  /** Analyzes a given {@link DoFn} class and extracts its {@link DoFnSignature}. */
  private static DoFnSignature parseSignature(Class<? extends DoFn> fnClass) {
    TypeToken<?> inputT = null;
    TypeToken<?> outputT = null;

    PCollection.IsBounded isBounded = null;

    // Extract the input and output type, and whether the fn is bounded.
    checkArgument(
        DoFn.class.isAssignableFrom(fnClass),
        "%s must be subtype of DoFn",
        fnClass.getSimpleName());
    TypeToken<? extends DoFn> fnToken = TypeToken.of(fnClass);
    for (TypeToken<?> supertype : fnToken.getTypes()) {
      if (supertype.getRawType().isAnnotationPresent(DoFn.Bounded.class)) {
        checkArgument(
            isBounded == null, "Both @Bounded and @Unbounded specified on %s", formatType(fnToken));
        isBounded = PCollection.IsBounded.BOUNDED;
      }
      if (supertype.getRawType().isAnnotationPresent(DoFn.Unbounded.class)) {
        checkArgument(
            isBounded == null, "Both @Bounded and @Unbounded specified on %s", formatType(fnToken));
        isBounded = PCollection.IsBounded.UNBOUNDED;
      }
      if (!supertype.getRawType().equals(DoFn.class)) {
        continue;
      }
      Type[] args = ((ParameterizedType) supertype.getType()).getActualTypeArguments();
      inputT = TypeToken.of(args[0]);
      outputT = TypeToken.of(args[1]);
    }
    checkNotNull(inputT, "Unable to determine input type from %s", fnClass);

    Method processElementMethod = findAnnotatedMethod(DoFn.ProcessElement.class, fnClass, true);
    Method startBundleMethod = findAnnotatedMethod(DoFn.StartBundle.class, fnClass, false);
    Method finishBundleMethod = findAnnotatedMethod(DoFn.FinishBundle.class, fnClass, false);
    Method setupMethod = findAnnotatedMethod(DoFn.Setup.class, fnClass, false);
    Method teardownMethod = findAnnotatedMethod(DoFn.Teardown.class, fnClass, false);

    Method getInitialRestrictionMethod =
        findAnnotatedMethod(DoFn.GetInitialRestriction.class, fnClass, false);
    Method splitRestrictionMethod =
        findAnnotatedMethod(DoFn.SplitRestriction.class, fnClass, false);
    Method getRestrictionCoderMethod =
        findAnnotatedMethod(DoFn.GetRestrictionCoder.class, fnClass, false);
    Method newTrackerMethod = findAnnotatedMethod(DoFn.NewTracker.class, fnClass, false);

    DoFnSignature.ProcessElementMethod processElement =
        analyzeProcessElementMethod(fnToken, processElementMethod, inputT, outputT);

    DoFnSignature.BundleMethod startBundle =
        (startBundleMethod == null)
            ? null
            : analyzeBundleMethod(fnToken, startBundleMethod, inputT, outputT);
    DoFnSignature.BundleMethod finishBundle =
        (finishBundleMethod == null)
            ? null
            : analyzeBundleMethod(fnToken, finishBundleMethod, inputT, outputT);
    DoFnSignature.LifecycleMethod setup =
        (setupMethod == null) ? null : analyzeLifecycleMethod(setupMethod);
    DoFnSignature.LifecycleMethod teardown =
        (teardownMethod == null) ? null : analyzeLifecycleMethod(teardownMethod);

    DoFnSignature.GetInitialRestrictionMethod getInitialRestriction =
        (getInitialRestrictionMethod == null)
            ? null
            : analyzeGetInitialRestrictionMethod(fnToken, getInitialRestrictionMethod, inputT);
    DoFnSignature.SplitRestrictionMethod splitRestriction =
        (splitRestrictionMethod == null)
            ? null
            : analyzeSplitRestrictionMethod(fnToken, splitRestrictionMethod, inputT);
    DoFnSignature.GetRestrictionCoderMethod getRestrictionCoder =
        (getRestrictionCoderMethod == null)
            ? null
            : analyzeGetRestrictionCoderMethod(fnToken, getRestrictionCoderMethod);
    DoFnSignature.NewTrackerMethod newTracker =
        (newTrackerMethod == null) ? null : analyzeNewTrackerMethod(fnToken, newTrackerMethod);

    // Additional validation for splittable DoFn's.
    if (processElement.isSplittable()) {
      if (isBounded == null) {
        isBounded =
            processElement.hasReturnValue()
                ? PCollection.IsBounded.UNBOUNDED
                : PCollection.IsBounded.BOUNDED;
      }
      checkNotNull(
          getInitialRestriction,
          "%s defines @ProcessElement method %s which is splittable, "
              + "but does not define a @GetInitialRestriction method",
          formatType(fnToken),
          format(processElementMethod));
      checkNotNull(
          newTracker,
          "%s defines @ProcessElement method %s which is splittable, "
              + "but does not define a @NewTracker method",
          formatType(fnToken),
          format(processElementMethod));
      checkArgument(
          processElement.trackerT().equals(newTracker.trackerT()),
          "%s defines @ProcessElement method %s with tracker type %s, "
              + "but a @NewTracker method %s with tracker type %s",
          formatType(fnToken),
          format(processElementMethod),
          formatType(processElement.trackerT()),
          format(newTrackerMethod),
          formatType(newTracker.trackerT()));
      checkArgument(
          getInitialRestriction.restrictionT().equals(newTracker.restrictionT()),
          "%s defines @GetInitialRestriction method %s with restriction type %s, "
              + " but a @NewTracker method %s with a different restriction type %s",
          formatType(fnToken),
          format(getInitialRestrictionMethod),
          formatType(getInitialRestriction.restrictionT()),
          format(newTrackerMethod),
          formatType(newTracker.restrictionT()));
      if (getRestrictionCoder != null) {
        checkArgument(
            getRestrictionCoder
                .coderT()
                .isSubtypeOf(coderTypeOf(getInitialRestriction.restrictionT())),
            "%s defines @GetInitialRestriction method %s with restriction type %s, "
                + "but a @GetRestrictionCoder method %s whose return type %s "
                + "is not a subtype of %s",
            formatType(fnToken),
            format(getInitialRestrictionMethod),
            formatType(getInitialRestriction.restrictionT()),
            format(getRestrictionCoderMethod),
            formatType(getRestrictionCoder.coderT()),
            formatType(coderTypeOf(getInitialRestriction.restrictionT())));
      }
      checkArgument(
          splitRestriction.restrictionT().equals(getInitialRestriction.restrictionT()),
          "%s defines @GetInitialRestriction method %s with restriction type %s, "
              + "but a @SplitRestriction method %s with a different restriction type %s",
          formatType(fnToken),
          format(getInitialRestrictionMethod),
          formatType(getInitialRestriction.restrictionT()),
          format(splitRestrictionMethod),
          formatType(splitRestriction.restrictionT()));
    } else {
      checkArgument(
          isBounded == null,
          "%s defines @ProcessElement method %s which is non-splittable, "
              + " but is annotated as "
              + ((isBounded == PCollection.IsBounded.BOUNDED) ? "@Bounded" : "@Unbounded"),
          formatType(fnToken),
          format(processElementMethod));
      isBounded = PCollection.IsBounded.BOUNDED;

      checkArgument(
          getInitialRestriction == null,
          "%s defines @ProcessElement method %s which is non-splittable, "
              + "but defines a @GetInitialRestriction method %s",
          formatType(fnToken),
          format(processElementMethod),
          format(getInitialRestrictionMethod));
      checkArgument(
          splitRestriction == null,
          "%s defines @ProcessElement method %s which is non-splittable, "
              + "but defines a @SplitRestriction method %s",
          formatType(fnToken),
          format(processElementMethod),
          format(splitRestrictionMethod));
      checkArgument(
          newTracker == null,
          "%s defines @ProcessElement method %s which is non-splittable, "
              + "but defines a @NewTracker method %s",
          formatType(fnToken),
          format(processElementMethod),
          format(newTrackerMethod));
      checkArgument(
          newTracker == null,
          "%s defines @ProcessElement method %s which is non-splittable, "
              + "but defines a @GetRestrictionCoder method %s",
          formatType(fnToken),
          format(processElementMethod),
          format(getRestrictionCoderMethod));
    }

    return DoFnSignature.create(
        fnClass,
        isBounded,
        processElement,
        startBundle,
        finishBundle,
        setup,
        teardown,
        getInitialRestriction,
        splitRestriction,
        getRestrictionCoder,
        newTracker);
  }

  /**
   * Generates a type token for {@code DoFn<InputT, OutputT>.ProcessContext} given {@code InputT}
   * and {@code OutputT}.
   */
  private static <InputT, OutputT>
      TypeToken<DoFn<InputT, OutputT>.ProcessContext> doFnProcessContextTypeOf(
          TypeToken<InputT> inputT, TypeToken<OutputT> outputT) {
    return new TypeToken<DoFn<InputT, OutputT>.ProcessContext>() {}.where(
            new TypeParameter<InputT>() {}, inputT)
        .where(new TypeParameter<OutputT>() {}, outputT);
  }

  /**
   * Generates a type token for {@code DoFn<InputT, OutputT>.Context} given {@code InputT} and
   * {@code OutputT}.
   */
  private static <InputT, OutputT> TypeToken<DoFn<InputT, OutputT>.Context> doFnContextTypeOf(
      TypeToken<InputT> inputT, TypeToken<OutputT> outputT) {
    return new TypeToken<DoFn<InputT, OutputT>.Context>() {}.where(
            new TypeParameter<InputT>() {}, inputT)
        .where(new TypeParameter<OutputT>() {}, outputT);
  }

  /** Generates a type token for {@code DoFn.InputProvider<InputT>} given {@code InputT}. */
  private static <InputT> TypeToken<DoFn.InputProvider<InputT>> inputProviderTypeOf(
      TypeToken<InputT> inputT) {
    return new TypeToken<DoFn.InputProvider<InputT>>() {}.where(
        new TypeParameter<InputT>() {}, inputT);
  }

  /** Generates a type token for {@code DoFn.OutputReceiver<OutputT>} given {@code OutputT}. */
  private static <OutputT> TypeToken<DoFn.OutputReceiver<OutputT>> outputReceiverTypeOf(
      TypeToken<OutputT> inputT) {
    return new TypeToken<DoFn.OutputReceiver<OutputT>>() {}.where(
        new TypeParameter<OutputT>() {}, inputT);
  }

  @VisibleForTesting
  static DoFnSignature.ProcessElementMethod analyzeProcessElementMethod(
      TypeToken<? extends DoFn> fnClass, Method m, TypeToken<?> inputT, TypeToken<?> outputT) {
    checkArgument(
        void.class.equals(m.getReturnType())
            || DoFn.ProcessContinuation.class.equals(m.getReturnType()),
        "%s must have a void or ProcessContinuation return type",
        format(m));
    checkArgument(!m.isVarArgs(), "%s must not have var args", format(m));

    TypeToken<?> processContextToken = doFnProcessContextTypeOf(inputT, outputT);

    Type[] params = m.getGenericParameterTypes();
    TypeToken<?> contextToken = null;
    if (params.length > 0) {
      contextToken = fnClass.resolveType(params[0]);
    }
    checkArgument(
        contextToken != null && contextToken.equals(processContextToken),
        "%s must take a %s as its first argument",
        format(m),
        formatType(processContextToken));

    List<DoFnSignature.Parameter> extraParameters = new ArrayList<>();
    TypeToken<?> trackerT = null;

    TypeToken<?> expectedInputProviderT = inputProviderTypeOf(inputT);
    TypeToken<?> expectedOutputReceiverT = outputReceiverTypeOf(outputT);
    for (int i = 1; i < params.length; ++i) {
      TypeToken<?> paramT = fnClass.resolveType(params[i]);
      Class<?> rawType = paramT.getRawType();
      if (rawType.equals(BoundedWindow.class)) {
        checkArgument(
            !extraParameters.contains(DoFnSignature.Parameter.BOUNDED_WINDOW),
            "Multiple BoundedWindow parameters in %s",
            format(m));
        extraParameters.add(DoFnSignature.Parameter.BOUNDED_WINDOW);
      } else if (rawType.equals(DoFn.InputProvider.class)) {
        checkArgument(
            !extraParameters.contains(DoFnSignature.Parameter.INPUT_PROVIDER),
            "Multiple InputProvider parameters in %s",
            format(m));
        checkArgument(
            paramT.equals(expectedInputProviderT),
            "Wrong type of InputProvider parameter for method %s: %s, should be %s",
            format(m),
            formatType(paramT),
            formatType(expectedInputProviderT));
        extraParameters.add(DoFnSignature.Parameter.INPUT_PROVIDER);
      } else if (rawType.equals(DoFn.OutputReceiver.class)) {
        checkArgument(
            !extraParameters.contains(DoFnSignature.Parameter.OUTPUT_RECEIVER),
            "Multiple OutputReceiver parameters in %s",
            format(m));
        checkArgument(
            paramT.equals(expectedOutputReceiverT),
            "Wrong type of OutputReceiver parameter for method %s: %s, should be %s",
            format(m),
            formatType(paramT),
            formatType(expectedOutputReceiverT));
        extraParameters.add(DoFnSignature.Parameter.OUTPUT_RECEIVER);
      } else if (RestrictionTracker.class.isAssignableFrom(rawType)) {
        checkArgument(
            !extraParameters.contains(DoFnSignature.Parameter.RESTRICTION_TRACKER),
            "Multiple RestrictionTracker parameters in %s",
            format(m));
        extraParameters.add(DoFnSignature.Parameter.RESTRICTION_TRACKER);
        trackerT = paramT;
      } else {
        List<String> allowedParamTypes =
            Arrays.asList(
                formatType(new TypeToken<BoundedWindow>() {}),
                formatType(new TypeToken<RestrictionTracker<?>>() {}));
        checkArgument(
            false,
            "%s is not a valid context parameter for method %s. Should be one of %s",
            formatType(paramT),
            format(m),
            allowedParamTypes);
      }
    }

    // A splittable DoFn can not have any other extra context parameters.
    if (extraParameters.contains(DoFnSignature.Parameter.RESTRICTION_TRACKER)) {
      checkArgument(
          extraParameters.size() == 1,
          "%s is splittable and must not have any extra context arguments apart from %s, "
              + "but has: %s",
          format(m),
          trackerT,
          extraParameters);
    }

    return DoFnSignature.ProcessElementMethod.create(
        m, extraParameters, trackerT, DoFn.ProcessContinuation.class.equals(m.getReturnType()));
  }

  @VisibleForTesting
  static DoFnSignature.BundleMethod analyzeBundleMethod(
      TypeToken<? extends DoFn> fnToken, Method m, TypeToken<?> inputT, TypeToken<?> outputT) {
    checkArgument(
        void.class.equals(m.getReturnType()), "%s must have a void return type", format(m));
    checkArgument(!m.isVarArgs(), "%s must not have var args", format(m));

    TypeToken<?> expectedContextToken = doFnContextTypeOf(inputT, outputT);

    Type[] params = m.getGenericParameterTypes();
    checkArgument(
        params.length == 1,
        "%s must have a single argument of type %s",
        format(m),
        formatType(expectedContextToken));
    TypeToken<?> contextToken = fnToken.resolveType(params[0]);
    checkArgument(
        contextToken.equals(expectedContextToken),
        "Wrong type of context argument to %s: %s, must be %s",
        format(m),
        formatType(contextToken),
        formatType(expectedContextToken));

    return DoFnSignature.BundleMethod.create(m);
  }

  private static DoFnSignature.LifecycleMethod analyzeLifecycleMethod(Method m) {
    checkArgument(
        void.class.equals(m.getReturnType()), "%s must have a void return type", format(m));
    checkArgument(
        m.getGenericParameterTypes().length == 0, "%s must take zero arguments", format(m));
    return DoFnSignature.LifecycleMethod.create(m);
  }

  static DoFnSignature.GetInitialRestrictionMethod analyzeGetInitialRestrictionMethod(
      TypeToken<? extends DoFn> fnToken, Method m, TypeToken<?> inputT) {
    // Method is of the form:
    // @GetInitialRestriction
    // RestrictionT getInitialRestriction(InputT element);
    checkArgument(!m.isVarArgs(), "%s must not have var args", format(m));

    Type[] params = m.getGenericParameterTypes();
    checkArgument(
        params.length == 1,
        "%s must have a single argument of type %s",
        format(m),
        formatType(inputT));
    TypeToken<?> paramT = fnToken.resolveType(params[0]);
    checkArgument(
        paramT.equals(inputT),
        "Wrong type of context argument to %s: %s, must be %s",
        format(m),
        formatType(paramT),
        formatType(inputT));

    return DoFnSignature.GetInitialRestrictionMethod.create(
        m, fnToken.resolveType(m.getGenericReturnType()));
  }

  /** Generates a type token for {@code List<T>} given {@code T}. */
  private static <T> TypeToken<List<T>> listTypeOf(TypeToken<T> elementT) {
    return new TypeToken<List<T>>() {}.where(new TypeParameter<T>() {}, elementT);
  }

  static DoFnSignature.SplitRestrictionMethod analyzeSplitRestrictionMethod(
      TypeToken<? extends DoFn> fnToken, Method m, TypeToken<?> inputT) {
    // Method is of the form:
    // @SplitRestriction
    // List<RestrictionT> splitRestriction(InputT element, RestrictionT restriction, int numParts);
    checkArgument(!m.isVarArgs(), "%s must not have var args", format(m));

    Type[] params = m.getGenericParameterTypes();
    checkArgument(params.length == 3, "%s must have exactly 3 arguments", format(m));
    checkArgument(
        fnToken.resolveType(params[0]).equals(inputT),
        "First argument of %s must be the element type %s",
        format(m),
        formatType(inputT));
    checkArgument(params[2].equals(int.class), "Second argument of %s must be int", format(m));
    TypeToken<?> restrictionT = fnToken.resolveType(params[1]);
    TypeToken<? extends List<?>> expectedReturnT = listTypeOf(restrictionT);
    checkArgument(
        fnToken.resolveType(m.getGenericReturnType()).equals(expectedReturnT),
        "Return value of %s must be %s",
        format(m),
        formatType(expectedReturnT));

    return DoFnSignature.SplitRestrictionMethod.create(m, restrictionT);
  }

  /** Generates a type token for {@code Coder<T>} given {@code T}. */
  private static <T> TypeToken<Coder<T>> coderTypeOf(TypeToken<T> elementT) {
    return new TypeToken<Coder<T>>() {}.where(new TypeParameter<T>() {}, elementT);
  }

  static DoFnSignature.GetRestrictionCoderMethod analyzeGetRestrictionCoderMethod(
      TypeToken<? extends DoFn> fnToken, Method m) {
    checkArgument(m.getParameterTypes().length == 0, "%s must have zero arguments", format(m));
    TypeToken<?> resT = fnToken.resolveType(m.getGenericReturnType());
    checkArgument(
        resT.isSubtypeOf(TypeToken.of(Coder.class)),
        "%s must return a Coder, but returns %s",
        format(m),
        formatType(resT));

    return DoFnSignature.GetRestrictionCoderMethod.create(m, resT);
  }

  /**
   * Generates a type token for {@code RestrictionTracker<RestrictionT>} given {@code RestrictionT}.
   */
  private static <RestrictionT>
      TypeToken<RestrictionTracker<RestrictionT>> restrictionTrackerTypeOf(
          TypeToken<RestrictionT> restrictionT) {
    return new TypeToken<RestrictionTracker<RestrictionT>>() {}.where(
        new TypeParameter<RestrictionT>() {}, restrictionT);
  }

  private static DoFnSignature.NewTrackerMethod analyzeNewTrackerMethod(
      TypeToken<? extends DoFn> fnToken, Method m) {
    // Method is of the form:
    // @NewTracker
    // TrackerT newTracker(RestrictionT restriction);
    checkArgument(!m.isVarArgs(), "%s must not have var args", format(m));

    Type[] params = m.getGenericParameterTypes();
    checkArgument(params.length == 1, "%s must have a single argument", format(m));

    TypeToken<?> restrictionT = fnToken.resolveType(params[0]);
    TypeToken<?> trackerT = fnToken.resolveType(m.getGenericReturnType());
    TypeToken<?> expectedTrackerT = restrictionTrackerTypeOf(restrictionT);
    checkArgument(
        trackerT.isSubtypeOf(expectedTrackerT),
        "The argument of %s is %s, but must be a subtype of %s",
        format(m),
        formatType(trackerT),
        formatType(expectedTrackerT));
    return DoFnSignature.NewTrackerMethod.create(m, restrictionT, trackerT);
  }

  private static Collection<Method> declaredMethodsWithAnnotation(
      Class<? extends Annotation> anno, Class<?> startClass, Class<?> stopClass) {
    Collection<Method> matches = new ArrayList<>();

    Class<?> clazz = startClass;
    LinkedHashSet<Class<?>> interfaces = new LinkedHashSet<>();

    // First, find all declared methods on the startClass and parents (up to stopClass)
    while (clazz != null && !clazz.equals(stopClass)) {
      for (Method method : clazz.getDeclaredMethods()) {
        if (method.isAnnotationPresent(anno)) {
          matches.add(method);
        }
      }

      Collections.addAll(interfaces, clazz.getInterfaces());

      clazz = clazz.getSuperclass();
    }

    // Now, iterate over all the discovered interfaces
    for (Method method : ReflectHelpers.getClosureOfMethodsOnInterfaces(interfaces)) {
      if (method.isAnnotationPresent(anno)) {
        matches.add(method);
      }
    }
    return matches;
  }

  private static Method findAnnotatedMethod(
      Class<? extends Annotation> anno, Class<?> fnClazz, boolean required) {
    Collection<Method> matches = declaredMethodsWithAnnotation(anno, fnClazz, DoFn.class);

    if (matches.size() == 0) {
      checkArgument(
          !required,
          "No method annotated with @%s found in %s",
          anno.getSimpleName(),
          fnClazz.getName());
      return null;
    }

    // If we have at least one match, then either it should be the only match
    // or it should be an extension of the other matches (which came from parent
    // classes).
    Method first = matches.iterator().next();
    for (Method other : matches) {
      checkArgument(
          first.getName().equals(other.getName())
              && Arrays.equals(first.getParameterTypes(), other.getParameterTypes()),
          "Found multiple methods annotated with @%s. [%s] and [%s]",
          anno.getSimpleName(),
          format(first),
          format(other));
    }

    // We need to be able to call it. We require it is public.
    checkArgument(
        (first.getModifiers() & Modifier.PUBLIC) != 0, "%s must be public", format(first));

    // And make sure its not static.
    checkArgument(
        (first.getModifiers() & Modifier.STATIC) == 0, "%s must not be static", format(first));

    return first;
  }

  private static String format(Method m) {
    return (m == null) ? "null" : ReflectHelpers.CLASS_AND_METHOD_FORMATTER.apply(m);
  }

  private static String formatType(TypeToken<?> t) {
    return (t == null) ? "null" : ReflectHelpers.TYPE_SIMPLE_DESCRIPTION.apply(t.getType());
  }
}
