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

import com.google.common.reflect.TypeToken;
import java.io.FileOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.NamingStrategy;
import net.bytebuddy.description.field.FieldDescription;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.description.type.TypeList;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.dynamic.scaffold.InstrumentedType;
import net.bytebuddy.dynamic.scaffold.subclass.ConstructorStrategy;
import net.bytebuddy.implementation.ExceptionMethod;
import net.bytebuddy.implementation.FixedValue;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.MethodDelegationBinder;
import net.bytebuddy.implementation.bind.annotation.TargetMethodAnnotationDrivenBinder;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.bytecode.StackManipulation;
import net.bytebuddy.implementation.bytecode.Throw;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import net.bytebuddy.implementation.bytecode.member.FieldAccess;
import net.bytebuddy.implementation.bytecode.member.MethodInvocation;
import net.bytebuddy.implementation.bytecode.member.MethodReturn;
import net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;
import net.bytebuddy.jar.asm.commons.LocalVariablesSorter;
import net.bytebuddy.matcher.ElementMatchers;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.util.UserCodeException;
import org.apache.beam.sdk.values.TypeDescriptor;

/** Dynamically generates {@link DoFnInvoker} instances for invoking a {@link DoFn}. */
public class DoFnInvokers {
  public static final DoFnInvokers INSTANCE = new DoFnInvokers();

  private static final String FN_DELEGATE_FIELD_NAME = "delegate";

  /**
   * A cache of constructors of generated {@link DoFnInvoker} classes, keyed by {@link DoFn} class.
   * Needed because generating an invoker class is expensive, and to avoid generating an excessive
   * number of classes consuming PermGen memory.
   */
  private final Map<Class<?>, Constructor<?>> byteBuddyInvokerConstructorCache =
      new LinkedHashMap<>();

  private DoFnInvokers() {}

  /** @return the {@link DoFnInvoker} for the given {@link DoFn}. */
  public <InputT, OutputT> DoFnInvoker<InputT, OutputT> newByteBuddyInvoker(
      DoFn<InputT, OutputT> fn) {
    return newByteBuddyInvoker(DoFnSignatures.INSTANCE.getOrParseSignature(fn.getClass()), fn);
  }

  /** @return the {@link DoFnInvoker} for the given {@link DoFn}. */
  public <InputT, OutputT> DoFnInvoker<InputT, OutputT> newByteBuddyInvoker(
      DoFnSignature signature, DoFn<InputT, OutputT> fn) {
    checkArgument(
        signature.fnClass().equals(fn.getClass()),
        "Signature is for class %s, but fn is of class %s",
        signature.fnClass(),
        fn.getClass());
    try {
      @SuppressWarnings("unchecked")
      DoFnInvoker<InputT, OutputT> invoker =
          (DoFnInvoker<InputT, OutputT>)
              getOrGenerateByteBuddyInvokerConstructor(signature).newInstance(fn);
      return invoker;
    } catch (InstantiationException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException
        | SecurityException e) {
      throw new RuntimeException("Unable to bind invoker for " + fn.getClass(), e);
    }
  }

  /**
   * Returns a generated constructor for a {@link DoFnInvoker} for the given {@link DoFn} class and
   * caches it.
   */
  private synchronized Constructor<?> getOrGenerateByteBuddyInvokerConstructor(
      DoFnSignature signature) {
    Class<? extends DoFn> fnClass = signature.fnClass();
    Constructor<?> constructor = byteBuddyInvokerConstructorCache.get(fnClass);
    if (constructor == null) {
      Class<? extends DoFnInvoker<?, ?>> invokerClass = generateInvokerClass(signature);
      try {
        constructor = invokerClass.getConstructor(fnClass);
      } catch (IllegalArgumentException | NoSuchMethodException | SecurityException e) {
        throw new RuntimeException(e);
      }
      byteBuddyInvokerConstructorCache.put(fnClass, constructor);
    }
    return constructor;
  }

  /** Default implementation of {@link DoFn.SplitRestriction}, for delegation by bytebuddy. */
  public static class DefaultSplitRestriction {
    /** Doesn't split the restriction. */
    @SuppressWarnings("unused")
    public static <InputT, RestrictionT> List<RestrictionT> invokeSplitRestriction(
        InputT element, RestrictionT restriction, int numParts) {
      return Collections.singletonList(restriction);
    }
  }

  /** Default implementation of {@link DoFn.SplitRestriction}, for delegation by bytebuddy. */
  public static class DefaultRestrictionCoder {
    private static final CoderRegistry CODER_REGISTRY = new CoderRegistry();

    static {
      CODER_REGISTRY.registerStandardCoders();
    }

    private final TypeToken<?> restrictionType;

    DefaultRestrictionCoder(TypeToken<?> restrictionType) {
      this.restrictionType = restrictionType;
    }

    /** Doesn't split the restriction. */
    @SuppressWarnings({"unused", "unchecked"})
    public <RestrictionT> Coder<RestrictionT> invokeGetRestrictionCoder()
        throws CannotProvideCoderException {
      return (Coder) CODER_REGISTRY.getCoder(TypeDescriptor.of(restrictionType.getType()));
    }
  }

  /** Generates a {@link DoFnInvoker} class for the given {@link DoFnSignature}. */
  private static Class<? extends DoFnInvoker<?, ?>> generateInvokerClass(DoFnSignature signature) {
    Class<? extends DoFn> fnClass = signature.fnClass();

    final TypeDescription clazzDescription = new TypeDescription.ForLoadedType(fnClass);

    DynamicType.Builder<?> builder =
        new ByteBuddy()
            // Create subclasses inside the target class, to have access to
            // private and package-private bits
            .with(
                new NamingStrategy.SuffixingRandom("auxiliary") {
                  @Override
                  public String subclass(TypeDescription.Generic superClass) {
                    return super.name(clazzDescription);
                  }
                })
            // Create a subclass of DoFnInvoker
            .subclass(DoFnInvoker.class, ConstructorStrategy.Default.NO_CONSTRUCTORS)
            .defineField(
                FN_DELEGATE_FIELD_NAME, fnClass, Visibility.PRIVATE, FieldManifestation.FINAL)
            .defineConstructor(Visibility.PUBLIC)
            .withParameter(fnClass)
            .intercept(new InvokerConstructor())
            .method(ElementMatchers.named("invokeProcessElement"))
            .intercept(new ProcessElementDelegation(signature.processElement()))
            .method(ElementMatchers.named("invokeStartBundle"))
            .intercept(delegateOrNoop(signature.startBundle()))
            .method(ElementMatchers.named("invokeFinishBundle"))
            .intercept(delegateOrNoop(signature.finishBundle()))
            .method(ElementMatchers.named("invokeSetup"))
            .intercept(delegateOrNoop(signature.setup()))
            .method(ElementMatchers.named("invokeTeardown"))
            .intercept(delegateOrNoop(signature.teardown()))
            .method(ElementMatchers.named("invokeGetInitialRestriction"))
            .intercept(delegateWithDowncastOrThrow(signature.getInitialRestriction()))
            .method(ElementMatchers.named("invokeSplitRestriction"))
            .intercept(
                (signature.splitRestriction() == null)
                    ? MethodDelegation.to(DefaultSplitRestriction.class)
                    : new DowncastingParametersMethodDelegation(
                        signature.splitRestriction().targetMethod()))
            .method(ElementMatchers.named("invokeGetRestrictionCoder"))
            .intercept(
                signature.processElement().isSplittable()
                    ? ((signature.getRestrictionCoder() == null)
                        ? MethodDelegation.to(
                            new DefaultRestrictionCoder(
                                signature.getInitialRestriction().restrictionT()))
                        : new DowncastingParametersMethodDelegation(
                            signature.getRestrictionCoder().targetMethod()))
                    : ExceptionMethod.throwing(UnsupportedOperationException.class))
            .method(ElementMatchers.named("invokeNewTracker"))
            .intercept(delegateWithDowncastOrThrow(signature.newTracker()));

    DynamicType.Unloaded<?> unloaded = builder.make();
    try {
      try (FileOutputStream w =
          new FileOutputStream("/usr/local/google/home/kirpichov/incubator-beam/foo.class")) {
        w.write(unloaded.getBytes());
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    @SuppressWarnings("unchecked")
    Class<? extends DoFnInvoker<?, ?>> res =
        (Class<? extends DoFnInvoker<?, ?>>)
            unloaded
                .load(DoFnInvokers.class.getClassLoader(), ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();
    return res;
  }

  /** Delegates to the given method if available, or does nothing. */
  private static Implementation delegateOrNoop(DoFnSignature.DoFnMethod method) {
    return (method == null)
        ? FixedValue.originType()
        : new SimpleMethodDelegation(method.targetMethod());
  }

  /** Delegates to the given method if available, or throws UnsupportedOperationException. */
  private static Implementation delegateWithDowncastOrThrow(DoFnSignature.DoFnMethod method) {
    return (method == null)
        ? ExceptionMethod.throwing(UnsupportedOperationException.class)
        : new DowncastingParametersMethodDelegation(method.targetMethod());
  }

  /** Implements an invoker method by delegating to a method of the target {@link DoFn}. */
  private abstract static class DoFnMethodDelegation implements Implementation {
    FieldDescription delegateField;

    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      // Remember the field description of the instrumented type.
      delegateField =
          instrumentedType
              .getDeclaredFields()
              .filter(ElementMatchers.named(FN_DELEGATE_FIELD_NAME))
              .getOnly();

      // Delegating the method call doesn't require any changes to the instrumented type.
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return new ByteCodeAppender() {
        @Override
        public Size apply(
            MethodVisitor methodVisitor,
            Context implementationContext,
            MethodDescription instrumentedMethod) {
          StackManipulation manipulation =
              new StackManipulation.Compound(
                  // Push "this" reference to the stack
                  MethodVariableAccess.REFERENCE.loadOffset(0),
                  // Access the delegate field of the the invoker
                  FieldAccess.forField(delegateField).getter(),
                  invokeTargetMethod(instrumentedMethod));
          StackManipulation.Size size = manipulation.apply(methodVisitor, implementationContext);
          return new Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
        }
      };
    }

    /**
     * Generates code to invoke the target method. When this is called the delegate field will be on
     * top of the stack. This should add any necessary arguments to the stack and then perform the
     * method invocation.
     */
    protected abstract StackManipulation invokeTargetMethod(MethodDescription instrumentedMethod);
  }

  /**
   * Implements the invoker's {@link DoFnInvoker#invokeProcessElement} method by delegating to the
   * {@link DoFn.ProcessElement} method.
   */
  private static final class ProcessElementDelegation extends DoFnMethodDelegation {
    private static final Map<DoFnSignature.Parameter, MethodDescription>
        EXTRA_CONTEXT_FACTORY_METHODS;
    private static final MethodDescription PROCESS_CONTINUATION_STOP_METHOD;

    static {
      try {
        Map<DoFnSignature.Parameter, MethodDescription> methods =
            new EnumMap<>(DoFnSignature.Parameter.class);
        methods.put(
            DoFnSignature.Parameter.BOUNDED_WINDOW,
            new MethodDescription.ForLoadedMethod(
                DoFn.ExtraContextFactory.class.getMethod("window")));
        methods.put(
            DoFnSignature.Parameter.INPUT_PROVIDER,
            new MethodDescription.ForLoadedMethod(
                DoFn.ExtraContextFactory.class.getMethod("inputProvider")));
        methods.put(
            DoFnSignature.Parameter.OUTPUT_RECEIVER,
            new MethodDescription.ForLoadedMethod(
                DoFn.ExtraContextFactory.class.getMethod("outputReceiver")));
        methods.put(
            DoFnSignature.Parameter.RESTRICTION_TRACKER,
            new MethodDescription.ForLoadedMethod(
                DoFn.ExtraContextFactory.class.getMethod("restrictionTracker")));
        EXTRA_CONTEXT_FACTORY_METHODS = Collections.unmodifiableMap(methods);
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to locate an ExtraContextFactory method that was expected to exist", e);
      }
      try {
        PROCESS_CONTINUATION_STOP_METHOD =
            new MethodDescription.ForLoadedMethod(DoFn.ProcessContinuation.class.getMethod("stop"));
      } catch (NoSuchMethodException e) {
        throw new RuntimeException("Failed to locate ProcessContinuation.stop()");
      }
    }

    private final DoFnSignature.ProcessElementMethod signature;

    /** Implementation of {@link MethodDelegation} for the {@link ProcessElement} method. */
    private ProcessElementDelegation(DoFnSignature.ProcessElementMethod signature) {
      this.signature = signature;
    }

    @Override
    protected StackManipulation invokeTargetMethod(MethodDescription instrumentedMethod) {
      MethodDescription targetMethod =
          new MethodCall.MethodLocator.ForExplicitMethod(
                  new MethodDescription.ForLoadedMethod(signature.targetMethod()))
              .resolve(instrumentedMethod);

      // Parameters of the wrapper invoker method:
      //   DoFn.ProcessContext, ExtraContextFactory.
      // Parameters of the wrapped DoFn method:
      //   DoFn.ProcessContext, [BoundedWindow, InputProvider, OutputReceiver] in any order
      ArrayList<StackManipulation> parameters = new ArrayList<>();
      // Push the ProcessContext argument.
      parameters.add(MethodVariableAccess.REFERENCE.loadOffset(1));
      // Push the extra arguments in their actual order.
      StackManipulation pushExtraContextFactory = MethodVariableAccess.REFERENCE.loadOffset(2);
      for (DoFnSignature.Parameter param : signature.extraParameters()) {
        parameters.add(
            new StackManipulation.Compound(
                pushExtraContextFactory,
                MethodInvocation.invoke(EXTRA_CONTEXT_FACTORY_METHODS.get(param)),
                // ExtraContextFactory.restrictionTracker() returns a RestrictionTracker,
                // but the @ProcessElement method expects a concrete subtype of it.
                // Insert a downcast.
                (param == DoFnSignature.Parameter.RESTRICTION_TRACKER)
                    ? TypeCasting.to(
                        new TypeDescription.ForLoadedType(signature.trackerT().getRawType()))
                    : StackManipulation.Trivial.INSTANCE));
      }

      return new StackManipulation.Compound(
          // Push the parameters
          new StackManipulation.Compound(parameters),
          // Invoke the target method
          wrapWithUserCodeException(
              MethodDelegationBinder.MethodInvoker.Simple.INSTANCE.invoke(targetMethod),
              targetMethod.getReturnType().asErasure(),
              instrumentedMethod),
          // Return from the instrumented @ProcessElement method:
          // if it returns void, then return null (meaning don't resume),
          // otherwise return the ProcessContinuation it returned.
          signature.hasReturnValue()
              ? MethodReturn.returning(targetMethod.getReturnType().asErasure())
              : new StackManipulation.Compound(
                  MethodInvocation.invoke(PROCESS_CONTINUATION_STOP_METHOD),
                  MethodReturn.REFERENCE));
    }
  }

  /**
   * Delegates to the given method, wrapping the call into a UserCodeException and optionally
   * downcasting parameters to the proper type.
   */
  private static class SimpleMethodDelegation extends DoFnMethodDelegation {
    private final Method method;

    protected SimpleMethodDelegation(Method method) {
      this.method = method;
    }

    @Override
    protected StackManipulation invokeTargetMethod(MethodDescription instrumentedMethod) {
      MethodDescription targetMethod =
          new MethodCall.MethodLocator.ForExplicitMethod(
                  new MethodDescription.ForLoadedMethod(method))
              .resolve(instrumentedMethod);
      return new StackManipulation.Compound(
          pushArgumentsOfInstrumentedMethod(targetMethod),
          // Invoke the target method
          wrapWithUserCodeException(
              MethodDelegationBinder.MethodInvoker.Simple.INSTANCE.invoke(targetMethod),
              targetMethod.getReturnType().asErasure(),
              instrumentedMethod),
          new StackManipulation.Compound(
              // Return from the instrumented method
              TargetMethodAnnotationDrivenBinder.TerminationHandler.Returning.INSTANCE.resolve(
                  Assigner.DEFAULT, instrumentedMethod, targetMethod)));
    }

    protected StackManipulation pushArgumentsOfInstrumentedMethod(MethodDescription targetMethod) {
      return MethodVariableAccess.allArgumentsOf(targetMethod);
    }
  }

  /**
   * Passes parameters to the delegated method by downcasting each parameter of non-primitive type
   * to its expected type.
   */
  private static class DowncastingParametersMethodDelegation extends SimpleMethodDelegation {
    protected DowncastingParametersMethodDelegation(Method method) {
      super(method);
    }

    @Override
    protected StackManipulation pushArgumentsOfInstrumentedMethod(MethodDescription targetMethod) {
      List<StackManipulation> pushParameters = new ArrayList<>();
      TypeList.Generic paramTypes = targetMethod.getParameters().asTypeList();
      for (int i = 0; i < paramTypes.size(); i++) {
        TypeDescription.Generic paramT = paramTypes.get(i);
        pushParameters.add(MethodVariableAccess.of(paramT).loadOffset(i + 1));
        if (!paramT.isPrimitive()) {
          pushParameters.add(TypeCasting.to(paramT));
        }
      }
      return new StackManipulation.Compound(pushParameters);
    }
  }

  /**
   * Wraps a given stack manipulation in a try catch block. Any exceptions thrown within the try are
   * wrapped with a {@link UserCodeException}.
   */
  private static StackManipulation wrapWithUserCodeException(
      final StackManipulation tryBody,
      final TypeDescription returnType,
      final MethodDescription instrumentedMethod) {
    final MethodDescription createUserCodeException;
    try {
      createUserCodeException =
          new MethodDescription.ForLoadedMethod(
              UserCodeException.class.getDeclaredMethod("wrap", Throwable.class));
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException("Unable to find UserCodeException.wrap", e);
    }

    return new StackManipulation() {
      @Override
      public boolean isValid() {
        return tryBody.isValid();
      }

      @Override
      public Size apply(MethodVisitor originalMV, Implementation.Context implementationContext) {
        LocalVariablesSorter mv =
            new LocalVariablesSorter(
                instrumentedMethod.getActualModifiers(),
                instrumentedMethod.getDescriptor(),
                originalMV);
        Label wrapStart = new Label();
        Label wrapEnd = new Label();
        Label tryBlockStart = new Label();
        Label tryBlockEnd = new Label();
        Label catchBlockStart = new Label();
        Label catchBlockEnd = new Label();

        mv.visitLabel(wrapStart);
        int returnVarIndex = mv.newLocal(Type.getType(returnType.getDescriptor()));

        String throwableName = new TypeDescription.ForLoadedType(Throwable.class).getInternalName();
        mv.visitTryCatchBlock(tryBlockStart, tryBlockEnd, catchBlockStart, throwableName);

        // The try block attempts to perform the expected operations, then jumps to success
        mv.visitLabel(tryBlockStart);
        Size trySize = tryBody.apply(mv, implementationContext);
        if (returnType != TypeDescription.VOID) {
          mv.visitVarInsn(Opcodes.ASTORE, returnVarIndex);
        }
        // After try body, should have same locals and empty stack.
        mv.visitFrame(Opcodes.F_NEW, 0, new Object[] {}, 0, new Object[] {});
        mv.visitJumpInsn(Opcodes.GOTO, catchBlockEnd);
        mv.visitLabel(tryBlockEnd);

        // The handler wraps the exception, and then throws.
        mv.visitLabel(catchBlockStart);
        // In catch block, should have same locals and {Throwable} on the stack.
        mv.visitFrame(Opcodes.F_NEW, 0, new Object[] {}, 1, new Object[] {throwableName});

        Size catchSize =
            new Compound(MethodInvocation.invoke(createUserCodeException), Throw.INSTANCE)
                .apply(mv, implementationContext);

        mv.visitLabel(catchBlockEnd);
        // After catch block, should have same locals and empty stack.
        mv.visitFrame(Opcodes.F_NEW, 0, new Object[] {}, 0, new Object[] {});

        if (returnType != TypeDescription.VOID) {
          mv.visitVarInsn(Opcodes.ALOAD, returnVarIndex);
        }
        mv.visitLabel(wrapEnd);
        if (returnType != TypeDescription.VOID) {
          mv.visitLocalVariable(
              "res",
              returnType.getDescriptor(),
              returnType.getGenericSignature(),
              wrapStart,
              wrapEnd,
              returnVarIndex);
        }

        return new Size(
            trySize.getSizeImpact() /* Same total size impact as wrapped body */,
            Math.max(trySize.getMaximalSize(), catchSize.getMaximalSize()));
      }
    };
  }

  /**
   * A constructor {@link Implementation} for a {@link DoFnInvoker class}. Produces the byte code
   * for a constructor that takes a single argument and assigns it to the delegate field.
   */
  private static final class InvokerConstructor implements Implementation {
    @Override
    public InstrumentedType prepare(InstrumentedType instrumentedType) {
      return instrumentedType;
    }

    @Override
    public ByteCodeAppender appender(final Target implementationTarget) {
      return new ByteCodeAppender() {
        @Override
        public Size apply(
            MethodVisitor methodVisitor,
            Context implementationContext,
            MethodDescription instrumentedMethod) {
          StackManipulation.Size size =
              new StackManipulation.Compound(
                      // Load the this reference
                      MethodVariableAccess.REFERENCE.loadOffset(0),
                      // Invoke the super constructor (default constructor of Object)
                      MethodInvocation.invoke(
                          new TypeDescription.ForLoadedType(Object.class)
                              .getDeclaredMethods()
                              .filter(
                                  ElementMatchers.isConstructor()
                                      .and(ElementMatchers.takesArguments(0)))
                              .getOnly()),
                      // Load the this reference
                      MethodVariableAccess.REFERENCE.loadOffset(0),
                      // Load the delegate argument
                      MethodVariableAccess.REFERENCE.loadOffset(1),
                      // Assign the delegate argument to the delegate field
                      FieldAccess.forField(
                              implementationTarget
                                  .getInstrumentedType()
                                  .getDeclaredFields()
                                  .filter(ElementMatchers.named(FN_DELEGATE_FIELD_NAME))
                                  .getOnly())
                          .putter(),
                      // Return void.
                      MethodReturn.VOID)
                  .apply(methodVisitor, implementationContext);
          return new Size(size.getMaximalSize(), instrumentedMethod.getStackSize());
        }
      };
    }
  }
}
