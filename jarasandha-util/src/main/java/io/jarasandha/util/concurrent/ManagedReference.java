/**
 *     Copyright 2018 The Jarasandha.io project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.jarasandha.util.concurrent;

import io.netty.util.AbstractReferenceCounted;
import io.netty.util.IllegalReferenceCountException;
import io.netty.util.ReferenceCounted;
import lombok.extern.slf4j.Slf4j;

import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A reference ({@link T}) whose count is tracked via {@link ReferenceCounted}.
 * <p>
 * Created by ashwin.jayaprakash.
 */
@Slf4j
public final class ManagedReference<T> extends AbstractReferenceCounted {
    private static final String ERR_MSG_VERIFICATION_FAILED = "The verification was not successful";
    private static final String ERR_MSG_DEALLOCATION_FAILED = "Error occurred during de-allocation";
    private static final IllegalReferenceCountException FIXED_EX_VERIFICATION_FAILED =
            new IllegalReferenceCountException(ERR_MSG_VERIFICATION_FAILED);
    private volatile T actualRef;
    private volatile Predicate<T> retainVerifier;
    private volatile Consumer<T> deallocator;

    /**
     * @param actualRef
     * @param retainVerifier Called with "actualRef" as the parameter before returning from {@link #retain()} or {@link
     *                       #retain(int)}. If false is returned, then the "retain" method throws an
     *                       {@link IllegalReferenceCountException}.
     * @param deallocator    Called automatically only when the {@link #refCnt()} drops to 0 with "actualRef" as the
     *                       parameter.
     */
    public ManagedReference(T actualRef, Predicate<T> retainVerifier, Consumer<T> deallocator) {
        this.actualRef = checkNotNull(actualRef, "actualRef");
        this.retainVerifier = checkNotNull(retainVerifier, "retainVerifier");
        this.deallocator = checkNotNull(deallocator, "deallocator");
        try {
            verify(actualRef, retainVerifier);
        } catch (IllegalReferenceCountException e) {
            deallocator.accept(actualRef);
            throw e;
        }
    }

    /**
     * Convenience constructor for simple references that only need the {@link AutoCloseable#close()} to be called.
     *
     * @param actualRef
     */
    public static <TC extends AutoCloseable> ManagedReference<TC> newManagedReference(TC actualRef) {
        return new ManagedReference<>(
                actualRef,
                //Default to always valid.
                refToVerify -> true,
                refToDeallocate -> {
                    try {
                        refToDeallocate.close();
                    } catch (Throwable throwable) {
                        log.warn(ERR_MSG_DEALLOCATION_FAILED, throwable);
                    }
                }
        );
    }

    public T actualRef() {
        return actualRef;
    }

    @Override
    public ManagedReference<T> retain() {
        return retain(1);
    }

    @Override
    public ManagedReference<T> retain(int increment) {
        try {
            verify(actualRef, retainVerifier);
        } catch (IllegalReferenceCountException e) {
            //Clean up.
            while (refCnt() > 0) {
                if (release()) {
                    break;
                }
            }
            throw e;
        }
        super.retain(increment);
        return this;
    }

    /**
     * @param reference
     * @param verifier
     * @param <T>
     * @throws IllegalReferenceCountException
     */
    private static <T> void verify(T reference, Predicate<T> verifier) {
        if (!verifier.test(reference)) {
            throw FIXED_EX_VERIFICATION_FAILED;
        }
    }

    @Override
    public ManagedReference<T> touch() {
        super.touch();
        return this;
    }

    @Override
    public ManagedReference<T> touch(Object hint) {
        return this;
    }

    @Override
    protected void deallocate() {
        final T actualRefCopy = this.actualRef;
        final Consumer<T> deallocatorCopy = deallocator;
        this.actualRef = null;
        this.deallocator = null;
        this.retainVerifier = null;
        try {
            deallocatorCopy.accept(actualRefCopy);
        } catch (Throwable throwable) {
            log.warn(ERR_MSG_DEALLOCATION_FAILED, throwable);
        }
    }
}
