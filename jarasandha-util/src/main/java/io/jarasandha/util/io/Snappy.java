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
package io.jarasandha.util.io;

import com.google.common.base.Throwables;
import io.jarasandha.util.misc.CallerMustRelease;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.*;
import io.netty.handler.codec.compression.SnappyFrameDecoder;
import io.netty.handler.codec.compression.SnappyFrameEncoder;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.EventExecutor;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ashwin.jayaprakash.
 */
public class Snappy {
    public static ByteBuf compress(ByteBufAllocator allocator, @CallerMustRelease ByteBuf raw) {
        final CustomSnappyFrameEncoder encoder = new CustomSnappyFrameEncoder();
        //At least 1/4 uncompressed.
        final int initialCapacity = Math.max(16, raw.readableBytes() / 4);
        final ByteBuf compressedByteBuf = allocator.buffer(initialCapacity);
        try {
            encoder.encode(raw, compressedByteBuf);
        } catch (Throwable t) {
            compressedByteBuf.release();
            Throwables.throwIfUnchecked(t);
            throw new RuntimeException(t);
        }
        return compressedByteBuf;
    }

    public static ByteBuf decompress(ByteBufAllocator allocator, @CallerMustRelease ByteBuf compressed) {
        final CustomSnappyFrameDecoder decoder = new CustomSnappyFrameDecoder();
        try {
            return decoder.decode(allocator, compressed);
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    private static class CustomSnappyFrameEncoder extends SnappyFrameEncoder {
        private void encode(ByteBuf in, ByteBuf out) throws Exception {
            super.encode(null, in, out);
        }
    }

    private static class CustomSnappyFrameDecoder extends SnappyFrameDecoder {
        @SuppressWarnings("unchecked")
        private ByteBuf decode(ByteBufAllocator allocator, ByteBuf in) throws Exception {
            final List<Object> listOfByteBufs = new ArrayList<>();
            while (in.isReadable()) {
                super.decode(new AllocatorContext(allocator), in, listOfByteBufs);
            }
            @SuppressWarnings("UnnecessaryLocalVariable") final List sameListToBypassWeirdNettyApi = listOfByteBufs;
            return allocator
                    .compositeBuffer(listOfByteBufs.size())
                    .addComponents(true, sameListToBypassWeirdNettyApi);
        }
    }

    private static class AllocatorContext implements ChannelHandlerContext {
        private final ByteBufAllocator allocator;

        private AllocatorContext(ByteBufAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public Channel channel() {
            return null;
        }

        @Override
        public EventExecutor executor() {
            return null;
        }

        @Override
        public String name() {
            return null;
        }

        @Override
        public ChannelHandler handler() {
            return null;
        }

        @Override
        public boolean isRemoved() {
            return false;
        }

        @Override
        public ChannelHandlerContext fireChannelRegistered() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelUnregistered() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelActive() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelInactive() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireExceptionCaught(Throwable throwable) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireUserEventTriggered(Object o) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelRead(Object o) {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelReadComplete() {
            return null;
        }

        @Override
        public ChannelHandlerContext fireChannelWritabilityChanged() {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress socketAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress socketAddress) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress socketAddress, SocketAddress socketAddress1) {
            return null;
        }

        @Override
        public ChannelFuture disconnect() {
            return null;
        }

        @Override
        public ChannelFuture close() {
            return null;
        }

        @Override
        public ChannelFuture deregister() {
            return null;
        }

        @Override
        public ChannelFuture bind(SocketAddress socketAddress, ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress socketAddress, ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelFuture connect(SocketAddress socketAddress, SocketAddress socketAddress1,
                                     ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelFuture disconnect(ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelFuture close(ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelFuture deregister(ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelHandlerContext read() {
            return null;
        }

        @Override
        public ChannelFuture write(Object o) {
            return null;
        }

        @Override
        public ChannelFuture write(Object o, ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelHandlerContext flush() {
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object o, ChannelPromise channelPromise) {
            return null;
        }

        @Override
        public ChannelFuture writeAndFlush(Object o) {
            return null;
        }

        @Override
        public ChannelPromise newPromise() {
            return null;
        }

        @Override
        public ChannelProgressivePromise newProgressivePromise() {
            return null;
        }

        @Override
        public ChannelFuture newSucceededFuture() {
            return null;
        }

        @Override
        public ChannelFuture newFailedFuture(Throwable throwable) {
            return null;
        }

        @Override
        public ChannelPromise voidPromise() {
            return null;
        }

        @Override
        public ChannelPipeline pipeline() {
            return null;
        }

        @Override
        public ByteBufAllocator alloc() {
            return allocator;
        }

        @Deprecated
        @Override
        public <T> Attribute<T> attr(AttributeKey<T> attributeKey) {
            return null;
        }

        @Deprecated
        @Override
        public <T> boolean hasAttr(AttributeKey<T> attributeKey) {
            return false;
        }
    }
}
