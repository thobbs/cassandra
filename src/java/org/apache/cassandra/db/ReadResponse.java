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
package org.apache.cassandra.db;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;

import org.apache.cassandra.db.filter.ColumnsSelection;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ReadResponse
{
    private static final Logger logger = LoggerFactory.getLogger(ReadResponse.class);

    public static final IVersionedSerializer<ReadResponse> serializer = new Serializer();
    public static final IVersionedSerializer<ReadResponse> legacyRangeSliceReplySerializer = new LegacyRangeSliceReplySerializer();

    public static ReadResponse createDataResponse(UnfilteredPartitionIterator data)
    {
        try (UnfilteredPartitionIterator iter = data)
        {
            return new DataResponse(data);
        }
    }

    public static ReadResponse createDigestResponse(UnfilteredPartitionIterator data)
    {
        try (UnfilteredPartitionIterator iter = data)
        {
            return new DigestResponse(makeDigest(data));
        }
    }

    public static ReadResponse createLocalDataResponse(UnfilteredPartitionIterator data)
    {
        // We don't consume the data ourselves so we shouldn't close it.
        return new LocalDataResponse(data);
    }

    public abstract UnfilteredPartitionIterator makeIterator();

    public abstract ByteBuffer digest();

    public abstract boolean isDigestQuery();

    protected static ByteBuffer makeDigest(UnfilteredPartitionIterator iterator)
    {
        MessageDigest digest = FBUtilities.threadLocalMD5Digest();
        UnfilteredPartitionIterators.digest(iterator, digest);
        return ByteBuffer.wrap(digest.digest());
    }

    public void maybeReverse()
    {
    }

    private static class DigestResponse extends ReadResponse
    {
        private final ByteBuffer digest;

        private DigestResponse(ByteBuffer digest)
        {
            assert digest.hasRemaining();
            this.digest = digest;
        }

        public UnfilteredPartitionIterator makeIterator()
        {
            throw new UnsupportedOperationException();
        }

        public ByteBuffer digest()
        {
            return digest;
        }

        public boolean isDigestQuery()
        {
            return true;
        }
    }

    private static class DataResponse extends ReadResponse
    {
        // The response, serialized in the current messaging version
        private final ByteBuffer data;
        private final SerializationHelper.Flag flag;

        private DataResponse(ByteBuffer data)
        {
            this.data = data;
            this.flag = SerializationHelper.Flag.FROM_REMOTE;
        }

        private DataResponse(UnfilteredPartitionIterator iter)
        {
            DataOutputBuffer buffer = new DataOutputBuffer();
            try
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iter, buffer, MessagingService.current_version);
                this.data = buffer.buffer();
                this.flag = SerializationHelper.Flag.LOCAL;
            }
            catch (IOException e)
            {
                // We're serializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public UnfilteredPartitionIterator makeIterator()
        {
            try
            {
                DataInput in = new DataInputStream(ByteBufferUtil.inputStream(data));
                return UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in, MessagingService.current_version, flag);
            }
            catch (IOException e)
            {
                // We're deserializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public ByteBuffer digest()
        {
            try (UnfilteredPartitionIterator iterator = makeIterator())
            {
                return makeDigest(iterator);
            }
        }

        public boolean isDigestQuery()
        {
            return false;
        }
    }

    private static class LocalDataResponse extends ReadResponse
    {
        private UnfilteredPartitionIterator iterator;
        private boolean returned;

        private LocalDataResponse(UnfilteredPartitionIterator iterator)
        {
            this.iterator = iterator;
        }

        public UnfilteredPartitionIterator makeIterator()
        {
            if (returned)
                throw new IllegalStateException();

            returned = true;
            return iterator;
        }

        public ByteBuffer digest()
        {
            throw new UnsupportedOperationException();
        }

        public boolean isDigestQuery()
        {
            return false;
        }
    }

    /**
     * A remote response from a pre-3.0 node.  This needs a separate class in order to cleanly handle reversal of
     * results when the read command calls for it.  (Pre-3.0 nodes always return results in the normal sorted order,
     * even if the query asks for reversed results.)
     */
    private static class LegacyRemoteDataResponse extends ReadResponse
    {
        private UnfilteredPartitionIterator iterator;
        private ByteBuffer data;

        private LegacyRemoteDataResponse(UnfilteredPartitionIterator iterator)
        {
            this.iterator = iterator;
            this.data = null;
        }

        private void buildDataBuffer()
        {
            DataOutputBuffer buffer = new DataOutputBuffer();
            try
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iterator, buffer, MessagingService.current_version);
                this.data = buffer.buffer();
            }
            catch (IOException e)
            {
                // We're serializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public UnfilteredPartitionIterator makeIterator()
        {
            if (data == null)
                buildDataBuffer();

            try
            {
                DataInput in = new DataInputStream(ByteBufferUtil.inputStream(data));
                return UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in, MessagingService.current_version, SerializationHelper.Flag.FROM_REMOTE);
            }
            catch (IOException e)
            {
                // We're deserializing in memory so this shouldn't happen
                throw new RuntimeException(e);
            }
        }

        @Override
        public void maybeReverse()
        {
            assert data == null : "reversal should have happened before data was accessed";

            final UnfilteredPartitionIterator unreversedPartitionIterator = iterator;
            iterator = new UnfilteredPartitionIterator()
            {
                UnfilteredRowIterator next;

                @Override
                public boolean isForThrift()
                {
                    return unreversedPartitionIterator.isForThrift();
                }

                @Override
                public boolean hasNext()
                {
                    return unreversedPartitionIterator.hasNext();
                }

                @Override
                public UnfilteredRowIterator next()
                {
                    final UnfilteredRowIterator unreversedNext = unreversedPartitionIterator.next();
                    next = ArrayBackedPartition.create(unreversedNext).unfilteredIterator(
                            ColumnsSelection.withoutSubselection(unreversedNext.columns()), Slices.ALL, true, unreversedNext.nowInSec());
                    return next;
                }

                @Override
                public void close()
                {
                    if (next != null)
                        next.close();
                }
            };
        }

        public ByteBuffer digest()
        {
            try (UnfilteredPartitionIterator iterator = makeIterator())
            {
                return makeDigest(iterator);
            }
        }

        public boolean isDigestQuery()
        {
            return false;
        }
    }

    private static class Serializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                boolean isDigest = response.isDigestQuery();
                out.writeInt(isDigest ? response.digest().remaining() : 0);
                ByteBuffer buffer = isDigest ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER;
                out.write(buffer);
                out.writeBoolean(isDigest);
                if (!isDigest)
                    UnfilteredPartitionIterators.serializerForIntraNode().serialize(response.makeIterator(), out, version);
                return;
            }

            assert !(response instanceof LocalDataResponse);
            boolean isDigest = response.isDigestQuery();
            ByteBufferUtil.writeWithShortLength(isDigest ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER, out);
            if (!isDigest)
            {
                // Note that we can only get there if version == 3.0, which is the current_version. When we'll change the
                // version, we'll have to deserialize/re-serialize the data to be in the proper version.
                assert version == MessagingService.VERSION_30;
                ByteBuffer data = ((DataResponse)response).data;
                ByteBufferUtil.writeWithLength(data, out);
            }
        }

        public ReadResponse deserialize(DataInput in, int version) throws IOException
        {
            if (version < MessagingService.VERSION_30)
            {
                byte[] digest = null;
                int digestSize = in.readInt();
                if (digestSize > 0)
                {
                    digest = new byte[digestSize];
                    in.readFully(digest, 0, digestSize);
                }
                boolean isDigest = in.readBoolean();
                assert isDigest == digestSize > 0;
                if (isDigest)
                {
                    assert digest != null;
                    return new DigestResponse(ByteBuffer.wrap(digest));
                }

                // ReadResponses from older versions are always single-partition (ranges are handled by RangeSliceReply)
                DecoratedKey key = StorageService.getPartitioner().decorateKey(ByteBufferUtil.readWithShortLength(in));

                UnfilteredRowIterator rowIterator;
                boolean present = in.readBoolean();
                if (!present)
                {
                    return new LegacyRemoteDataResponse(UnfilteredPartitionIterators.EMPTY);
                }

                rowIterator = UnfilteredPartitionIterators.serializerForIntraNode().deserializePartition(in, key, version);
                UnfilteredPartitionIterator iterator = new UnfilteredPartitionIterators.SingletonPartitionIterator(rowIterator, true);
                return new LegacyRemoteDataResponse(iterator);
            }

            ByteBuffer digest = ByteBufferUtil.readWithShortLength(in);
            if (digest.hasRemaining())
                return new DigestResponse(digest);

            assert version == MessagingService.VERSION_30;
            ByteBuffer data = ByteBufferUtil.readWithLength(in);
            return new DataResponse(data);
        }

        public long serializedSize(ReadResponse response, int version)
        {
            TypeSizes sizes = TypeSizes.NATIVE;
            boolean isDigest = response.isDigestQuery();

            if (version < MessagingService.VERSION_30)
            {
                long size = ByteBufferUtil.serializedSizeWithLength(isDigest ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER, sizes);
                size += sizes.sizeof(isDigest);
                // TODO is the partition iterator reusable?
                if (!isDigest)
                    size += UnfilteredPartitionIterators.serializerForIntraNode().serializedSize(response.makeIterator(), version);
                return size;
            }

            long size = ByteBufferUtil.serializedSizeWithShortLength(isDigest ? response.digest() : ByteBufferUtil.EMPTY_BYTE_BUFFER, sizes);

            if (!isDigest)
            {
                // Note that we can only get there if version == 3.0, which is the current_version. When we'll change the
                // version, we'll have to deserialize/re-serialize the data to be in the proper version.
                assert version == MessagingService.VERSION_30;
                ByteBuffer data = ((DataResponse)response).data;
                size += ByteBufferUtil.serializedSizeWithLength(data, sizes);
            }
            return size;
        }
    }

    private static class LegacyRangeSliceReplySerializer implements IVersionedSerializer<ReadResponse>
    {
        public void serialize(ReadResponse response, DataOutputPlus out, int version) throws IOException
        {
            // TODO this is currently really inefficient because we have to iterate over all of the results twice
            int numPartitions = 0;
            try (UnfilteredPartitionIterator iterator = response.makeIterator())
            {
                while (iterator.hasNext())
                {
                    try (UnfilteredRowIterator atomIterator = iterator.next())
                    {
                        numPartitions++;

                        // we have to fully exhaust the subiterator
                        while(atomIterator.hasNext())
                            atomIterator.next();
                    }
                }
            }
            out.writeInt(numPartitions);
            try (UnfilteredPartitionIterator iterator = response.makeIterator())
            {
                UnfilteredPartitionIterators.serializerForIntraNode().serialize(iterator, out, version);
            }
        }

        public ReadResponse deserialize(DataInput in, int version) throws IOException
        {
            return new LegacyRemoteDataResponse(UnfilteredPartitionIterators.serializerForIntraNode().deserialize(in, version, SerializationHelper.Flag.FROM_REMOTE));
        }

        public long serializedSize(ReadResponse response, int version)
        {
            int size = TypeSizes.NATIVE.sizeof(0);  // number of partitions
            try (UnfilteredPartitionIterator iterator = response.makeIterator())
            {
                size += UnfilteredPartitionIterators.serializerForIntraNode().serializedSize(iterator, version);
            }
            return size;
        }
    }
}
