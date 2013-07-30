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
package org.apache.cassandra.transport.messages;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.transport.ProtocolException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.Message;

/**
 * Message to indicate that the server is ready to receive requests.
 */
public class CredentialsMessage extends Message.Request
{
    public static final Message.Codec<CredentialsMessage> codec = new Message.Codec<CredentialsMessage>()
    {
        public CredentialsMessage decode(ChannelBuffer body, int version)
        {
            if (version > 1)
                throw new ProtocolException("Legacy credentials authentication is not supported in " +
                        "protocol versions > 1. Please use SASL authentication via a SaslResponse message");

            CredentialsMessage msg = new CredentialsMessage();
            int count = body.readUnsignedShort();
            for (int i = 0; i < count; i++)
            {
                String key = CBUtil.readString(body);
                String value = CBUtil.readString(body);
                msg.credentials.put(key, value);
            }
            return msg;
        }

        public ChannelBuffer encode(CredentialsMessage msg, int version)
        {
            ChannelBuffer cb = ChannelBuffers.dynamicBuffer();

            cb.writeShort(msg.credentials.size());
            for (Map.Entry<String, String> entry : msg.credentials.entrySet())
            {
                cb.writeBytes(CBUtil.stringToCB(entry.getKey()));
                cb.writeBytes(CBUtil.stringToCB(entry.getValue()));
            }
            return cb;
        }
    };

    public final Map<String, String> credentials = new HashMap<String, String>();

    public CredentialsMessage()
    {
        super(Message.Type.CREDENTIALS);
    }

    public ChannelBuffer encode(int version)
    {
        return codec.encode(this, version);
    }

    public Message.Response execute(QueryState state)
    {
        try
        {
            AuthenticatedUser user = DatabaseDescriptor.getAuthenticator().authenticate(credentials);
            state.getClientState().login(user);
            return new ReadyMessage();
        }
        catch (AuthenticationException e)
        {
            return ErrorMessage.fromException(e);
        }
    }

    @Override
    public String toString()
    {
        return "CREDENTIALS " + credentials;
    }
}
