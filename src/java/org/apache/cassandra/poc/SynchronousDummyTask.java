package org.apache.cassandra.poc;

import org.apache.cassandra.poc.events.Event;
import org.apache.cassandra.transport.Message;
import uk.co.real_logic.agrona.TimerWheel.Timer;

import java.util.function.Supplier;

public class SynchronousDummyTask extends Task<Message.Response>
{
    private final Supplier<Message.Response> runnable;

    public SynchronousDummyTask(Supplier<Message.Response> runnable)
    {
        this.runnable = runnable;
    }

    @Override
    public Status start(EventLoop eventLoop)
    {
        this.eventLoop = eventLoop;
        try
        {
            return complete(runnable.get());
        }
        catch (Throwable t)
        {
            // TODO JVMStabilityInspector?
            return fail(t);
        }
    }

    @Override
    public Status resume(EventLoop eventLoop)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Status handleEvent(EventLoop eventLoop, Event event)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Status handleTimeout(EventLoop eventLoop, Timer timer)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cleanup(EventLoop eventLoop)
    {
    }
}
