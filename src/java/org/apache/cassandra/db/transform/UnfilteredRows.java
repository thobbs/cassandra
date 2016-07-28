package org.apache.cassandra.db.transform;

import io.reactivex.Observable;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.reactivestreams.Subscription;

final class UnfilteredRows extends BaseRows<Unfiltered, UnfilteredRowIterator> implements UnfilteredRowIterator
{
    private DeletionTime partitionLevelDeletion;

    public UnfilteredRows(UnfilteredRowIterator input)
    {
        super(input);
        partitionLevelDeletion = input.partitionLevelDeletion();
    }

    @Override
    void add(Transformation add)
    {
        super.add(add);
        partitionLevelDeletion = add.applyToDeletion(partitionLevelDeletion);
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public EncodingStats stats()
    {
        return input.stats();
    }

    @Override
    public boolean isEmpty()
    {
        return staticRow().isEmpty() && partitionLevelDeletion().isLive() && !hasNext();
    }

    public Observable<Unfiltered> asObservable()
    {
        return Observable.create(subscriber -> {
            subscriber.onSubscribe(new Subscription()
            {
                public void request(long l)
                {

                }

                public void cancel()
                {

                }
            });
            while(hasNext())
                subscriber.onNext(next());

            subscriber.onComplete();
        });
    }
}
