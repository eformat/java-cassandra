package com.baeldung.cassandra.java.client.repository;


import com.datastax.driver.core.*;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.MoreExecutors;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/*
CREATE KEYSPACE IF NOT EXISTS inmem
  WITH REPLICATION = {
   'class' : 'SimpleStrategy',
   'replication_factor' : 1
  };
 */
public class BulkLoader {

    private final int threads;
    private final InetSocketAddress[] contactHosts;

    public BulkLoader(int threads, InetSocketAddress... contactHosts) {
        this.threads = threads;
        this.contactHosts = contactHosts;
    }

    //callback class
    public static class IngestCallback implements FutureCallback<ResultSet> {

        @Override
        public void onSuccess(ResultSet result) {
            //placeholder: put any logging or on success logic here.
        }

        @Override
        public void onFailure(Throwable t) {
            //go ahead and wrap in a runtime exception for this case, but you can do logging or start counting errors.
            throw new RuntimeException(t);
        }
    }

    public void ingest(Iterator<Object[]> boundItemsIterator, String insertCQL) throws InterruptedException {
        Cluster cluster = Cluster.builder()
                .addContactPointsWithPorts(contactHosts)
                .build();
        Session session = cluster.newSession();
        //fixed thread pool that closes on app exit
        ExecutorService executor = MoreExecutors.getExitingExecutorService(
                (ThreadPoolExecutor) Executors.newFixedThreadPool(threads));
        final PreparedStatement statement = session.prepare(insertCQL);
        while (boundItemsIterator.hasNext()) {
            BoundStatement boundStatement = statement.bind(boundItemsIterator.next()[0], boundItemsIterator.next()[1], boundItemsIterator.next()[2]);
            ResultSetFuture future = session.executeAsync(boundStatement);
            Futures.addCallback(future, new IngestCallback(), executor);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) { //dosomething}
            session.close();
            cluster.close();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Iterator<Object[]> rows = new Iterator<Object[]>() {
            int i = 0;
            Random random = new Random();

            @Override
            public boolean hasNext() {
                return i != 10;
            }

            @Override
            public Object[] next() {
                i++;
                return new Object[]{String.valueOf(i), String.valueOf(random.nextLong()), UUID.randomUUID().toString()};
            }
        };

        System.out.println("Starting benchmark");
        Stopwatch watch = Stopwatch.createStarted();
        new BulkLoader(1, new InetSocketAddress("127.0.0.1", 9042)).ingest(rows,
                "INSERT INTO inmem.users (userid,first_name,last_name) VALUES (?,?,?) IF NOT EXISTS;");
        System.out.println("total time seconds = " + watch.elapsed(TimeUnit.SECONDS));
        watch.stop();
        System.exit(0);
    }
}
