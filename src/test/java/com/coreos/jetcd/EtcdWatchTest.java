package com.coreos.jetcd;


//import com.coreos.jetcd.data.ByteSequence;
//import com.coreos.jetcd.data.EtcdHeader;
import com.coreos.jetcd.exception.AuthFailedException;
import com.coreos.jetcd.exception.ConnectException;
import com.coreos.jetcd.options.WatchOption;
import com.coreos.jetcd.watch.WatchEvent;
import com.google.protobuf.ByteString;

import io.grpc.stub.StreamObserver;

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.testng.asserts.Assertion;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.*;

/**
 * watch test case.
 */
public class EtcdWatchTest {

   private EtcdClient client;
   private EtcdWatch watchClient;
   private EtcdKV kvClient;
   private BlockingQueue<WatchEvent> eventsQueue = new LinkedBlockingDeque<>();

   private ByteString key = ByteString.copyFromUtf8("test_key");
   private ByteString value = ByteString.copyFromUtf8("test_val");
//   private EtcdWatchImpl.Watcher watcher;
   private Closeable watchCloser;

   private Assertion test = new Assertion();

   @BeforeTest
   public void newEtcdClient() throws AuthFailedException, ConnectException {
      client = EtcdClientBuilder.newBuilder().endpoints("localhost:2379").build();
      watchClient = client.getWatchClient();
      kvClient = client.getKVClient();
   }

   @Test
   public void testWatch() throws ExecutionException, InterruptedException {
      WatchOption option = WatchOption.DEFAULT;
      watchCloser = watchClient.watch(key, option, new StreamObserver<WatchEvent>() {

          /**
           * onNext will be called when watcher receive any events
           *
           * @param header
           * @param events received events
           */
        @Override
        public void onNext(WatchEvent event) {
            EtcdWatchTest.this.eventsQueue.add(event);
            
        }

        @Override
        public void onError(Throwable t) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void onCompleted() {
            cdl.countDown();
        }

      });

   }

   /**
    * watch put operation on key
    * assert whether receive put event
    */
   @Test(dependsOnMethods = "testWatch")
   public void testWatchPut() throws InterruptedException {
      kvClient.put(key, value);
      WatchEvent event = eventsQueue.poll(5, TimeUnit.MINUTES);
      test.assertEquals(event.getKeyValue().getKey(), key);
      test.assertEquals(event.getEventType(), WatchEvent.EventType.PUT);
   }

   /**
    * watch delete operation on key
    * assert whether receive delete event
    */
   @Test(dependsOnMethods = "testWatchPut")
   public void testWatchDelete() throws InterruptedException {
      kvClient.delete(key);
      WatchEvent event = eventsQueue.poll(5, TimeUnit.SECONDS);
      test.assertEquals(event.getKeyValue().getKey(), key);
      test.assertEquals(event.getEventType(), WatchEvent.EventType.DELETE);
   }

   final CountDownLatch cdl = new CountDownLatch(1);
   
   /**
    * cancel watch test case
    * assert whether receive cancel response
    */
   @Test(dependsOnMethods = "testWatchDelete")
   public void testCancelWatch() throws IOException, InterruptedException, TimeoutException {
       watchCloser.close();
       test.assertTrue(cdl.await(5L, TimeUnit.SECONDS));
       
//      CompletableFuture<Boolean> future = watcher.cancel();
//      test.assertTrue(future.get(5, TimeUnit.SECONDS));
   }
}