package org.spiros;


import com.couchbase.client.core.transaction.CoreTransactionGetResult;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.transactions.TransactionGetResult;
import org.junit.jupiter.api.Test;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;
import org.testcontainers.shaded.okhttp3.Credentials;
import org.testcontainers.shaded.okhttp3.FormBody;
import org.testcontainers.shaded.okhttp3.OkHttpClient;
import org.testcontainers.shaded.okhttp3.Request;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class CouchbaseTransactionsTest {

    public static final String COUCHBASE_SERVER_7_1_1 = "couchbase/server:7.1.1";
    public static final String MYBUCKET = "myBucket";
    public static final String MY_SCOPE = "myScope";
    public static final String MY_COLLECTION = "myCollection";


    /**
     * Test to demonstrate 3 problems with couchbase Transactions,
     * with the latest java client com.couchbase.client:java-client:3.3.3
     * <p>
     * <p>
     * - Problem 1 (affects only testing):
     * See: "Problem 1 non-proper solution" in the code below
     * <p>
     * As we can see we cannot write test for Couchbase Transactions.
     * <p>
     * Fails with "DurabilityImpossibleException:
     * With the current cluster configuration, the requested durability guarantees are impossible"
     * <p>
     * A proper fix is impossible since there is no way to set Bucket.replicaNumber to zero (default = 1)
     * Also there is no way to set up a cluster of two servers that would also solve this problem.
     * <p>
     * Therefore, a proper solution is impossible.
     * <p>
     * <p>
     * <p>
     * - Problem 2 (affects production code):
     * See: "Problem 2 non-proper solution" in the code below
     * <p>
     * The `cas` value is not available in the returned objects inside transactions.
     * <p>
     * We had to use java reflection to locate and read the cas value.
     * We need the cas value in our use cases from an insert/replace calls.
     * (Use case: We return the cas value in our API for later updates with optimistic locking)
     * <p>
     * <p>
     * <p>
     * MOST IMPORTANT PROBLEM that prevent us for using the client:
     * Problem 3 (affects production code):
     * See: "Problem 3 There is no solution" in the code below.
     * <p>
     * After a simple insert inside a Transaction context, a HUGE amount (every 1ms) of logs appears.
     * <p>
     * Sample log:
     * [cb-events] INFO com.couchbase.transactions.cleanup.lost - [com.couchbase.transactions.cleanup.lost][LogEvent] Client fa245 stop on CollectionIdentifier{bucket='myBucket', scope=Optional[_default], collection=Optional[_default], isDefault=true} = false
     * <p>
     * As we can see, even if the log is `INFO` level has two basic problems that prevent us for using it:
     * 1. The "com.couchbase.transactions.cleanup.lost" seems like a problem. Probably something is lost.
     * 2. Huge amount of logs that might affect performance.
     */
    @Test
    void couchbaseTransactionsTest_demonstrateTheThreeProblems() throws IOException, InterruptedException {

        CouchbaseContainer container = new CouchbaseContainer(DockerImageName.parse(COUCHBASE_SERVER_7_1_1))
                .withEnabledServices(CouchbaseService.KV, CouchbaseService.QUERY, CouchbaseService.INDEX)
                .withStartupTimeout(Duration.ofSeconds(60));
        //.withBucket(bucketDefinition); No bucket is created here

        container.start();

        //TODO: Problem 1 non-proper solution. @Couchbase Please check
        createANewBucketUsingCouchbaseAdminRest(container, MYBUCKET);
        wait10Sec();

        Cluster cluster = Cluster.connect(
                container.getConnectionString(),
                container.getUsername(),
                container.getPassword()
        );

        cluster.bucket(MYBUCKET).collections().createScope(MY_SCOPE);
        wait10Sec();

        cluster.bucket(MYBUCKET).collections().createCollection(CollectionSpec.create(MY_COLLECTION, MY_SCOPE));
        wait10Sec();

        Collection collection = cluster.bucket(MYBUCKET).scope(MY_SCOPE).collection(MY_COLLECTION);

        cluster.transactions().run((ctx) -> {

            TransactionGetResult attemptContext = ctx.insert(collection, "doc2", JsonObject.create());

            try {
                Method internalMethod = TransactionGetResult.class.getDeclaredMethod("internal");
                internalMethod.setAccessible(true);
                //TODO: Problem 2 non-proper solution. @Couchbase Please check
                CoreTransactionGetResult internalResult = (CoreTransactionGetResult) internalMethod.invoke(attemptContext);
                long cas = internalResult.cas();

                System.out.println("-------> Cas value: " + cas);

            } catch (Exception ignore) {
            }
        });

        assertNotNull(collection.get("doc2"));

        wait10Sec();
        wait10Sec();
        wait10Sec();

        System.out.println("------> Please check the problematic logs above!");

        //TODO: Problem 3 There is no solution. @Couchbase Please check
        //Here, a HUGE amount (every 1ms) of logs appears. Just check the logs in the console.
        //Sample:
        //[cb-events] INFO com.couchbase.transactions.cleanup.lost - [com.couchbase.transactions.cleanup.lost][LogEvent] Client fa245 stop on CollectionIdentifier{bucket='myBucket', scope=Optional[_default], collection=Optional[_default], isDefault=true} = false
    }

    private static void wait10Sec() throws InterruptedException {
        Thread.sleep(10000);
    }

    /**
     * Mandatory if you want to test Transactions. Sets Bucket's replicaNumber to zero to avoid
     * DurabilityImpossibleException
     */
    private static void createANewBucketUsingCouchbaseAdminRest(CouchbaseContainer couchbaseContainer, String bucket) throws IOException {
        String createBucketRestEndpoint =
                "http://"
                        + couchbaseContainer.getHost()
                        + ":"
                        + couchbaseContainer.getMappedPort(8091)
                        + "/pools/default/buckets/";

        Request.Builder requestBuilder = new Request.Builder().url(createBucketRestEndpoint);

        requestBuilder =
                requestBuilder.header(
                        "Authorization",
                        Credentials.basic(couchbaseContainer.getUsername(), couchbaseContainer.getPassword()));

        requestBuilder =
                requestBuilder.method(
                        "POST",
                        new FormBody.Builder()
                                .add("name", bucket)
                                .add("ramQuotaMB", Integer.toString(100))
                                .add("flushEnabled", "0")
                                .add("replicaNumber", "0")
                                .build());

        new OkHttpClient().newCall(requestBuilder.build()).execute();
    }
}
