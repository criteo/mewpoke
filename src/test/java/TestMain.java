import com.couchbase.client.core.CouchbaseCore;
import com.couchbase.client.core.RequestHandler;
import com.couchbase.client.core.lang.Tuple2;
import com.couchbase.client.core.message.kv.UpsertRequest;
import com.couchbase.client.core.node.Node;
import com.couchbase.client.core.utils.Blocking;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.criteo.nosql.mewpoke.config.Config;
import com.criteo.nosql.mewpoke.discovery.Service;
import com.criteo.nosql.mewpoke.memcached.MemcachedMetrics;
import io.prometheus.client.exporter.HTTPServer;
import rx.Observable;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class TestMain {

    //@Test
    public void test() throws IOException, InterruptedException {

        Config cfg = Config.fromFile(Config.DEFAULT_PATH);
        HTTPServer server = new HTTPServer(8080);

        HashMap m = new HashMap();
        m.put(InetSocketAddress.createUnresolved("toto", 8080), 50L);
        MemcachedMetrics toto = new MemcachedMetrics(new Service("a", "b"));
        toto.updateGetLatency(m);

        Thread.sleep(60000L);
        toto.close();
        Thread.sleep(60000L);

    }

    //@Test
    public void test2() throws Exception {
        // Create a cluster reference
        CouchbaseCluster cluster = CouchbaseCluster.create("cluster01.domain.local");

        // Connect to the bucket and open it
        Bucket bucket = cluster.openBucket("Bucket01");
        CouchbaseCore c = ((CouchbaseCore) bucket.core());

        Field f = c.getClass().getDeclaredField("requestHandler");
        f.setAccessible(true);
        RequestHandler r = (RequestHandler) f.get(c);

        Field nodesF = r.getClass().getDeclaredField("nodes");
        nodesF.setAccessible(true);
        CopyOnWriteArrayList<Node> nodes = (CopyOnWriteArrayList<Node>) nodesF.get(r);
        SortedSet<String> seenNodes = new TreeSet<>();

        for (int i = 0; i < 300; i++) {
            Document doc = JsonDocument.create("mewpoke_" + i, null, -1);
            Tuple2<Observable<Document>, UpsertRequest> ret = bucket.async().upsertWithRequest(doc, PersistTo.MASTER, ReplicateTo.NONE);
            long start = System.currentTimeMillis();
            try {
                Blocking.blockForSingle(ret.value1(), 20, TimeUnit.SECONDS);
            } catch (Throwable e) {

            } finally {
                long stop = System.currentTimeMillis();
                System.out.println(ret.value2().node.hostname().hostname() + " : " + (stop - start));
                seenNodes.add(ret.value2().node.hostname().hostname());
            }
        }
        System.out.println(seenNodes);
        System.out.println(seenNodes.size());
    }

}
