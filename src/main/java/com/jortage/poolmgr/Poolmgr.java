package com.jortage.poolmgr;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URI;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import sun.misc.Signal;
import org.eclipse.jetty.server.*;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.gaul.s3proxy.*;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.*;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobAccess;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import com.jortage.poolmgr.http.OuterHandler;
import com.jortage.poolmgr.http.RedirHandler;
import com.jortage.poolmgr.rivet.RivetHandler;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.*;
import com.google.common.hash.HashCode;
import timshel.s3dedupproxy.BackendConfig;
import timshel.s3dedupproxy.GlobalConfig;

/**
 * Poolmgr is responsible for managing blob storage, S3 proxy, and backup operations.
 * It initializes the blob store, starts S3 servers, handles configuration, and manages backups.
 */
public class Poolmgr {
    public static BlobStore backingBlobStore, backingBackupBlobStore, dumpsStore;
    public static DataSource dataSource;
    public static Map<String, String> users;
    public static volatile boolean readOnly = false;
    private static boolean backingUp = false;
    public static boolean useNewUrls;
    public static final Table<String, String, Object> provisionalMaps = HashBasedTable.create();

    /**
     * Initializes and starts the Poolmgr service.
     *
     * @param config The global configuration for the service.
     * @throws Exception If initialization fails.
     */
    public static void start(GlobalConfig config) throws Exception {
        try {
            initializeDumpsStore();
            Stopwatch initTimer = Stopwatch.createStarted();
            loadConfig(config);

            startS3Proxy(config);
            startRedirectorServer(config);
            startRivetServer(config);
            registerBackupHandler(config);
            
            System.err.println("Initialization complete in " + initTimer);
        } catch (Throwable t) {
            System.err.println("Initialization failed.");
            t.printStackTrace();
        }
    }

    /**
     * Initializes the filesystem-based blob store for dump storage.
     */
    private static void initializeDumpsStore() {
        Properties dumpsProps = new Properties();
        dumpsProps.setProperty(FilesystemConstants.PROPERTY_BASEDIR, "dumps");
        dumpsStore = ContextBuilder.newBuilder("filesystem")
                .overrides(dumpsProps)
                .build(BlobStoreContext.class)
                .getBlobStore();
    }

    /**
     * Starts the S3 Proxy service.
     */
    private static void startS3Proxy(GlobalConfig config) throws Exception {
        System.err.print("Starting S3 server... ");
        
        S3Proxy s3Proxy = S3Proxy.builder()
                .awsAuthentication(AuthenticationType.AWS_V2_OR_V4, "DUMMY", "DUMMY")
                .endpoint(URI.create("http://localhost:23278"))
                .jettyMaxThreads(24)
                .v4MaxNonChunkedRequestSize(128L * 1024L * 1024L)
                .ignoreUnknownHeaders(true)
                .build();
        
        configureS3ProxyServer(s3Proxy);
        s3Proxy.start();
        System.err.println("ready on http://localhost:23278");
    }

    /**
     * Configures the S3 Proxy server with custom handlers.
     */
    private static void configureS3ProxyServer(S3Proxy s3Proxy) throws Exception {
        Field serverField = S3Proxy.class.getDeclaredField("server");
        serverField.setAccessible(true);
        Server s3ProxyServer = (Server) serverField.get(s3Proxy);
        s3ProxyServer.setHandler(new OuterHandler(s3ProxyServer.getHandler()));
        QueuedThreadPool pool = (QueuedThreadPool) s3ProxyServer.getThreadPool();
        pool.setName("Jetty-Common");
    }

    /**
     * Starts the redirector server.
     */
    private static void startRedirectorServer(GlobalConfig config) throws Exception {
        System.err.print("Starting redirector server... ");
        Server redirServer = new Server(new QueuedThreadPool(24));
        ServerConnector connector = new ServerConnector(redirServer);
        connector.setHost("localhost");
        connector.setPort(23279);
        redirServer.addConnector(connector);
        redirServer.setHandler(new OuterHandler(new RedirHandler(config.backend().publicHost(), dumpsStore)));
        redirServer.start();
        System.err.println("ready on http://localhost:23279");
    }

    /**
     * Starts the Rivet server if enabled.
     */
    private static void startRivetServer(GlobalConfig config) throws Exception {
        if (config.rivet().enabled()) {
            System.err.print("Starting Rivet server... ");
            Server rivetServer = new Server(new QueuedThreadPool(24));
            ServerConnector connector = new ServerConnector(rivetServer);
            connector.setHost("localhost");
            connector.setPort(23280);
            rivetServer.addConnector(connector);
            rivetServer.setHandler(new OuterHandler(new RivetHandler(config.backend().bucket(), config.backend().publicHost())));
            rivetServer.start();
            System.err.println("ready on http://localhost:23280");
        } else {
            System.err.println("Rivet server not enabled.");
        }
    }

    /**
     * Loads configuration settings and initializes blob stores.
     */
    private static void loadConfig(GlobalConfig config) {
        try {
            System.err.print("Constructing blob stores...");
            backingBlobStore = createBlobStore(config.backend());
            backingBackupBlobStore = config.backupBackend().nonEmpty() ? createBlobStore(config.backupBackend().get()) : null;
            users = config.users().asJava();
            users.forEach((key, value) -> dumpsStore.createContainerInLocation(null, key));
            readOnly = config.readOnly();
        } catch (Exception e) {
            System.err.println(" failed");
            e.printStackTrace();
        }
    }

    /**
     * Creates a BlobStore based on the provided configuration.
     */
    private static BlobStore createBlobStore(BackendConfig conf) {
        String protocol = "s3".equals(conf.protocol()) ? "aws-s3" : conf.protocol();
        return ContextBuilder.newBuilder(protocol)
                .credentials(conf.accessKeyId(), conf.secretAccessKey())
                .modules(ImmutableList.of(new SLF4JLoggingModule()))
                .endpoint(conf.endpoint())
                .build(BlobStoreContext.class)
                .getBlobStore();
    }
}
