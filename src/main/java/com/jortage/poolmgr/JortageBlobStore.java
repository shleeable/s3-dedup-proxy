package com.jortage.poolmgr;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import javax.sql.DataSource;

import org.jclouds.blobstore.*;
import org.jclouds.blobstore.domain.*;
import org.jclouds.blobstore.domain.internal.MutableBlobMetadataImpl;
import org.jclouds.blobstore.options.*;
import org.jclouds.domain.*;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.FilePayload;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.hash.*;
import com.google.common.io.*;

/**
 * JortageBlobStore is a specialized BlobStore that integrates with a backup system.
 * It manages primary storage and separate dump storage for archival purposes.
 * Implements additional safety and deduplication measures.
 */
public class JortageBlobStore extends ForwardingBlobStore {
    private final BlobStore dumpsStore;
    private final String identity;
    private final String bucket;
    private final DataSource dataSource;

    /**
     * Constructor to initialize the JortageBlobStore with required dependencies.
     *
     * @param blobStore Primary BlobStore instance
     * @param dumpsStore Secondary BlobStore instance for backups/dumps
     * @param bucket The primary storage bucket
     * @param identity Unique identifier for the store
     * @param dataSource Database connection for metadata management
     */
    public JortageBlobStore(BlobStore blobStore, BlobStore dumpsStore, String bucket, String identity, DataSource dataSource) {
        super(blobStore);
        this.dumpsStore = dumpsStore;
        this.bucket = bucket;
        this.identity = identity;
        this.dataSource = dataSource;
    }

    /**
     * Ensures the container name matches the access identity.
     * Throws an exception if there's a mismatch.
     */
    private void checkContainer(String container) {
        if (!Objects.equal(container, identity)) {
            throw new IllegalArgumentException("Bucket name must match your access ID");
        }
    }

    /**
     * Generates a hashed path for the given container and blob name.
     */
    private String getMapPath(String container, String name) {
        checkContainer(container);
        return Poolmgr.hashToPath(Queries.getMap(dataSource, container, name));
    }

    /**
     * Determines whether the given blob name belongs to a dump (backup) storage.
     */
    private boolean isDump(String name) {
        return name.startsWith("backups/dumps") || name.startsWith("/backups/dumps");
    }

    @Override
    public BlobStoreContext getContext() {
        return delegate().getContext();
    }

    @Override
    public BlobBuilder blobBuilder(String name) {
        return isDump(name) ? dumpsStore.blobBuilder(name) : delegate().blobBuilder(name);
    }

    @Override
    public Blob getBlob(String container, String name) {
        return isDump(name) ? dumpsStore.getBlob(container, name) : delegate().getBlob(bucket, getMapPath(container, name));
    }

    @Override
    public boolean blobExists(String container, String name) {
        return isDump(name) ? dumpsStore.blobExists(container, name) : delegate().blobExists(bucket, getMapPath(container, name));
    }

    @Override
    public BlobMetadata blobMetadata(String container, String name) {
        return isDump(name) ? dumpsStore.blobMetadata(container, name) : delegate().blobMetadata(bucket, getMapPath(container, name));
    }

    @Override
    public String putBlob(String container, Blob blob) {
        Poolmgr.checkReadOnly();
        checkContainer(container);
        
        String blobName = blob.getMetadata().getName();
        if (isDump(blobName)) {
            return dumpsStore.putBlob(container, blob, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ));
        }

        File tempFile = null;
        Object mutex = new Object();
        synchronized (Poolmgr.provisionalMaps) {
            Poolmgr.provisionalMaps.put(identity, blobName, mutex);
        }
        
        try {
            tempFile = File.createTempFile("jortage-proxy-", ".dat");
            String contentType = blob.getPayload().getContentMetadata().getContentType();
            HashCode hash;
            
            try (InputStream is = blob.getPayload().openStream();
                 OutputStream fos = Files.newOutputStream(tempFile.toPath())) {
                HashingOutputStream hos = new HashingOutputStream(Hashing.sha512(), fos);
                FileReprocessor.reprocess(is, hos);
                hash = hos.hash();
            }

            try (Payload payload = new FilePayload(tempFile)) {
                String hashPath = Poolmgr.hashToPath(hash);
                payload.getContentMetadata().setContentType(contentType);
                
                BlobMetadata existingMetadata = delegate().blobMetadata(bucket, hashPath);
                if (existingMetadata != null) {
                    Queries.putMap(dataSource, identity, blobName, hash);
                    return existingMetadata.getETag();
                }
                
                Blob newBlob = blobBuilder(hashPath)
                        .payload(payload)
                        .userMetadata(blob.getMetadata().getUserMetadata())
                        .build();
                
                String etag = delegate().putBlob(bucket, newBlob, new PutOptions().setBlobAccess(BlobAccess.PUBLIC_READ).multipart());
                Queries.putPendingBackup(dataSource, hash);
                Queries.putMap(dataSource, identity, blobName, hash);
                Queries.putFilesize(dataSource, hash, tempFile.length());
                return etag;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            if (tempFile != null) tempFile.delete();
            synchronized (Poolmgr.provisionalMaps) {
                Poolmgr.provisionalMaps.remove(identity, blobName);
            }
            synchronized (mutex) {
                mutex.notifyAll();
            }
        }
    }

    @Override
    public void removeBlob(String container, String name) {
        Poolmgr.checkReadOnly();
        checkContainer(container);
        
        if (isDump(name)) {
            dumpsStore.removeBlob(container, name);
            return;
        }
        
        HashCode hashCode = Queries.getMap(dataSource, identity, name);
        if (Queries.removeMap(dataSource, identity, name)) {
            if (Queries.getMapCount(dataSource, hashCode) == 0) {
                delegate().removeBlob(bucket, Poolmgr.hashToPath(hashCode));
                Queries.removeFilesize(dataSource, hashCode);
                Queries.removePendingBackup(dataSource, hashCode);
            }
        }
    }
}
