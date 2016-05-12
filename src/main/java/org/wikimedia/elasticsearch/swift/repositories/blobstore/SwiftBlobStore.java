package org.wikimedia.elasticsearch.swift.repositories.blobstore;

import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.model.StoredObject;

/**
 * Our blob store
 */
public class SwiftBlobStore extends AbstractComponent implements BlobStore {
    // How much to buffer our blobs by
    private final int bufferSizeInBytes;

    // Our Swift container. This is important.
    private final Container swift;
    
  

    /**
     * Constructor. Sets up the container mostly.
     * @param settings Settings for our repository. Only care about buffer size.
     * @param auth
     * @param container
     */
    public SwiftBlobStore(Settings settings, Account auth, String container) {
        super(settings);
        this.bufferSizeInBytes = (int)settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).bytes();
        swift = auth.getContainer(container);
        if (!swift.exists()) {
            swift.create();
            swift.makePublic();
        }
    }

    public boolean copyblobStorage(String sourceblob,String destinationblob){
    	StoredObject sourceObject = swift.getObject(sourceblob);
    	if(sourceObject.exists()){
    		StoredObject newObject = swift.getObject(destinationblob);
        	sourceObject.copyObject(swift, newObject);
        	sourceObject.delete();
        	return true;
    	}
    	return false;
    }
    
    
    /**
     * Get the container
     */
    public Container swift() {
        return swift;
    }

    /**
     * Get our buffer size
     */
    public int bufferSizeInBytes() {
        return bufferSizeInBytes;
    }

    /**
     * Factory for getting blob containers for a path
     * @param path The blob path to search
     */
    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new SwiftBlobContainer(path, this);
    }

    /**
     * Delete an arbitrary BlobPath from our store.
     * @param path The blob path to delete
     */
    @Override
    public void delete(BlobPath path) {
        String keyPath = path.buildAsString("/");
        if (!keyPath.isEmpty()) {
            keyPath = keyPath + "/";
        }
        StoredObject obj = swift().getObject(keyPath);
        if (obj.exists()) {
            obj.delete();
        }
    }

    
  
    
    /**
     * Close the store. No-op for us.
     */
    @Override
    public void close() {
    }
}
