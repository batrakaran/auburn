package com.adobe.auburn.migration.util;

import com.day.cq.dam.api.Asset;
import com.day.cq.dam.api.AssetManager;
import com.day.cq.dam.api.DamConstants;
import com.day.cq.wcm.api.NameConstants;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class ContentMigrationUtiils {


    public static Asset createAssetInDAM( ResourceResolver resourceResolver, Resource parentFolder, String fileName, String valueString ,String assetType) throws RepositoryException, PersistenceException {
        AssetManager assetManager = resourceResolver.adaptTo(AssetManager.class);
        Asset asset = assetManager.createAsset(parentFolder.getPath() + "/" + fileName, new ByteArrayInputStream(valueString.getBytes(StandardCharsets.UTF_8)), assetType, true);

        if( asset != null && parentFolder != null && StringUtils.isNotBlank(assetType)){
            Node assetNode = asset.adaptTo( Node.class );

            Node metadataNode = assetNode.getNode( NameConstants.NN_CONTENT + "/" + DamConstants.METADATA_FOLDER );

            if( metadataNode != null ){
                metadataNode.setProperty( DamConstants.DC_FORMAT, assetType);
                resourceResolver.commit();
            }
        }
        // set after the file created.
        return asset;

    }
}
