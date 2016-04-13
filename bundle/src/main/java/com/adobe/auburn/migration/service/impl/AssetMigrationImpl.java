package com.adobe.auburn.migration.service.impl;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.sling.api.resource.LoginException;
import org.apache.sling.api.resource.PersistenceException;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adobe.auburn.migration.ContentMigrationConstants;
import com.adobe.auburn.migration.service.AssetMigration;
@Component(name="External Asset Migration",label="External Asset Migration",description = "This service " ,metatype=true)
public class AssetMigrationImpl implements AssetMigration {
	private static final Logger log = LoggerFactory.getLogger(AssetMigrationImpl.class);
	@Reference
	 private ResourceResolverFactory resolverFactory;

	@Override
	public void copyExternalAsset(String sourcePath, String destinationPath) {
		// TODO Auto-generated method stub

	}

	@Override
	public void processCopiedAsset(String sourcePath , String destinationPath) {
		// TODO Auto-generated method stub
		ResourceResolver resolver = null;
		Resource sourceFolder = null;
		Resource destinationFolder = null;
		try {
			 resolver = resolverFactory.getAdministrativeResourceResolver(null);
			 if(null != resolver){
				 sourceFolder = resolver.getResource(sourcePath);
			 }
			 if(null != resolver){
				 destinationFolder = resolver.getResource(destinationPath);
			 }
			 Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
			 String pathPrefix = calendar.HOUR_OF_DAY+"-"+calendar.MINUTE+"-"+calendar.SECOND;
			 Map<String, Object> folderProperties = new HashMap();
			 folderProperties.put("jcr:primaryType", "sling:Folder");
			 //create the destination folder if source folder exists
			 if(null != sourceFolder && null == destinationFolder){
				 destinationFolder = resolver.create(resolver.getResource(ContentMigrationConstants.PROCESSED_ASSETS_ROOT_PATH), pathPrefix, folderProperties);
			 }
			 if(null != sourceFolder){
				 Iterable<Resource> childAssets = sourceFolder.getChildren();
				 for(Resource childAsset:childAssets){

				 }
			 }else{
				 log.error("Source Folder at path {} does not exist : terminating the copy",sourcePath);
			 }


		} catch (LoginException | PersistenceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
