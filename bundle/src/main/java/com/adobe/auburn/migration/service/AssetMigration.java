package com.adobe.auburn.migration.service;

public interface AssetMigration {
public void copyExternalAsset(String sourcePath , String destinationPath );
public void processCopiedAsset(String path);
}
