package com.marvellmist.jtt1078.minio;

import io.minio.MinioClient;
import io.minio.errors.MinioException;
import io.minio.messages.Bucket;
import io.minio.PutObjectArgs;
import io.minio.MakeBucketArgs;
import io.minio.BucketExistsArgs;
import io.minio.DownloadObjectArgs;
import io.minio.UploadObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.messages.Item;
import io.minio.Result;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class MinioExample {
    public static void main(String[] args) {
        try {
            // client initialization
            MinioClient minioClient = MinioClient.builder()
                .endpoint(MinioEndpoint)
                .credentials(User, Password)
                .build();

            // Checking for Buckets
            boolean isBucketExist = minioClient.bucketExists(BucketExistsArgs.builder().bucket(BucketName).build());
            if (!isBucketExist) {
                    // Bucket Creation
                    minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
                    System.out.println("Bucket created: " + BucketName);
            } else {
                System.out.println("Bucket already exists: " + BucketName);
            }

            // Uploading Objects
            minioClient.uploadObject(
                UploadObjectArgs.builder()
                    .bucket(bucketName)
                    .object(ObjectName)
                    .filename(InputPath)
                    .build());

            // Listing/Finding Objects
            Iterable<Result<Item>> results = minioClient.listObjects(ListObjectsArgs.builder().bucket(BucketName).build());
            for (Result<Item> result : results) {
                Item item = result.get();
                System.out.println("Object name: " + item.objectName());
            }

            // File Download
            minioClient.downloadObject(
                DownloadObjectArgs.builder()
                    .bucket(BucketName)
                    .object(ObjectName)
                    .filename(OutputPath)
                    .build());


        } catch (MinioException | IOException | InvalidKeyException | NoSuchAlgorithmException e) {
            System.err.println("Error occurred: " + e.getMessage());
        }
    }
}

