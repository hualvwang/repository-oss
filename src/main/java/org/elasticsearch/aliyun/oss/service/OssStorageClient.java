package org.elasticsearch.aliyun.oss.service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.aliyun.oss.*;
import com.aliyun.oss.model.*;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.aliyun.oss.blobstore.OssBlobContainer;
import org.elasticsearch.aliyun.oss.service.exception.CreateStsOssClientException;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.repository.oss.OssRepository;
import org.elasticsearch.utils.DateHelper;
import org.elasticsearch.utils.HttpClientHelper;

import static java.lang.Thread.sleep;

/**
 * @author hanqing.zhq@alibaba-inc.com
 */
public class OssStorageClient {
    private static final Logger logger = LogManager.getLogger(OssBlobContainer.class);

    private RepositoryMetaData metadata;
    private OSS client;
    private Date stsTokenExpiration;
    private String ECS_METADATA_SERVICE = "http://100.100.100.200/latest/meta-data/ram/security-credentials/";
    private final int IN_TOKEN_EXPIRED_MS = 5000;
    private final String ACCESS_KEY_ID = "AccessKeyId";
    private final String ACCESS_KEY_SECRET = "AccessKeySecret";
    private final String SECURITY_TOKEN = "SecurityToken";
    private final int REFRESH_RETRY_COUNT = 3;
    private boolean isStsOssClient;
    private ReadWriteLock readWriteLock;

    private final String EXPIRATION = "Expiration";

    public OssStorageClient(RepositoryMetaData metadata) throws CreateStsOssClientException {
        this.metadata = metadata;
        isStsOssClient = StringUtils.isNotEmpty(OssClientSettings.ECS_RAM_ROLE.get(metadata.settings()).toString());
        readWriteLock = new ReentrantReadWriteLock();
        client = createClient(metadata);

    }

    public boolean isStsOssClient() {
        return isStsOssClient;
    }

    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest deleteObjectsRequest)
        throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.deleteObjects(deleteObjectsRequest);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.deleteObjects(deleteObjectsRequest);
        }
    }

    public boolean doesObjectExist(String bucketName, String key)
        throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.doesObjectExist(bucketName, key);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.doesObjectExist(bucketName, key);
        }
    }

    public boolean doesBucketExist(String bucketName)
        throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.doesBucketExist(bucketName);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.doesBucketExist(bucketName);
        }
    }

    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
        throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.listObjects(listObjectsRequest);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.listObjects(listObjectsRequest);
        }
    }

    public OSSObject getObject(String bucketName, String key)
        throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                restoreObject(bucketName, key);
                return this.client.getObject(bucketName, key);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            restoreObject(bucketName, key);
            return this.client.getObject(bucketName, key);
        }
    }

    private void restoreObject(String bucketName, String key){
        ObjectMetadata objectMetadata = this.client.getObjectMetadata(bucketName, key);
        StorageClass storageClass = objectMetadata.getObjectStorageClass();
        if (storageClass == StorageClass.Archive) {
            this.client.restoreObject(bucketName, key);
            try {
                do {
                    Thread.sleep(100);
                    objectMetadata = this.client.getObjectMetadata(bucketName, key);
                    logger.debug("restore object:{}",objectMetadata);
                } while (!objectMetadata.isRestoreCompleted());
            } catch (InterruptedException e) {
                logger.error("can not restore object {}/{}", bucketName,key,e);
            }
        }
    }
    public PutObjectResult putObject(String bucketName, String key, InputStream input,
        ObjectMetadata metadata) throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client.putObject(bucketName, key, input, metadata);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client.putObject(bucketName, key, input, metadata);
        }
    }

    public void deleteObject(String bucketName, String key)
        throws OSSException, ClientException {
        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                this.client.deleteObject(bucketName, key);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            this.client.deleteObject(bucketName, key);
        }

    }

    public CopyObjectResult copyObject(String sourceBucketName, String sourceKey,
        String destinationBucketName, String destinationKey) throws OSSException, ClientException {

        if (isStsOssClient) {
            readWriteLock.readLock().lock();
            try {
                return this.client
                    .copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
            } finally {
                readWriteLock.readLock().unlock();
            }
        } else {
            return this.client
                .copyObject(sourceBucketName, sourceKey, destinationBucketName, destinationKey);
        }
    }

    public void refreshStsOssClient() throws CreateStsOssClientException {
        int retryCount = 0;
        while (isStsTokenExpired() || isTokenWillExpired()) {
            retryCount++;
            if (retryCount > REFRESH_RETRY_COUNT) {
                logger.error("Can't get valid token after retry {} times", REFRESH_RETRY_COUNT);
                throw new CreateStsOssClientException(
                    "Can't get valid token after retry " + REFRESH_RETRY_COUNT + " times");
            }
            this.client = createStsOssClient(this.metadata);
            try {
                if (isStsTokenExpired() || isTokenWillExpired()) {
                    sleep(IN_TOKEN_EXPIRED_MS * 2);
                }
            } catch (InterruptedException e) {
                logger.error("refresh sleep exception", e);
                throw new CreateStsOssClientException(e);
            }
        }
    }

    public void shutdown() {
        if (isStsOssClient) {
            readWriteLock.writeLock().lock();
            try {
                if (null != this.client) {
                    this.client.shutdown();
                }
            } finally {
                readWriteLock.writeLock().unlock();
            }
        } else {
            if (null != this.client) {
                this.client.shutdown();
            }
        }
    }

    private boolean isStsTokenExpired() {
        boolean expired = true;
        Date now = new Date();
        if (null != stsTokenExpiration) {
            if (stsTokenExpiration.after(now)) {
                expired = false;
            }
        }
        return expired;
    }

    private boolean isTokenWillExpired() {
        boolean in = true;
        Date now = new Date();
        long millisecond = stsTokenExpiration.getTime() - now.getTime();
        if (millisecond >= IN_TOKEN_EXPIRED_MS) {
            in = false;
        }
        return in;
    }

    private OSS createClient(RepositoryMetaData repositoryMetaData) throws CreateStsOssClientException {
        OSS client;

        String ecsRamRole = OssClientSettings.ECS_RAM_ROLE.get(repositoryMetaData.settings()).toString();
        String stsToken = OssClientSettings.SECURITY_TOKEN.get(repositoryMetaData.settings()).toString();
        /*
         * If ecsRamRole exist
         * means use ECS metadata service to get ststoken for auto snapshot.
         * */
        if (StringUtils.isNotEmpty(ecsRamRole)) {
            client = createStsOssClient(repositoryMetaData);
        } else if (StringUtils.isNotEmpty(stsToken)) {
            //no used still now.
            client = createAKStsTokenClient(repositoryMetaData);
        } else {
            client = createAKOssClient(repositoryMetaData);
        }
        return client;
    }

    private ClientBuilderConfiguration extractClientConfiguration(RepositoryMetaData repositoryMetaData) {
        ClientBuilderConfiguration configuration = new ClientBuilderConfiguration();
        configuration.setSupportCname(OssRepository.getSetting(OssClientSettings.SUPPORT_CNAME, repositoryMetaData));
        return configuration;
    }

    private OSS createAKOssClient(RepositoryMetaData repositoryMetaData) {
        SecureString accessKeyId =
            OssRepository.getSetting(OssClientSettings.ACCESS_KEY_ID, repositoryMetaData);
        SecureString secretAccessKey =
            OssRepository.getSetting(OssClientSettings.SECRET_ACCESS_KEY, repositoryMetaData);
        String endpoint = OssRepository.getSetting(OssClientSettings.ENDPOINT, repositoryMetaData);
        return new OSSClientBuilder().build(endpoint,accessKeyId.toString(),secretAccessKey.toString(),
                extractClientConfiguration(repositoryMetaData));
    }

    private OSS createAKStsTokenClient(RepositoryMetaData repositoryMetaData) {
        SecureString securityToken = OssClientSettings.SECURITY_TOKEN.get(repositoryMetaData.settings());
        SecureString accessKeyId =
            OssRepository.getSetting(OssClientSettings.ACCESS_KEY_ID, repositoryMetaData);
        SecureString secretAccessKey =
            OssRepository.getSetting(OssClientSettings.SECRET_ACCESS_KEY, repositoryMetaData);
        String endpoint = OssRepository.getSetting(OssClientSettings.ENDPOINT, repositoryMetaData);
        return new OSSClientBuilder().build(endpoint, accessKeyId.toString(), secretAccessKey.toString(),
            securityToken.toString(), extractClientConfiguration(repositoryMetaData));
    }

    private synchronized OSS createStsOssClient(RepositoryMetaData repositoryMetaData)
        throws CreateStsOssClientException {
        if (isStsTokenExpired() || isTokenWillExpired()) {
            try {
                if (null == repositoryMetaData) {
                    throw new IOException("repositoryMetaData is null");
                }
                String ecsRamRole = OssClientSettings.ECS_RAM_ROLE.get(repositoryMetaData.settings()).toString();
                String endpoint = OssRepository.getSetting(OssClientSettings.ENDPOINT, repositoryMetaData);

                String fullECSMetaDataServiceUrl = ECS_METADATA_SERVICE + ecsRamRole;
                Response response = HttpClientHelper.httpRequest(fullECSMetaDataServiceUrl);
                if (!response.isSuccessful()) {
                    throw new IOException("ECS meta service server error");
                }
                String jsonStringResponse = Objects.isNull(response.body()) ? StringUtils.EMPTY : response.body().string();
                JSONObject jsonObjectResponse = JSON.parseObject(jsonStringResponse);
                String accessKeyId = jsonObjectResponse.getString(ACCESS_KEY_ID);
                String accessKeySecret = jsonObjectResponse.getString(ACCESS_KEY_SECRET);
                String securityToken = jsonObjectResponse.getString(SECURITY_TOKEN);
                stsTokenExpiration = DateHelper.convertStringToDate(jsonObjectResponse.getString(EXPIRATION));
                try {
                    readWriteLock.writeLock().lock();
                    if (null != this.client) {
                        this.client.shutdown();
                    }
                    this.client = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret, securityToken,
                        extractClientConfiguration(repositoryMetaData));
                } finally {
                    readWriteLock.writeLock().unlock();
                }
                response.close();
            } catch (IOException e) {
                logger.error("create stsOssClient exception", e);
                throw new CreateStsOssClientException(e);
            }
            return this.client;
        } else {
            return this.client;
        }
    }
}
