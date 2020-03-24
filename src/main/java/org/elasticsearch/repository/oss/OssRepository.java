package org.elasticsearch.repository.oss;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.aliyun.oss.blobstore.OssBlobContainer;
import org.elasticsearch.aliyun.oss.blobstore.OssBlobStore;
import org.elasticsearch.aliyun.oss.service.OssClientSettings;
import org.elasticsearch.aliyun.oss.service.OssService;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * An oss repository working for snapshot and restore.
 * Implementations are responsible for reading and writing both metadata and shard data to and from
 * a repository backend.
 * Created by yangkongshi on 2017/11/24.
 */
public class OssRepository extends BlobStoreRepository {

    private static final Logger logger = LogManager.getLogger(OssBlobContainer.class);

    public static final String TYPE = "oss";
    private final BlobPath basePath;
    private final ByteSizeValue chunkSize;
    private final String bucket;
    private final OssService ossService;

    public OssRepository(RepositoryMetaData metadata, Environment environment,
                         NamedXContentRegistry namedXContentRegistry, ThreadPool threadPool, OssService ossService) {
        super(metadata, getSetting(OssClientSettings.COMPRESS, metadata), namedXContentRegistry, threadPool);
        this.ossService = ossService;
        String ecsRamRole = OssClientSettings.ECS_RAM_ROLE.get(metadata.settings()).toString();
        if (StringUtils.isNotEmpty(ecsRamRole)) {
            this.bucket = getSetting(OssClientSettings.AUTO_SNAPSHOT_BUCKET, metadata).toString();
        } else {
            this.bucket = getSetting(OssClientSettings.BUCKET, metadata);
        }
        String basePath = OssClientSettings.BASE_PATH.get(metadata.settings());
        if (StringUtils.containsAny(basePath, "\\", ":", "*", "?", "\"", "<", ">", "|")) {
            throw new IllegalArgumentException("base path has invalid char,check please.(\\ : * ? \" < > | )");
        }
        if (Strings.hasLength(basePath)) {
            BlobPath path = new BlobPath();
            for (String elem : StringUtils.splitPreserveAllTokens(basePath, "/")) {
                if (StringUtils.isBlank(elem)) continue;
                path = path.add(elem);
            }
            this.basePath = path;
        } else {
            this.basePath = BlobPath.cleanPath();
        }
        this.chunkSize = getSetting(OssClientSettings.CHUNK_SIZE, metadata);
        logger.info("Using base_path [{}], chunk_size [{}]",
            basePath, chunkSize);
    }
    protected BlobStore createBlobStore() {
        return new OssBlobStore(bucket, ossService);
    }

    @Override
    public BlobPath basePath() {
        return this.basePath;
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    public static <T> T getSetting(Setting<T> setting, RepositoryMetaData metadata) {
        T value = setting.get(metadata.settings());
        if (value == null) {
            throw new RepositoryException(metadata.name(),
                "Setting [" + setting.getKey() + "] is not defined for repository");
        }
        if ((value instanceof String) && (Strings.hasText((String)value)) == false) {
            throw new RepositoryException(metadata.name(),
                "Setting [" + setting.getKey() + "] is empty for repository");
        }
        return value;
    }

    /** mainly for test **/
    public OssService getOssService() {
        return ossService;
    }
}
