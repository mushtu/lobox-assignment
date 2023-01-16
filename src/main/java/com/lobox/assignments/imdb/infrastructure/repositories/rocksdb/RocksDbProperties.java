package com.lobox.assignments.imdb.infrastructure.repositories.rocksdb;

import org.rocksdb.CompactionStyle;
import org.rocksdb.util.SizeUnit;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "rocksdb")
public class RocksDbProperties {
    private String dataDir;
    private boolean createIfMissing = true;
    private boolean createMissingColumnFamilies = true;
    private long writeBufferSize = 512 * SizeUnit.MB;
    private int maxWriteBufferNumber = 5;
    private long blockSize = 32 * SizeUnit.KB;
    private long blockCacheSize = 64 * SizeUnit.MB;
    private boolean cacheIndexAndFilterBlocks = true;
    private long targetFileSizeBase = 64 * SizeUnit.MB;
    private CompactionStyle compactionStyle = CompactionStyle.LEVEL;
    private int levelZeroFileNumCompactionTrigger = 4;
    private int levelZeroSlowdownWritesTrigger = 20;
    private int levelZeroStopWritesTrigger = 30;
    private int numLevels = 4;
    private long maxBytesForLevelBase = 64 * 4 * SizeUnit.MB;
    private int maxOpenFiles = -1;
    private int minWriteBufferNumberToMerge = 2;

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public boolean isCreateIfMissing() {
        return createIfMissing;
    }

    public void setCreateIfMissing(boolean createIfMissing) {
        this.createIfMissing = createIfMissing;
    }

    public long getWriteBufferSize() {
        return writeBufferSize;
    }

    public void setWriteBufferSize(long writeBufferSize) {
        this.writeBufferSize = writeBufferSize;
    }

    public int getMaxWriteBufferNumber() {
        return maxWriteBufferNumber;
    }

    public void setMaxWriteBufferNumber(int maxWriteBufferNumber) {
        this.maxWriteBufferNumber = maxWriteBufferNumber;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
    }

    public long getBlockCacheSize() {
        return blockCacheSize;
    }

    public void setBlockCacheSize(long blockCacheSize) {
        this.blockCacheSize = blockCacheSize;
    }

    public boolean isCacheIndexAndFilterBlocks() {
        return cacheIndexAndFilterBlocks;
    }

    public void setCacheIndexAndFilterBlocks(boolean cacheIndexAndFilterBlocks) {
        this.cacheIndexAndFilterBlocks = cacheIndexAndFilterBlocks;
    }

    public long getTargetFileSizeBase() {
        return targetFileSizeBase;
    }

    public void setTargetFileSizeBase(long targetFileSizeBase) {
        this.targetFileSizeBase = targetFileSizeBase;
    }

    public CompactionStyle getCompactionStyle() {
        return compactionStyle;
    }

    public void setCompactionStyle(CompactionStyle compactionStyle) {
        this.compactionStyle = compactionStyle;
    }

    public int getLevelZeroFileNumCompactionTrigger() {
        return levelZeroFileNumCompactionTrigger;
    }

    public void setLevelZeroFileNumCompactionTrigger(int levelZeroFileNumCompactionTrigger) {
        this.levelZeroFileNumCompactionTrigger = levelZeroFileNumCompactionTrigger;
    }

    public int getLevelZeroSlowdownWritesTrigger() {
        return levelZeroSlowdownWritesTrigger;
    }

    public void setLevelZeroSlowdownWritesTrigger(int levelZeroSlowdownWritesTrigger) {
        this.levelZeroSlowdownWritesTrigger = levelZeroSlowdownWritesTrigger;
    }

    public int getLevelZeroStopWritesTrigger() {
        return levelZeroStopWritesTrigger;
    }

    public void setLevelZeroStopWritesTrigger(int levelZeroStopWritesTrigger) {
        this.levelZeroStopWritesTrigger = levelZeroStopWritesTrigger;
    }

    public int getNumLevels() {
        return numLevels;
    }

    public void setNumLevels(int numLevels) {
        this.numLevels = numLevels;
    }

    public long getMaxBytesForLevelBase() {
        return maxBytesForLevelBase;
    }

    public void setMaxBytesForLevelBase(long maxBytesForLevelBase) {
        this.maxBytesForLevelBase = maxBytesForLevelBase;
    }

    public boolean isCreateMissingColumnFamilies() {
        return createMissingColumnFamilies;
    }

    public void setCreateMissingColumnFamilies(boolean createMissingColumnFamilies) {
        this.createMissingColumnFamilies = createMissingColumnFamilies;
    }

    public int getMaxOpenFiles() {
        return maxOpenFiles;
    }

    public void setMaxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
    }

    public int getMinWriteBufferNumberToMerge() {
        return minWriteBufferNumberToMerge;
    }

    public void setMinWriteBufferNumberToMerge(int minWriteBufferNumberToMerge) {
        this.minWriteBufferNumberToMerge = minWriteBufferNumberToMerge;
    }
}
