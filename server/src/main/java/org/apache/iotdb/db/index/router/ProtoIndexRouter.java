package org.apache.iotdb.db.index.router;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.iotdb.db.engine.fileSystem.SystemFileFactory;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.index.IndexProcessor;
import org.apache.iotdb.db.index.common.IndexInfo;
import org.apache.iotdb.db.index.common.IndexType;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The index involve
 */
public class ProtoIndexRouter implements IIndexRouter {

  private static final Logger logger = LoggerFactory.getLogger(ProtoIndexRouter.class);

  /**
   * index series path -> index processor
   */
  private Map<String, Pair<IndexInfo, IndexProcessor>> fullPathProcessorMap;
  private Map<PartialPath, Pair<IndexInfo, IndexProcessor>> wildCardProcessorMap;
  private Map<String, List<String>> sgToIndexSeriesMap;
  private final String routerFilePath;
  private final MManager mManager;
  private final File routerFile;

  public ProtoIndexRouter(String routerFilePath) {
    this.routerFilePath = routerFilePath;
    fullPathProcessorMap = new ConcurrentHashMap<>();
    sgToIndexSeriesMap = new ConcurrentHashMap<>();
    routerFile = SystemFileFactory.INSTANCE.getFile(routerFilePath);
    mManager = MManager.getInstance();
  }

  public void serialize() {
    try (ObjectOutputStream routerOutputStream = new ObjectOutputStream(
        new FileOutputStream(routerFile, false))) {
      routerOutputStream.writeObject(this);
    } catch (IOException e) {
      logger.error("Error when serialize router. Given up.", e);
    }
  }

  public void deserialize() {
    try (ObjectInputStream routerInputStream = new ObjectInputStream(
        new FileInputStream(routerFile))) {
      ProtoIndexRouter p = (ProtoIndexRouter) routerInputStream.readObject();
      this.fullPathProcessorMap = p.fullPathProcessorMap;
      this.sgToIndexSeriesMap = p.sgToIndexSeriesMap;
    } catch (IOException | ClassNotFoundException e) {
      logger.error("Error when deserialize router. Given up.", e);
    }
  }


  @Override
  public Iterable<IndexProcessor> getIndexProcessorByPath(PartialPath path) {
    List<IndexProcessor> res = new ArrayList<>();
    if (fullPathProcessorMap.containsKey(path.getFullPath())) {
      res.add(fullPathProcessorMap.get(path.getFullPath()).right);
    }
    else{
      wildCardProcessorMap.forEach((k, v) -> {
        if (k.matchFullPath(path)){
          res.add(fullPathProcessorMap.get(path.getFullPath()).right);
        }
      });
    }
    return res;
  }

  @Override
  public boolean hasIndexProcessor(PartialPath indexSeriesPath) {
    if(fullPathProcessorMap.containsKey(indexSeriesPath.getFullPath()))
      return true;
    for (Entry<PartialPath, Pair<IndexInfo, IndexProcessor>> entry : wildCardProcessorMap
        .entrySet()) {
      PartialPath k = entry.getKey();
      Pair<IndexInfo, IndexProcessor> v = entry.getValue();
      if (k.matchFullPath(indexSeriesPath)) {
        return true;
      }
    }
    return false;
  }


  @Override
  public List<IndexProcessor> getIndexProcessorByStorageGroup(String storageGroupPath) {
    return null;
  }

  @Override
  public void removeIndexProcessorByStorageGroup(String storageGroupPath) {

  }

  @Override
  public synchronized boolean addIndexIntoRouter(PartialPath partialPath, IndexInfo indexInfo)
      throws MetadataException {
    // record the relationship between storage groups and ?
    StorageGroupMNode storageGroupMNode = mManager.getStorageGroupNodeByPath(partialPath);
    String storageGroupPath = storageGroupMNode.getPartialPath().getFullPath();
    List<String> list = new ArrayList<>();
    List<String> preList = sgToIndexSeriesMap.putIfAbsent(storageGroupPath, list);
    if (preList != null) {
      list = preList;
    }
    list.add(partialPath.getFullPath());

    return true;
  }

  @Override
  public synchronized boolean removeIndexFromRouter(PartialPath prefixPaths, IndexType indexType)
      throws MetadataException {
    return false;
  }

  @Override
  public Map<String, IndexProcessor> getProcessorsByStorageGroup(String storageGroup) {
    return null;
  }

  @Override
  public Iterable<IndexProcessor> getAllIndexProcessors() {
    return null;
  }

}
