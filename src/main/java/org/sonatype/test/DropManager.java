package org.sonatype.test;

import java.util.List;

import com.google.common.collect.Lists;
import com.orientechnologies.orient.core.collate.OCaseInsensitiveCollate;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexDefinitionFactory;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class DropManager
{
  private static final String TYPE_NAME = "drop";

  private static final String NAME = "name";

  public static final String BUCKET = "bucket";

  public static final String GROUP = "group";

  public static final String VERSION = "version";

  private final ODatabaseDocumentTx db;

  public DropManager(final ODatabaseDocumentTx db) {this.db = db;}

  void createType(OClass bucketClass) {
    OSchemaProxy oSchemaProxy = db.getMetadata().getSchema();
    if (oSchemaProxy.getClass(TYPE_NAME) == null) {
      OClass oClass = oSchemaProxy.createClass(TYPE_NAME);

      oClass.createProperty(NAME, OType.STRING)
          .setMandatory(true)
          .setNotNull(true);

      oClass.createProperty(BUCKET, OType.LINK, bucketClass)
          .setMandatory(true)
          .setNotNull(true);

      oClass.createProperty(GROUP, OType.STRING);

      oClass.createProperty(VERSION, OType.STRING);

      ODocument metadata = db.newInstance()
          .field("ignoreNullValues", false)
          .field("mergeKeys", false);

      //A few indexes we make like this
      oClass.createIndex("bucket_group_name_version", INDEX_TYPE.UNIQUE.name(), null, metadata,
          new String[]{BUCKET, GROUP, NAME, VERSION});

      oClass.createIndex("bucket_name_version", INDEX_TYPE.UNIQUE.name(), null, metadata,
          new String[]{BUCKET, NAME, VERSION});

      //We aren't using this index now, it doesn't seem to make a difference.
      //oClass.createIndex("bucket_idx", INDEX_TYPE.NOTUNIQUE.name(), null, metadata,
      //    new String[]{BUCKET});

      //a few other indexes we make like this
      List<String> propertiesForCaseInsensitiveIndex1 = singletonList(NAME);
      List<OType> propertyTypesForCaseInsensitiveIndex1 = singletonList(OType.STRING);
      List<OCollate> collatesForCaseInsensitiveIndex1 = Lists
          .transform(propertiesForCaseInsensitiveIndex1, n -> new OCaseInsensitiveCollate());

      OIndexDefinition indexDefinition1 = OIndexDefinitionFactory
          .createIndexDefinition(oClass, propertiesForCaseInsensitiveIndex1, propertyTypesForCaseInsensitiveIndex1,
              collatesForCaseInsensitiveIndex1, INDEX_TYPE.NOTUNIQUE.name(), null);

      db.getMetadata().getIndexManager().createIndex("name_case_insensitive", INDEX_TYPE.NOTUNIQUE.name(),
          indexDefinition1, oClass.getPolymorphicClusterIds(), null, null);

      List<String> propertiesForCaseInsensitiveIndex2 = asList(GROUP, NAME, VERSION);
      List<OType> propertyTypesForCaseInsensitiveIndex2 = asList(OType.STRING, OType.STRING, OType.STRING);
      List<OCollate> collatesForCaseInsensitiveIndex2 = Lists
          .transform(propertiesForCaseInsensitiveIndex2, n -> new OCaseInsensitiveCollate());

      OIndexDefinition indexDefinition2 = OIndexDefinitionFactory
          .createIndexDefinition(oClass, propertiesForCaseInsensitiveIndex2, propertyTypesForCaseInsensitiveIndex2,
              collatesForCaseInsensitiveIndex2, INDEX_TYPE.NOTUNIQUE.name(), null);

      db.getMetadata().getIndexManager().createIndex("group_name_version_case_insensitive", INDEX_TYPE.NOTUNIQUE.name(),
          indexDefinition2, oClass.getPolymorphicClusterIds(), null, null);

    }
  }
  
  ODocument createDrop(String group, String name, String version, ODocument bucket){
    ODatabaseRecordThreadLocal.INSTANCE.set(db);
    return db.newInstance(TYPE_NAME)
        .field(BUCKET, bucket.getIdentity())
        .field(GROUP, group)
        .field(NAME, name)
        .field(VERSION, version)
        .save();
  }
}
