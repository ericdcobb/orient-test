package org.sonatype.test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;


class BucketManager
{
  private static final String TYPE_NAME = "bucket";

  private static final String NAME = "name";

  private final ODatabaseDocumentTx db;

  private OClass bucketClass;

  public OClass getBucketClass() {
    return bucketClass;
  }

  public void setBucketClass(final OClass bucketClass) {
    this.bucketClass = bucketClass;
  }

  BucketManager(final ODatabaseDocumentTx db) {this.db = db;}

  void createType() {
    OSchemaProxy oSchemaProxy = db.getMetadata().getSchema();
    OClass found = oSchemaProxy.getClass(TYPE_NAME);
    if (found == null) {
      OClass oClass = oSchemaProxy.createClass(TYPE_NAME);
      oClass.createProperty(NAME, OType.STRING)
          .setMandatory(true)
          .setNotNull(true);
      this.setBucketClass(oClass);
    }
    else {
      this.setBucketClass(found);
    }
  }

  ODocument createBucket(String bucketName) {
    return db.newInstance(TYPE_NAME)
        .field(NAME, bucketName)
        .save();
  }

}
