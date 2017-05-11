package org.sonatype.test;


import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

public class OrientTest
{
  public static final Integer itemsToCreate = 100000;

  private static final org.apache.log4j.Logger logger = Logger.getLogger(OrientTest.class.getName());

  public static void main(String[] args)
      throws Exception
  {
    Options options = new Options();
    options.addOption(new Option("test", false, "run test"));

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(options, args);

    logger.info("starting test");
    OServer oServer = OServerMain.create();
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("remote:localhost:2424/test");

    try {
      //start the server
      oServer.startup(OrientTest.class.getResourceAsStream("/db.config"));
      oServer.activate();

      //create the database if it does not exist
      OServerAdmin serverAdmin = new OServerAdmin("remote:localhost").connect("root", "root");
      if (!serverAdmin.existsDatabase("test", "plocal")) {
        logger.info("Creating database");
        serverAdmin.createDatabase("test", "document", "plocal");
      }
      serverAdmin.close();

      db.open("root", "root");

      if (cmd.hasOption("test")) {
        runTest(db);
      }
      else {
        logger.info("Not running test (start with arg 'test' to run) but starting orient server.");
      }
    }
    finally {
      db.close();
    }
  }

  private static void runTest(final ODatabaseDocumentTx db) {
    //The 'bucket'
    BucketManager bucketManager = new BucketManager(db);
    bucketManager.createType();

    ODocument bucket = bucketManager.createBucket(UUID.randomUUID().toString());

    logger.info("Created bucket " + bucket.getIdentity());

    //Drops
    DropManager dropManager = new DropManager(db);
    dropManager.createType(bucketManager.getBucketClass());

    String group = UUID.randomUUID().toString();

    IntStream.range(0, itemsToCreate).parallel()
        .forEach(i -> {
          String name = "test-" + i;
          logger.info("Created: " + name);
          dropManager.createDrop(group, name, Integer.toString(i), bucket);
        });

    logger.info("Created " + itemsToCreate + " items, querying");

    List<ODocument> docs = db.query(new OSQLSynchQuery<>(
            "SELECT FROM drop WHERE (bucket = :bucket) ORDER BY group ASC, name ASC, version ASC SKIP 0 limit 300"),
        bucket.getIdentity());

    logger.info("Query Success. " + docs.size() + " items found.");
  }
}
