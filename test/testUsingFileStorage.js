var MongoFileHandler = require("./../lib/gridFileManager.js").MongoFileHandler;
var mongoDb = require("mongodb");
var Server = mongoDb.Server;
var Db = mongoDb.Db;
var fs = require("fs");
var assert = require("assert");
var lodash = require("lodash");
var GridStore = mongoDb.GridStore;
var async = require('async');
var archiver = require('archiver');
var rimraf = require("rimraf");

var databaseDev, databaseLive;


var testFilePath = "./test/Fixtures/testFiles/";
var testOutputFilesPath = "./test/Fixtures/testOutput/";

var LOGLEVEL_ERROR = 0;
var LOGLEVEL_WARNING = 1;
var LOGLEVEL_INFO = 2;
var LOGLEVEL_DEBUG = 3;

var loglevel = LOGLEVEL_DEBUG;

var logger = {
  info: (loglevel >= LOGLEVEL_INFO)?function (msg) {
    console.log("INFO   ", msg);
  }:function(){},
  debug: (loglevel >= LOGLEVEL_DEBUG)?function (msg) {
    console.log("DEBUG  ", msg);
  }:function(){},
  warning: (loglevel >= LOGLEVEL_WARNING)?function (msg) {
    console.log("WARNING", msg);
  }:function(){},
  error: (loglevel >= LOGLEVEL_ERROR)?function (msg) {
    console.log("ERROR  ", msg);
  }:function(){}
};

var config = {"fileStorage":
  {
    "LOG_LEVEL": 0,
    "THUMBNAIL_PREPEND":"thumbnail_",
    "DEFAULT_THUMBNAIL_WIDTH": 200,
    "DEFAULT_THUMBNAIL_HEIGHT":200,
    "MAX_MONGO_QUEUE_LENGTH": 1000,
    "ROOT_COLLECTION": "testingCollection"
  }
}

var fileHandler = new MongoFileHandler(config);

////////////////// HANDY FUNCTIONS FOR TESTING ///////////////////////////////

var checkFileDetails = function(database, groupId, name, version, contentType, thumbnail, cb){
  logger.debug("In checkFileDetails");
  //The fileId returned should exist in the database
  database.collection(config.fileStorage.ROOT_COLLECTION + ".files", function(err, collection){
    assert.ok(!err);

    collection.find({"metadata.groupId": groupId, "metadata.version": version}, function(err, data){
      assert.ok(!err);
      assert.ok(data);

      data.toArray(function(err, dataArray){
        assert.ok(!err);

        //The array should only be size 1
        assert.ok(dataArray.length == 1);

        //The file entry should be version 1 and set to application/pdf
        var fileEntry = dataArray[0];
        assert.equal(fileEntry.filename, name);
        assert.equal(fileEntry.contentType, contentType);
        assert.ok(fileEntry.metadata);
        assert.equal(fileEntry.metadata.version, version);
        assert.ok(lodash.isEqual(fileEntry.metadata.groupId, groupId));
        //If a thumbnail is expected, check that the id actually exists
        if(thumbnail){
          assert.ok(fileEntry.metadata.thumbnail);
        }
        return cb();
      });
    });
  });
}

var checkFileData = function(database, groupId, version, name, thumbnail, cb){
  logger.debug("In checkFileData");
  //Need to load the file from disk and database, convert to base64 string and compare

  //Need to get the fileId first,

  database.collection(config.fileStorage.ROOT_COLLECTION + ".files", function(err, collection){
    assert.ok(!err);

    collection.find({"metadata.groupId": groupId, "metadata.version": version}, function(err, data){
      assert.ok(!err);
      assert.ok(data);

      data.toArray(function(err, dataArray){
        assert.ok(!err);


        //The array should only be size 1
        assert.ok(dataArray.length == 1);

        //The file entry should be version 1 and set to application/pdf
        var fileEntry = dataArray[0];

        var id = fileEntry._id;

        var gridStore = new GridStore(database, id, "r", {"root": config.fileStorage.ROOT_COLLECTION});

        gridStore.open(function(err, fileDetails){
          assert.ok(!err);

          gridStore.read(function(err, fileData) {
            assert.ok(!err);


            var filePath = testFilePath + name;

            if(name.indexOf("Base64") > -1){
              filePath = testFilePath + "testBase64";
            }

            var fileDiskData = fs.readFileSync(filePath);
            var fileSize = fs.statSync(filePath).size;




            if(name.indexOf("Base64") > -1){
              assert.ok(fileData.toString('base64') === fileDiskData.toString(),"Error file data compare");
              assert.equal(fileData.toString('base64').length, fileSize);
            } else {
              assert.ok(fileData.toString('base64') === fileDiskData.toString('base64'),"Error file data (base64) compare");
              assert.equal(fileData.length, fileSize);
            }



            if(thumbnail){
              assert.ok(fileDetails.metadata.thumbnail);

              var thumbGridStore = new GridStore(database, fileDetails.metadata.thumbnail, "r", {"root": config.fileStorage.ROOT_COLLECTION});

              thumbGridStore.open(function(err, thumbFileDetails){
                assert.ok(!err);
                assert.ok(thumbFileDetails);

                thumbGridStore.read(function(err, thumbnailFileData) {
                  assert.ok(!err);
                  assert.ok(thumbnailFileData.length > 0);
                  return cb();
                });
              });
            }else{
              return cb();
            }
          });
        });
      });
    });
  });
}

var compareFiles = function(filePath1, filePath2, cb){
  logger.debug("In compareFiles");
  var fileDiskData1 = fs.readFileSync(filePath1);
  var fileSize1 = fs.statSync(filePath1).size;

  var fileDiskData2 = fs.readFileSync(filePath2);
  var fileSize2 = fs.statSync(filePath2).size;

  assert.equal(fileDiskData1.toString('base64'), fileDiskData2.toString('base64'));
  assert.equal(fileSize1, fileSize2);

  cb();

}

var checkFile = function(database, groupId, name, version, contentType, thumbnail, cb){
  logger.debug("In checkFile");
  async.series([
    async.apply(checkFileDetails, database, groupId, name, version, contentType, thumbnail),
    async.apply(checkFileData, database, groupId, version, name, thumbnail)
  ], function(err){
    assert.ok(!err,"Error checkfile function");

    return cb(err);
  });
}

var saveFile = function (database, type, options, cb){
  logger.debug("In saveFile");
  var fileName = "test." + type;
  if(options.testVersion){
    fileName = "test" + options.testVersion + "." + type;
  }

  if(options.decodeBase64){
    fileName = "testBase64." + type;
  }

  var filePath = testFilePath + fileName;

  if(options.decodeBase64){
    filePath = testFilePath + "testBase64";
  }

  var fileStream = fs.createReadStream(filePath);
  fileHandler.saveFile(database, fileName, fileStream, options, function(err, saveResult){
    assert.ok(!err);
    assert.ok(saveResult);
    cb(undefined, saveResult);
  });
}

var saveTestJpg = function(database, options, cb){
  logger.debug("In saveTestJpg");
  saveFile(database, "jpg", options, cb);
}

var saveTestPng = function(database, options, cb){
  logger.debug("In saveTestPng");
  saveFile(database, "png", options, cb);
}

var saveTestGif = function(database, options, cb){
  logger.debug("In saveTestGif");
  saveFile(database, "gif", options, cb);
}

var saveTestPdf = function(database, options, cb){
  logger.debug("In saveTestPdf");
  saveFile(database, "pdf", options, cb);
}

var saveTestPpt = function(database, options, cb){
  logger.debug("In saveTestPpt");
  saveFile(database, "ppt", options, cb);
}

var saveTestZip = function(database, options, cb){
  logger.debug("In saveTestZip");
  saveFile(database, "zip", options, cb);
}

var saveTestDmg = function(database, options, cb){
  logger.debug("In saveTestGif");
  saveFile(database, "dmg", options, cb);
}

var saveTestDocx = function(database, options, cb){
  logger.debug("In saveTestDocx");
  saveFile(database, "docx", options, cb);
}

var saveTestHtml = function(database, options, cb){
  logger.debug("In saveTestHtml");
  saveFile(database, "html", options, cb);
}

var saveTestMp4 = function(database, options, cb){
  logger.debug("In saveTestMp4");
  saveFile(database, "mp4", options, cb);
}

var saveTestXml = function(database, options, cb){
  logger.debug("In saveTestXml");
  saveFile(database, "xml", options, cb);
}

var saveBase64Jpg = function(database, options, cb){
  logger.debug("In saveTestBase64Jpg");
  options.decodeBase64 = true;
  saveFile(database, "jpg", options, cb);
}

var cleanDatabase = function(db, cb) {
  logger.debug("In cleanDatabase");
  db.listCollections().toArray( function(err, collectionList) {
    assert.ok(!err);
    async.eachSeries(collectionList, function(collectionName, cb) {
      db.collection(collectionName.name, {"strict": true}, function(err, collection) {
        assert.ok(!err);
        if(collectionName.name.indexOf("system.") == -1) {
          collection.drop(cb);
        } else {
          cb();
        }
      });
    }, function(err) {
      assert.ok(!err);
      return cb(err);
    });
  });
}

var getDatabase = function(dbName, cb) {
  logger.debug("In getDatabase");
  var server = new Server(process.env.MONGODB_HOST || 'localhost', 27017);
  var database = new Db(dbName, server, {w:1});
  database.open(function(err, db){
    assert.ok(!err);
    db.authenticate("admin", "admin", {"authSource":"admin"}, function(err, result){
      assert.ok(!err);
      return cb(err, db);
    });
  });
}


var setUpDatabases = function(cb){
  logger.debug("In setUpDatabases");
  getDatabase("testFileStorageDev", function(err, dbDev){
    assert.ok(!err);
    getDatabase("testFileStorageLive", function(err, dbLive){
      assert.ok(!err);

      return cb(err, dbDev, dbLive);
    });
  });
}

var deleteFile = function(database, groupId, version, cb){
  logger.debug("In deleteFile");
  //FileExists, now delete it.
  fileHandler.deleteFile(database, {"groupId": groupId, "version": version}, function(err, result){
    logger.debug(err);
    assert.ok(!err);

    //File deleted, how check if it exists.
    verifyFileDeletion(database, groupId, version, cb);
  });
}

var setGroupIdInLiveToDev = function(database, fromGroupId, toGroupId, cb){
  database.collection(config.fileStorage.ROOT_COLLECTION + ".files", {"strict": true}, function(err, collection){
    assert.ok(!err);

    collection.update({"metadata.groupId": toGroupId}, {"$set": {"metadata.groupId": fromGroupId}}, function(err, results){
      assert.ok(!err);

      cb(err);
    });
  });
}



var streamFileTest = function(database, searchOptions, fileOptions, fileName, cb){
  logger.debug("In streamFileTest");
  fileHandler.streamFile(database, searchOptions, fileOptions, function(err, fileStream, fileDetails){
    assert.ok(!err);


    if(searchOptions.thumbnail){
      fileName = "thumbnail_" + fileName;
    }

    var outputFileName = "output" + fileName;



    var outputFullPath = testOutputFilesPath + outputFileName;

    var testWriteStream = fs.createWriteStream(outputFullPath);

    testWriteStream.on("error", function(err){
      assert.fail(err);
    });

    testWriteStream.on("close", function(){
      compareFiles(testFilePath + fileName, outputFullPath, function(err){
        assert.ok(!err);

        logger.debug("Finished streamFileTest");
        cb();
      });
    });

    fileStream.pipe(testWriteStream);
    fileStream.resume();
  });
}

var verifyFileDeletion = function(database, groupId, version, cb){
  logger.debug("In verifyFileDeletion");
  database.collection(config.fileStorage.ROOT_COLLECTION + ".files", function(err, collection){
    assert.ok(!err);

    //looking for groupId as the file identifier.
    collection.find({"metadata.groupId": groupId, "metadata.version":version}, {}, function(err, results){
      assert.ok(!err);

      results.toArray(function(err, resultsArray){
        assert.ok(!err);

        //Should be no entries for this file name.
        assert.ok(resultsArray.length == 0);
        logger.debug("Finished verifyFileDeletion");
        cb();
      });
    });
  });
}


////////////////// ACTUAL TESTING FUNCTIONS ///////////////////////////////


exports.beforeEach = function(done){
  logger.info("ARGS: " + arguments.length);
  logger.info("setUp Called");

  setUpDatabases(function(err, dbDev, dbLive) {
    assert.ok(!err);
    assert.ok(dbDev);
    assert.ok(dbLive);

    databaseDev = dbDev;
    databaseLive = dbLive;
    done();
  });
}

exports.testSaveBase64Decoded = function(done){
  logger.info("In testSaveBase64Decoded");

  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);
    saveBase64Jpg(databaseDev, {}, function(err, saveResult){
      assert.ok(!err);
      assert.ok(saveResult);

      //Fields to be returned when a file is saved.
      assert.ok(saveResult.fileName);
      assert.ok(saveResult.contentType);
      assert.ok(saveResult.version);
      assert.ok(saveResult.hash);
      assert.ok(saveResult.groupId);
      assert.ok(saveResult._id);

      checkFile(databaseDev, saveResult.groupId, "testBase64.jpg", 1, "image/jpeg", undefined, function(err){
        assert.ok(!err);

        done();
      });
    });
  });
}

exports.testSaveSingleFile = function(done){
  logger.info("In testSaveSingleFile");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    saveTestPdf(databaseDev, {}, function(err, saveResult){
      assert.ok(!err);
      assert.ok(saveResult);

      //Fields to be returned when a file is saved.
      assert.ok(saveResult.fileName);
      assert.ok(saveResult.contentType);
      assert.ok(saveResult.version);
      assert.ok(saveResult.hash);
      assert.ok(saveResult.groupId);
      assert.ok(saveResult._id);

      assert.equal(saveResult.fileName, "test.pdf");
      assert.equal(saveResult.contentType, "application/pdf");
      assert.equal(saveResult.version, 1);
      assert.equal(saveResult.groupId, saveResult._id);

      checkFile(databaseDev, saveResult.groupId, "test.pdf", 1, "application/pdf", undefined, function(err){
        assert.ok(!err);

        logger.debug("Finished testSaveSingleFile");
        done();
      });
    });
  });
}

exports.testSaveMultileFiles = function(done){
  logger.info("In testSaveMultileFiles");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    //These streams are being saved at the same time and they should all succeed.
    async.parallel({
      "pdf": async.apply(saveTestPdf, databaseDev, {}),
      "ppt": async.apply(saveTestPpt, databaseDev, {}),
      "zip": async.apply(saveTestZip, databaseDev, {})
    }, function(err, saveResults){
      assert.ok(!err);
      assert.ok(saveResults);

      //For each of the results, check the files for consistency
      async.series([
        async.apply(checkFile, databaseDev, saveResults.pdf.groupId, "test.pdf", 1, "application/pdf", undefined),
        async.apply(checkFile, databaseDev, saveResults.ppt.groupId, "test.ppt", 1, "application/vnd.ms-powerpoint", undefined),
        async.apply(checkFile, databaseDev, saveResults.zip.groupId, "test.zip", 1, "application/zip", undefined)
      ], function(err){
        assert.ok(!err);
        logger.debug("Finished testSaveMultileFiles");
        done();
      });
    });
  });
}

exports.testSaveSingleImage = function(done){
  logger.info("In testSaveSingleImage");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    saveTestJpg(databaseDev, {"thumbnail": {"width":200, "height":200}}, function(err, saveResult){
      assert.ok(!err);

      //Fields to be returned when a file is saved.
      assert.ok(saveResult.fileName);
      assert.ok(saveResult.contentType);
      assert.ok(saveResult.version);
      assert.ok(saveResult.hash);
      assert.ok(saveResult.groupId);
      assert.ok(saveResult._id);
      assert.ok(saveResult.thumbnailHash);
      assert.ok(saveResult.thumbnailFileName);

      assert.equal(saveResult.fileName, "test.jpg");
      assert.equal(saveResult.contentType, "image/jpeg");
      assert.equal(saveResult.version, 1);
      assert.equal(saveResult.groupId, saveResult._id);

      checkFile(databaseDev, saveResult.groupId, "test.jpg", 1, "image/jpeg", true, function(err){
        assert.ok(!err);
        logger.debug("Finished testSaveSingleImage");
        done();
      });
    });
  });
}

exports.testSaveMultipleImages = function(done){
  logger.info("In testSaveMultipleImages");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    //These streams are being saved at the same time and they should all succeed.
    async.parallel({
      "gif": async.apply(saveTestGif, databaseDev, {}),
      "png": async.apply(saveTestPng, databaseDev, {}),
      "jpg": async.apply(saveTestJpg, databaseDev, {})
    }, function(err, results){
      assert.ok(!err);
      assert.ok(results);

      //For each of the results, check the files for consistency
      async.series([
        async.apply(checkFile, databaseDev, results.gif.groupId, "test.gif", 1, "image/gif", undefined),
        async.apply(checkFile, databaseDev, results.png.groupId, "test.png", 1, "image/png", undefined),
        async.apply(checkFile, databaseDev, results.jpg.groupId, "test.jpg", 1, "image/jpeg", undefined)
      ], function(err){
        assert.ok(!err);
        logger.debug("Finished testSaveMultipleImages");
        done();
      });
    });
  });
}

//File versioning is based on the fileId parameter, not the file name.
//Therefore, saving two files with the same name will produce two different fileIds.
exports.testSaveFileSameName = function(done){
  logger.info("In testSaveFileSameName");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    async.series({

      "pdf1":async.apply(saveTestPdf, databaseDev, {}),
      "pdf2":async.apply(saveTestPdf, databaseDev, {})

    }, function(err, results){
      assert.ok(!err);

      async.series([
        async.apply(checkFile, databaseDev, results.pdf1.groupId, "test.pdf", 1, "application/pdf", undefined),
        async.apply(checkFile, databaseDev, results.pdf2.groupId, "test.pdf", 1, "application/pdf", undefined)// Two files with the same name can have the same version if they have different fileIds.
      ], function(err){
        assert.ok(!err);
        logger.debug("Finished saveTestMp4");
        done();
      });
    });
  });
}


exports.testDeleteSingleFile = function(done){
  logger.info("In testDeleteSingleFile");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    saveTestPdf(databaseDev, {}, function(err, saveResult){
      assert.ok(!err);
      assert.ok(saveResult);

      checkFile(databaseDev, saveResult.groupId, "test.pdf", 1, "application/pdf", undefined, function(err){
        assert.ok(!err);

        deleteFile(databaseDev, saveResult.groupId, 1, function(err){
          assert.ok(!err);
          done();
        });
      });
    });
  });
}

exports.testDeleteMultipleFiles = function(done){
  logger.info("In testDeleteMultipleFiles");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    async.series({
      "file1": async.apply(saveTestPdf, databaseDev, {}),
      "file2": async.apply(saveTestPdf, databaseDev, {})
  }, function(err, result){
      assert.ok(!err);

      async.series([
        async.apply(checkFile, databaseDev, result.file1.groupId, "test.pdf", 1, "application/pdf", undefined),
        async.apply(checkFile, databaseDev, result.file2.groupId, "test.pdf", 1, "application/pdf", undefined)
      ], function(err){
        assert.ok(!err);

        //Should be able to delete files in any order, at any time
        async.parallel([
          async.apply(deleteFile, databaseDev, result.file1.groupId, 1),
          async.apply(deleteFile, databaseDev, result.file2.groupId, 1)

        ], function(err){
          logger.debug("Finished testDeleteMultipleFiles");
          assert.ok(!err);
          done();
        });
      });
    });
  });
}

exports.testDeleteAllFileVersions = function(done){
  logger.info("In testDeleteAllFileVersions");

  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    saveTestPdf(databaseDev, {}, function(err, saveResult){
      assert.ok(!err);

      saveTestPdf(databaseDev, {"groupId": saveResult.groupId}, function(err, secondSaveResult){
        assert.ok(!err);

        async.series([
          async.apply(checkFile, databaseDev, saveResult.groupId, "test.pdf", 1, "application/pdf", undefined),
          async.apply(checkFile, databaseDev, secondSaveResult.groupId, "test.pdf", 2, "application/pdf", undefined)
        ],function(err){
          assert.ok(!err);

          fileHandler.deleteAllFileVersions(databaseDev, saveResult.groupId, function(err){
            assert.ok(!err);

            async.series([
              async.apply(verifyFileDeletion, databaseDev, saveResult.groupId, 1),
              async.apply(verifyFileDeletion, databaseDev, secondSaveResult.groupId, 2)
            ], function(err){
              assert.ok(!err);

              done();
            });
          });
        });
      });
    });
  });
}

exports.testStreamSingleFile = function(done){
  logger.info("In testStreamSingleFile");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    saveTestPdf(databaseDev, {}, function(err, saveResult){
      assert.ok(!err);

      checkFile(databaseDev, saveResult.groupId, "test.pdf", 1, "application/pdf", undefined, function(err){
        assert.ok(!err);

        streamFileTest(databaseDev, {"groupId": saveResult.groupId, "version": saveResult.version}, {}, "test.pdf", function(err){
          assert.ok(!err);
          done();
        });
      });
    });
  });
}

exports.testStreamSingleFileNoVersion = function(done){
  logger.info("In testStreamSingleFile");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    saveTestPdf(databaseDev, {}, function(err, saveResult){
      assert.ok(!err);

      saveTestPdf(databaseDev, {"groupId": saveResult.groupId}, function(err, saveResult){
        assert.ok(!err);

        checkFile(databaseDev, saveResult.groupId, "test.pdf", 1, "application/pdf", undefined, function(err){
          assert.ok(!err);

          checkFile(databaseDev, saveResult.groupId, "test.pdf", 2, "application/pdf", undefined, function(err){
            assert.ok(!err);

            streamFileTest(databaseDev, {"groupId": saveResult.groupId}, {}, "test.pdf", function(err){
              assert.ok(!err);
              done();
            });
          });
        });
      });
    });
  });
}

exports.testStreamSingleFileByHash = function(done){
  logger.info("In testStreamSingleFileByHash");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    saveTestPdf(databaseDev, {}, function(err, saveResult){
      assert.ok(!err);

      checkFile(databaseDev, saveResult.groupId, "test.pdf", 1, "application/pdf", undefined, function(err){
        assert.ok(!err);

        streamFileTest(databaseDev, {"hash": saveResult.hash}, {}, "test.pdf", function(err){
          assert.ok(!err);
          done();
        });
      });
    });
  });
}

exports.testStreamSingleImage = function(done){
  logger.info("In testStreamSingleFile");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    saveTestJpg(databaseDev, {"thumbnail": {"width":200, "height":200}}, function(err, saveResult){
      assert.ok(!err);

      checkFile(databaseDev, saveResult.groupId, "test.jpg", 1, "image/jpeg", undefined, function(err){
        assert.ok(!err);

        streamFileTest(databaseDev, {"groupId": saveResult.groupId, "version": saveResult.version}, {"thumbnail": true}, "test.jpg", function(err){
          assert.ok(!err);
          done();
        });
      });
    });
  });
}

exports.testStreamMultipleFiles = function(done){
  logger.info("In testStreamMultipleFiles");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    async.parallel({
      "file1": async.apply(saveTestPdf, databaseDev, {}),
      "file2": async.apply(saveTestZip, databaseDev, {})
    }, function(err, result){
      assert.ok(!err);


      async.series([
        async.apply(checkFile, databaseDev, result.file1.groupId, "test.pdf", 1, "application/pdf", undefined),
        async.apply(checkFile, databaseDev, result.file2.groupId, "test.zip", 1, "application/zip", undefined)
      ], function(err){
        assert.ok(!err);


        async.parallel([
          async.apply(streamFileTest, databaseDev, {"groupId": result.file1.groupId, "version": result.file1.version}, {},  "test.pdf"),
          async.apply(streamFileTest, databaseDev, {"groupId": result.file2.groupId, "version": result.file2.version}, {}, "test.zip")
        ], function(err){
          assert.ok(!err);
          logger.debug("Finished testStreamMultipleFiles");
          done();
        });
      });
    });
  });
}

exports.testGetFileHistory = function(done){
  logger.info("In testGetFileVersions");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    saveTestZip(databaseDev, {}, function(err, saveResult){
      assert.ok(!err);
      assert.ok(saveResult);


      async.series({
        "file2": async.apply(saveTestZip, databaseDev, {"groupId": saveResult.groupId}),
        "file3": async.apply(saveTestZip, databaseDev, {"groupId": saveResult.groupId}),
        "file4": async.apply(saveTestPdf, databaseDev, {"groupId": saveResult.groupId}),
        "file5": async.apply(saveTestZip, databaseDev, {"groupId": saveResult.groupId})
      }, function(err, result){
        assert.ok(!err);

        async.series([
          async.apply(checkFile, databaseDev, saveResult.groupId, "test.zip", 1, "application/zip", undefined),
          async.apply(checkFile, databaseDev, result.file2.groupId, "test.zip", 2, "application/zip", undefined),
          async.apply(checkFile, databaseDev, result.file3.groupId, "test.zip", 3, "application/zip", undefined),
          async.apply(checkFile, databaseDev, result.file4.groupId, "test.pdf", 4, "application/pdf", undefined),
          async.apply(checkFile, databaseDev, result.file5.groupId, "test.zip", 5, "application/zip", undefined)
        ], function(err){
          assert.ok(!err);

          fileHandler.getFileHistory(databaseDev, saveResult.groupId, function(err, fileHistory){
            assert.ok(!err);
            assert.ok(fileHistory);

            assert.ok(fileHistory.length == 5);
            //Testing the order of the versions.
            assert.equal(fileHistory[0].version, 5);
            assert.equal(fileHistory[1].version, 4);
            assert.equal(fileHistory[2].version, 3);
            assert.equal(fileHistory[3].version, 2);
            assert.equal(fileHistory[4].version, 1);

            assert.equal(fileHistory[0].fileName, "test.zip");
            assert.equal(fileHistory[1].fileName, "test.pdf");
            assert.equal(fileHistory[2].fileName, "test.zip");
            assert.equal(fileHistory[3].fileName, "test.zip");
            assert.equal(fileHistory[4].fileName, "test.zip");

            assert.equal(fileHistory[0].contentType, "application/zip");
            assert.equal(fileHistory[1].contentType, "application/pdf");
            assert.equal(fileHistory[2].contentType, "application/zip");
            assert.equal(fileHistory[3].contentType, "application/zip");
            assert.equal(fileHistory[4].contentType, "application/zip");

            assert.ok(lodash.isEqual(fileHistory[0].groupId, saveResult.groupId));
            assert.ok(lodash.isEqual(fileHistory[1].groupId, saveResult.groupId));
            assert.ok(lodash.isEqual(fileHistory[2].groupId, saveResult.groupId));
            assert.ok(lodash.isEqual(fileHistory[3].groupId, saveResult.groupId));
            assert.ok(lodash.isEqual(fileHistory[4].groupId, saveResult.groupId));

            logger.debug("Finished testGetFileVersions");
            done();
          });
        });
      });
    });
  });
}

exports.testMigrateSingleFile = function(done){
  logger.info("In testMigrateSingleFile");

  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    cleanDatabase(databaseLive, function(err){
      assert.ok(!err);

      saveTestZip(databaseDev, {}, function(err, saveResult){
        assert.ok(!err);

        fileHandler.migrateFiles(databaseDev, databaseLive, [{"groupId": saveResult.groupId, "version": saveResult.version}], function(err, migratedFiles){

          logger.debug(err, migratedFiles);
          assert.ok(!err);
          assert.ok(migratedFiles);
          assert.ok(migratedFiles[saveResult.groupId]);

          //Fields to be returned when a file is saved.
          assert.ok(migratedFiles[saveResult.groupId].fileName);
          assert.ok(migratedFiles[saveResult.groupId].contentType);
          assert.ok(migratedFiles[saveResult.groupId].version);
          assert.ok(migratedFiles[saveResult.groupId].hash);
          assert.ok(migratedFiles[saveResult.groupId].groupId);
          assert.ok(migratedFiles[saveResult.groupId]._id);

          assert.equal(migratedFiles[saveResult.groupId].fileName, "test.zip");
          assert.equal(migratedFiles[saveResult.groupId].contentType, "application/zip");
          assert.equal(migratedFiles[saveResult.groupId].version, 1);
          assert.equal(migratedFiles[saveResult.groupId].groupId, saveResult._id);

          //Files migrated, verify that the groupId correspond to the same file --
          checkFile(databaseDev, saveResult.groupId, "test.zip", 1, "application/zip", undefined, function(err){
            assert.ok(!err,"Error test migrate single file (db dev)");

            checkFile(databaseLive, saveResult.groupId, "test.zip", 1, "application/zip", undefined, function(err){// When files are migrated, they must keep the same groupdId as the file being copied to ensure consistency.
              assert.ok(!err,"Error test migrate single file (db live)");
              logger.debug("Finished testMigrateSingleFile");
              done();
            });
          });
        });
      });
    });
  });
}

exports.testMigrateSameGroupId = function(done){
  logger.info("In testMigrateSameGroupId");

  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    cleanDatabase(databaseLive, function(err){
      assert.ok(!err);

      saveTestZip(databaseDev, {}, function(err, saveResult){
        assert.ok(!err);

        fileHandler.migrateFiles(databaseDev, databaseLive, [{"groupId": saveResult.groupId, "version": saveResult.version}, {"groupId": saveResult.groupId, "version": saveResult.version}], function(err, migratedFiles){
          assert.ok(err);
          assert.equal("Duplicate groupId entries for groupId " + saveResult.groupId, err.message);

          done();
        });
      });
    });
  });
}

exports.testMigrateImageWithThumbnail = function(done){
  logger.info("In testMigrateImageWithThumbnail");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    cleanDatabase(databaseLive, function(err){
      assert.ok(!err);

      saveTestJpg(databaseDev, {"thumbnail": {"width":200, "height": 200}}, function(err, saveResult){
        assert.ok(!err);

        checkFile(databaseDev, saveResult.groupId, "test.jpg", 1, "image/jpeg", true, function(err){
          assert.ok(!err);

          fileHandler.migrateFiles(databaseDev, databaseLive, [{"groupId": saveResult.groupId, "version": saveResult.version}], function(err, migratedFiles){
            assert.ok(!err);
            assert.ok(migratedFiles);
            assert.ok(migratedFiles[saveResult.groupId]);

            //Fields to be returned when a file is saved.
            assert.ok(migratedFiles[saveResult.groupId].fileName);
            assert.ok(migratedFiles[saveResult.groupId].contentType);
            assert.ok(migratedFiles[saveResult.groupId].version);
            assert.ok(migratedFiles[saveResult.groupId].hash);
            assert.ok(migratedFiles[saveResult.groupId].groupId);
            assert.ok(migratedFiles[saveResult.groupId]._id);
            assert.ok(migratedFiles[saveResult.groupId].thumbnailHash);
            assert.ok(migratedFiles[saveResult.groupId].thumbnailFileName);

            assert.equal(migratedFiles[saveResult.groupId].fileName, "test.jpg");
            assert.equal(migratedFiles[saveResult.groupId].thumbnailFileName, "thumbnail_test.jpg");
            assert.equal(migratedFiles[saveResult.groupId].contentType, "image/jpeg");
            assert.equal(migratedFiles[saveResult.groupId].version, 1);
            assert.equal(migratedFiles[saveResult.groupId].groupId, saveResult._id);

            checkFile(databaseLive, saveResult.groupId, "test.jpg", 1, "image/jpeg", true, function(err){
              assert.ok(!err, "Error test migrate image with thumbnail");
              logger.debug("Finished testMigrateImageWithThumbnail");
              done();
            });
          });
        });
      });
    });
  });
}

exports.testMigrateMultipleFiles = function(done){
  logger.info("In testMigrateMultipleFiles");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    cleanDatabase(databaseLive, function(err){
      assert.ok(!err);

      async.series({
        "file1": async.apply(saveTestZip, databaseDev, {}),
        "file2": async.apply(saveTestPpt, databaseDev, {}),
        "file3": async.apply(saveTestXml, databaseDev, {}),
        "file4": async.apply(saveTestPdf, databaseDev, {}),
        "file5": async.apply(saveTestDocx, databaseDev, {}),
        "file6": async.apply(saveTestHtml, databaseDev, {})
      }, function(err, saveResults){
        assert.ok(!err);
        assert.ok(saveResults);

        fileHandler.migrateFiles(databaseDev, databaseLive, [{"groupId": saveResults.file1.groupId, "version": saveResults.file1.version}, {"groupId": saveResults.file2.groupId, "version": saveResults.file2.version}, {"groupId": saveResults.file3.groupId, "version": saveResults.file3.version}, {"groupId": saveResults.file4.groupId, "version": saveResults.file4.version}, {"groupId": saveResults.file5.groupId, "version": saveResults.file5.version}, {"groupId": saveResults.file6.groupId, "version": saveResults.file6.version}], function(err, migratedFiles){
          assert.ok(!err);

          async.series([
            async.apply(checkFile, databaseDev, saveResults.file1.groupId, "test.zip", 1, "application/zip", undefined),
            async.apply(checkFile, databaseDev, saveResults.file2.groupId, "test.ppt", 1, "application/vnd.ms-powerpoint", undefined),
            async.apply(checkFile, databaseDev, saveResults.file3.groupId, "test.xml", 1, "application/xml", undefined),
            async.apply(checkFile, databaseDev, saveResults.file4.groupId, "test.pdf", 1, "application/pdf", undefined),
            async.apply(checkFile, databaseDev, saveResults.file5.groupId, "test.docx", 1, "application/vnd.openxmlformats-officedocument.wordprocessingml.document", undefined),
            async.apply(checkFile, databaseDev, saveResults.file6.groupId, "test.html", 1, "text/html", undefined)
          ], function(err){
            assert.ok(!err);

            async.series([
              async.apply(checkFile, databaseLive, saveResults.file1.groupId, "test.zip", 1, "application/zip", undefined),
              async.apply(checkFile, databaseLive, saveResults.file2.groupId, "test.ppt", 1, "application/vnd.ms-powerpoint", undefined),
              async.apply(checkFile, databaseLive, saveResults.file3.groupId, "test.xml", 1, "application/xml", undefined),
              async.apply(checkFile, databaseLive, saveResults.file4.groupId, "test.pdf", 1, "application/pdf", undefined),
              async.apply(checkFile, databaseLive, saveResults.file5.groupId, "test.docx", 1, "application/vnd.openxmlformats-officedocument.wordprocessingml.document", undefined),
              async.apply(checkFile, databaseLive, saveResults.file6.groupId, "test.html", 1, "text/html", undefined)
            ], function(err){
              assert.ok(!err);

              logger.debug("Finished testMigrateMultipleFiles");
              done();
            });
          });
        });
      });
    });
  });
}


exports.testMigrateFileExists = function(done){
  logger.info("In testMigrateFileExists");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    cleanDatabase(databaseLive, function(err){
      assert.ok(!err);

      saveTestZip(databaseDev, {}, function(err, fromSaveResult){
        assert.ok(!err);

        saveTestZip(databaseLive, {}, function(err, toSaveResult){
          assert.ok(!err);


          setGroupIdInLiveToDev(databaseLive, fromSaveResult.groupId, toSaveResult.groupId, function(err){
            assert.ok(!err);

            checkFile(databaseLive, fromSaveResult.groupId, "test.zip", 1, "application/zip", undefined, function(err){
              assert.ok(!err);

              fileHandler.migrateFiles(databaseDev, databaseLive, [{"groupId": fromSaveResult.groupId, "version": 1}], function(err, migratedFiles){
                assert.ok(!err);

                logger.debug(migratedFiles);

                //Files migrated, verify that the fileIds correspond to the same file --
                checkFile(databaseDev, fromSaveResult.groupId, "test.zip", 1, "application/zip", undefined, function(err){
                  assert.ok(!err,"Error test migrate file exists (db dev)");

                  logger.debug("toFileId", fromSaveResult.groupId);

                  checkFile(databaseLive, fromSaveResult.groupId, "test.zip", 1, "application/zip", undefined, function(err){
                    assert.ok(!err,"Error test migrate file exists (db live)");

                    checkFile(databaseLive, fromSaveResult.groupId, "test.zip", 2, "application/zip", undefined, function(err){
                      assert.ok(!err,"Error test migrate file exists groupId (db live)");

                      logger.debug("Finished testMigrateFileExists");
                      done();
                    });
                  });
                });
              });
            });
          });
        });
      });
    });
  });
}

exports.testMigrateBothDirections = function(done){
  logger.info("In testMigrateBothDirections");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    cleanDatabase(databaseLive, function(err){
      assert.ok(!err);

      async.series({
        "file1": async.apply(saveTestPdf, databaseDev, {}),
        "file2": async.apply(saveTestMp4, databaseDev, {}),
        "file3": async.apply(saveTestPng, databaseDev, {"thumbnail": {"width":200, "height":200}})
      }, function(err, result){
        assert.ok(!err);

        fileHandler.migrateFiles(databaseDev, databaseLive, [{"groupId": result.file1.groupId}, {"groupId": result.file2.groupId, "version":1}, {"groupId": result.file3.groupId, "version":1}], function(err, migrationResults){
          assert.ok(!err);

          //Now I want to save some files in the live database
          async.series({
            "file1": async.apply(saveTestMp4, databaseLive, {"groupId": result.file1.groupId}),
            "file2": async.apply(saveTestPdf, databaseLive, {"groupId": result.file2.groupId}),
            "file3": async.apply(saveTestZip, databaseLive, {"groupId": result.file3.groupId})
          }, function(err, saveLiveResults){
            assert.ok(!err);

            //Now I want to save some files in the dev after migrate
            async.series({
              "file1": async.apply(saveTestPpt, databaseDev, {"groupId": result.file1.groupId}),
              "file2": async.apply(saveTestXml, databaseDev, {"groupId": result.file2.groupId}),
              "file3": async.apply(saveTestJpg, databaseDev, {"groupId": result.file3.groupId})
            }, function(err, saveDevResults){
              assert.ok(!err);

              //Now migrate the files back from live to Dev.
              fileHandler.migrateFiles(databaseLive, databaseDev, [{"groupId": result.file1.groupId}, {"groupId": result.file2.groupId}, {"groupId": result.file3.groupId}], function(err, migrateBackResults){
                assert.ok(!err);

                //Now check the file histories of the dev

                async.series({
                  "file1History": async.apply(fileHandler.getFileHistory, databaseDev, result.file1.groupId),
                  "file2History": async.apply(fileHandler.getFileHistory, databaseDev, result.file2.groupId),
                  "file3History": async.apply(fileHandler.getFileHistory, databaseDev, result.file3.groupId)
                }, function(err, historyResults){
                  assert.ok(!err);

                  //Checking the history is correct
                  assert.ok(historyResults.file1History);
                  assert.ok(Array.isArray(historyResults.file1History));
                  assert.equal(historyResults.file1History.length, 3);

                  /////////////// FILE 1 //////////////////
                  assert.equal(historyResults.file1History[0].fileName, "test.mp4");
                  assert.ok(historyResults.file1History[0].hash);
                  assert.equal(historyResults.file1History[0].contentType, "video/mp4");
                  assert.equal(historyResults.file1History[0].version, 3);

                  assert.equal(historyResults.file1History[1].fileName, "test.ppt");
                  assert.ok(historyResults.file1History[1].hash);
                  assert.equal(historyResults.file1History[1].contentType, "application/vnd.ms-powerpoint");
                  assert.equal(historyResults.file1History[1].version, 2);

                  assert.equal(historyResults.file1History[2].fileName, "test.pdf");
                  assert.ok(historyResults.file1History[2].hash);
                  assert.equal(historyResults.file1History[2].contentType, "application/pdf");
                  assert.equal(historyResults.file1History[2].version, 1);

                  /////////////// FILE 2 //////////////////
                  assert.equal(historyResults.file2History.length, 3);
                  assert.equal(historyResults.file2History[0].fileName, "test.pdf");
                  assert.ok(historyResults.file2History[0].hash);
                  assert.equal(historyResults.file2History[0].contentType, "application/pdf");
                  assert.equal(historyResults.file2History[0].version, 3);

                  assert.equal(historyResults.file2History[1].fileName, "test.xml");
                  assert.ok(historyResults.file2History[1].hash);
                  assert.equal(historyResults.file2History[1].contentType, "application/xml");
                  assert.equal(historyResults.file2History[1].version, 2);

                  assert.equal(historyResults.file2History[2].fileName, "test.mp4");
                  assert.ok(historyResults.file2History[2].hash);
                  assert.equal(historyResults.file2History[2].contentType, "video/mp4");
                  assert.equal(historyResults.file1History[2].version, 1);

                  /////////////// FILE 3 //////////////////
                  assert.equal(historyResults.file3History.length, 3);
                  assert.equal(historyResults.file3History[0].fileName, "test.zip");
                  assert.ok(historyResults.file3History[0].hash);
                  assert.equal(historyResults.file3History[0].contentType, "application/zip");
                  assert.equal(historyResults.file3History[0].version, 3);

                  assert.equal(historyResults.file3History[1].fileName, "test.jpg");
                  assert.ok(historyResults.file3History[1].hash);
                  assert.equal(historyResults.file3History[1].contentType, "image/jpeg");
                  assert.equal(historyResults.file3History[1].version, 2);

                  assert.equal(historyResults.file3History[2].fileName, "test.png");
                  assert.ok(historyResults.file3History[2].hash);
                  assert.equal(historyResults.file3History[2].contentType, "image/png");
                  assert.equal(historyResults.file3History[2].version, 1);

                  done();
                });
              });
            });
          });
        });
      });
    });
  });
}

exports.testExportZipSingleFile = function(done){
  logger.info("In testExportZipSingleFile");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    saveTestMp4(databaseDev, {}, function(err, saveResult){
      assert.ok(!err);

      var archive = archiver('zip');
      var outputFileStream = fs.createWriteStream(testOutputFilesPath + "singleFileExport.zip");

      archive.pipe(outputFileStream);

      fileHandler.exportFilesStream(databaseDev, [{"groupId": saveResult.groupId, "version": 1}], archive, function(err){
        assert.ok(!err);

        archive.finalize(function(err, bytesWritten){
          assert.ok(!err);
          assert.ok(bytesWritten);

          fs.exists(testOutputFilesPath + "singleFileExport.zip", function(exists){
            assert.equal(exists, true);

            logger.debug("Finished testExportZipSingleFile");
            done();
          });
        });
      });
    });
  });
}

exports.testExportZipMultipleFiles = function(done){
  logger.info("In testExportZipMultipleFiles");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);

    async.series({
      "file1": async.apply(saveTestPpt, databaseDev, {}),
      "file2": async.apply(saveTestPng, databaseDev, {"thumbnail": {"width": 200, "height": 200}}),
      "file3": async.apply(saveTestPdf, databaseDev, {}),
      "file4": async.apply(saveTestMp4, databaseDev, {}),
      "file5": async.apply(saveTestMp4, databaseDev, {"testVersion": 2})
    }, function(err, result){
      assert.ok(!err);

      var archive = archiver('zip');
      var outputFileStream = fs.createWriteStream(testOutputFilesPath + "multipleFileExport.zip");

      archive.pipe(outputFileStream);

      fileHandler.exportFilesStream(databaseDev, [
        {"groupId": result.file1.groupId, "version": 1},
        {"groupId": result.file2.groupId, "version": 1},
        {"groupId": result.file3.groupId, "version": 1},
        {"groupId": result.file4.groupId, "version": 1},
        {"groupId": result.file5.groupId, "version": 1}
      ], archive, function(err){
        assert.ok(!err);

        archive.finalize(function(err, bytesWritten){
          assert.ok(!err);
          assert.ok(bytesWritten);

          fs.exists(testOutputFilesPath + "multipleFileExport.zip", function(exists){
            assert.equal(exists, true);

            logger.debug("Finished testExportZipMultipleFiles");
            done();
          });
        });
      });
    });
  });
}

exports.testSaveMultipleLargeFiles = function(done){
  logger.info("In testSaveMultipleLargeFiles");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err, "There was an error");

    async.parallel({
      "file1": async.apply(saveTestMp4, databaseDev, {}),
      "file2": async.apply(saveTestMp4, databaseDev, {"testVersion":2}),
      "file3": async.apply(saveTestMp4, databaseDev, {"testVersion":3}),
      "file4": async.apply(saveTestMp4, databaseDev, {"testVersion":4}),
      "file5": async.apply(saveTestMp4, databaseDev, {"testVersion":5})
    }, function(err, result){
      assert.ok(!err, "There was an error");


      async.series([
        async.apply(checkFile, databaseDev, result.file1.groupId, "test.mp4", 1, "video/mp4", undefined),
        async.apply(checkFile, databaseDev, result.file2.groupId, "test2.mp4", 1, "video/mp4", undefined),
        async.apply(checkFile, databaseDev, result.file3.groupId, "test3.mp4", 1, "video/mp4", undefined),
        async.apply(checkFile, databaseDev, result.file4.groupId, "test4.mp4", 1, "video/mp4", undefined),
        async.apply(checkFile, databaseDev, result.file5.groupId, "test5.mp4", 1, "video/mp4", undefined)
      ], function(err){
        assert.ok(!err, "There was an error");
        done();
      });
    });
  });
}

exports.afterEach = function(done){
  logger.info("TearDown Called");
  cleanDatabase(databaseDev, function(err){
    assert.ok(!err);
    cleanDatabase(databaseLive, function(err){
      assert.ok(!err);
      logger.info("Closing databases ");
      databaseDev.close();
      databaseLive.close();
      done();
    });
  });
}



//////////////////END OF ACTUAL TESTING FUNCTIONS ///////////////////////////////
