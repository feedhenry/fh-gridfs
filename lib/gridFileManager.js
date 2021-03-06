var mongodb = require("mongodb");
var GridStore = mongodb.GridStore;
var ObjectId = mongodb.ObjectID;
var GridFsStream = require("gridfs-stream");
var async = require("async");
var mime = require("mime");
var streamBase64 = require("base64-stream");

var LOGLEVEL_ERROR = 0;
var LOGLEVEL_WARNING = 1;
var LOGLEVEL_INFO = 2;
var LOGLEVEL_DEBUG = 3;

var constants = {"LOG_LEVEL": LOGLEVEL_ERROR, "LOWEST_VERSION": 1, "THUMBNAIL_PREPEND":"thumbnail_", "DEFAULT_THUMBNAIL_WIDTH": 200, "DEFAULT_THUMBNAIL_HEIGHT":200, "MAX_MONGO_QUEUE_LENGTH": 1000, "ROOT_COLLECTION": "fileStorage"};

var defaultLogger = undefined;

var MongoFileHandler = function(config, logger) {

  if (config && config.fileStorage) {
    var fileStorageConfig = config.fileStorage;
    for (var configKey in fileStorageConfig) { // eslint-disable-line guard-for-in
      constants[configKey] = fileStorageConfig[configKey];
    }
  }

  if (logger) {
    defaultLogger = logger;
  } else {
    defaultLogger = {
      "info": (constants.LOG_LEVEL >= LOGLEVEL_INFO)?function(msg) {
        console.log("INFO   ", msg);
      }:function() {},
      "debug": (constants.LOG_LEVEL >= LOGLEVEL_DEBUG)?function(msg) {
        console.log("DEBUG  ", msg);
      }:function() {},
      "warning": (constants.LOG_LEVEL >= LOGLEVEL_WARNING)?function(msg) {
        console.log("WARNING", msg);
      }:function() {},
      "error": (constants.LOG_LEVEL >= LOGLEVEL_ERROR)?function(msg) {
        console.log("ERROR  ", msg);
      }:function() {}
    };
  }


};

MongoFileHandler.prototype.saveFile = function(db, fileName, fileReadStream, options, cb) {

  if (!cb || typeof(cb) !== "function") {
    return new Error("Callback function is required for saveFile request.");
  }

  if (!db || !fileName || !fileReadStream || !options) {
    return cb(new Error("Incorrect parameters for saveFile request"));
  }

  fileReadStream.pause();

  saveFile(db, fileName, fileReadStream, options, cb);
};

MongoFileHandler.prototype.deleteFile = function(db, searchOptions, cb) {
  defaultLogger.debug("In deleteFile");


  if (!cb || typeof(cb) !== "function") {
    return new Error("Callback function is required for deleteFile request.");
  }

  if (!db || !searchOptions || !searchOptions.groupId) {
    return cb(new Error("Incorrect parameters for deleteFile request"));
  }

  fileExists(db, searchOptions, function(err, exists) {
    if (err) {
      defaultLogger.error(err); return cb(err);
    }

    if (exists) {
      deleteFile(db, searchOptions, cb);
    } else {
      cb(new Error("No file exists with the groupId " + searchOptions.groupId));
    }
  });
};

MongoFileHandler.prototype.deleteAllFileVersions = function(db, groupId, cb) {
  defaultLogger.debug("In deleteAllFileVersions");

  if (!cb || typeof(cb) !== "function") {
    return new Error("Callback function is required for deleteAllFileVersions request.");
  }

  if (!db || !groupId) {
    return cb(new Error("Incorrect parameters for deleteAllFileVersions request"));
  }

  fileExists(db, {"groupId": groupId}, function(err, exists) {
    if (err) {
      defaultLogger.error(err); return cb(err);
    }

    if (exists) {
      getAllFileVersions(db, groupId, function(err, fileVersions) {
        if (err) {
          defaultLogger.error(err); return cb(err);
        }

        async.eachSeries(fileVersions, function(fileVersion, cb) {
          deleteFile(db, {"groupId": groupId, "version": fileVersion}, cb);
        }, function(err) {
          cb(err);
        });
      });
    } else {
      cb(new Error("No file exists with the provided search options"));
    }
  });
};

MongoFileHandler.prototype.migrateFiles = function(dbFrom, dbTo, migrationEntries, cb) {
  defaultLogger.debug("In migrateFiles");
  var migratedFiles = {};

  if (!cb || typeof(cb) !== "function") {
    return new Error("Callback function is required for migrateFiles request.");
  }

  if (!dbFrom || !dbTo || !migrationEntries) {
    return cb(new Error("Incorrect parameters for migrateFiles request "));
  }

  sanityCheckMigrateFilesList(dbFrom, migrationEntries, function(err) {
    if (err) {
      defaultLogger.error(err); return cb(err);
    }

    async.eachSeries(migrationEntries, function(oldFileEntry, cb) {
      getFileDetails(dbFrom, oldFileEntry, {}, function(err, oldFileDetails) {
        if (err) {
          defaultLogger.error(err); return cb(err);
        }

        getFileStream(dbFrom, oldFileEntry, {}, function(err, fileStream) {
          if (err) {
            defaultLogger.error(err); return cb(err);
          }

          saveFile(dbTo, oldFileDetails.filename, fileStream, {"groupId": oldFileEntry.groupId, "migratingFile": true}, function(err, migratedFile) {
            if (err) {
              defaultLogger.error(err); return cb(err);
            }

            if (oldFileDetails.metadata.thumbnail) {
              generateThumbnailFileName(oldFileDetails.filename, function(thumbFileName) {
                getFileStream(dbFrom, {"groupId": oldFileDetails.metadata.groupId, "version": oldFileDetails.metadata.version}, {"thumbnail": true}, function(err, thumbFileStream) {
                  if (err) {
                    defaultLogger.error(err); return cb(err);
                  }

                  saveFile(dbTo, thumbFileName, thumbFileStream, {"migratingThumbnail": true, "migratedThumbnailVersion": migratedFile.version}, function(err, thumbFileDetails) {
                    if (err) {
                      defaultLogger.error(err); return cb(err);
                    }

                    var gridStore = new GridStore(dbTo, migratedFile._id, "w+", {"root": constants.ROOT_COLLECTION});
                    gridStore.open(function(err, file) {
                      if (err) {
                        defaultLogger.error(err); return cb(err);
                      }

                      file.metadata.thumbnail = thumbFileDetails._id;
                      migratedFile.thumbnailHash = thumbFileDetails.hash;
                      migratedFile.thumbnailFileName = thumbFileDetails.fileName;

                      gridStore.close(function(err) {
                        migratedFiles[oldFileDetails.metadata.groupId] = migratedFile;
                        cb(err);
                      });
                    });
                  });
                });
              });
            } else { // No thumbnail -- finished with this file.
              migratedFiles[oldFileDetails.metadata.groupId] = migratedFile;
              cb(err);
            }
          });
        });
      });
    }, function(err) {
      cb(err, migratedFiles);
    });
  });
};

MongoFileHandler.prototype.getFileHistory = function(db, groupId, cb) {
  defaultLogger.debug("In getFileHistory");
  var fileDetails = [];

  if (!db || !groupId || !cb) {
    return cb(new Error("Incorrect parameters for getFileHistory request "));
  }

  getFileDetails(db, {"groupId": groupId}, {"allMatchingFiles": true}, function(err, allGroupFileDetails) {
    if (err) {
      defaultLogger.error(err); return cb(err);
    }

    async.eachSeries(allGroupFileDetails, function(fileEntry, cb) {

      var returnEntry = {"fileName": fileEntry.filename, "version": fileEntry.metadata.version, "contentType": fileEntry.contentType, "groupId": fileEntry.metadata.groupId, "hash": fileEntry.md5};
      fileDetails.push(returnEntry);
      cb(undefined);

    }, function(err) {
      cb(err, fileDetails);
    });
  });
};

MongoFileHandler.prototype.streamFile = function(db, searchOptions, fileOptions, cb) {
  defaultLogger.debug("In streamFile");

  if (!cb || typeof(cb) !== "function") {
    return new Error("Callback function is required for streamFile request.");
  }

  if (!db || !searchOptions || !fileOptions) {
    return cb(new Error("Incorrect parameters for streamFile request "));
  }

  fileExists(db, searchOptions, function(err, exists) {
    if (err) {
      defaultLogger.error(err); return cb(err);
    }

    if (exists) {
      getFileStream(db, searchOptions, fileOptions, cb);
    } else {
      cb(new Error("The file with params " + JSON.stringify(searchOptions) + " does not exist."));
    }
  });
};

MongoFileHandler.prototype.exportFilesStream = function(db, exportEntries, zipStream, cb) {
  defaultLogger.debug("In exportFilesStream");

  if (!cb || typeof(cb) !== "function") {
    return new Error("Callback function is required for exportFilesStream request.");
  }

  if (!db || !exportEntries || !zipStream) {
    return cb(new Error("Incorrect parameters for exportFilesStream request "));
  }

  var gridFileStream = new GridFsStream(db, mongodb);

  sanityCheckExportParameters(db, exportEntries, function(err) {
    if (err) {
      defaultLogger.error(err); return cb(err);
    }

    async.eachSeries(exportEntries, function(fileEntry, cb) {

      getFileDetails(db, fileEntry, {}, function(err, fileDetails) {
        if (err) {
          defaultLogger.error(err); return cb(err);
        }

        var fileReadStream = gridFileStream.createReadStream({"_id": fileDetails._id, "root": constants.ROOT_COLLECTION});

        //adding this file into the zipStream
        zipStream.append(fileReadStream, {"name": fileDetails.filename}, cb);
      });

    }, function(err) {
      cb(err);
    });
  });
};

MongoFileHandler.prototype.getFileDetails = function(db, groupId, cb) {
  getFileDetails(db, {"groupId": groupId}, {}, cb);
};

function sanityCheckExportParameters(db, exportEntries, cb) {
  var groupIdEntries = {};
  async.eachSeries(exportEntries, function(fileEntry, cb) {
    //GroupId is required to export a file
    if (!(fileEntry.groupId)) {
      return cb(new Error("Export must contain a groupId. Aborting. " + JSON.stringify(fileEntry)));
    }

    //Can't migrate two files with the same groupId
    if (groupIdEntries[fileEntry.groupId]) {
      return cb(new Error("Duplicate groupId entries for groupId " + fileEntry.groupId));
    } else {
      groupIdEntries[fileEntry.groupId] = true;
    }

    fileExists(db, fileEntry, function(err, exists) {
      if (err) {
        defaultLogger.error(err); return cb(err);
      }

      if (!exists) {
        return cb(new Error("File " + JSON.stringify(fileEntry) + " does not exist"));
      } else {
        return cb();
      }
    });

  }, function(err) {
    return cb(err);
  });
}

function sanityCheckMigrateFilesList(db, migrationEntries, cb) {

  var groupIdEntries = {};
  async.eachSeries(migrationEntries, function(fileEntry, cb) {

    //Should not be able to migrate files by hash value.
    if (fileEntry.hash) {
      return cb(new Error("Cannot migrate files by hash value. Aborting. " + fileEntry.hash));
    }

    //GroupId is required to migrate a file
    if (!(fileEntry.groupId)) {
      return cb(new Error("Migration must contain a groupId. Aborting. " + JSON.stringify(fileEntry)));
    }

    //Can't migrate two files with the same groupId
    if (groupIdEntries[fileEntry.groupId]) {
      return cb(new Error("Duplicate groupId entries for groupId " + fileEntry.groupId));
    } else {
      groupIdEntries[fileEntry.groupId] = true;
    }

    fileExists(db, fileEntry, function(err, exists) {
      if (err) {
        defaultLogger.error(err); return cb(err);
      }

      if (!exists) {
        return cb(new Error("File " + JSON.stringify(fileEntry) + " does not exist"));
      } else {
        return cb();
      }
    });
  }, function(err) {
    return cb(err);
  });
}

function saveFile(db, fileName, fileReadStream, options, cb) {
  defaultLogger.debug("In saveFile");

  if (!options.groupId) {// If there is no groupId specified, then need to create a new file
    createNewFile(db, fileName, fileReadStream, options, function(err, result) {
      return cb(err, result);
    });
  } else {
    fileExists(db, {"groupId": options.groupId}, function(err, exists) {
      if (err) {
        defaultLogger.error(err); return cb(err);
      }

      if (exists) {
        updateExistingFile(db, fileName, fileReadStream, options, function(err, result) {
          return cb(err, result);
        });
      } else {
        if (options.migratingFile) {//Exception to the rule that the groupId is required is when a file is being migrated. If that is the case and the group does not exist, just create the file
          createNewFile(db, fileName, fileReadStream, options, function(err, result) {
            return cb(err, result);
          });
        } else {
          return cb(new Error("A groupId parameter " + options.groupId + " was specified. No file group exists for this groupId. Aborting."));
        }
      }
    });
  }


}

function getAllFileVersions(db, groupId, cb) {

  defaultLogger.debug("In getAllFileVersions");
  var gridStore = new GridStore(db, null, "r", {"root": constants.ROOT_COLLECTION});
  gridStore.collection(function(err, collection) {
    if (err) {
      defaultLogger.error(err); return cb(err);
    }


    collection.find({"metadata.groupId": groupId},{sort:{"metadata.version":-1},"fields": {"metadata.version":1}}, function(err, files) {
      if (err) {
        defaultLogger.error(err); return cb(err);
      }


      files.toArray(function(err, filesArray) {
        if (err) {
          defaultLogger.error(err); return cb(err);
        }
        if (!(filesArray && Array.isArray(filesArray) && filesArray.length > 0)) {
          return cb(new Error("Collection query is empty"));
        }

        var returnedVersions = [];

        async.eachSeries(filesArray, function(fileItem, cb) {

          returnedVersions.push(fileItem.metadata.version);

          cb();
        }, function(err) {
          if (err) {
            defaultLogger.error(err); return cb(err);
          }

          gridStore.close(function(err) {
            return cb(err, returnedVersions);
          });
        });
      });
    });
  });
}

function deleteFile(db, searchOptions, cb) {
  defaultLogger.debug("In deleteFile");
  getFileDetails(db, searchOptions, {}, function(err, fileDetails) {
    if (err) {
      defaultLogger.error(err); return cb(err);
    }

    var fileId = new ObjectId(fileDetails._id.toString());

    var gridStore = new GridStore(db, fileId, "r", {"root": constants.ROOT_COLLECTION});

    gridStore.open(function(err, openGridStore) {
      if (err) {
        defaultLogger.error(err); return cb(err);
      }

      openGridStore.unlink(function(err) {
        if (err) {
          defaultLogger.error(err); return cb(err);
        }

        gridStore.close(function(err) {
          cb(err);
        });
      });
    });
  });
}

function generateThumbnailFileName(fileName, cb) {
  defaultLogger.debug("In generateThumbnailFileName");
  return cb(constants.THUMBNAIL_PREPEND + fileName);
}

function createImageThumbnail(db, fileName, fullImageDetails, version, options, cb) {
  //Need to pipe the readStream to graphics magic, that will then pipe it to storage
  defaultLogger.debug("In createImageThumbnail");
  var gridFileStream = new GridFsStream(db, mongodb);

  getFileContentType(fileName, function(fileContentType) {

    generateThumbnailFileName(fileName, function(thumbFileName) {
      //Default thumbnail size if not specified
      if (!options.thumbnail.width) {
        options.thumbnail.width = constants.DEFAULT_THUMBNAIL_WIDTH;
      }
      if (!options.thumbnail.height) {
        options.thumbnail.height = constants.DEFAULT_THUMBNAIL_HEIGHT;
      }

      var thumbnailFileReadStream =  gridFileStream.createReadStream({"_id": fullImageDetails._id, "root": constants.ROOT_COLLECTION});
      var thumbnailFileWriteStream = gridFileStream.createWriteStream({"mode": "w", "_id": new ObjectId(), "filename":thumbFileName, "content_type":fileContentType, "metadata":{"version": version, "width": options.thumbnail.width, "height": options.thumbnail.height, "isThumbnail": true}, "limit": constants.MAX_MONGO_QUEUE_LENGTH, "root": constants.ROOT_COLLECTION});
      defaultLogger.debug("After generateThumbnailFileName", thumbFileName, options.thumbnail.width, options.thumbnail.height);

      //Creating a thumbnail from the input stream, process and then storing.


      //When it's finished, can call the callback
      thumbnailFileWriteStream.on("close", function(thumbnailFile) {
        //Setting the thumbnail Id of the file
        defaultLogger.debug("thumbnail fileWriteStream Closed");
        var gridStore = new GridStore(db, fullImageDetails._id, "w+", {"root": constants.ROOT_COLLECTION});
        gridStore.open(function(err, parentFile) {
          if (err) {
            defaultLogger.error(err); return cb(err);
          }
          parentFile.metadata.thumbnail = thumbnailFile._id;

          gridStore.close(function(err) {
            if (err) {
              defaultLogger.error(err); return cb(err);
            }
            fullImageDetails["thumbnailHash"] = thumbnailFile.md5;
            fullImageDetails["thumbnailFileName"] = thumbnailFile.filename;
            return cb(undefined, fullImageDetails);
          });
        });
      });

      thumbnailFileReadStream.pipe(thumbnailFileWriteStream);
    });
  });
}

function createFileWithVersion(db, fileName, fileStream, version, options, cb) {
  defaultLogger.debug("In createFileWithVersion");
  var gridFileStream = new GridFsStream(db, mongodb);

  getFileContentType(fileName, function(fileContentType) {
    if (!(fileContentType && fileContentType.length > 0)) {
      return cb(new Error("No file content type available for file " + fileName));
    }

    var fileObjectId = new ObjectId();
    var groupId = fileObjectId.toString();
    if (options.groupId) {
      groupId = options.groupId;
    }
    //If it is an image and options.thumbnail is true
    if (fileContentType.indexOf("image/") !== -1 && options.thumbnail) {
      defaultLogger.debug("File " + fileName + " IS AN IMAGE");

      defaultLogger.debug("AFTER createImageThumbnail");

      var fileWriteStream = gridFileStream.createWriteStream({"mode": "w", "_id": fileObjectId, "filename":fileName, "content_type": fileContentType.toString(), "metadata":{"version": version, "groupId": groupId}, "limit":constants.MAX_MONGO_QUEUE_LENGTH, "root": constants.ROOT_COLLECTION});

      fileStream.on("error", function(err) {
        defaultLogger.error(err); return cb(err);
      });

      fileWriteStream.on("close", function(file) {
        createImageThumbnail(db, fileName, {"fileName": file.filename, "contentType": file.contentType, "hash": file.md5, "version": file.metadata.version, "groupId": file.metadata.groupId, "_id": file._id}, version, options, cb);
      });

      fileStream.resume();

      //DecodingBase64 file to binary and streaming it to the database.
      if (options.decodeBase64) {
        fileStream.pipe(streamBase64.Decode()).pipe(fileWriteStream);
      } else {
        fileStream.pipe(fileWriteStream);
      }
    } else {
      var fileMetadata = {};
      //If moving a thumbnail, ensure the version of the thumbnail matches the version of the image it came from.
      if (options.migratingThumbnail) {
        fileMetadata.version = options.migratedThumbnailVersion;
      } else {
        fileMetadata.version = version;
        fileMetadata.groupId = groupId;
      }

      if (options.migratingFile) { //If migrating a file, then the groupId will migrate also.
        fileMetadata.groupId = options.groupId;
      }

      fileWriteStream = gridFileStream.createWriteStream({"mode": "w", "_id": fileObjectId, "filename":fileName, "content_type": fileContentType.toString(), "metadata": fileMetadata , "limit":constants.MAX_MONGO_QUEUE_LENGTH, "root": constants.ROOT_COLLECTION});

      defaultLogger.debug("WriteStream Created");

      fileStream.on("error", function(err) {
        defaultLogger.error(err); return cb(err);
      });


      fileWriteStream.on("close", function(file) {
        defaultLogger.debug("Write Stream Closed");
        return cb(undefined, {"fileName": file.filename, "contentType": file.contentType, "hash": file.md5, "version": file.metadata.version, "groupId": file.metadata.groupId, "_id": file._id});
      }).on("error", function(err) {
        defaultLogger.error(err); return cb(err);
      });

      fileStream.resume();

      if (options.decodeBase64) {
        fileStream.pipe(streamBase64.Decode()).pipe(fileWriteStream);
      } else {
        fileStream.pipe(fileWriteStream);
      }
    }
  });
}

function fileExists(db, searchOptions, cb) {
  defaultLogger.debug("In fileExists ");
  var searchQuery = {};
  if (searchOptions.groupId) {

    searchQuery["metadata.groupId"] = searchOptions.groupId;

  } else if (searchOptions.hash) {
    searchQuery.md5 = searchOptions.hash;
  } else {
    return cb(new Error("No search options specified for fileExists. Aborting."));
  }

  //Checking for specific version
  if (searchOptions.version) {
    searchQuery["metadata.version"] = searchOptions.version;
  }

  db.collection(constants.ROOT_COLLECTION + ".files", function(err, collection) {
    if (err) {
      defaultLogger.error(err); return cb(err);
    }

    collection.find(searchQuery, {}, function(err, foundFiles) {
      if (err) {
        defaultLogger.error(err); return cb(err);
      }

      foundFiles.count(function(err, numFiles) {
        var groupExists = numFiles > 0;
        return cb(err, groupExists);
      });
    });
  });
}

//Utility function to search for files in the database.
function getFileDetails(db, fileSelectionCriteria, fileOptions, cb) {

  defaultLogger.debug("In getFileDetails ");
  var selectionQuery = undefined;
  if (fileSelectionCriteria.groupId) {

    selectionQuery= {"metadata.groupId": fileSelectionCriteria.groupId};


  } else if (fileSelectionCriteria.hash) {
    selectionQuery= {"md5": fileSelectionCriteria.hash};
  }

  var gridStore = new GridStore(db, null, "r", {"root": constants.ROOT_COLLECTION});
  var fileOfInterest = undefined;

  gridStore.collection(function(err, collection) {
    collection.find(selectionQuery, { "sort": {"metadata.version":-1}}, function(err, files) {
      if (err) {
        defaultLogger.error(err); return cb(err);
      }

      files.toArray(function(err, filesArray) {
        if (err) {
          defaultLogger.error(err); return cb(err);
        }
        if (!(filesArray && Array.isArray(filesArray) && filesArray.length > 0)) {
          return cb(new Error("No files exist for groupId " + fileSelectionCriteria.groupId));
        }

        //If the file details are needed for all files in the groupId, then an array containing all of the file details is returned.
        if (fileOptions.allMatchingFiles == true) { // eslint-disable-line eqeqeq
          fileOfInterest = filesArray;
        } else {// Just want the details of a single file.
          //If there is no version, get the latest version

          //If no version is found or we have a hash query, just want the first entry of the array.
          if (!fileSelectionCriteria.version || fileSelectionCriteria.hash) {
            fileOfInterest = filesArray[0];
          } else {
            fileOfInterest = filesArray.filter(function(file) {
              return file.metadata.version === fileSelectionCriteria.version;
            });

            if (fileOfInterest.length === 1) {
              fileOfInterest = fileOfInterest[0];
            } else {
              return cb(new Error("Unexpected number of files returned for groupId " + fileSelectionCriteria.groupId, fileOfInterest.length));
            }
          }
        }

        if (!Array.isArray(fileOfInterest)) {
          //Actually want the thumbnail for the file, return the details for that file instead.
          if (fileOptions.thumbnail) {
            if (fileOfInterest.metadata.thumbnail) {
              getThumbnailFileDetails(db, fileOfInterest.metadata.thumbnail, function(err, thumbFileDetails) {
                if (err) {
                  defaultLogger.error(err); return cb(err);
                }

                fileOfInterest = thumbFileDetails;

                gridStore.close(function(err) {
                  return cb(err, fileOfInterest);
                });
              });
            } else {
              return cb(new Error("Thumbnail for file " + JSON.stringify(fileSelectionCriteria) + " does not exist."));
            }
          } else {
            gridStore.close(function(err) {
              return cb(err, fileOfInterest);
            });
          }
        } else {
          gridStore.close(function(err) {
            return cb(err, fileOfInterest);
          });
        }
      });
    });
  });
}

function getThumbnailFileDetails(db, thumbId, cb) {
  var gridStore = new GridStore(db, null, "r", {"root": constants.ROOT_COLLECTION});
  gridStore.collection(function(err, collection) {
    if (err) {
      defaultLogger.error(err); return cb(err);
    }

    collection.find({"_id": thumbId}, {}, function(err, files) {
      if (err) {
        defaultLogger.error(err); return cb(err);
      }

      files.toArray(function(err, filesArray) {
        if (err) {
          defaultLogger.error(err); return cb(err);
        }

        if (filesArray.length === 0) {
          return cb(new Error("Search For Unique Thumbnail with Id " + thumbId + " found no files. Aborting"));
        } else if (filesArray.length > 1) {
          return cb(new Error("Search For Unique Thumbnail with Id " + thumbId + " found more than one file. Aborting"));
        } else {
          return cb(undefined, filesArray[0]);
        }
      });
    });
  });
}

//Increment function can increment in any way needed
function incrementFileVersion(oldFileVersion, cb) {
  defaultLogger.debug("In incrementFileVersion");
  return cb(oldFileVersion + 1);
}

//Updating an existing file means that the new file is saved with the same name with an incremented version.
function updateExistingFile(db, fileName, fileReadStream, options, cb) {
  defaultLogger.debug("In updateExistingFile");

  getFileDetails(db, {"groupId": options.groupId}, {}, function(err, fileInfo) {
    if (err) {
      defaultLogger.error(err); return cb(err);
    }
    var latestFileVersion = fileInfo.metadata.version;
    incrementFileVersion(latestFileVersion, function(newFileVersion) {
      if (!newFileVersion) {
        return cb(new Error("File version was not incremented for file " + fileName));
      }
      defaultLogger.debug("New File Version ", newFileVersion);
      createFileWithVersion(db, fileName, fileReadStream, newFileVersion, options, cb);
    });
  });
}

//Creating a new file means creating a new file with fileName with version 0;
function createNewFile(db, fileName, fileReadStream, options, cb) {
  defaultLogger.debug("In createNewFile");
  createFileWithVersion(db, fileName, fileReadStream, constants.LOWEST_VERSION, options, cb);
}


function getFileStream(db, searchOptions, fileOptions, cb) {
  defaultLogger.debug("In getFileStream");

  getFileDetails(db, searchOptions, fileOptions, function(err, fileDetails) {
    if (err) {
      defaultLogger.error(err); return cb(err);
    }

    var gridFileStream = new GridFsStream(db, mongodb);
    var fileReadStream = gridFileStream.createReadStream({"_id": fileDetails._id, "root": constants.ROOT_COLLECTION});
    fileReadStream.pause();//Should be paused immediately.


    return cb(err, fileReadStream, fileDetails);

  });
}

function getFileContentType(fileName, cb) {
  defaultLogger.debug("In getFileContentType");
  var mimeType = mime.lookup(fileName);
  defaultLogger.debug(mimeType);

  return cb(mimeType);
}


module.exports.MongoFileHandler = MongoFileHandler;
