"use strict";
const AWS = require("aws-sdk");
const BbPromise = require("bluebird");
const _ = require("lodash");
const path = require("path");
const fs = require("fs");
const oboe = require("oboe");

// DynamoDB has a 25 item limit in batch requests
// https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
const MAX_MIGRATION_CHUNK = 25;

class ObjectStore {
  constructor(factory) {
    this.factory = factory;
    this.pool = new Array;
    this.len = 0;
  }

  get() {
    if (this.len > 0) {
      const out = this.pool[this.len -1]
      this.len--
      return out
    }
    return this.factory();
  }

  store(item) {
    this.pool[this.len] = item
    this.len++
  }
}
class ArrayStore {
  constructor(factory) {
    this.factory = factory;
    this.pool = new Array;
    this.len = 0;
  }

  get() {
    if (this.len > 0) {
      const out = this.pool[this.len -1]
      this.len--
      return out
    }
    return this.factory();
  }

  clean(array) {
    array.length = 0
    return array
  }

  store(item) {
    this.pool[this.len] = this.clean(item)
    this.len++
  }
}

const store = new ObjectStore(() => new Object())
const arrayStore = new ArrayStore(() => new Array())

/**
 * Writes a batch chunk of migration seeds to DynamoDB. DynamoDB has a limit on the number of
 * items that may be written in a batch operation.
 * @param {function} dynamodbWriteFunction The DynamoDB DocumentClient.batchWrite or DynamoDB.batchWriteItem function
 * @param {string} tableName The table name being written to
 * @param {any[]} seeds The migration seeds being written to the table
 */
function writeSeedBatch(dynamodbWriteFunction, tableName, seeds) {
  const params = store.get();
  params.RequestItems = {
      [tableName]: seeds.map((seed) => ({
        PutRequest: {
          Item: seed,
        },
      })),
  };
  return new BbPromise((resolve, reject) => {
    // interval lets us know how much time we have burnt so far. This lets us have a backoff mechanism to try
    // again a few times in case the Database resources are in the middle of provisioning.
    let interval = 0;
    function execute(interval) {
      setTimeout(() => dynamodbWriteFunction(params, (err) => {
        if (err) {
          if (err.code === "ResourceNotFoundException" && interval <= 5000) {
            execute(interval + 1000);
          } else {
            reject(err);
          }
        } else {
          resolve();
        }
      }), interval);
    }
    execute(interval);
  })
  .finally(()=> store.store(params))
}

/**
 * A promise-based function that determines if a file exists
 * @param {string} fileName The path to the file
 */
function fileExists(fileName) {
  return new BbPromise((resolve) => {
    fs.exists(fileName, (exists) => resolve(exists));
  });
}

/**
 * Transform all selerialized Buffer value in a Buffer value inside a json object
 *
 * @param {json} json with serialized Buffer value.
 * @return {json} json with Buffer object.
 */
function unmarshalBuffer(json) {
  _.forEach(json, function(value, key) {
    // Null check to prevent creation of Buffer when value is null
    if (value !== null && value.type==="Buffer") {
      json[key]= Buffer.from(value.data);
    }
  });
  return json;
}

/**
 * Scrapes seed files out of a given location. This file may contain
 * either a simple json object, or an array of simple json objects. An array
 * of json objects is returned.
 *
 * @param {function} dynamodbWriteFunction The DynamoDB DocumentClient.batchWrite or DynamoDB.batchWriteItem function
 * @param {string} location The filename to read seeds from.
 * @param {string} table The name of the dynamodb table to write to
 */
function getSeedsAtLocation(dynamodbWriteFunction, location, table) {
  // load the file as JSON
  let seeds = arrayStore.get();
  oboe(fs.createReadStream(location))
    .node("!.*", (thing) => {
      const data = unmarshalBuffer(thing);
      seeds.push(data)
      if(seeds.length >= MAX_MIGRATION_CHUNK){
        writeSeedBatch(dynamodbWriteFunction, table, seeds)
        arrayStore.store(seeds)
        seeds = arrayStore.get();
      }
      return oboe.drop;
    })
    .done(function(){
      console.log('File finished loading into ', table);
    })
}

/**
 * Locates seeds given a set of files to scrape
 * @param {string[]} sources The filenames to scrape for seeds
 */
function writeSeeds(db, sources, table, cwd) {
  sources = sources || [];
  cwd = cwd || process.cwd();

  const locations = sources.map((source) => path.join(cwd, source));
  return BbPromise.map(locations, (location) => {
    return fileExists(location).then((exists) => {
      if(!exists) {
        throw new Error("source file " + location + " does not exist");
      }
      const data = getSeedsAtLocation(db, location, table);
      return data;
    });
  // Smash the arrays together
  }).then((seedArrays) => [].concat.apply([], seedArrays));
}

module.exports = { writeSeeds };
