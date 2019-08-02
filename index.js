const path = require('path');
const log4js = require('log4js');
const MongoClient = require('mongodb').MongoClient;
const renderId = require('render-id');

const utils = require('./utils');

const logger = log4js.getLogger('mcrud');

logger.level = process.env.LOG_LEVEL || 'info';

log4js.configure({
    appenders: { mcrud: { type: 'file', filename: path.join(process.cwd(), 'logs/mcrud.log') } },
    categories: { default: { appenders: ['mcrud'], level: logger.level } }
});

/**
 * @param {{url:string,database:string,collection:string,customId:boolean,idPattern:string}} options CRUD options
 */
module.exports.getCRUDMethods = (options) => {
    if (options.idPattern && options.idPattern.trim()) {
        options.customId = true;
    }
    const e = {};
    /**
     * @param {{key:value}} filter mongodb filter
     * @returns {Promise<number>} count of records
     */
    e.count = (filter) => {
        return new Promise((resolve, reject) => {
            try {
                if (!filter) {
                    filter = {}
                }
                if (typeof filter === 'string') {
                    filter = JSON.parse(filter);
                }
                MongoClient.connect(options.url).then(client => {
                    logger.debug('Connected to :', options.url);
                    const collection = client.db(options.database).collection(options.collection);
                    logger.debug('Using db :', options.database);
                    collection.find(filter).count().then(doc => {
                        logger.debug(doc + ' no of documents found in :', options.collection);
                        client.close();
                        logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                        resolve(doc);
                    }).catch(err => {
                        logger.error(err);
                        reject(err);
                    });
                }).catch(err => {
                    logger.error(err);
                    reject(err);
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    /**
     * @param {{filter:{key:value},sort:string,page:number,count:number,select:string}} params get record options
     * @returns {Promise} Array of documents
     */
    e.list = (params) => {
        return new Promise((resolve, reject) => {
            try {
                if (!params) {
                    params = {};
                }
                if (!params.filter) {
                    params.filter = {};
                }
                if (typeof params.filter === 'string') {
                    params.filter = JSON.parse(params.filter);
                }
                if (!params.count) {
                    params.count = 30;
                }
                if (!params.page) {
                    params.page = 1;
                }
                MongoClient.connect(options.url).then(client => {
                    logger.debug('Connected to :', options.url);
                    const collection = client.db(options.database).collection(options.collection);
                    logger.debug('Using db :', options.database);
                    logger.debug(JSON.stringify(params.filter) + ' filter applied in :', options.collection);
                    let query = collection.find(params.filter);
                    logger.debug(params.count + ' count applied in :', options.collection);
                    if (params.select) {
                        query = query.project(getAsObject(params.select));
                        logger.debug(params.select + ' select applied in :', options.collection);
                    }
                    if (params.count != -1) {
                        const skip = (params.page - 1) * params.count;
                        query = query.limit(params.count).skip(skip);
                        logger.debug(params.page + ' page applied in :', options.collection);
                    }
                    if (params.sort) {
                        query = query.sort(getAsObject(params.sort));
                        logger.debug(params.sort + ' sort applied in :', options.collection);
                    }
                    query.toArray().then(doc => {
                        logger.debug(doc.length + ' results found in :', options.collection);
                        client.close();
                        logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                        resolve(doc);
                    }).catch(err => {
                        logger.error(err);
                        reject(err);
                    });
                }).catch(err => {
                    logger.error(err);
                    reject(err);
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    /**
     * @param {string} id record Id
     * @param {string} select select fields
     * @returns {Promise} Single record
     */
    e.get = (id, select) => {
        return new Promise((resolve, reject) => {
            try {
                if (!id) {
                    reject({
                        message: 'Invalid Id'
                    });
                    return;
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    logger.debug('Connected to :', options.url);
                    const collection = client.db(options.database).collection(options.collection);
                    logger.debug('Using db :', options.database);
                    logger.debug(id + ' ID applied in :', options.collection);
                    let query = collection.findOne({ _id: id });
                    if (select) {
                        query = query.project(getAsObject(select));
                        logger.debug(select + ' select applied in :', options.collection);
                    }
                    query.then(doc => {
                        client.close();
                        logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                        resolve(doc);
                    }).catch(err => {
                        logger.error(err);
                        reject(err);
                    });
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    /**
     * @param {{key:value}} data data to be insterted as new record
     * @returns {Promise} The saved document
     */
    e.post = (data) => {
        return new Promise((resolve, reject) => {
            try {
                MongoClient.connect(options.url).then(client => {
                    logger.debug('Connected to :', options.url);
                    const collection = client.db(options.database).collection(options.collection);
                    logger.debug('Using db :', options.database);
                    generateIdIfRequired(data).then(newData => {
                        let method = 'insertOne';
                        if (Array.isArray(newData)) {
                            method = 'insertMany';
                        }
                        logger.debug('Using method ' + method + ' for :', options.collection);
                        collection[method](newData).then(status => {
                            logger.debug(status.insertedCount + ' document(s) inserted in :', options.collection);
                            let ids;
                            if (status.insertedIds) {
                                ids = Object.values(status.insertedIds);
                            } else {
                                ids = [status.insertedId];
                            }
                            collection.find({ _id: { $in: ids } }).toArray().then(docs => {
                                client.close();
                                logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                                resolve(docs);
                            }).catch(err => {
                                logger.error(err);
                                reject(err);
                            });
                        }).catch(err => {
                            logger.error(err);
                            reject(err);
                        });
                    }).catch(err => {
                        logger.error(err);
                        reject(err);
                    });
                }).catch(err => {
                    logger.error(err);
                    reject(err);
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    /**
     * @param {string} id recordId
     * @param {{key:value}} data data to be updated for the recordId
     * @returns {Promise} Status of the operation
     */
    e.put = (id, data) => {
        return new Promise((resolve, reject) => {
            try {
                if (!id) {
                    reject({
                        message: 'Invalid Id'
                    });
                    return;
                }
                MongoClient.connect(options.url).then(client => {
                    logger.debug('Connected to :', options.url);
                    const collection = client.db(options.database).collection(options.collection);
                    logger.debug('Using db :', options.database);
                    collection.updateOne({ _id: id }, { $set: data }).then(doc => {
                        logger.debug(doc.modifiedCount + ' document(s) updated in :', options.collection);
                        client.close();
                        logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                        resolve(doc.result);
                    }).catch(err => {
                        logger.error(err);
                        reject(err);
                    });
                }).catch(err => {
                    logger.error(err);
                    reject(err);
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    /**
     * @param {string} id recordId
     * @returns {Promise} Status of the operation
     */
    e.delete = (id) => {
        return new Promise((resolve, reject) => {
            try {
                if (!id) {
                    reject({
                        message: 'Invalid Id'
                    });
                    return;
                }
                MongoClient.connect(options.url).then(client => {
                    logger.debug('Connected to :', options.url);
                    const collection = client.db(options.database).collection(options.collection);
                    logger.debug('Using db :', options.database);
                    collection.deleteOne({ _id: id }).then(status => {
                        logger.debug(status.deletedCount + ' document(s) deleted in :', options.collection);
                        client.close();
                        logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
                        resolve(status.result);
                    }).catch(err => {
                        logger.error(err);
                        reject(err);
                    });
                }).catch(err => {
                    logger.error(err);
                    reject(err);
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };

    /**
     * 
     * @returns {Promise<*>} The mongo collection object
     */
    e.collection = () => {
        return new Promise((resolve, reject) => {
            try {
                MongoClient.connect(options.url).then(client => {
                    logger.debug('Connected to :', options.url);
                    const collection = client.db(options.database).collection(options.collection);
                    logger.debug('Using db :', options.database);
                    resolve(collection);
                }).catch(err => {
                    logger.error(err);
                    reject(err);
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };

    function generateIdIfRequired(data) {
        return new Promise((resolve, reject) => {
            if (Array.isArray(data)) {
                let pattern = options.collection.prefix(3).toUpperCase();
                if (options.idPattern) {
                    pattern = options.idPattern;
                }
                let idCounter = -1;
                utils.getNextCounter(options).then(next => {
                    data.forEach((item, i) => {
                        if (!item._id && options.customId) {
                            idCounter++;
                            item._id = renderId.render(pattern, next + idCounter);
                        }
                    });
                    utils.setNextCounter(options, next + idCounter + 1).then(doc => {
                        resolve(data);
                    }).catch(err => {
                        reject(err);
                    });
                }).catch(err => {
                    reject(err);
                });
            } else {
                if (data._id || !options.customId) {
                    resolve(data);
                } else {
                    utils.getNextId(options).then(id => {
                        data._id = id;
                        resolve(data);
                    }).catch(err => {
                        reject(err);
                    });
                }
            }
        });
    }

    /**
     * 
     * @param {string} value
     * @returns {{key:number}}
     */
    function getAsObject(value) {
        try {
            let temp = value.split(',');
            temp = temp.map(e => {
                let key = e;
                if (key.startsWith('-')) {
                    key = key.substr(1, key.length);
                }
                return Object.defineProperty({}, key, {
                    value: e.startsWith('-') ? -1 : 1,
                    enumerable: true,
                    writable: true,
                    configurable: true
                });
            });
            temp = Object.assign.apply({}, temp);
            return temp;
        } catch (e) {
            logger.error(e);
            throw e;
        }
    }
    return e;
};


/**
 * @param {{url:string,database:string,collection:string,idPattern:string}} options ID options
 */
module.exports.getIdCounterMethods = (options) => {
    const e = {};
    e.getNextCounter = () => {
        return new Promise((resolve, reject) => {
            utils.getNextCounter(options).then(nextVal => {
                resolve(nextVal);
            }).catch(err => {
                reject(err);
            });
        })
    };
    /**
     * @param {number} counter Counter Value to set
     */
    e.setNextCounter = (counter) => {
        return new Promise((resolve, reject) => {
            utils.setNextCounter(options, counter).then(nextVal => {
                resolve(nextVal);
            }).catch(err => {
                reject(err);
            });
        })
    };

    e.getNextId = () => {
        return new Promise((resolve, reject) => {
            utils.getNextId(options).then(id => {
                resolve(id);
            }).catch(err => {
                reject(err);
            });
        });
    };

    return e;
}

