const log4js = require('log4js');
const MongoClient = require('mongodb').MongoClient;
const renderId = require('render-id');

const utils = require('./utils');

const logger = log4js.getLogger('mcrud');

logger.level = process.env.LOG_LEVEL || 'info';

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
     * @returns {Promise} Promise<any>
     */
    e.count = (filter) => {
        return new Promise((resolve, reject) => {
            try {
                if (!filter) {
                    filter = {}
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    const collection = client.db(options.database).collection(options.collection);
                    collection.find(filter).count((err2, doc) => {
                        if (err2) throw err2;
                        resolve(doc);
                        client.close();
                    });
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    /**
     * @param {{filter:{key:value},sort:string,page:number,count:number}} params get record options
     * @returns {Promise} Promise<any>
     */
    e.get = (params) => {
        return new Promise((resolve, reject) => {
            try {
                if (!params) {
                    params = {};
                }
                if (!params.filter) {
                    params.filter = {};
                }
                if (!params.count) {
                    params.count = 30;
                }
                if (!params.page) {
                    params.page = 1;
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    const collection = client.db(options.database).collection(options.collection);
                    let query = collection.find(params.filter);
                    if (params.count != -1) {
                        const skip = (params.page - 1) * params.count;
                        query = query.limit(params.count).skip(skip);
                    }
                    if (params.sort) {
                        query = query.sort(getSortObject(params.sort));
                    }
                    query.toArray((err2, doc) => {
                        if (err2) throw err2;
                        resolve(doc);
                        client.close();
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
     * @returns {Promise} Promise<any>
     */
    e.post = (data) => {
        return new Promise((resolve, reject) => {
            try {
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    const collection = client.db(options.database).collection(options.collection);
                    generateIdIfRequired(data).then(newData => {
                        let method = 'insertOne';
                        if (Array.isArray(newData)) {
                            method = 'insertMany';
                        }
                        collection[method](newData, (err2, doc) => {
                            if (err2) throw err2;
                            resolve(doc)
                            client.close();
                        });
                    }).catch(err => {
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
     * @param {{key:value}} filter mongodb filter
     * @param {{key:value}} data data to be insterted as new record
     * @returns {Promise} Promise<any>
     */
    e.put = (filter, data) => {
        return new Promise((resolve, reject) => {
            try {
                if (!filter) {
                    filter = {};
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    const collection = client.db(options.database).collection(options.collection);
                    collection.update(filter, data, (err2, doc) => {
                        if (err2) throw err2;
                        resolve(doc)
                        client.close();
                    });
                });
            } catch (e) {
                logger.error(e);
                reject(e);
            }
        });
    };
    /**
     * @param {{key:value}} filter mongodb filter
     * @returns {Promise} Promise<any>
     */
    e.delete = (filter) => {
        return new Promise((resolve, reject) => {
            try {
                if (!filter) {
                    filter = {};
                }
                MongoClient.connect(options.url, (err1, client) => {
                    if (err1) throw err1;
                    const collection = client.db(options.database).collection(options.collection);
                    collection.deleteMany(filter, (err2, doc) => {
                        if (err2) throw err2
                        resolve(doc)
                        client.close();
                    });
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
                let idCounter = 0;
                utils.getNextCounter(options).then(next => {
                    data.forEach((item, i) => {
                        if (!item._id && options.customId) {
                            idCounter++;
                            item._id = renderId.render(pattern, next + 1);
                        }
                    });
                    utils.setNextCounter(options, next + idCounter).then(doc => {
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
    function getSortObject(sort) {
        try {
            let temp = sort.split(',');
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
