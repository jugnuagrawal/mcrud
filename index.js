const log4js = require('log4js');
const { MongoClient } = require('mongodb');
const renderId = require('render-id');


const LOG_LEVEL = process.env.LOG_LEVEL || 'info';

log4js.configure({
    appenders: { mcrud: { type: 'stdout' } },
    categories: { default: { appenders: ['mcrud'], level: LOG_LEVEL } }
});

const logger = log4js.getLogger('mcrud');

const utils = require('./utils');


/**
 * @param {object} options CRUD options
 * @param {string} options.url
 * @param {string} options.database
 * @param {string} options.collection
 * @param {boolean} options.customId
 * @param {string} options.idPattern
 */
function MCRUD(options) {
    this.options = options;
}

MCRUD.prototype.count = async function (filter) {
    let client;
    try {
        if (!filter) {
            filter = {}
        }
        if (typeof filter === 'string') {
            filter = JSON.parse(filter);
        }
        client = await MongoClient.connect(this.options.url);
        const collection = client.db(this.options.database).collection(this.options.collection);
        logger.trace('Using db :', this.options.database);
        const doc = await collection.find(filter).count();
        logger.trace(doc + ' no of documents found in :', this.options.collection);
        return doc
    } catch (e) {
        logger.error(e);
        throw e;
    } finally {
        logger.trace('Connection closed : Database : ' + this.options.database, 'Collection : ' + this.options.collection);
        client.close(true);
    }
};



MCRUD.prototype.list = async function (params) {
    let client;
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
        client = await MongoClient.connect(this.options.url);
        const collection = client.db(this.options.database).collection(this.options.collection);
        logger.trace('Using db :', this.options.database);
        logger.trace(JSON.stringify(params.filter) + ' filter applied in :', this.options.collection);
        let query = collection.find(params.filter);
        logger.trace(params.count + ' count applied in :', this.options.collection);
        if (params.select) {
            query = query.project(getAsObject(params.select));
            logger.trace(params.select + ' select applied in :', this.options.collection);
        }
        if (params.count != -1) {
            const skip = (params.page - 1) * params.count;
            query = query.limit(params.count).skip(skip);
            logger.trace(params.page + ' page applied in :', this.options.collection);
        }
        if (params.sort) {
            query = query.sort(getAsObject(params.sort));
            logger.trace(params.sort + ' sort applied in :', this.options.collection);
        }
        const docs = await query.toArray();
        logger.trace(docs.length + ' results found in :', this.options.collection);
        return docs;
    } catch (e) {
        logger.error(e);
        throw e;
    } finally {
        logger.trace('Connection closed : Database : ' + this.options.database, 'Collection : ' + this.options.collection);
        client.close(true);
    }
};


MCRUD.prototype.get = async function (id, select) {
    let client;
    try {
        if (!id) {
            throw new Error('Invalid Id');
        }
        client = await MongoClient.connect(this.options.url);
        const collection = client.db(this.options.database).collection(this.options.collection);
        logger.trace('Using db :', this.options.database);
        logger.trace(id + ' ID applied in :', this.options.collection);
        let query = collection.find({ _id: id });
        if (select) {
            query = query.project(getAsObject(select));
            logger.trace(select + ' select applied in :', this.options.collection);
        }
        const docs = await query.toArray();
        if (docs && docs.length > 0) {
            logger.trace(id + ' found in :', this.options.collection);
            return docs[0];
        } else {
            logger.trace(id + ' not found in :', this.options.collection);
            return null;
        }
    } catch (e) {
        logger.error(e);
        reject(e);
    } finally {
        logger.trace('Connection closed : Database : ' + this.options.database, 'Collection : ' + this.options.collection);
        client.close(true);
    }
};


MCRUD.prototype.post = async function (data) {
    let client;
    try {
        client = await MongoClient.connect(this.options.url);
        const collection = client.db(this.options.database).collection(this.options.collection);
        logger.trace('Using db :', this.options.database);
        const newData = await generateIdIfRequired(this.options, data);
        let method = 'insertOne';
        if (Array.isArray(newData)) {
            method = 'insertMany';
        }
        logger.trace('Using method ' + method + ' for :', this.options.collection);
        const status = await collection[method](newData);
        logger.trace(status.insertedCount + ' document(s) inserted in :', this.options.collection);
        let ids;
        if (status.insertedIds) {
            ids = Object.values(status.insertedIds);
        } else {
            ids = [status.insertedId];
        }
        return await collection.find({ _id: { $in: ids } }).toArray();
    } catch (e) {
        logger.error(e);
        throw e;
    } finally {
        logger.trace('Connection closed : Database : ' + this.options.database, 'Collection : ' + this.options.collection);
        client.close(true);
    }
};

MCRUD.prototype.put = async function (id, data, upsert) {
    let client;
    try {
        client = await MongoClient.connect(this.options.url);
        const collection = client.db(this.options.database).collection(this.options.collection);
        logger.trace('Using db :', this.options.database);
        const oldDoc = await collection.findOne({ _id: id });
        if (!oldDoc && !upsert) {
            throw new Error('No Document Found!');
        }
        data = _.merge(oldDoc, data);
        const status = await collection.findOneAndUpdate({ _id: id }, { $set: data }, { returnDocument: 'after', upsert });
        logger.trace(status.modifiedCount + ' document(s) updated in :', this.options.collection);
        return status.value;
    } catch (e) {
        logger.error(e);
        throw e;
    } finally {
        logger.trace('Connection closed : Database : ' + this.options.database, 'Collection : ' + this.options.collection);
        client.close(true);
    }
};

MCRUD.prototype.delete = async function (id) {
    let client;
    try {
        client = await MongoClient.connect(this.options.url);
        const collection = client.db(this.options.database).collection(this.options.collection);
        logger.trace('Using db :', this.options.database);
        const status = await collection.deleteOne({ _id: id });
        logger.trace(status.deletedCount + ' document(s) deleted in :', this.options.collection);
        return status.deletedCount;
    } catch (e) {
        logger.error(e);
        throw e;
    } finally {
        logger.trace('Connection closed : Database : ' + this.options.database, 'Collection : ' + this.options.collection);
        client.close(true);
    }
};


MCRUD.prototype.collection = async function () {
    let client;
    let self = this;
    try {
        client = await MongoClient.connect(this.options.url);
        const collection = client.db(this.options.database).collection(this.options.collection);
        logger.trace('Using db :', this.options.database);
        return {
            collection,
            close: function () {
                logger.trace('Connection closed : Database : ' + self.options.database, 'Collection : ' + self.options.collection);
                client.close(true);
            }
        }
    } catch (e) {
        logger.error(e);
        throw e;
    }
};


MCRUD.prototype.getNextCounter = async function () {
    try {
        return await utils.getNextCounter(this.options);
    } catch (e) {
        logger.error(e);
        throw e;
    }
};

MCRUD.prototype.setNextCounter = async function (counter) {
    try {
        return await utils.setNextCounter(this.options, counter);
    } catch (e) {
        logger.error(e);
        throw e;
    }
};


MCRUD.prototype.getNextId = async function () {
    try {
        return await utils.getNextId(this.options);
    } catch (e) {
        logger.error(e);
        throw e;
    }
};


function getAsObject(value) {
    try {
        const temp = value.split(',');
        return temp.reduce((prev, curr) => {
            let key = curr;
            if (key.startsWith('-')) {
                key = key.substr(1, key.length);
            }
            prev[key] = curr.startsWith('-') ? -1 : 1;
            return prev;
        }, {});
    } catch (e) {
        logger.error(e);
        throw e;
    }
}

async function generateIdIfRequired(options, data) {
    try {
        if (Array.isArray(data)) {
            let pattern = options.collection.prefix(3).toUpperCase();
            if (options.idPattern) {
                pattern = options.idPattern;
            }
            let idCounter = -1;
            const next = await utils.getNextCounter(options);
            data.forEach((item, i) => {
                if (!item._id && options.customId) {
                    idCounter++;
                    item._id = renderId.render(pattern, next + idCounter);
                }
            });
            await utils.setNextCounter(options, next + idCounter + 1);
        } else {
            if (data._id || !options.customId) {
                return data;
            }
            data._id = await utils.getNextId(options);
        }
        return data;
    } catch (e) {
        logger.error(e);
        throw e;
    }
}


module.exports = MCRUD;