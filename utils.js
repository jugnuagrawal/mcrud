const log4js = require('log4js');
const { MongoClient } = require('mongodb');
const renderId = require('render-id');

const logger = log4js.getLogger('mcrud');


String.prototype.prefix = function (length) {
    return this.substr(0, length);
}

/**
 * @param {object} options CRUD options
 * @param {string} options.url
 * @param {string} options.database
 * @param {string} options.collection
 * @param {boolean} options.customId
 * @param {string} options.idPattern
 * @returns {Promise}
 */
async function getNextId(options) {
    let client;
    try {
        client = await MongoClient.connect(options.url);
        logger.debug('Connected to :', options.url);
        let pattern = options.collection.prefix(3).toUpperCase() + '#####';
        if (options.idPattern) {
            pattern = options.idPattern;
        }
        const collection = client.db(options.database).collection('counters');
        logger.debug('Using db :', options.database);
        const doc = await collection.findOneAndUpdate({ _id: options.collection }, { $inc: { next: 1 } }, { upsert: true, returnDocument: 'after' });
        return renderId.render(pattern, doc.value.next);
    } catch (e) {
        logger.error(e);
        throw e;
    } finally {
        logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
        client.close(true);
    }
}


/**
 * @param {object} options CRUD options
 * @param {string} options.url
 * @param {string} options.database
 * @param {string} options.collection
 * @param {boolean} options.customId
 * @param {string} options.idPattern
 * @returns {Promise}
 */
async function getNextCounter(options) {
    let client;
    try {
        client = await MongoClient.connect(options.url);
        logger.debug('Connected to :', options.url);
        const collection = client.db(options.database).collection('counter');
        logger.debug('Using db :', options.database);
        const doc = await collection.findOne({ _id: options.collection });
        if (!doc) {
            logger.debug('Counter not found for collection :', options.collection);
            return 1;
        } else {
            logger.debug('Counter found for collection :', options.collection);
            return doc.next;
        }
    } catch (e) {
        logger.error(e);
        throw e;
    } finally {
        logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
        client.close(true);
    }
}

/**
 * @param {object} options CRUD options
 * @param {string} options.url
 * @param {string} options.database
 * @param {string} options.collection
 * @param {boolean} options.customId
 * @param {string} options.idPattern
 * @param {number} counter The next counter value to be stored
 * @returns {Promise}
 */
async function setNextCounter(options, counter) {
    let client;
    try {
        client = await MongoClient.connect(options.url);
        logger.debug('Connected to :', options.url);
        const collection = client.db(options.database).collection('counter');
        logger.debug('Using db :', options.database);
        await collection.findOneAndUpdate({ _id: options.collection }, { $set: { next: counter } }, { upsert: true, returnDocument: 'after' });
        logger.debug('Counter updated for collection :', options.collection);
    } catch (e) {
        logger.error(e);
        throw e;
    } finally {
        logger.debug('Connection closed :', options.url, 'Database : ' + options.database, 'Collection : ' + options.collection);
        client.close(true);
    }
}

module.exports = {
    getNextId,
    getNextCounter,
    setNextCounter
};