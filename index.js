const MongoClient = require('mongodb').MongoClient;

const utils = require('./utils');

module.exports.getCRUDMethods = (options = { url: '', database: '', collection: '' }) => {
    let collection;
    let error;
    MongoClient.connect(options.url, (err, client) => {
        error = err;
        collection = client.db(options.database).collection(options.collection);
        collection.find();
    });
    const e = {};
    e.count = (filter) => {
        return new Promise((resolve, reject) => {
            if (error) {
                reject(error);
                return;
            }
            try {
                if (!filter) {
                    filter = {}
                }
                collection.find(filter).count((err, doc) => {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve(doc);
                });
            } catch (e) {
                reject(err);
            }
        });
    };
    e.get = (options = { filter: {}, sort: '', page: 1, count: 30 }) => {
        return new Promise((resolve, reject) => {
            if (error) {
                reject(error);
                return;
            }
            try {
                if (!options) {
                    options = {};
                }
                if (!options.filter) {
                    options.filter = {};
                }
                if (!options.count) {
                    options.count = 30;
                }
                if (!options.page) {
                    options.page = 1;
                }
                let query = collection.find(options.filter);
                if (!options.count == -1) {
                    const skip = (options.page - 1) * options.count;
                    query = query.limit(options.count).skip(skip);
                }
                if (!options.sort) {
                    query = query.sort(options.sort);
                }
                query.toArray((err, doc) => {
                    if (err) {
                        reject(err);
                        return;
                    }
                    resolve(doc);
                });
            } catch (e) {
                reject(e);
            }
        });
    };
    e.post = (data) => {
        return new Promise((resolve, reject) => {
            if (error) {
                reject(error);
                return;
            }
            try {
                generateIdIfNot(options, data).then(newData => {
                    collection.insert(newData, (err, doc) => {
                        if (err) {
                            reject(err);
                            return;
                        }
                        resolve(doc)
                    });
                }).catch(err => {
                    reject(err);
                });
            } catch (e) {
                reject(e);
            }
        });
    };
    e.put = (id, data) => {
        return new Promise((resolve, reject) => {
            if (error) {
                reject(error);
                return;
            }
            try {
                collection.findOneAndUpdate({ _id: id }, data, (err, doc) => {
                    if (err) {
                        reject(err)
                        return;
                    }
                    resolve(doc)
                });
            } catch (e) {
                reject(e);
            }
        });
    };
    e.delete = (id) => {
        return new Promise((resolve, reject) => {
            if (error) {
                reject(error);
                return;
            }
            try {
                collection.findOneAndDelete({ _id: id }, (err, doc) => {
                    if (err) {
                        reject(err)
                        return;
                    }
                    resolve(doc)
                });
            } catch (e) {
                reject(e);
            }
        });
    };
    function generateIdIfNot(data) {
        return new Promise((resolve, reject) => {
            if (data._id) {
                resolve(data);
            } else {
                utils.getNextId(options).then(id => {
                    data._id = id;
                    resolve(data);
                }).catch(err => {
                    reject(err);
                });
            }
        });
    }
    return e;
};
