# mcrud

Smallest CRUD wrapper over mongodb client with Custom ID generator.

## APIs

- count : Get number of records with filter
- list : Get records with filter, select, pagination and sorting
- get : Get record selected fields
- post : Create one or many record with custom ID generation
- put : Update a record with recordId
- delete : Delete a record with recordId
- collection : the MongoDB Collection Object


## Example POST

```javascript
const mcrud = require('mcrud');

const crud = mcrud.getCRUDMethods({
    collection: 'test',
    database: 'mcrud',
    url: 'mongodb://localhost:27017',
    idPattern: 'TE#####ST##'
});

const payload = {
    firstName: 'John',
    lastName: 'Doe',
    email: 'john@doe.com',
    gender: 'Male'
};

crud.post(payload).then(doc => {
    console.log('document inserted');
    /**
     * Inserted document
     * {
     *     _id:'TE00000ST01',
     *     firstName: 'John',
     *     lastName: 'Doe',
     *     email: 'john@doe.com',
     *     gender: 'Male'
     * }
    */
}).catch(err => {
    console.log(err);
});

//or insert many

const payload = [
    {
        firstName: 'John',
        lastName: 'Doe',
        email: 'john@doe.com',
        gender: 'Male'
    },
    {
        firstName: 'Jane',
        lastName: 'Doe',
        email: 'jane@doe.com',
        gender: 'Female'
    }
];

crud.post(payload).then(docs => {
    console.log('documents inserted');
    /**
     * Inserted documents
     * {
     *     _id:'TE00000ST02',
     *     firstName: 'John',
     *     lastName: 'Doe',
     *     email: 'john@doe.com',
     *     gender: 'Male'
     * }
     * {
     *     _id:'TE00000ST03',
     *     firstName: 'Jane',
     *     lastName: 'Doe',
     *     email: 'jane@doe.com',
     *     gender: 'Female'
     * }
    */
}).catch(err => {
    console.log(err);
});

```