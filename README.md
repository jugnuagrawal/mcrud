# mcrud

Smallest CRUD wrapper over mongodb client with Custom ID generator.

## APIs

- count : get count of records with filter
- get : get records with filter, select, pagination and sorting
- post : create one or many record with custom ID generation
- put : update one or many record with filter
- delete : delete one or many record with filter


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