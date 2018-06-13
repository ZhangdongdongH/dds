// SERVER-8625: Test that dbAdmins can view index definitions.
var conn = MongoRunner.runMongod({auth: ""});

var adminDB = conn.getDB("admin");
var testDB = conn.getDB("testdb");
var indexName = 'idx_a';

adminDB.createUser({user: 'root', pwd: 'Github@12', roles: ['root'], "passwordDigestor" : "server"});
adminDB.auth('root', 'password');
testDB.foo.insert({a: 1});
testDB.createUser({user: 'dbAdmin', pwd: 'Github@12', roles: ['dbAdmin'], "passwordDigestor" : "server"});
adminDB.logout();

testDB.auth('dbAdmin', 'Github@12');
testDB.foo.ensureIndex({a: 1}, {name: indexName});
assert.eq(2, testDB.foo.getIndexes().length);  // index on 'a' plus default _id index
var indexList = testDB.foo.getIndexes().filter(function(idx) {
    return idx.name === indexName;
});
assert.eq(1, indexList.length, tojson(indexList));
assert.docEq(indexList[0].key, {a: 1}, tojson(indexList));
