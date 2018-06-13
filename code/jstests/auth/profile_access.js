var conn = MongoRunner.runMongod({auth: ""});

var adminDb = conn.getDB("admin");
var testDb = conn.getDB("testdb");

adminDb.createUser({
    user: 'admin',
    pwd: 'Github@12',
    roles: ['userAdminAnyDatabase', 'dbAdminAnyDatabase', 'readWriteAnyDatabase'],
    "passwordDigestor" : "server"
});



adminDb.auth('admin', 'Github@12');
testDb.createUser({user: 'readUser', pwd: 'Github@12', roles: ['read'], "passwordDigestor" : "server"});
adminDb.createUser({user: 'monitor', pwd: 'Github@12', roles: [{ 'role': 'dbAdmin', 'db':'testdb'}], "passwordDigestor" : "server"});
adminDb.getSiblingDB('admin').createUser({
    user: 'backupuser',
    pwd: 'Github@12',
    roles: [{role: 'dbAdminAnyDatabase', db: 'admin'}],
    "passwordDigestor" : "server"
});
testDb.setProfilingLevel(2);
testDb.foo.findOne();
adminDb.logout();
testDb.auth('readUser', 'Github@12');
assert.throws(function() {
    testDb.system.profile.findOne();
});
testDb.logout();

// SERVER-14355
testDb.getSiblingDB('admin').auth('monitor', 'Github@12');
testDb.setProfilingLevel(0);
testDb.system.profile.drop();
assert.commandWorked(testDb.createCollection("system.profile", {capped: true, size: 1024}));
testDb.logout();

// SERVER-16944
testDb.getSiblingDB('admin').auth('backupuser', 'Github@12');
testDb.setProfilingLevel(0);
testDb.system.profile.drop();
assert.commandWorked(testDb.createCollection("system.profile", {capped: true, size: 1024}));