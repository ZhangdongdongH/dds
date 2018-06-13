/*
 * This file tests that credentials are flushed
 * from the Mongo shell authCache when logging out.
 * It is a regression test for SERVER-8798.
 */

var conn = MongoRunner.runMongod({auth: "", remember: true});

// create user with rw permissions and login
var testDB = conn.getDB('test');
var adminDB = conn.getDB('admin');
adminDB.createUser({user: 'admin', pwd: 'Github@12', roles: ['userAdminAnyDatabase'], "passwordDigestor" : "server"});
adminDB.auth('admin', 'Github@12');
testDB.createUser({user: 'rwuser', pwd: 'Github@12', roles: ['readWrite'], "passwordDigestor" : "server"});
adminDB.logout();
testDB.auth('rwuser', 'Github@12');

// verify that the rwuser can read and write
testDB.foo.insert({a: 1});
assert.eq(1, testDB.foo.find({a: 1}).count(), "failed to read");

// assert that the user cannot read unauthenticated
testDB.logout();
assert.throws(function() {
    testDB.foo.findOne();
}, [], "user should not be able to read after logging out");

MongoRunner.stopMongod(conn);
conn = MongoRunner.runMongod({restart: conn, noCleanData: true});

// expect to fail on first attempt since the socket is no longer valid
try {
    val = testDB.foo.findOne();
} catch (err) {
}

// assert that credentials were not autosubmitted on reconnect
assert.throws(function() {
    testDB.foo.findOne();
}, [], "user should not be able to read after logging out");

MongoRunner.stopMongod(conn);

print("SUCCESS logout_reconnect.js");
