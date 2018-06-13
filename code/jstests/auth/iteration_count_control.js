// Test SCRAM iterationCount control.
load('./jstests/multiVersion/libs/auth_helpers.js');

var conn = MongoRunner.runMongod({auth: ''});

var testIterationCountControl = function() {
    var adminDB = conn.getDB('admin');
    adminDB.createUser({user: 'admin', pwd: 'Github@12', roles: jsTest.adminUserRoles, "passwordDigestor" : "server"});
    assert(adminDB.auth({user: 'admin', pwd: 'Github@12'}));

    var userDoc = getUserDoc(adminDB, 'admin');
    assert.eq(10000, userDoc.credentials['SCRAM-SHA-1'].iterationCount);

    // Changing iterationCount should not affect existing users.
    assert.commandWorked(adminDB.runCommand({setParameter: 1, scramIterationCount: 5000}));
    userDoc = getUserDoc(adminDB, 'admin');
    assert.eq(10000, userDoc.credentials['SCRAM-SHA-1'].iterationCount);

    // But it should take effect when the user's password is changed.
    adminDB.updateUser('admin', {pwd: 'Github@12', roles: jsTest.adminUserRoles, "passwordDigestor" : "server"});
    userDoc = getUserDoc(adminDB, 'admin');
    assert.eq(5000, userDoc.credentials['SCRAM-SHA-1'].iterationCount);

    // Test invalid values for iterationCount. 5000 is the minimum value.
    assert.commandFailed(adminDB.runCommand({setParameter: 1, scramIterationCount: 4999}));
    assert.commandFailed(adminDB.runCommand({setParameter: 1, scramIterationCount: -5000}));
};

testIterationCountControl();
MongoRunner.stopMongod(conn);
