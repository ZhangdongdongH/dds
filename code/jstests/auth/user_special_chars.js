// Test creating and authenticating a user with special characters.
var conn = MongoRunner.runMongod({auth: ''});

var testUserSpecialCharacters = function() {
    // Create a user with special characters, make sure it can auth.
    var adminDB = conn.getDB('admin');
    adminDB.createUser(
        {user: '~`!@#$%^&*()-_+={}[]||;:",.//><', pwd: 'Github@12', roles: jsTest.adminUserRoles, "passwordDigestor" : "server"});
    assert(adminDB.auth({user: '~`!@#$%^&*()-_+={}[]||;:",.//><', pwd: 'Github@12'}));
};

testUserSpecialCharacters();
MongoRunner.stopMongod(conn);
