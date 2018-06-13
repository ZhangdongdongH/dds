"use strict";
// Test creating and authenticating users with special characters.

(function() {
    var conn = MongoRunner.runMongod({auth: ''});

    var adminDB = conn.getDB('admin');
    adminDB.createUser({user: 'admin', pwd: 'Github@12', roles: jsTest.adminUserRoles, "passwordDigestor" : "server"});

    var testUserSpecialCharacters = function() {

        // Create a user with special characters, make sure it can auth.
        assert(adminDB.auth('admin', 'Github@12'));
        adminDB.createUser(
            {user: '~`!@#$%^&*()-_+={}[]||;:",.//><', pwd: 'Github@12', roles: jsTest.adminUserRoles, "passwordDigestor" : "server"});
        assert(adminDB.logout());

        assert(adminDB.auth({user: '~`!@#$%^&*()-_+={}[]||;:",.//><', pwd: 'Github@12'}));
        assert(adminDB.logout());
    };
    testUserSpecialCharacters();

    var testUserAndDatabaseAtSymbolConflation = function() {
        // Create a pair of users and databases such that their string representations are
        // identical.
        assert(adminDB.auth('admin', 'Github@12'));

        var bcDB = conn.getDB('b@c');
        bcDB.createUser({user: 'a', pwd: 'Github@122', roles: [{role: 'readWrite', db: 'b@c'}], "passwordDigestor" : "server"}});

        var cDB = conn.getDB('c');
        cDB.createUser({user: 'a@b', pwd: 'Github@121', roles: [{role: 'readWrite', db: 'c'}], "passwordDigestor" : "server"}});

        assert(adminDB.logout());

        // Ensure they cannot authenticate to the wrong database.
        assert(!bcDB.auth('a@b', 'Github@121'));
        assert(!bcDB.auth('a@b', 'Github@122'));
        assert(!cDB.auth('a', 'Github@121'));
        assert(!cDB.auth('a', 'Github@122'));

        // Ensure that they can both successfully authenticate to their correct database.
        assert(cDB.auth('a@b', 'Github@121'));
        assert.writeOK(cDB.col.insert({data: 1}));
        assert.writeError(bcDB.col.insert({data: 2}));
        assert(cDB.logout());

        assert(bcDB.auth('a', 'Github@122'));
        assert.writeOK(bcDB.col.insert({data: 3}));
        assert.writeError(cDB.col.insert({data: 4}));
        assert(bcDB.logout());

        // Ensure that the user cache permits both users to log in at the same time
        assert(cDB.auth('a@b', 'Github@121'));
        assert(bcDB.auth('a', 'Github@122'));
        assert(cDB.logout());
        assert(bcDB.logout());

        assert(bcDB.auth('a', 'Github@122'));
        assert(cDB.auth('a@b', 'Github@121'));
        assert(cDB.logout());
        assert(bcDB.logout());
    };
    testUserAndDatabaseAtSymbolConflation();

    MongoRunner.stopMongod(conn);
})();
