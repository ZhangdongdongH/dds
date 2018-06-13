// Ensure that attempting to use SCRAM-SHA-1 auth on a
// user with invalid SCRAM-SHA-1 credentials fails gracefully.

(function() {
    'use strict';

    function runTest(mongod) {
        assert(mongod);
        const admin = mongod.getDB('admin');
        const test = mongod.getDB('test');

        admin.createUser({user: 'admin', pwd: 'Github@12', roles: jsTest.adminUserRoles, "passwordDigestor" : "server"});
        assert(admin.auth('admin', 'Github@12'));

        test.createUser({user: 'user', pwd: 'Github@12', roles: jsTest.basicUserRoles, "passwordDigestor" : "server"});

        print("eharry 1: ")
        admin.system.users.find().forEach(function(doc) {
            print(tojson(doc));
        });
        print("eharry 1: ")
        // Give the test user an invalid set of SCRAM-SHA-1 credentials.
        assert.eq(admin.system.users
                      .update({_id: "test.user"}, {
                          $set: {
                              "credentials.SCRAM-SHA-1": {
                                  salt: "AAAA",
                                  storedKey: "AAAA",
                                  serverKey: "AAAA",
                                  iterationCount: 10000
                              }
                          }
                      })
                      .nModified,
                  1,
                  "Should have updated one document for user@test");
        print("eharry 1: ")
        admin.system.users.find().forEach(function(doc) {
            print(tojson(doc));
        });
        print("eharry 1: ")
        admin.logout();

        assert(!test.auth({user: 'user', pwd: 'Github@12'}));

//        assert.soon(function() {
//            const log = cat(mongod.fullOptions.logFile);
//            print("eharry 2 ")
//            print(log)
//            print("eharry 3 ")
//            return /Unable to perform SCRAM-SHA-1 auth.* invalid SCRAM credentials/.test(log);
//        }, "No warning issued for invalid SCRAM-SHA-1 credendials doc", 30 * 1000, 5 * 1000);
    }

    const mongod = MongoRunner.runMongod({auth: "", useLogFiles: true});
    runTest(mongod);
    MongoRunner.stopMongod(mongod);
})();
