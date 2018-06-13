/**
 * Tests that only the internal user will be able to advance the config server opTime.
 */
(function() {

    "use strict";

    var st = new ShardingTest({shards: 1, other: {keyFile: 'jstests/libs/key1'}});

    var adminUser = {
        db: "admin",
        username: "admin",
        password: "Github@12"
    };

    st.s.getDB(adminUser.db).createUser({user: 'admin', pwd: 'Github@12', roles: jsTest.adminUserRoles, "passwordDigestor" : "server"});

    st.s.getDB('admin').auth('admin', 'Github@12');

    st.adminCommand({enableSharding: 'test'});
    st.adminCommand({shardCollection: 'test.user', key: {x: 1}});

    st.d0.getDB('admin').createUser({user: 'omuser', pwd: 'Github@12', roles: jsTest.adminUserRoles, "passwordDigestor" : "server"});
    st.d0.getDB('admin').auth('omuser', 'Github@12');

    var maxSecs = Math.pow(2, 32) - 1;
    var metadata = {
        configsvr: {opTime: {ts: Timestamp(maxSecs, 0), t: maxSecs}}
    };
    var res = st.d0.getDB('test').runCommandWithMetadata("ping", {ping: 1}, metadata);

    assert.commandFailedWithCode(res.commandReply, ErrorCodes.Unauthorized);

    // Make sure that the config server optime did not advance.
    var status = st.d0.getDB('test').runCommand({serverStatus: 1});
    assert.neq(null, status.sharding);
    assert.lt(status.sharding.lastSeenConfigServerOpTime.t, maxSecs);

    st.d0.getDB('admin').createUser({user: 'internal', pwd: 'Github@12', roles: ['__system'], "passwordDigestor" : "server"});
    st.d0.getDB('admin').auth('internal', 'Github@12');

    res = st.d0.getDB('test').runCommandWithMetadata("ping", {ping: 1}, metadata);
    assert.commandWorked(res.commandReply);

    status = st.d0.getDB('test').runCommand({serverStatus: 1});
    assert.neq(null, status.sharding);
    assert.eq(status.sharding.lastSeenConfigServerOpTime.t, maxSecs);

    st.stop();

})();
