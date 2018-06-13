/**
 *    Copyright (C) 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include "mongo/s/catalog/sharding_catalog_manager_impl.h"

#include <iomanip>
#include <vector>

#include "mongo/base/status.h"
#include "mongo/base/status_with.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/util/bson_extract.h"
#include "mongo/client/connection_string.h"
#include "mongo/client/read_preference.h"
#include "mongo/client/remote_command_targeter.h"
#include "mongo/client/replica_set_monitor.h"
#include "mongo/client/global_conn_pool.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/feature_compatibility_version.h"
#include "mongo/db/commands/feature_compatibility_version_command_parser.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/repl_client_info.h"
#include "mongo/db/s/balancer/balancer_policy.h"
#include "mongo/db/s/balancer/type_migration.h"
#include "mongo/db/s/sharding_state.h"
#include "mongo/db/s/type_shard_identity.h"
#include "mongo/db/wire_version.h"
#include "mongo/executor/network_interface.h"
#include "mongo/executor/task_executor.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/catalog/config_server_version.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/catalog/type_config_version.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/s/catalog/type_lockpings.h"
#include "mongo/s/catalog/type_locks.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_tags.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/cluster_identity_loader.h"
#include "mongo/s/grid.h"
#include "mongo/s/set_shard_version_request.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/s/write_ops/batched_command_request.h"
#include "mongo/s/write_ops/batched_command_response.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/scopeguard.h"
#include "mongo/base/remote_command_timeout.h"

namespace mongo {

MONGO_FP_DECLARE(dontUpsertShardIdentityOnNewShards);

using std::string;
using std::vector;
using str::stream;
std::mutex shardName_lock;

namespace {

using CallbackHandle = executor::TaskExecutor::CallbackHandle;
using CallbackArgs = executor::TaskExecutor::CallbackArgs;
using RemoteCommandCallbackArgs = executor::TaskExecutor::RemoteCommandCallbackArgs;
using RemoteCommandCallbackFn = executor::TaskExecutor::RemoteCommandCallbackFn;

const Seconds kDefaultFindHostMaxWaitTime(20);

const ReadPreferenceSetting kConfigReadSelector(ReadPreference::Nearest, TagSet{});
const ReadPreferenceSetting kConfigPrimarySelector(ReadPreference::PrimaryOnly);
const WriteConcernOptions kNoWaitWriteConcern(1, WriteConcernOptions::SyncMode::UNSET, Seconds(0));

/**
 * Append min, max and version information from chunk to the buffer for logChange purposes.
*/
void _appendShortVersion(BufBuilder* b, const ChunkType& chunk) {
    BSONObjBuilder bb(*b);
    bb.append(ChunkType::min(), chunk.getMin());
    bb.append(ChunkType::max(), chunk.getMax());
    if (chunk.isVersionSet())
        chunk.getVersion().addToBSON(bb, ChunkType::DEPRECATED_lastmod());
    bb.done();
}

/**
 * Checks if the given key range for the given namespace conflicts with an existing key range.
 * Note: range should have the full shard key.
 * Returns ErrorCodes::RangeOverlapConflict is an overlap is detected.
 */
Status checkForOveralappedZonedKeyRange(OperationContext* txn,
                                        Shard* configServer,
                                        const NamespaceString& ns,
                                        const ChunkRange& range,
                                        const string& zoneName,
                                        const KeyPattern& shardKeyPattern) {
    DistributionStatus chunkDist(ns, ShardToChunksMap{});

    auto tagStatus = configServer->exhaustiveFindOnConfig(txn,
                                                          kConfigPrimarySelector,
                                                          repl::ReadConcernLevel::kLocalReadConcern,
                                                          NamespaceString(TagsType::ConfigNS),
                                                          BSON(TagsType::ns(ns.ns())),
                                                          BSONObj(),
                                                          0);
    if (!tagStatus.isOK()) {
        return tagStatus.getStatus();
    }

    const auto& tagDocList = tagStatus.getValue().docs;
    for (const auto& tagDoc : tagDocList) {
        auto tagParseStatus = TagsType::fromBSON(tagDoc);
        if (!tagParseStatus.isOK()) {
            return tagParseStatus.getStatus();
        }

        // Always extend ranges to full shard key to be compatible with tags created before
        // the zone commands were implemented.
        const auto& parsedTagDoc = tagParseStatus.getValue();
        auto overlapStatus = chunkDist.addRangeToZone(
            ZoneRange(shardKeyPattern.extendRangeBound(parsedTagDoc.getMinKey(), false),
                      shardKeyPattern.extendRangeBound(parsedTagDoc.getMaxKey(), false),
                      parsedTagDoc.getTag()));
        if (!overlapStatus.isOK()) {
            return overlapStatus;
        }
    }

    auto overlapStatus =
        chunkDist.addRangeToZone(ZoneRange(range.getMin(), range.getMax(), zoneName));
    if (!overlapStatus.isOK()) {
        return overlapStatus;
    }

    return Status::OK();
}

/**
 * Returns a new range based on the given range with the full shard key.
 * Returns:
 * - ErrorCodes::NamespaceNotSharded if ns is not sharded.
 * - ErrorCodes::ShardKeyNotFound if range is not compatible (for example, not a prefix of shard
 * key) with the shard key of ns.
 */
StatusWith<ChunkRange> includeFullShardKey(OperationContext* txn,
                                           Shard* configServer,
                                           const NamespaceString& ns,
                                           const ChunkRange& range,
                                           KeyPattern* shardKeyPatternOut) {
    auto findCollStatus =
        configServer->exhaustiveFindOnConfig(txn,
                                             kConfigPrimarySelector,
                                             repl::ReadConcernLevel::kLocalReadConcern,
                                             NamespaceString(CollectionType::ConfigNS),
                                             BSON(CollectionType::fullNs(ns.ns())),
                                             BSONObj(),
                                             1);

    if (!findCollStatus.isOK()) {
        return findCollStatus.getStatus();
    }

    const auto& findCollResult = findCollStatus.getValue().docs;

    if (findCollResult.size() < 1) {
        return {ErrorCodes::NamespaceNotSharded, str::stream() << ns.ns() << " is not sharded"};
    }

    auto parseStatus = CollectionType::fromBSON(findCollResult.front());
    if (!parseStatus.isOK()) {
        return parseStatus.getStatus();
    }

    auto collDoc = parseStatus.getValue();
    if (collDoc.getDropped()) {
        return {ErrorCodes::NamespaceNotSharded, str::stream() << ns.ns() << " is not sharded"};
    }

    const auto& shardKeyPattern = collDoc.getKeyPattern();
    const auto& shardKeyBSON = shardKeyPattern.toBSON();
    *shardKeyPatternOut = shardKeyPattern;

    if (!range.getMin().isFieldNamePrefixOf(shardKeyBSON)) {
        return {ErrorCodes::ShardKeyNotFound,
                str::stream() << "min: " << range.getMin() << " is not a prefix of the shard key "
                              << shardKeyBSON
                              << " of ns: "
                              << ns.ns()};
    }

    if (!range.getMax().isFieldNamePrefixOf(shardKeyBSON)) {
        return {ErrorCodes::ShardKeyNotFound,
                str::stream() << "max: " << range.getMax() << " is not a prefix of the shard key "
                              << shardKeyBSON
                              << " of ns: "
                              << ns.ns()};
    }

    return ChunkRange(shardKeyPattern.extendRangeBound(range.getMin(), false),
                      shardKeyPattern.extendRangeBound(range.getMax(), false));
}

BSONArray buildMergeChunksApplyOpsUpdates(const std::vector<ChunkType>& chunksToMerge,
                                          const ChunkVersion& mergeVersion) {
    BSONArrayBuilder updates;

    // Build an update operation to expand the first chunk into the newly merged chunk
    {
        BSONObjBuilder op;
        op.append("op", "u");
        op.appendBool("b", false);  // no upsert
        op.append("ns", ChunkType::ConfigNS);

        // expand first chunk into newly merged chunk
        ChunkType mergedChunk(chunksToMerge.front());
        mergedChunk.setMax(chunksToMerge.back().getMax());

        // fill in additional details for sending through applyOps
        mergedChunk.setVersion(mergeVersion);

        // add the new chunk information as the update object
        op.append("o", mergedChunk.toBSON());

        // query object
        op.append("o2", BSON(ChunkType::name(mergedChunk.getName())));

        updates.append(op.obj());
    }

    // Build update operations to delete the rest of the chunks to be merged. Remember not
    // to delete the first chunk we're expanding
    for (size_t i = 1; i < chunksToMerge.size(); ++i) {
        BSONObjBuilder op;
        op.append("op", "d");
        op.append("ns", ChunkType::ConfigNS);

        op.append("o", BSON(ChunkType::name(chunksToMerge[i].getName())));

        updates.append(op.obj());
    }

    return updates.arr();
}

BSONArray buildMergeChunksApplyOpsPrecond(const std::vector<ChunkType>& chunksToMerge,
                                          const ChunkVersion& collVersion) {
    BSONArrayBuilder preCond;

    for (auto chunk : chunksToMerge) {
        BSONObjBuilder b;
        b.append("ns", ChunkType::ConfigNS);
        b.append(
            "q",
            BSON("query" << BSON(ChunkType::ns(chunk.getNS()) << ChunkType::min(chunk.getMin())
                                                              << ChunkType::max(chunk.getMax()))
                         << "orderby"
                         << BSON(ChunkType::DEPRECATED_lastmod() << -1)));
        b.append("res",
                 BSON(ChunkType::DEPRECATED_epoch(collVersion.epoch())
                      << ChunkType::shard(chunk.getShard().toString())));
        preCond.append(b.obj());
    }
    return preCond.arr();
}

}  // namespace

ShardingCatalogManagerImpl::ShardingCatalogManagerImpl(
    ShardingCatalogClient* catalogClient, std::unique_ptr<executor::TaskExecutor> addShardExecutor)
    : _catalogClient(catalogClient), _executorForAddShard(std::move(addShardExecutor))
{
     _shardServerManager = new ShardServerManager();
     //must new rootFolderManager instance
     _rootFolderManager = new RootFolderManager();
    _stateMachine = new StateMachine();
}

ShardingCatalogManagerImpl::~ShardingCatalogManagerImpl() {
    delete(_rootFolderManager);
    delete(_shardServerManager);
    delete(_stateMachine);
}

Status ShardingCatalogManagerImpl::startup() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    if (_started) {
        return Status::OK();
    }
    _started = true;
    _executorForAddShard->startup();
    return Status::OK();
}

void ShardingCatalogManagerImpl::shutDown(OperationContext* txn) {
    LOG(1) << "ShardingCatalogManagerImpl::shutDown() called.";
    {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        _inShutdown = true;
    }

    _executorForAddShard->shutdown();
    _executorForAddShard->join();
}

StatusWith<Shard::CommandResponse> ShardingCatalogManagerImpl::_runCommandForAddShard(
    OperationContext* txn,
    RemoteCommandTargeter* targeter,
    const std::string& dbName,
    const BSONObj& cmdObj) {
    auto host = targeter->findHost(txn, ReadPreferenceSetting{ReadPreference::PrimaryOnly});
    if (!host.isOK()) {
        return host.getStatus();
    }

    executor::RemoteCommandRequest request(
        host.getValue(), dbName, cmdObj, rpc::makeEmptyMetadata(), nullptr, 
        Milliseconds(kIsMasterTimeoutMS));
    executor::RemoteCommandResponse swResponse =
        Status(ErrorCodes::InternalError, "Internal error running command");

    auto callStatus = _executorForAddShard->scheduleRemoteCommand(
        request, [&swResponse](const executor::TaskExecutor::RemoteCommandCallbackArgs& args) {
            swResponse = args.response;
        });
    if (!callStatus.isOK()) {
        return callStatus.getStatus();
    }

    // Block until the command is carried out
    _executorForAddShard->wait(callStatus.getValue());

    if (!swResponse.isOK()) {
        if (swResponse.status.compareCode(ErrorCodes::ExceededTimeLimit)) {
            LOG(0) << "Operation for addShard timed out with status " << swResponse.status;
        }
        if (!Shard::shouldErrorBePropagated(swResponse.status.code())) {
            swResponse.status = {ErrorCodes::OperationFailed,
                                 stream() << "failed to run command " << cmdObj
                                          << " when attempting to add shard "
                                          << targeter->connectionString().toString()
                                          << causedBy(swResponse.status)};
        }
        return swResponse.status;
    }

    BSONObj responseObj = swResponse.data.getOwned();
    BSONObj responseMetadata = swResponse.metadata.getOwned();

    Status commandStatus = getStatusFromCommandResult(responseObj);
    if (!Shard::shouldErrorBePropagated(commandStatus.code())) {
        commandStatus = {ErrorCodes::OperationFailed,
                         stream() << "failed to run command " << cmdObj
                                  << " when attempting to add shard "
                                  << targeter->connectionString().toString()
                                  << causedBy(commandStatus)};
    }

    Status writeConcernStatus = getWriteConcernStatusFromCommandResult(responseObj);
    if (!Shard::shouldErrorBePropagated(writeConcernStatus.code())) {
        writeConcernStatus = {ErrorCodes::OperationFailed,
                              stream() << "failed to satisfy writeConcern for command " << cmdObj
                                       << " when attempting to add shard "
                                       << targeter->connectionString().toString()
                                       << causedBy(writeConcernStatus)};
    }

    return Shard::CommandResponse(std::move(responseObj),
                                  std::move(responseMetadata),
                                  std::move(commandStatus),
                                  std::move(writeConcernStatus));
}

StatusWith<boost::optional<ShardType>> ShardingCatalogManagerImpl::_checkIfShardExists(
    OperationContext* txn,
    const std::string& shardName,
    const ConnectionString& shardConnectionString,
    long long maxSize,
    const std::string& processIdentity) {
    HostAndPort hostandport = shardConnectionString.getServers()[0];
    const auto findShardStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                txn,
                kConfigPrimarySelector,
                repl::ReadConcernLevel::kLocalReadConcern,
                NamespaceString(ShardType::ConfigNS),
                BSON(ShardType::host() << BSON("$regex" << hostandport.toString()) << 
                     ShardType::processIdentity() << processIdentity),
                BSONObj(),
                boost::none); // no limit

    if (!findShardStatus.isOK()) {
        return Status(findShardStatus.getStatus().code(),
                      str::stream() << "Failed to find existing shards during addShard"
                                    << causedBy(findShardStatus.getStatus()));
    }

    const auto shardDocs = findShardStatus.getValue().docs;

    if (shardDocs.size() > 1) {
        return Status(ErrorCodes::TooManyMatchingDocuments,
            str::stream() << "More than one document for shard " <<
                shardName << " in config databases");
    }

    if (shardDocs.size() == 0) {
        return Status(ErrorCodes::ShardNotFound,
                str::stream() << "Shard " << shardName << " does not exist");
    }

    auto existingShardParseStatus = ShardType::fromBSON(shardDocs.front());
    if (!existingShardParseStatus.isOK()) {
        return existingShardParseStatus.getStatus();
    }

    ShardType existingShard = existingShardParseStatus.getValue();

    auto swExistingShardConnStr = ConnectionString::parse(existingShard.getHost());
    if (!swExistingShardConnStr.isOK()) {
        return swExistingShardConnStr.getStatus();
    }
    auto existingShardConnStr = std::move(swExistingShardConnStr.getValue());

    // Function for determining if the options for the shard that is being added match the
    // options of an existing shard that conflicts with it.
    auto shardsAreEquivalent = [&]() {
        /*if (shardName != existingShard.getName()) {
            return false;
        }*/
        if (shardConnectionString != existingShardConnStr) {
            return false;
        }
        if (maxSize != existingShard.getMaxSizeMB()) {
            return false;
        }
        return true;
    };

    if (existingShard.getState() == ShardType::ShardState::kShardRegistering ||
        existingShard.getState() == ShardType::ShardState::kShardRestarting) {
        if (shardsAreEquivalent()) {
            return {boost::none};
        }
        else {
            return Status(ErrorCodes::IllegalOperation,
                str::stream() << "A shard (state: NotShardAware) named " << shardName
                    << " with different setting " << existingShard.toBSON().toString()
                    << " already exists, current " << shardConnectionString.toString());
        }
    }
    else { // existingShard.getState() == ShardType::ShardState::kShardActive
        if (shardsAreEquivalent()) {
            return {existingShard};
        }
        else {
            return Status(ErrorCodes::IllegalOperation,
                str::stream() << "A shard (state: kShardActive) named " << shardName 
                    << " with different setting " << existingShard.toBSON().toString()
                    << " already exists");
        }
    }
}

StatusWith<ShardType> ShardingCatalogManagerImpl::_validateHostAsShard(
    OperationContext* txn,
    std::shared_ptr<RemoteCommandTargeter> targeter,
    const std::string* shardProposedName,
    const ConnectionString& connectionString) {

    // Check if the node being added is a mongos or a version of mongod too old to speak the current
    // communication protocol.
    auto swCommandResponse =
        _runCommandForAddShard(txn, targeter.get(), "admin", BSON("isMaster" << 1));
    if (!swCommandResponse.isOK()) {
        if (swCommandResponse.getStatus() == ErrorCodes::RPCProtocolNegotiationFailed) {
            // Mongos to mongos commands are no longer supported in the wire protocol
            // (because mongos does not support OP_COMMAND), similarly for a new mongos
            // and an old mongod. So the call will fail in such cases.
            // TODO: If/When mongos ever supports opCommands, this logic will break because
            // cmdStatus will be OK.
            return {ErrorCodes::RPCProtocolNegotiationFailed,
                    str::stream() << targeter->connectionString().toString()
                                  << " does not recognize the RPC protocol being used. This is"
                                  << " likely because it contains a node that is a mongos or an old"
                                  << " version of mongod."};
        } else {
            return swCommandResponse.getStatus();
        }
    }

    // Check for a command response error
    auto resIsMasterStatus = std::move(swCommandResponse.getValue().commandStatus);
    if (!resIsMasterStatus.isOK()) {
        return {resIsMasterStatus.code(),
                str::stream() << "Error running isMaster against "
                              << targeter->connectionString().toString()
                              << ": "
                              << causedBy(resIsMasterStatus)};
    }

    auto resIsMaster = std::move(swCommandResponse.getValue().response);

    // Check that the node being added is a new enough version.
    // If we're running this code, that means the mongos that the addShard request originated from
    // must be at least version 3.4 (since 3.2 mongoses don't know about the _configsvrAddShard
    // command).  Since it is illegal to have v3.4 mongoses with v3.2 shards, we should reject
    // adding any shards that are not v3.4.  We can determine this by checking that the
    // maxWireVersion reported in isMaster is at least COMMANDS_ACCEPT_WRITE_CONCERN.
    // TODO(SERVER-25623): This approach won't work to prevent v3.6 mongoses from adding v3.4
    // shards, so we'll have to rethink this during the 3.5 development cycle.

    long long maxWireVersion;
    Status status = bsonExtractIntegerField(resIsMaster, "maxWireVersion", &maxWireVersion);
    if (!status.isOK()) {
        return Status(status.code(),
                      str::stream() << "isMaster returned invalid 'maxWireVersion' "
                                    << "field when attempting to add "
                                    << connectionString.toString()
                                    << " as a shard: "
                                    << status.reason());
    }
    if (maxWireVersion < WireVersion::COMMANDS_ACCEPT_WRITE_CONCERN) {
        return Status(ErrorCodes::IncompatibleServerVersion,
                      str::stream() << "Cannot add " << connectionString.toString()
                                    << " as a shard because we detected a mongod with server "
                                       "version older than 3.4.0.  It is invalid to add v3.2 and "
                                       "older shards through a v3.4 mongos.");
    }


    // Check whether there is a master. If there isn't, the replica set may not have been
    // initiated. If the connection is a standalone, it will return true for isMaster.
    bool isMaster;
    status = bsonExtractBooleanField(resIsMaster, "ismaster", &isMaster);
    if (!status.isOK()) {
        return Status(status.code(),
                      str::stream() << "isMaster returned invalid 'ismaster' "
                                    << "field when attempting to add "
                                    << connectionString.toString()
                                    << " as a shard: "
                                    << status.reason());
    }
    if (!isMaster) {
        return {ErrorCodes::NotMaster,
                str::stream()
                    << connectionString.toString()
                    << " does not have a master. If this is a replica set, ensure that it has a"
                    << " healthy primary and that the set has been properly initiated."};
    }

    const string providedSetName = connectionString.getSetName();
    const string foundSetName = resIsMaster["setName"].str();

    // Make sure the specified replica set name (if any) matches the actual shard's replica set
    if (providedSetName.empty() && !foundSetName.empty()) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "host is part of set " << foundSetName << "; "
                              << "use replica set url format "
                              << "<setname>/<server1>,<server2>, ..."};
    }

    if (!providedSetName.empty() && foundSetName.empty()) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "host did not return a set name; "
                              << "is the replica set still initializing? "
                              << resIsMaster};
    }

    // Make sure the set name specified in the connection string matches the one where its hosts
    // belong into
    if (!providedSetName.empty() && (providedSetName != foundSetName)) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "the provided connection string (" << connectionString.toString()
                              << ") does not match the actual set name "
                              << foundSetName};
    }

    // Is it a config server?
    if (resIsMaster.hasField("configsvr")) {
        return {ErrorCodes::OperationFailed,
                str::stream() << "Cannot add " << connectionString.toString()
                              << " as a shard since it is a config server"};
    }

    // If the shard is part of a replica set, make sure all the hosts mentioned in the connection
    // string are part of the set. It is fine if not all members of the set are mentioned in the
    // connection string, though.
    if (!providedSetName.empty()) {
        std::set<string> hostSet;

        BSONObjIterator iter(resIsMaster["hosts"].Obj());
        while (iter.more()) {
            hostSet.insert(iter.next().String());  // host:port
        }

        if (resIsMaster["passives"].isABSONObj()) {
            BSONObjIterator piter(resIsMaster["passives"].Obj());
            while (piter.more()) {
                hostSet.insert(piter.next().String());  // host:port
            }
        }

        if (resIsMaster["arbiters"].isABSONObj()) {
            BSONObjIterator piter(resIsMaster["arbiters"].Obj());
            while (piter.more()) {
                hostSet.insert(piter.next().String());  // host:port
            }
        }

        vector<HostAndPort> hosts = connectionString.getServers();
        for (size_t i = 0; i < hosts.size(); i++) {
            const string host = hosts[i].toString();  // host:port
            if (hostSet.find(host) == hostSet.end()) {
                return {ErrorCodes::OperationFailed,
                        str::stream() << "in seed list " << connectionString.toString() << ", host "
                                      << host
                                      << " does not belong to replica set "
                                      << foundSetName
                                      << "; found "
                                      << resIsMaster.toString()};
            }
        }
    }

    string actualShardName;

    if (shardProposedName) {
        actualShardName = *shardProposedName;
    } else if (!foundSetName.empty()) {
        // Default it to the name of the replica set
        actualShardName = foundSetName;
    }

    // Disallow adding shard replica set with name 'config'
    if (actualShardName == "config") {
        return {ErrorCodes::BadValue, "use of shard replica set with name 'config' is not allowed"};
    }

    // Retrieve the most up to date connection string that we know from the replica set monitor (if
    // this is a replica set shard, otherwise it will be the same value as connectionString).
    ConnectionString actualShardConnStr = targeter->connectionString();

    ShardType shard;
    shard.setName(actualShardName);
    shard.setHost(actualShardConnStr.toString());
    shard.setState(ShardType::ShardState::kShardActive);

    return shard;
}

StatusWith<std::vector<std::string>> ShardingCatalogManagerImpl::_getDBNamesListFromShard(
    OperationContext* txn, std::shared_ptr<RemoteCommandTargeter> targeter) {

    auto swCommandResponse =
        _runCommandForAddShard(txn, targeter.get(), "admin", BSON("listDatabases" << 1));
    if (!swCommandResponse.isOK()) {
        return swCommandResponse.getStatus();
    }

    auto cmdStatus = std::move(swCommandResponse.getValue().commandStatus);
    if (!cmdStatus.isOK()) {
        return cmdStatus;
    }

    auto cmdResult = std::move(swCommandResponse.getValue().response);

    vector<string> dbNames;

    for (const auto& dbEntry : cmdResult["databases"].Obj()) {
        const string& dbName = dbEntry["name"].String();

        if (!(dbName == "local" || dbName == "admin")) {
            dbNames.push_back(dbName);
        }
    }

    return dbNames;
}


StatusWith<ShardType> ShardingCatalogManagerImpl::findShardByHost(OperationContext* txn,const std::string& ns,const std::string& host)
{
     auto findShardStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            kConfigReadSelector,
            repl::ReadConcernLevel::kLocalReadConcern,
            NamespaceString(ns),
            BSON(ShardType::host() << BSON("$regex" << host)),
            BSONObj(),
            boost::none); // no limit
    
            
    if (!findShardStatus.isOK()) {
        return findShardStatus.getStatus();
    }
    const auto shardDocs = findShardStatus.getValue().docs;
    if (shardDocs.size() > 1) {
        return Status(ErrorCodes::TooManyMatchingDocuments,
                      str::stream() << "more than one shard document found for host " 
                                    << host << " in config databases");
    }
    if (shardDocs.size() == 0) {
        return Status(ErrorCodes::ShardNotFound,
                str::stream() << "Shard " << host << " does not exist");
    }
    
    auto shardDocStatus = ShardType::fromBSON(shardDocs.front());
    if (!shardDocStatus.isOK()) {
         return shardDocStatus.getStatus();
    } 
    return shardDocStatus;
}

StatusWith<string> ShardingCatalogManagerImpl::addShard(
    OperationContext* txn,
    const std::string &shardName,
    const ConnectionString& shardConnectionString,
    const long long maxSize,
    const std::string& processIdentity) {
    /*if (shardConnectionString.type() != ConnectionString::SET) {
        return Status(ErrorCodes::BadValue,
            str::stream() << "Connection string type is not a set: "
                << shardConnectionString.toString());
    }*/
    //if we specify shardName to add shard , it will return error
    /*if (shardName != shardConnectionString.getSetName()) {
        return Status(ErrorCodes::BadValue,
            str::stream() << "Shard name (" << shardName
                << ") does not match set name in connection string: "
                << shardConnectionString.toString());
    }*/
    LOG(0) << "addShard : " << shardName;
    // Only one addShard operation can be in progress at a time.
    Lock::ExclusiveLock lk(txn->lockState(), _kShardMembershipLock);

    // Check if this shard has already been added (can happen in the case of a retry after a network
    // error, for example) and thus this addShard request should be considered a no-op.
    auto existingShard =
        _checkIfShardExists(txn, shardName, shardConnectionString, maxSize, processIdentity);
    if (!existingShard.isOK()) {
        LOG(0) << "check shard exists failed";
        return existingShard.getStatus();
    }
    if (existingShard.getValue()) {
        // These hosts already belong to an existing shard, so report success and terminate the
        // addShard request.  Make sure to set the last optime for the client to the system last
        // optime so that we'll still wait for replication so that this state is visible in the
        // committed snapshot.
        repl::ReplClientInfo::forClient(txn->getClient()).setLastOpToSystemLastOpTime(txn);
        return existingShard.getValue()->getName();
    }

    HostAndPort hostAndPort = shardConnectionString.getServers()[0];
    auto findShardStatus = findShardByHost(txn, ShardType::ConfigNS, hostAndPort.toString());
    if (!findShardStatus.isOK()){
        return findShardStatus.getStatus();
    }
    auto shardInShards = findShardStatus.getValue();
    bool isRestart = false;
    if (shardInShards.getState() == ShardType::ShardState::kShardRestarting) {
        isRestart = true;
    }
    // TODO: Don't create a detached Shard object, create a detached RemoteCommandTargeter instead.
    std::shared_ptr<Shard> shard;
    shard = Grid::get(txn)->shardRegistry()->createConnection(shardConnectionString);
    invariant(shard);
    auto targeter = shard->getTargeter();
    auto stopMonitoringGuard = MakeGuard([&] {
        if (shardConnectionString.type() == ConnectionString::SET) {
            // This is a workaround for the case were we could have some bad shard being
            // requested to be added and we put that bad connection string on the global replica set
            // monitor registry. It needs to be cleaned up so that when a correct replica set is
            // added, it will be recreated.
            ReplicaSetMonitor::remove(shardConnectionString.getSetName());
        }
    });

    // Check that none of the existing shard candidate's dbs exist already
    auto dbNamesStatus = _getDBNamesListFromShard(txn, targeter);
    if (!dbNamesStatus.isOK()) {
        return dbNamesStatus.getStatus();
    }

    for (const string& dbName : dbNamesStatus.getValue()) {
        auto dbt = _catalogClient->getDatabase(txn, dbName);
        if (dbt.isOK()) {
            const auto& dbDoc = dbt.getValue().value;
            return Status(ErrorCodes::OperationFailed,
                          str::stream() << "can't add shard "
                                        << "'"
                                        << shardConnectionString.toString()
                                        << "'"
                                        << " because a local database '"
                                        << dbName
                                        << "' exists in another "
                                        << dbDoc.getPrimary());
        } else if (dbt != ErrorCodes::NamespaceNotFound) {
            return dbt.getStatus();
        }
    }

    // If the minimum allowed version for the cluster is 3.4, set the featureCompatibilityVersion to
    // 3.4 on the shard.
    if (serverGlobalParams.featureCompatibility.version.load() ==
        ServerGlobalParams::FeatureCompatibility::Version::k34) {
        auto versionResponse =
            _runCommandForAddShard(txn,
                                   targeter.get(),
                                   "admin",
                                   BSON(FeatureCompatibilityVersion::kCommandName
                                        << FeatureCompatibilityVersionCommandParser::kVersion34));
        if (!versionResponse.isOK()) {
            return versionResponse.getStatus();
        }

        if (!versionResponse.getValue().commandStatus.isOK()) {
            if (versionResponse.getStatus().code() == ErrorCodes::CommandNotFound) {
                return Status(ErrorCodes::OperationFailed,
                              "featureCompatibilityVersion for cluster is 3.4, cannot add a shard "
                              "with version below 3.4. See "
                              "http://dochub.mongodb.org/core/3.4-feature-compatibility.");
            }
            return versionResponse.getValue().commandStatus;
        }
    }
    if (!MONGO_FAIL_POINT(dontUpsertShardIdentityOnNewShards)) {
        auto commandRequest = createShardIdentityUpsertForAddShard(txn, shardName);

        LOG(1) << "going to insert shardIdentity document for shard " << shardName;

        auto swCommandResponse =
            _runCommandForAddShard(txn, targeter.get(), "admin", commandRequest);
        if (!swCommandResponse.isOK()) {
            return swCommandResponse.getStatus();
        }

        auto commandResponse = std::move(swCommandResponse.getValue());

        BatchedCommandResponse batchResponse;
        auto batchResponseStatus =
            Shard::CommandResponse::processBatchWriteResponse(commandResponse, &batchResponse);
        if (!batchResponseStatus.isOK()) {
            return batchResponseStatus;
        }
    }
   
    LOG(2) << "going to update state for shard " << shardName 
           << " in config.shards during addShard";
   /*ConnectionString fakeConnectionString(
                ConnectionString::SET,
                shardConnectionString.getServers(),
                shardName);*/
    shardInShards.setName(shardName);
    shardInShards.setState(ShardType::ShardState::kShardActive);
    shardInShards.setHost(shardConnectionString.toString());

    auto updateShardStatus = updateShardStateInConfig(txn, shardInShards, ShardType::ConfigNS,hostAndPort.toString());
    if (!updateShardStatus.isOK()) {
        return {updateShardStatus.getStatus().code(),
                      str::stream() << "couldn't updating shard: " << shardName
                      << " during addShard;" << causedBy(updateShardStatus.getStatus())};
    }


    auto updateShardMapStatus = Grid::get(txn)->catalogManager()->getShardServerManager()->updateShardInMemory(
                                                                                        hostAndPort,
                                                                                        shardInShards);
    if (!updateShardMapStatus.isOK()) {
        log() << "fail to update shard state in shardMap cause by " << updateShardMapStatus.getStatus();
        return updateShardMapStatus.getStatus();
    } 
 
    // Add all databases which were discovered on the new shard
    for (const string& dbName : dbNamesStatus.getValue()) {
        DatabaseType dbt;
        dbt.setName(dbName);
        dbt.setPrimary(shardName);
        dbt.setSharded(false);

        Status status = _catalogClient->updateDatabase(txn, dbName, dbt);
        if (!status.isOK()) {
            log() << "adding shard " << shardConnectionString.toString()
                  << " even though could not add database " << dbName;
        }
    }
 

     // Ensure the added shard is visible to this process.
    auto shardRegistry = Grid::get(txn)->shardRegistry();
    if (!shardRegistry->getShard(txn, shardName).isOK()) {
        return {ErrorCodes::OperationFailed,
                "Could not find shard metadata for shard after adding it. This most likely "
                "indicates that the shard was removed immediately after it was added."};
    }
    stopMonitoringGuard.Dismiss();
   
    // Record in changelog
    BSONObjBuilder shardDetails;
    shardDetails.append("name", shardName);
    shardDetails.append("state", 
        static_cast<std::underlying_type<ShardType::ShardState>::type>(
            ShardType::ShardState::kShardActive));
    _catalogClient->logChange(
        txn, "addShard", "", shardDetails.obj(), ShardingCatalogClient::kMajorityWriteConcern);

    
    return shardName;
}


StatusWith<std::string> ShardingCatalogManagerImpl::updateShardStateInConfig(OperationContext* txn, ShardType shard, const std::string& ns,const std::string& host){

    auto findShardStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            kConfigPrimarySelector,
            repl::ReadConcernLevel::kLocalReadConcern,
            NamespaceString(ns),
            BSON(ShardType::host() << BSON("$regex" << host)),
            BSONObj(),
            boost::none); // no limit
            
    if (!findShardStatus.isOK()) {
        return findShardStatus.getStatus();
    }

    const auto shardDocs = findShardStatus.getValue().docs;

    if (shardDocs.size() > 1) {
        return Status(ErrorCodes::TooManyMatchingDocuments,
            str::stream() << "more than one document for shard " <<
                host << " in config databases");
    }

    if (shardDocs.size() == 0) {
        return Status(ErrorCodes::ShardNotFound,
                str::stream() << "host " << host << " does not exist");
    }

    auto shardDocStatus = ShardType::fromBSON(shardDocs.front());
    if (!shardDocStatus.isOK()) {
        return shardDocStatus.getStatus();
    }

    auto removeStatus = Grid::get(txn)->catalogClient(txn)->removeConfigDocuments(
        txn,
        ns,
        BSON(ShardType::host() << BSON("$regex" << host)),
        ShardingCatalogClient::kMajorityWriteConcern);

    if (!removeStatus.isOK()) {
        return removeStatus;
    }

    
    auto insertStatus = Grid::get(txn)->catalogClient(txn)->insertConfigDocument(
            txn,
            ns,
            shard.toBSON(),
            ShardingCatalogClient::kMajorityWriteConcern);
    
    if (!insertStatus.isOK()) {
        return insertStatus;
    }

    // Record in changelog
    BSONObjBuilder shardDetails;
    shardDetails.append("name", shard.getName());
    shardDetails.append("state", 
        static_cast<std::underlying_type<ShardType::ShardState>::type>(
            ShardType::ShardState::kShardActive));
    _catalogClient->logChange(
        txn, "updateShardDuringAddShard", "", shardDetails.obj(), 
            ShardingCatalogClient::kMajorityWriteConcern);

    return shard.getName();
}


Status ShardingCatalogManagerImpl::addShardToZone(OperationContext* txn,
                                                  const std::string& shardName,
                                                  const std::string& zoneName) {
    Lock::ExclusiveLock lk(txn->lockState(), _kZoneOpLock);

    auto updateStatus = _catalogClient->updateConfigDocument(
        txn,
        ShardType::ConfigNS,
        BSON(ShardType::name(shardName)),
        BSON("$addToSet" << BSON(ShardType::tags() << zoneName)),
        false,
        kNoWaitWriteConcern);

    if (!updateStatus.isOK()) {
        return updateStatus.getStatus();
    }

    if (!updateStatus.getValue()) {
        return {ErrorCodes::ShardNotFound,
                str::stream() << "shard " << shardName << " does not exist"};
    }

    return Status::OK();
}

Status ShardingCatalogManagerImpl::removeShardFromZone(OperationContext* txn,
                                                       const std::string& shardName,
                                                       const std::string& zoneName) {
    Lock::ExclusiveLock lk(txn->lockState(), _kZoneOpLock);

    auto configShard = Grid::get(txn)->shardRegistry()->getConfigShard();
    const NamespaceString shardNS(ShardType::ConfigNS);

    //
    // Check whether the shard even exist in the first place.
    //

    auto findShardExistsStatus =
        configShard->exhaustiveFindOnConfig(txn,
                                            kConfigPrimarySelector,
                                            repl::ReadConcernLevel::kLocalReadConcern,
                                            shardNS,
                                            BSON(ShardType::name() << shardName),
                                            BSONObj(),
                                            1);

    if (!findShardExistsStatus.isOK()) {
        return findShardExistsStatus.getStatus();
    }

    if (findShardExistsStatus.getValue().docs.size() == 0) {
        return {ErrorCodes::ShardNotFound,
                str::stream() << "shard " << shardName << " does not exist"};
    }

    //
    // Check how many shards belongs to this zone.
    //

    auto findShardStatus =
        configShard->exhaustiveFindOnConfig(txn,
                                            kConfigPrimarySelector,
                                            repl::ReadConcernLevel::kLocalReadConcern,
                                            shardNS,
                                            BSON(ShardType::tags() << zoneName),
                                            BSONObj(),
                                            2);

    if (!findShardStatus.isOK()) {
        return findShardStatus.getStatus();
    }

    const auto shardDocs = findShardStatus.getValue().docs;

    if (shardDocs.size() == 0) {
        // The zone doesn't exists, this could be a retry.
        return Status::OK();
    }

    if (shardDocs.size() == 1) {
        auto shardDocStatus = ShardType::fromBSON(shardDocs.front());
        if (!shardDocStatus.isOK()) {
            return shardDocStatus.getStatus();
        }

        auto shardDoc = shardDocStatus.getValue();
        if (shardDoc.getName() != shardName) {
            // The last shard that belongs to this zone is a different shard.
            // This could be a retry, so return OK.
            return Status::OK();
        }

        auto findChunkRangeStatus =
            configShard->exhaustiveFindOnConfig(txn,
                                                kConfigPrimarySelector,
                                                repl::ReadConcernLevel::kLocalReadConcern,
                                                NamespaceString(TagsType::ConfigNS),
                                                BSON(TagsType::tag() << zoneName),
                                                BSONObj(),
                                                1);

        if (!findChunkRangeStatus.isOK()) {
            return findChunkRangeStatus.getStatus();
        }

        if (findChunkRangeStatus.getValue().docs.size() > 0) {
            return {ErrorCodes::ZoneStillInUse,
                    "cannot remove a shard from zone if a chunk range is associated with it"};
        }
    }

    //
    // Perform update.
    //

    auto updateStatus =
        _catalogClient->updateConfigDocument(txn,
                                             ShardType::ConfigNS,
                                             BSON(ShardType::name(shardName)),
                                             BSON("$pull" << BSON(ShardType::tags() << zoneName)),
                                             false,
                                             kNoWaitWriteConcern);

    if (!updateStatus.isOK()) {
        return updateStatus.getStatus();
    }

    // The update did not match a document, another thread could have removed it.
    if (!updateStatus.getValue()) {
        return {ErrorCodes::ShardNotFound,
                str::stream() << "shard " << shardName << " no longer exist"};
    }

    return Status::OK();
}


Status ShardingCatalogManagerImpl::assignKeyRangeToZone(OperationContext* txn,
                                                        const NamespaceString& ns,
                                                        const ChunkRange& givenRange,
                                                        const string& zoneName) {
    Lock::ExclusiveLock lk(txn->lockState(), _kZoneOpLock);

    auto configServer = Grid::get(txn)->shardRegistry()->getConfigShard();

    KeyPattern shardKeyPattern{BSONObj()};
    auto fullShardKeyStatus =
        includeFullShardKey(txn, configServer.get(), ns, givenRange, &shardKeyPattern);
    if (!fullShardKeyStatus.isOK()) {
        return fullShardKeyStatus.getStatus();
    }

    const auto& fullShardKeyRange = fullShardKeyStatus.getValue();

    auto zoneExistStatus =
        configServer->exhaustiveFindOnConfig(txn,
                                             kConfigPrimarySelector,
                                             repl::ReadConcernLevel::kLocalReadConcern,
                                             NamespaceString(ShardType::ConfigNS),
                                             BSON(ShardType::tags() << zoneName),
                                             BSONObj(),
                                             1);

    if (!zoneExistStatus.isOK()) {
        return zoneExistStatus.getStatus();
    }

    auto zoneExist = zoneExistStatus.getValue().docs.size() > 0;
    if (!zoneExist) {
        return {ErrorCodes::ZoneNotFound,
                str::stream() << "zone " << zoneName << " does not exist"};
    }

    auto overlapStatus = checkForOveralappedZonedKeyRange(
        txn, configServer.get(), ns, fullShardKeyRange, zoneName, shardKeyPattern);
    if (!overlapStatus.isOK()) {
        return overlapStatus;
    }

    BSONObj updateQuery(
        BSON("_id" << BSON(TagsType::ns(ns.ns()) << TagsType::min(fullShardKeyRange.getMin()))));

    BSONObjBuilder updateBuilder;
    updateBuilder.append("_id",
                         BSON(TagsType::ns(ns.ns()) << TagsType::min(fullShardKeyRange.getMin())));
    updateBuilder.append(TagsType::ns(), ns.ns());
    updateBuilder.append(TagsType::min(), fullShardKeyRange.getMin());
    updateBuilder.append(TagsType::max(), fullShardKeyRange.getMax());
    updateBuilder.append(TagsType::tag(), zoneName);

    auto updateStatus = _catalogClient->updateConfigDocument(
        txn, TagsType::ConfigNS, updateQuery, updateBuilder.obj(), true, kNoWaitWriteConcern);

    if (!updateStatus.isOK()) {
        return updateStatus.getStatus();
    }

    return Status::OK();
}

Status ShardingCatalogManagerImpl::removeKeyRangeFromZone(OperationContext* txn,
                                                          const NamespaceString& ns,
                                                          const ChunkRange& range) {
    Lock::ExclusiveLock lk(txn->lockState(), _kZoneOpLock);

    auto configServer = Grid::get(txn)->shardRegistry()->getConfigShard();

    KeyPattern shardKeyPattern{BSONObj()};
    auto fullShardKeyStatus =
        includeFullShardKey(txn, configServer.get(), ns, range, &shardKeyPattern);
    if (!fullShardKeyStatus.isOK()) {
        return fullShardKeyStatus.getStatus();
    }

    BSONObjBuilder removeBuilder;
    removeBuilder.append("_id", BSON(TagsType::ns(ns.ns()) << TagsType::min(range.getMin())));
    removeBuilder.append(TagsType::max(), range.getMax());

    return _catalogClient->removeConfigDocuments(
        txn, TagsType::ConfigNS, removeBuilder.obj(), kNoWaitWriteConcern);
}

Status ShardingCatalogManagerImpl::commitChunkSplit(OperationContext* txn,
                                                    const NamespaceString& ns,
                                                    const OID& requestEpoch,
                                                    const ChunkRange& range,
                                                    const std::vector<BSONObj>& splitPoints,
                                                    const std::string& shardName) {
    // Take _kChunkOpLock in exclusive mode to prevent concurrent chunk splits, merges, and
    // migrations
    // TODO(SERVER-25359): Replace with a collection-specific lock map to allow splits/merges/
    // move chunks on different collections to proceed in parallel
    Lock::ExclusiveLock lk(txn->lockState(), _kChunkOpLock);

    // Acquire GlobalLock in MODE_X twice to prevent yielding.
    // GlobalLock and the following lock on config.chunks are only needed to support
    // mixed-mode operation with mongoses from 3.2
    // TODO(SERVER-25337): Remove GlobalLock and config.chunks lock after 3.4
    Lock::GlobalLock firstGlobalLock(txn->lockState(), MODE_X, UINT_MAX);
    Lock::GlobalLock secondGlobalLock(txn->lockState(), MODE_X, UINT_MAX);

    // Acquire lock on config.chunks in MODE_X
    AutoGetCollection autoColl(txn, NamespaceString(ChunkType::ConfigNS), MODE_X);

    // Get the chunk with highest version for this namespace
    auto findStatus = grid.shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
        txn,
        ReadPreferenceSetting{ReadPreference::PrimaryOnly},
        repl::ReadConcernLevel::kLocalReadConcern,
        NamespaceString(ChunkType::ConfigNS),
        BSON("ns" << ns.ns()),
        BSON(ChunkType::DEPRECATED_lastmod << -1),
        1);

    if (!findStatus.isOK()) {
        return findStatus.getStatus();
    }

    const auto& chunksVector = findStatus.getValue().docs;
    if (chunksVector.empty())
        return {ErrorCodes::IllegalOperation,
                "collection does not exist, isn't sharded, or has no chunks"};

    ChunkVersion collVersion =
        ChunkVersion::fromBSON(chunksVector.front(), ChunkType::DEPRECATED_lastmod());

    // Return an error if epoch of chunk does not match epoch of request
    if (collVersion.epoch() != requestEpoch) {
        return {ErrorCodes::StaleEpoch,
                "epoch of chunk does not match epoch of request. This most likely means "
                "that the collection was dropped and re-created."};
    }

    std::vector<ChunkType> newChunks;

    ChunkVersion currentMaxVersion = collVersion;

    auto startKey = range.getMin();
    auto newChunkBounds(splitPoints);
    newChunkBounds.push_back(range.getMax());

    BSONArrayBuilder updates;

    for (const auto& endKey : newChunkBounds) {
        // Verify the split points are all within the chunk
        if (endKey.woCompare(range.getMax()) != 0 && !range.containsKey(endKey)) {
            return {ErrorCodes::InvalidOptions,
                    str::stream() << "Split key " << endKey << " not contained within chunk "
                                  << range.toString()};
        }

        // Verify the split points came in increasing order
        if (endKey.woCompare(startKey) < 0) {
            return {
                ErrorCodes::InvalidOptions,
                str::stream() << "Split keys must be specified in strictly increasing order. Key "
                              << endKey
                              << " was specified after "
                              << startKey
                              << "."};
        }

        // Verify that splitPoints are not repeated
        if (endKey.woCompare(startKey) == 0) {
            return {ErrorCodes::InvalidOptions,
                    str::stream() << "Split on lower bound of chunk "
                                  << ChunkRange(startKey, endKey).toString()
                                  << "is not allowed"};
        }

        // verify that splits don't create too-big shard keys
        Status shardKeyStatus = ShardKeyPattern::checkShardKeySize(endKey);
        if (!shardKeyStatus.isOK()) {
            return shardKeyStatus;
        }

        // splits only update the 'minor' portion of version
        currentMaxVersion.incMinor();

        // build an update operation against the chunks collection of the config database
        // with upsert true
        BSONObjBuilder op;
        op.append("op", "u");
        op.appendBool("b", true);
        op.append("ns", ChunkType::ConfigNS);

        // add the modified (new) chunk information as the update object
        BSONObjBuilder n(op.subobjStart("o"));
        n.append(ChunkType::name(), ChunkType::genID(ns.ns(), startKey));
        currentMaxVersion.addToBSON(n, ChunkType::DEPRECATED_lastmod());
        n.append(ChunkType::ns(), ns.ns());
        n.append(ChunkType::min(), startKey);
        n.append(ChunkType::max(), endKey);
        n.append(ChunkType::shard(), shardName);
        n.done();

        // add the chunk's _id as the query part of the update statement
        BSONObjBuilder q(op.subobjStart("o2"));
        q.append(ChunkType::name(), ChunkType::genID(ns.ns(), startKey));
        q.done();

        updates.append(op.obj());

        // remember this chunk info for logging later
        ChunkType chunk;
        chunk.setMin(startKey);
        chunk.setMax(endKey);
        chunk.setVersion(currentMaxVersion);

        newChunks.push_back(std::move(chunk));

        startKey = endKey;
    }

    BSONArrayBuilder preCond;
    {
        BSONObjBuilder b;
        b.append("ns", ChunkType::ConfigNS);
        b.append("q",
                 BSON("query" << BSON(ChunkType::ns(ns.ns()) << ChunkType::min() << range.getMin()
                                                             << ChunkType::max()
                                                             << range.getMax())
                              << "orderby"
                              << BSON(ChunkType::DEPRECATED_lastmod() << -1)));
        {
            BSONObjBuilder bb(b.subobjStart("res"));
            bb.append(ChunkType::DEPRECATED_epoch(), requestEpoch);
            bb.append(ChunkType::shard(), shardName);
        }
        preCond.append(b.obj());
    }

    // apply the batch of updates to remote and local metadata
    Status applyOpsStatus =
        grid.catalogClient(txn)->applyChunkOpsDeprecated(txn,
                                                         updates.arr(),
                                                         preCond.arr(),
                                                         ns.ns(),
                                                         currentMaxVersion,
                                                         WriteConcernOptions(),
                                                         repl::ReadConcernLevel::kLocalReadConcern);
    if (!applyOpsStatus.isOK()) {
        return applyOpsStatus;
    }

    // log changes
    BSONObjBuilder logDetail;
    {
        BSONObjBuilder b(logDetail.subobjStart("before"));
        b.append(ChunkType::min(), range.getMin());
        b.append(ChunkType::max(), range.getMax());
        collVersion.addToBSON(b, ChunkType::DEPRECATED_lastmod());
    }

    if (newChunks.size() == 2) {
        _appendShortVersion(&logDetail.subobjStart("left"), newChunks[0]);
        _appendShortVersion(&logDetail.subobjStart("right"), newChunks[1]);

        grid.catalogClient(txn)->logChange(
            txn, "split", ns.ns(), logDetail.obj(), WriteConcernOptions());
    } else {
        BSONObj beforeDetailObj = logDetail.obj();
        BSONObj firstDetailObj = beforeDetailObj.getOwned();
        const int newChunksSize = newChunks.size();

        for (int i = 0; i < newChunksSize; i++) {
            BSONObjBuilder chunkDetail;
            chunkDetail.appendElements(beforeDetailObj);
            chunkDetail.append("number", i + 1);
            chunkDetail.append("of", newChunksSize);
            _appendShortVersion(&chunkDetail.subobjStart("chunk"), newChunks[i]);

            grid.catalogClient(txn)->logChange(
                txn, "multi-split", ns.ns(), chunkDetail.obj(), WriteConcernOptions());
        }
    }

    return applyOpsStatus;
}

Status ShardingCatalogManagerImpl::commitChunkMerge(OperationContext* txn,
                                                    const NamespaceString& ns,
                                                    const OID& requestEpoch,
                                                    const std::vector<BSONObj>& chunkBoundaries,
                                                    const std::string& shardName) {
    // This method must never be called with empty chunks to merge
    invariant(!chunkBoundaries.empty());

    // Take _kChunkOpLock in exclusive mode to prevent concurrent chunk splits, merges, and
    // migrations
    // TODO(SERVER-25359): Replace with a collection-specific lock map to allow splits/merges/
    // move chunks on different collections to proceed in parallel
    Lock::ExclusiveLock lk(txn->lockState(), _kChunkOpLock);

    // Acquire GlobalLock in MODE_X twice to prevent yielding.
    // GLobalLock and the following lock on config.chunks are only needed to support
    // mixed-mode operation with mongoses from 3.2
    // TODO(SERVER-25337): Remove GlobalLock and config.chunks lock after 3.4
    Lock::GlobalLock firstGlobalLock(txn->lockState(), MODE_X, UINT_MAX);
    Lock::GlobalLock secondGlobalLock(txn->lockState(), MODE_X, UINT_MAX);

    // Acquire lock on config.chunks in MODE_X
    AutoGetCollection autoColl(txn, NamespaceString(ChunkType::ConfigNS), MODE_X);

    // Get the chunk with the highest version for this namespace
    auto findStatus = grid.shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
        txn,
        ReadPreferenceSetting{ReadPreference::PrimaryOnly},
        repl::ReadConcernLevel::kLocalReadConcern,
        NamespaceString(ChunkType::ConfigNS),
        BSON("ns" << ns.ns()),
        BSON(ChunkType::DEPRECATED_lastmod << -1),
        1);

    if (!findStatus.isOK()) {
        return findStatus.getStatus();
    }

    const auto& chunksVector = findStatus.getValue().docs;
    if (chunksVector.empty())
        return {ErrorCodes::IllegalOperation,
                "collection does not exist, isn't sharded, or has no chunks"};

    ChunkVersion collVersion =
        ChunkVersion::fromBSON(chunksVector.front(), ChunkType::DEPRECATED_lastmod());

    // Return an error if epoch of chunk does not match epoch of request
    if (collVersion.epoch() != requestEpoch) {
        return {ErrorCodes::StaleEpoch,
                "epoch of chunk does not match epoch of request. This most likely means "
                "that the collection was dropped and re-created."};
    }

    // Build chunks to be merged
    std::vector<ChunkType> chunksToMerge;

    ChunkType itChunk;
    itChunk.setMax(chunkBoundaries.front());
    itChunk.setNS(ns.ns());
    itChunk.setShard(shardName);

    // Do not use the first chunk boundary as a max bound while building chunks
    for (size_t i = 1; i < chunkBoundaries.size(); ++i) {
        itChunk.setMin(itChunk.getMax());

        // Ensure the chunk boundaries are strictly increasing
        if (chunkBoundaries[i].woCompare(itChunk.getMin()) <= 0) {
            return {
                ErrorCodes::InvalidOptions,
                str::stream()
                    << "Chunk boundaries must be specified in strictly increasing order. Boundary "
                    << chunkBoundaries[i]
                    << " was specified after "
                    << itChunk.getMin()
                    << "."};
        }

        itChunk.setMax(chunkBoundaries[i]);
        chunksToMerge.push_back(itChunk);
    }

    ChunkVersion mergeVersion = collVersion;
    mergeVersion.incMinor();

    auto updates = buildMergeChunksApplyOpsUpdates(chunksToMerge, mergeVersion);
    auto preCond = buildMergeChunksApplyOpsPrecond(chunksToMerge, collVersion);

    // apply the batch of updates to remote and local metadata
    Status applyOpsStatus =
        grid.catalogClient(txn)->applyChunkOpsDeprecated(txn,
                                                         updates,
                                                         preCond,
                                                         ns.ns(),
                                                         mergeVersion,
                                                         WriteConcernOptions(),
                                                         repl::ReadConcernLevel::kLocalReadConcern);
    if (!applyOpsStatus.isOK()) {
        return applyOpsStatus;
    }

    // log changes
    BSONObjBuilder logDetail;
    {
        BSONArrayBuilder b(logDetail.subarrayStart("merged"));
        for (auto chunkToMerge : chunksToMerge) {
            b.append(chunkToMerge.toBSON());
        }
    }
    collVersion.addToBSON(logDetail, "prevShardVersion");
    mergeVersion.addToBSON(logDetail, "mergedVersion");

    grid.catalogClient(txn)->logChange(
        txn, "merge", ns.ns(), logDetail.obj(), WriteConcernOptions());

    return applyOpsStatus;
}

void ShardingCatalogManagerImpl::appendConnectionStats(executor::ConnectionPoolStats* stats) {
    _executorForAddShard->appendConnectionStats(stats);
}

Status ShardingCatalogManagerImpl::initializeConfigDatabaseIfNeeded(OperationContext* txn) {
    {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        if (_configInitialized) {
            return {ErrorCodes::AlreadyInitialized,
                    "Config database was previously loaded into memory"};
        }
    }

    Status status = _initConfigIndexes(txn);
    if (!status.isOK()) {
        return status;
    }

    // Make sure to write config.version last since we detect rollbacks of config.version and
    // will re-run initializeConfigDatabaseIfNeeded if that happens, but we don't detect rollback
    // of the index builds.
    status = _initConfigVersion(txn);
    if (!status.isOK()) {
        return status;
    }

    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _configInitialized = true;

    return Status::OK();
}

void ShardingCatalogManagerImpl::discardCachedConfigDatabaseInitializationState() {
    stdx::lock_guard<stdx::mutex> lk(_mutex);
    _configInitialized = false;
}

Status ShardingCatalogManagerImpl::_initConfigVersion(OperationContext* txn) {
    auto versionStatus =
        _catalogClient->getConfigVersion(txn, repl::ReadConcernLevel::kLocalReadConcern);
    if (!versionStatus.isOK()) {
        return versionStatus.getStatus();
    }

    auto versionInfo = versionStatus.getValue();
    if (versionInfo.getMinCompatibleVersion() > CURRENT_CONFIG_VERSION) {
        return {ErrorCodes::IncompatibleShardingConfigVersion,
                str::stream() << "current version v" << CURRENT_CONFIG_VERSION
                              << " is older than the cluster min compatible v"
                              << versionInfo.getMinCompatibleVersion()};
    }

    if (versionInfo.getCurrentVersion() == UpgradeHistory_EmptyVersion) {
        VersionType newVersion;
        newVersion.setClusterId(OID::gen());
        newVersion.setMinCompatibleVersion(MIN_COMPATIBLE_CONFIG_VERSION);
        newVersion.setCurrentVersion(CURRENT_CONFIG_VERSION);

        BSONObj versionObj(newVersion.toBSON());
        auto insertStatus = _catalogClient->insertConfigDocument(
            txn, VersionType::ConfigNS, versionObj, kNoWaitWriteConcern);

        return insertStatus;
    }

    if (versionInfo.getCurrentVersion() == UpgradeHistory_UnreportedVersion) {
        return {ErrorCodes::IncompatibleShardingConfigVersion,
                "Assuming config data is old since the version document cannot be found in the "
                "config server and it contains databases besides 'local' and 'admin'. "
                "Please upgrade if this is the case. Otherwise, make sure that the config "
                "server is clean."};
    }

    if (versionInfo.getCurrentVersion() < CURRENT_CONFIG_VERSION) {
        return {ErrorCodes::IncompatibleShardingConfigVersion,
                str::stream() << "need to upgrade current cluster version to v"
                              << CURRENT_CONFIG_VERSION
                              << "; currently at v"
                              << versionInfo.getCurrentVersion()};
    }

    return Status::OK();
}

Status ShardingCatalogManagerImpl::_initConfigIndexes(OperationContext* txn) {
    const bool unique = true;
    auto configShard = Grid::get(txn)->shardRegistry()->getConfigShard();

    Status result =
        configShard->createIndexOnConfig(txn,
                                         NamespaceString(ChunkType::ConfigNS),
                                         BSON(ChunkType::ns() << 1 << ChunkType::min() << 1),
                                         unique);
    if (!result.isOK()) {
        return Status(result.code(),
                      str::stream() << "couldn't create ns_1_min_1 index on config db"
                                    << causedBy(result));
    }

    result = configShard->createIndexOnConfig(
        txn,
        NamespaceString(ChunkType::ConfigNS),
        BSON(ChunkType::ns() << 1 << ChunkType::shard() << 1 << ChunkType::min() << 1),
        unique);
    if (!result.isOK()) {
        return Status(result.code(),
                      str::stream() << "couldn't create ns_1_shard_1_min_1 index on config db"
                                    << causedBy(result));
    }

    result = configShard->createIndexOnConfig(
        txn,
        NamespaceString(ChunkType::ConfigNS),
        BSON(ChunkType::ns() << 1 << ChunkType::DEPRECATED_lastmod() << 1),
        unique);
    if (!result.isOK()) {
        return Status(result.code(),
                      str::stream() << "couldn't create ns_1_lastmod_1 index on config db"
                                    << causedBy(result));
    }

    result = configShard->createIndexOnConfig(
        txn,
        NamespaceString(ChunkType::ConfigNS),
        BSON(ChunkType::status() << 1),
        false);
    if (!result.isOK()) {
        return Status(result.code(),
                      str::stream() << "couldn't create status_1 index on config db"
                                    << causedBy(result));
    }

    result = configShard->createIndexOnConfig(
        txn,
        NamespaceString(MigrationType::ConfigNS),
        BSON(MigrationType::ns() << 1 << MigrationType::min() << 1),
        unique);
    if (!result.isOK()) {
        return Status(result.code(),
                      str::stream() << "couldn't create ns_1_min_1 index on config.migrations"
                                    << causedBy(result));
    }

    result = configShard->createIndexOnConfig(
        txn, NamespaceString(ShardType::ConfigNS), BSON(ShardType::host() << 1), unique);
    if (!result.isOK()) {
        return Status(result.code(),
                      str::stream() << "couldn't create host_1 index on config db"
                                    << causedBy(result));
    }

    result = configShard->createIndexOnConfig(
        txn, NamespaceString(LocksType::ConfigNS), BSON(LocksType::lockID() << 1), !unique);
    if (!result.isOK()) {
        return Status(result.code(),
                      str::stream() << "couldn't create lock id index on config db"
                                    << causedBy(result));
    }

    result =
        configShard->createIndexOnConfig(txn,
                                         NamespaceString(LocksType::ConfigNS),
                                         BSON(LocksType::state() << 1 << LocksType::process() << 1),
                                         !unique);
    if (!result.isOK()) {
        return Status(result.code(),
                      str::stream() << "couldn't create state and process id index on config db"
                                    << causedBy(result));
    }

    result = configShard->createIndexOnConfig(
        txn, NamespaceString(LockpingsType::ConfigNS), BSON(LockpingsType::ping() << 1), !unique);
    if (!result.isOK()) {
        return Status(result.code(),
                      str::stream() << "couldn't create lockping ping time index on config db"
                                    << causedBy(result));
    }

    result = configShard->createIndexOnConfig(txn,
                                              NamespaceString(TagsType::ConfigNS),
                                              BSON(TagsType::ns() << 1 << TagsType::min() << 1),
                                              unique);
    if (!result.isOK()) {
        return Status(result.code(),
                      str::stream() << "couldn't create ns_1_min_1 index on config db"
                                    << causedBy(result));
    }

    result = configShard->createIndexOnConfig(txn,
                                              NamespaceString(TagsType::ConfigNS),
                                              BSON(TagsType::ns() << 1 << TagsType::tag() << 1),
                                              !unique);
    if (!result.isOK()) {
        return Status(result.code(),
                      str::stream() << "couldn't create ns_1_tag_1 index on config db"
                                    << causedBy(result));
    }

    return Status::OK();
}

Status ShardingCatalogManagerImpl::initializeShardingAwarenessOnUnawareShards(
    OperationContext* txn) {
    auto swShards = _getAllShardingUnawareShards(txn);
    if (!swShards.isOK()) {
        return swShards.getStatus();
    } else {
        auto shards = std::move(swShards.getValue());
        for (const auto& shard : shards) {
            auto status = upsertShardIdentityOnShard(txn, shard);
            if (!status.isOK()) {
                return status;
            }
        }
    }

    // Note: this OK status means only that tasks to initialize sharding awareness on the shards
    // were scheduled against the task executor, not that the tasks actually succeeded.
    return Status::OK();
}

StatusWith<std::vector<ShardType>> ShardingCatalogManagerImpl::_getAllShardingUnawareShards(
    OperationContext* txn) {
    std::vector<ShardType> shards;
    auto findStatus = Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
        txn,
        ReadPreferenceSetting{ReadPreference::PrimaryOnly},
        repl::ReadConcernLevel::kLocalReadConcern,
        NamespaceString(ShardType::ConfigNS),
        BSON(
            "state" << BSON("$ne" << static_cast<std::underlying_type<ShardType::ShardState>::type>(
                                ShardType::ShardState::kShardActive))),  // shard is sharding unaware
        BSONObj(),                                                      // no sort
        boost::none);                                                   // no limit
    if (!findStatus.isOK()) {
        return findStatus.getStatus();
    }

    for (const BSONObj& doc : findStatus.getValue().docs) {
        auto shardRes = ShardType::fromBSON(doc);
        if (!shardRes.isOK()) {
            return {ErrorCodes::FailedToParse,
                    stream() << "Failed to parse shard " << causedBy(shardRes.getStatus()) << doc};
        }

        Status validateStatus = shardRes.getValue().validate();
        if (!validateStatus.isOK()) {
            return {validateStatus.code(),
                    stream() << "Failed to validate shard " << causedBy(validateStatus) << doc};
        }

        shards.push_back(shardRes.getValue());
    }

    return shards;
}

Status ShardingCatalogManagerImpl::upsertShardIdentityOnShard(OperationContext* txn,
                                                              ShardType shardType) {

    auto commandRequest = createShardIdentityUpsertForAddShard(txn, shardType.getName());

    auto swConnString = ConnectionString::parse(shardType.getHost());
    if (!swConnString.isOK()) {
        return swConnString.getStatus();
    }

    // TODO: Don't create a detached Shard object, create a detached RemoteCommandTargeter
    // instead.
    const std::shared_ptr<Shard> shard{
        Grid::get(txn)->shardRegistry()->createConnection(swConnString.getValue())};
    invariant(shard);
    auto targeter = shard->getTargeter();

    _scheduleAddShardTask(
        std::move(shardType), std::move(targeter), std::move(commandRequest), false);

    return Status::OK();
}

void ShardingCatalogManagerImpl::cancelAddShardTaskIfNeeded(const ShardId& shardId) {
    stdx::lock_guard<stdx::mutex> lk(_addShardHandlesMutex);
    if (_hasAddShardHandle_inlock(shardId)) {
        auto cbHandle = _getAddShardHandle_inlock(shardId);
        _executorForAddShard->cancel(cbHandle);
        // Untrack the handle here so that if this shard is re-added before the CallbackCanceled
        // status is delivered to the callback, a new addShard task for the shard will be
        // created.
        _untrackAddShardHandle_inlock(shardId);
    }
}

void ShardingCatalogManagerImpl::_scheduleAddShardTaskUnlessCanceled(
    const CallbackArgs& cbArgs,
    const ShardType shardType,
    std::shared_ptr<RemoteCommandTargeter> targeter,
    const BSONObj commandRequest) {
    if (cbArgs.status == ErrorCodes::CallbackCanceled) {
        return;
    }
    _scheduleAddShardTask(
        std::move(shardType), std::move(targeter), std::move(commandRequest), true);
}

void ShardingCatalogManagerImpl::_scheduleAddShardTask(
    const ShardType shardType,
    std::shared_ptr<RemoteCommandTargeter> targeter,
    const BSONObj commandRequest,
    const bool isRetry) {
    stdx::lock_guard<stdx::mutex> lk(_addShardHandlesMutex);

    if (isRetry) {
        // Untrack the handle from scheduleWorkAt, and schedule a new addShard task.
        _untrackAddShardHandle_inlock(shardType.getName());
    } else {
        // We should never be able to schedule an addShard task while one is running, because
        // there is a unique index on the _id field in config.shards.
        invariant(!_hasAddShardHandle_inlock(shardType.getName()));
    }

    // Schedule the shardIdentity upsert request to run immediately, and track the handle.

    auto swHost = targeter->findHostWithMaxWait(ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                                                Milliseconds(kDefaultFindHostMaxWaitTime));
    if (!swHost.isOK()) {
        // A 3.2 mongos must have previously successfully communicated with hosts in this shard,
        // so a failure to find a host here is probably transient, and it is safe to retry.
        warning() << "Failed to find host for shard " << shardType
                  << " when trying to upsert a shardIdentity document, "
                  << causedBy(swHost.getStatus());
        const Date_t now = _executorForAddShard->now();
        const Date_t when = now + getAddShardTaskRetryInterval();
        _trackAddShardHandle_inlock(
            shardType.getName(),
            _executorForAddShard->scheduleWorkAt(
                when,
                stdx::bind(&ShardingCatalogManagerImpl::_scheduleAddShardTaskUnlessCanceled,
                           this,
                           stdx::placeholders::_1,
                           shardType,
                           std::move(targeter),
                           std::move(commandRequest))));
        return;
    }

    executor::RemoteCommandRequest request(
        swHost.getValue(), "admin", commandRequest, rpc::makeEmptyMetadata(), nullptr, 
        Milliseconds(kShardIdentityUpsertTimeoutMS));

    const RemoteCommandCallbackFn callback =
        stdx::bind(&ShardingCatalogManagerImpl::_handleAddShardTaskResponse,
                   this,
                   stdx::placeholders::_1,
                   shardType,
                   std::move(targeter));

    if (isRetry) {
        log() << "Retrying upsert of shardIdentity document into shard " << shardType.getName();
    }
    _trackAddShardHandle_inlock(shardType.getName(),
                                _executorForAddShard->scheduleRemoteCommand(request, callback));
}

void ShardingCatalogManagerImpl::_handleAddShardTaskResponse(
    const RemoteCommandCallbackArgs& cbArgs,
    ShardType shardType,
    std::shared_ptr<RemoteCommandTargeter> targeter) {
    stdx::unique_lock<stdx::mutex> lk(_addShardHandlesMutex);

    // If the callback has been canceled (either due to shutdown or the shard being removed), we
    // do not need to reschedule the task or update config.shards.
    Status responseStatus = cbArgs.response.status;
    if (responseStatus == ErrorCodes::CallbackCanceled) {
        return;
    }

    // If the handle no longer exists, the shard must have been removed, but the callback must not
    // have been canceled until after the task had completed. In this case as well, we do not need
    // to reschedule the task or update config.shards.
    if (!_hasAddShardHandle_inlock(shardType.getName())) {
        return;
    }

    // Untrack the handle from scheduleRemoteCommand regardless of whether the command
    // succeeded. If it failed, we will track the handle for the rescheduled task before
    // releasing the mutex.
    _untrackAddShardHandle_inlock(shardType.getName());

    // Examine the response to determine if the upsert succeeded.

    bool rescheduleTask = false;

    auto swResponse = cbArgs.response;
    if (!swResponse.isOK()) {
        warning() << "Failed to upsert shardIdentity document during addShard into shard "
                  << shardType.getName() << "(" << shardType.getHost()
                  << "). The shardIdentity upsert will continue to be retried. "
                  << causedBy(swResponse.status);
        rescheduleTask = true;
    } else {
        // Create a CommandResponse object in order to use processBatchWriteResponse.
        BSONObj responseObj = swResponse.data.getOwned();
        BSONObj responseMetadata = swResponse.metadata.getOwned();
        Status commandStatus = getStatusFromCommandResult(responseObj);
        Status writeConcernStatus = getWriteConcernStatusFromCommandResult(responseObj);
        Shard::CommandResponse commandResponse(std::move(responseObj),
                                               std::move(responseMetadata),
                                               std::move(commandStatus),
                                               std::move(writeConcernStatus));

        BatchedCommandResponse batchResponse;
        auto batchResponseStatus =
            Shard::CommandResponse::processBatchWriteResponse(commandResponse, &batchResponse);
        if (!batchResponseStatus.isOK()) {
            if (batchResponseStatus == ErrorCodes::DuplicateKey) {
                warning()
                    << "Received duplicate key error when inserting the shardIdentity "
                       "document into "
                    << shardType.getName() << "(" << shardType.getHost()
                    << "). This means the shard has a shardIdentity document with a clusterId "
                       "that differs from this cluster's clusterId. It may still belong to "
                       "or not have been properly removed from another cluster. The "
                       "shardIdentity upsert will continue to be retried.";
            } else {
                warning() << "Failed to upsert shardIdentity document into shard "
                          << shardType.getName() << "(" << shardType.getHost()
                          << ") during addShard. The shardIdentity upsert will continue to be "
                             "retried. "
                          << causedBy(batchResponseStatus);
            }
            rescheduleTask = true;
        }
    }

    if (rescheduleTask) {
        // If the command did not succeed, schedule the upsert shardIdentity task again with a
        // delay.
        const Date_t now = _executorForAddShard->now();
        const Date_t when = now + getAddShardTaskRetryInterval();

        // Track the handle from scheduleWorkAt.
        _trackAddShardHandle_inlock(
            shardType.getName(),
            _executorForAddShard->scheduleWorkAt(
                when,
                stdx::bind(&ShardingCatalogManagerImpl::_scheduleAddShardTaskUnlessCanceled,
                           this,
                           stdx::placeholders::_1,
                           shardType,
                           std::move(targeter),
                           std::move(cbArgs.request.cmdObj))));
        return;
    }

    // If the command succeeded, update config.shards to mark the shard as shardAware.

    // Release the _addShardHandlesMutex before updating config.shards, since it involves disk
    // I/O.
    // At worst, a redundant addShard task will be scheduled by a new primary if the current
    // primary fails during that write.
    lk.unlock();

    // This thread is part of a thread pool owned by the addShard TaskExecutor. Threads in that
    // pool are not created with Client objects associated with them, so a Client is created and
    // attached here to do the local update. The Client is destroyed at the end of the scope,
    // leaving the thread state as it was before.
    Client::initThread(getThreadName().c_str());
    ON_BLOCK_EXIT([&] { Client::destroy(); });

    // Use the thread's Client to create an OperationContext to perform the local write to
    // config.shards. This OperationContext will automatically be destroyed when it goes out of
    // scope at the end of this code block.
    auto txnPtr = cc().makeOperationContext();

    // Use kNoWaitWriteConcern to prevent waiting in this callback, since we don't handle a
    // failed response anyway. If the write is rolled back, the new config primary will attempt to
    // initialize sharding awareness on this shard again, and this update to config.shards will
    // be automatically retried then. If it fails because the shard was removed through the normal
    // removeShard path (so the entry in config.shards was deleted), no new addShard task will
    // get scheduled on the next transition to primary.
    auto updateStatus = _catalogClient->updateConfigDocument(
        txnPtr.get(),
        ShardType::ConfigNS,
        BSON(ShardType::name(shardType.getName())),
        BSON("$set" << BSON(ShardType::state()
                            << static_cast<std::underlying_type<ShardType::ShardState>::type>(
                                ShardType::ShardState::kShardActive))),
        false,
        kNoWaitWriteConcern);

    if (!updateStatus.isOK()) {
        warning() << "Failed to mark shard " << shardType.getName() << "(" << shardType.getHost()
                  << ") as shardAware in config.shards. This will be retried the next time a "
                     "config server transitions to primary. "
                  << causedBy(updateStatus.getStatus());
    }
}

BSONObj ShardingCatalogManagerImpl::createShardIdentityUpsertForAddShard(
    OperationContext* txn, const std::string& shardName) {
    std::unique_ptr<BatchedUpdateDocument> updateDoc(new BatchedUpdateDocument());

    BSONObjBuilder query;
    query.append("_id", "shardIdentity");
    query.append(ShardIdentityType::shardName(), shardName);
    query.append(ShardIdentityType::clusterId(), ClusterIdentityLoader::get(txn)->getClusterId());
    updateDoc->setQuery(query.obj());

    BSONObjBuilder update;
    {
        BSONObjBuilder set(update.subobjStart("$set"));
        set.append(ShardIdentityType::configsvrConnString(),
                   Grid::get(txn)->shardRegistry()->getConfigServerConnectionString().toString());
    }
    updateDoc->setUpdateExpr(update.obj());
    updateDoc->setUpsert(true);

    std::unique_ptr<BatchedUpdateRequest> updateRequest(new BatchedUpdateRequest());
    updateRequest->addToUpdates(updateDoc.release());

    BatchedCommandRequest commandRequest(updateRequest.release());
    commandRequest.setNS(NamespaceString::kConfigCollectionNamespace);
    commandRequest.setWriteConcern(ShardingCatalogClient::kMajorityWriteConcern.toBSON());

    return commandRequest.toBSON();
}


bool ShardingCatalogManagerImpl::_hasAddShardHandle_inlock(const ShardId& shardId) {
    return _addShardHandles.find(shardId) != _addShardHandles.end();
}

const CallbackHandle& ShardingCatalogManagerImpl::_getAddShardHandle_inlock(
    const ShardId& shardId) {
    invariant(_hasAddShardHandle_inlock(shardId));
    return _addShardHandles.find(shardId)->second;
}

void ShardingCatalogManagerImpl::_trackAddShardHandle_inlock(
    const ShardId shardId, const StatusWith<CallbackHandle>& swHandle) {
    if (swHandle.getStatus() == ErrorCodes::ShutdownInProgress) {
        return;
    }
    fassert(40219, swHandle.getStatus());
    _addShardHandles.insert(std::pair<ShardId, CallbackHandle>(shardId, swHandle.getValue()));
}

void ShardingCatalogManagerImpl::_untrackAddShardHandle_inlock(const ShardId& shardId) {
    auto it = _addShardHandles.find(shardId);
    invariant(it != _addShardHandles.end());
    _addShardHandles.erase(shardId);
}

Status ShardingCatalogManagerImpl::setFeatureCompatibilityVersionOnShards(
    OperationContext* txn, const std::string& version) {

    // No shards should be added until we have forwarded featureCompatibilityVersion to all shards.
    Lock::SharedLock lk(txn->lockState(), _kShardMembershipLock);

    std::vector<ShardId> shardIds;
    grid.shardRegistry()->getAllShardIds(&shardIds);
    for (const ShardId& shardId : shardIds) {
        const auto shardStatus = grid.shardRegistry()->getShard(txn, shardId);
        if (!shardStatus.isOK()) {
            continue;
        }
        const auto shard = shardStatus.getValue();

        auto response = shard->runCommandWithFixedRetryAttempts(
            txn,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            "admin",
            BSON(FeatureCompatibilityVersion::kCommandName << version),
            Shard::RetryPolicy::kIdempotent);
        if (!response.isOK()) {
            return response.getStatus();
        }
        if (!response.getValue().commandStatus.isOK()) {
            return response.getValue().commandStatus;
        }
        if (!response.getValue().writeConcernStatus.isOK()) {
            return response.getValue().writeConcernStatus;
        }
    }

    return Status::OK();
}

StatusWith<ShardType> ShardingCatalogManagerImpl::insertOrUpdateShardDocument(
    OperationContext* txn,
     ShardType& shardType,
     const ConnectionString& shardServerConn) {
    if (shardType.getState() == ShardType::ShardState::kShardRestarting) {
        BSONObjBuilder updateBuilder{};
        updateBuilder.append(ShardType::processIdentity(), shardType.getProcessIdentity());
        updateBuilder.append(ShardType::state(), 
                             static_cast<std::underlying_type<ShardType::ShardState>::type>(
                                shardType.getState()));

        auto updateShardDocumentResult = 
            _catalogClient->updateConfigDocument(
                txn,
                ShardType::ConfigNS,
                BSON(ShardType::name() << shardType.getName()),
                BSON("$set" << updateBuilder.obj()),
                false,
                ShardingCatalogClient::kMajorityWriteConcern 
            );
        
        if (!updateShardDocumentResult.isOK()) {
            return updateShardDocumentResult.getStatus();
        }

        if (!updateShardDocumentResult.getValue()) {
            return Status(ErrorCodes::BadValue, "update document count is 0");
        }

    } else if (shardType.getState() == ShardType::ShardState::kShardRegistering) {
        //generate a shardName
        shardName_lock.lock();
        StatusWith<std::string> shardNameResult = _shardServerManager->generateNewShardName(txn);
        shardName_lock.unlock();
        if (!shardNameResult.isOK()) {
            return shardNameResult.getStatus();
        }  
        std::string shardName = shardNameResult.getValue();

        // Make a fake ConnectionString with setName = shardName
        /*ConnectionString fakeConnectionString(
                ConnectionString::SET,
                shardServerConn.getServers(),
                shardName);
        */
        shardType.setName(shardName);
        shardType.setHost(shardServerConn.toString());
  
        auto insertStatus = Grid::get(txn)->catalogClient(txn)->insertConfigDocument(
                txn,
                ShardType::ConfigNS,
                shardType.toBSON(),
                ShardingCatalogClient::kMajorityWriteConcern);

        if (!insertStatus.isOK()) {
            return insertStatus;
        }
    } else {
        return Status(ErrorCodes::BadValue, 
                      stream() << "state should not be: " << 
                        static_cast<std::underlying_type<ShardType::ShardState>::type>(
                            shardType.getState()));
    }

    // Record in changelog
    BSONObjBuilder shardDetails;
    shardDetails.append("name", shardType.getName());
    shardDetails.append("host", shardType.getHost());
    shardDetails.append("state", 
        static_cast<std::underlying_type<ShardType::ShardState>::type>(
                shardType.getState()));
    shardDetails.append("extendIPs", shardType.getExtendIPs());
    shardDetails.append("processIdentity", shardType.getProcessIdentity());
    Grid::get(txn)->catalogClient(txn)->logChange(
            txn, 
            "insertOrUpdateShardDocument", 
            "", 
            shardDetails.obj(), 
            ShardingCatalogClient::kMajorityWriteConcern);

    return shardType;
}

StatusWith<ShardType> ShardingCatalogManagerImpl::insertShardDocument(
    OperationContext* txn,
    const ConnectionString& shardServerConn,
    const std::string& extendIPs,
    const std::string& processIdentity,
    bool& isRestart) {    
    HostAndPort hostAndPort = shardServerConn.getServers()[0];

    LOG(2) << "[insertShardDocument]: " << hostAndPort.toString();

    // Get shard doc from configDB
    auto findShardStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            kConfigPrimarySelector,
            repl::ReadConcernLevel::kLocalReadConcern,
            NamespaceString(ShardType::ConfigNS),
            BSON(ShardType::host() << BSON("$regex" << hostAndPort.toString())),
            BSONObj(),
            boost::none); // no limit

    if (!findShardStatus.isOK()) {
        return findShardStatus.getStatus();
    }

    const auto shardDocs = findShardStatus.getValue().docs;

    LOG(2) << "[insertShardDocument]: " << hostAndPort.toString() << ", document num: " << shardDocs.size();

    if (shardDocs.size() > 1) {
        return Status(ErrorCodes::TooManyMatchingDocuments,
            str::stream() << "more than one shard document found for host " <<
                hostAndPort.toString() << " in config databases");
    }

    ShardType shardType;
    if (shardDocs.size() == 1) {
        auto shardDocStatus = ShardType::fromBSON(shardDocs.front());
        if (!shardDocStatus.isOK()) {
            return shardDocStatus.getStatus();
        }

        shardType = shardDocStatus.getValue();

        // Make a fake ConnectionString with setName = shardName
        /*ConnectionString fakeConnectionString(
            ConnectionString::SET,
            shardServerConn.getServers(),
            shardType.getName());
        */
        if (shardType.getHost() != shardServerConn.toString()) {
            return Status(ErrorCodes::IllegalOperation,
                stream() << "not allowed to re-register a shard with a different host (" 
                        << shardServerConn.toString() << "): " << shardType.toString()); 
        }

        if (processIdentity.compare(shardType.getProcessIdentity()) == 0) {
            if (shardType.getState() != ShardType::ShardState::kShardRegistering) {
                return Status(ErrorCodes::IllegalOperation,
                              stream() << "not allowed to re-register a shard with state != kShardRegistering for same identity: "
                                       << shardType.toString());
            }
                
            // check chunks collection, to determine whether shard is re-launched after failover
            auto findChunkStatus = 
                Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                    txn,
                    kConfigPrimarySelector,
                    repl::ReadConcernLevel::kLocalReadConcern,
                    NamespaceString(ChunkType::ConfigNS),
                    BSON(ChunkType::shard() << shardType.getName()),
                    BSONObj(),
                    boost::none
                );
               
            if (!findChunkStatus.isOK()) {
                return Status(findChunkStatus.getStatus());
            }

            const auto findChunkDocs = findChunkStatus.getValue().docs;
              
            if (findChunkDocs.size() > 0) {
                isRestart = true;
            } else {
                isRestart = false;
            }

        } else {
            BSONObjBuilder updateBuilder{};
            updateBuilder.append(ShardType::processIdentity(), processIdentity);
            updateBuilder.append(ShardType::state(), 
                                 static_cast<std::underlying_type<ShardType::ShardState>::type>(
                                    ShardType::ShardState::kShardRegistering));

            auto updateProcessIdentityStatus = 
                _catalogClient->updateConfigDocument(
                    txn,
                    ShardType::ConfigNS,
                    BSON(ShardType::name() << shardType.getName()),
                    BSON("$set" << updateBuilder.obj()),
                    false,
                    ShardingCatalogClient::kMajorityWriteConcern 
                );
            
            if (!updateProcessIdentityStatus.isOK()) {
                return updateProcessIdentityStatus.getStatus();
            }
            
            isRestart = true;
            shardType.setState(ShardType::ShardState::kShardRegistering);
            shardType.setProcessIdentity(processIdentity);
        }
    } else {
        //generate a shardName
        StatusWith<std::string> shardNameResult = _shardServerManager->generateNewShardName(txn);
        if (!shardNameResult.isOK()) {
            return shardNameResult.getStatus();
    }  
        std::string shardName = shardNameResult.getValue();

        // Make a fake ConnectionString with setName = shardName
        /*ConnectionString fakeConnectionString(
                ConnectionString::SET,
                shardServerConn.getServers(),
                shardName);
         */
            shardType.setName(shardName);
            shardType.setHost(shardServerConn.toString());
            shardType.setState(ShardType::ShardState::kShardRegistering);
            shardType.setExtendIPs(extendIPs);
            shardType.setProcessIdentity(processIdentity);        

            auto insertStatus = Grid::get(txn)->catalogClient(txn)->insertConfigDocument(
                    txn,
                    ShardType::ConfigNS,
                    shardType.toBSON(),
                    ShardingCatalogClient::kMajorityWriteConcern);

            if (!insertStatus.isOK()) {
                return insertStatus;
            }
            
            isRestart = false;
    }

    // Record in changelog
    BSONObjBuilder shardDetails;
    shardDetails.append("name", shardType.getName());
    shardDetails.append("host", shardType.getHost());
    shardDetails.append("state",
        static_cast<std::underlying_type<ShardType::ShardState>::type>(
                ShardType::ShardState::kShardRegistering));
    shardDetails.append("extendIPs", shardType.getExtendIPs());
    shardDetails.append("processIdentity", shardType.getProcessIdentity());
    Grid::get(txn)->catalogClient(txn)->logChange(
            txn,
            "insertShardDocument",
            "",
            shardDetails.obj(),
            ShardingCatalogClient::kMajorityWriteConcern);

    return shardType;
}

StatusWith<std::string> ShardingCatalogManagerImpl::updateShardStateWhenReady(
    OperationContext* txn, 
    const std::string& shardName,
    const std::string& processIdentity) {
            

    LOG(2) << "going to update state for shard " << shardName 
           << " in config.shards during addShard";

    auto updateShardStatus = 
        Grid::get(txn)->catalogClient(txn)->updateConfigDocument(
                        txn,
                        ShardType::ConfigNS,
                        BSON(ShardType::name() << shardName << 
                             ShardType::processIdentity() << processIdentity),
                        BSON("$set" << BSON(ShardType::state() 
                            << static_cast<std::underlying_type<ShardType::ShardState>::type>(
                                ShardType::ShardState::kShardActive))),
                        false,//bool upsert
                        ShardingCatalogClient::kMajorityWriteConcern); 

    if (!updateShardStatus.isOK()) {
        return {updateShardStatus.getStatus().code(),
                      str::stream() << "couldn't updating shard: " << shardName 
                      << " during addShard;" << causedBy(updateShardStatus.getStatus())};
    }

    // Record in changelog
    BSONObjBuilder shardDetails;
    shardDetails.append("name", shardName);
    shardDetails.append("processIdentity", processIdentity);
    shardDetails.append("state",
        static_cast<std::underlying_type<ShardType::ShardState>::type>(
            ShardType::ShardState::kShardActive));
    _catalogClient->logChange(
        txn, "addShard", "", shardDetails.obj(), ShardingCatalogClient::kMajorityWriteConcern);

    return shardName;

}

Status ShardingCatalogManagerImpl::updateShardStateDuringFailover(
    OperationContext* txn, 
    const ShardType& shardType,
    const ShardType::ShardState& targetState) {
    // Get shard doc from configDB
    BSONObjBuilder updateBuilder{};
    updateBuilder.append(ShardType::state(), 
                         static_cast<std::underlying_type<ShardType::ShardState>::type>(
                            targetState));

    auto updateShardDocumentResult = 
        _catalogClient->updateConfigDocument(
            txn,
            ShardType::ConfigNS,
            BSON(ShardType::name() << shardType.getName() << 
                 ShardType::processIdentity() << shardType.getProcessIdentity()),
            BSON("$set" << updateBuilder.obj()),
            false,
            ShardingCatalogClient::kMajorityWriteConcern 
        );
    
    if (!updateShardDocumentResult.isOK()) {
        return updateShardDocumentResult.getStatus();
    }

    if (!updateShardDocumentResult.getValue()) {
        return Status(ErrorCodes::ShardServerNotFound, "update document count is 0");
    }

    // Record in changelog
    BSONObjBuilder shardDetails;
    shardDetails.append("name", shardType.getName());
    shardDetails.append("state",
        static_cast<std::underlying_type<ShardType::ShardState>::type>(
            targetState));

    _catalogClient->logChange(
        txn, "updateShardStateDuringFailover", "", shardDetails.obj(),
            ShardingCatalogClient::kMajorityWriteConcern);

    return Status::OK();
}


Status ShardingCatalogManagerImpl::updateMultiChunkStatePendingOpen(
    OperationContext* txn, const std::string& shardName) {
    // update chunk state be PendingOpen
    BSONObjBuilder updatePendingOpenBuilder{};
    updatePendingOpenBuilder.append("status",
        static_cast<std::underlying_type<ChunkType::ChunkStatus>::type>(
            ChunkType::ChunkStatus::kOffloaded));

    BSONObjBuilder failShardChunkFilter;
    failShardChunkFilter.append(ChunkType::shard(), shardName);
    failShardChunkFilter.append(ChunkType::rootFolder(), BSON("$ne" << "stalerootfolder"));
    failShardChunkFilter.append(ChunkType::status(), BSON("$ne" << static_cast<int>(ChunkType::ChunkStatus::kDisabled)));

    auto updateStatus = Grid::get(txn)->catalogClient(txn)->updateConfigDocuments(
        txn,
        ChunkType::ConfigNS,
        failShardChunkFilter.obj(),
        BSON("$set" << updatePendingOpenBuilder.obj()),
        false,//bool upsert
        ShardingCatalogClient::kMajorityWriteConcern);

    if (!updateStatus.isOK()) {
        return updateStatus;
    }

    // Record in changelog
    BSONObjBuilder chunkDetails;
    chunkDetails.append("shard", shardName);
    chunkDetails.append("status",
        static_cast<std::underlying_type<ChunkType::ChunkStatus>::type>(
            ChunkType::ChunkStatus::kOffloaded));

    _catalogClient->logChange(
        txn, "updateChunkStatePendingOpen", "", chunkDetails.obj(),
            ShardingCatalogClient::kMajorityWriteConcern);

    return Status::OK();
}
Status ShardingCatalogManagerImpl::verifyShardConnectionString(
    OperationContext* txn,
    const std::string& shardName,
    const ConnectionString& shardServerConn) {
    // Get shard doc from configDB
    auto findShardStatus =
        Grid::get(txn)->shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
            txn,
            kConfigPrimarySelector,
            repl::ReadConcernLevel::kLocalReadConcern,
            NamespaceString(ShardType::ConfigNS),
            BSON(ShardType::name() << shardName),
            BSONObj(),
            boost::none); // no limit

    if (!findShardStatus.isOK()) {
        return findShardStatus.getStatus();
    }

    const auto shardDocs = findShardStatus.getValue().docs;

    if (shardDocs.size() > 1) {
        return Status(ErrorCodes::TooManyMatchingDocuments,
            str::stream() << "more than one document for shard " <<
                shardName << " in config databases");
    }

    if (shardDocs.size() == 0) {
        return Status(ErrorCodes::ShardNotFound,
            str::stream() << "shard " << shardName << " does not exist");
    }

    auto shardDocStatus = ShardType::fromBSON(shardDocs.front());
    if (!shardDocStatus.isOK()) {
        return shardDocStatus.getStatus();
    }

    ShardType shardType = shardDocStatus.getValue();

    if (shardType.getHost() != shardServerConn.toString()) {
        return Status(ErrorCodes::BadValue, 
            str::stream() << "inconsistent shard server host for shard: " << shardName
                << ", received: " << shardServerConn.toString() 
                << ", stored: " << shardType.getHost());
    }
    return Status::OK();
}

ShardServerManager* ShardingCatalogManagerImpl::getShardServerManager() {
    return _shardServerManager;
}

StateMachine* ShardingCatalogManagerImpl::getStateMachine() {
    return _stateMachine;
}

Status ShardingCatalogManagerImpl::createRootFolder(
    OperationContext* txn,
    const std::string& chunkId,
    std::string& chunkRootFolder) {

    log() << "Start createChunkRootFolder for chunk " << chunkId;

    Status createRootFolderStatus = _rootFolderManager->createChunkRootFolder(
        txn, 
        chunkId, 
        chunkRootFolder);

    if (!createRootFolderStatus.isOK()) {
        return createRootFolderStatus;
    }

    log() << "Finish createChunkRootFolder for chunk " << chunkId;  
    return Status::OK();
}

Status ShardingCatalogManagerImpl::deleteRootFolder(
    OperationContext* txn,
    const std::string& chunkId) {

    log() << "Start deleteRootFolder for chunk " << chunkId;
    Status deleteRootFolderStatus = _rootFolderManager->deleteChunkRootFolder(
        txn,
        chunkId);
    log() << "Finish deleteRootFolder for chunk " << chunkId;
    return deleteRootFolderStatus;
}

Status ShardingCatalogManagerImpl::deleteRootFolder(
    const std::string& chunkRootFolder) {

    log() << "Start deleteRootFolder with chunkRootFolder.";
    Status deleteRootFolderStatus = _rootFolderManager->deleteChunkRootFolder(chunkRootFolder);
    log() << "Finish deleteRootFolder with chunkRootFolder.";
    return deleteRootFolderStatus;
}

void ShardingCatalogManagerImpl::resetMaxChunkVersionMap() {
    stdx::lock_guard<stdx::mutex> lk(_maxChunkVersionMapMutex);
    _maxChunkVersionMap.clear();
    return;
}

StatusWith<uint64_t> ShardingCatalogManagerImpl::newMaxChunkVersion(
    OperationContext* txn,
    const std::string& ns) {
    stdx::lock_guard<stdx::mutex> lk(_maxChunkVersionMapMutex);

    auto it = _maxChunkVersionMap.find(ns);
    if (it == _maxChunkVersionMap.end()) {
        AutoGetCollection autoColl(txn, NamespaceString(ChunkType::ConfigNS), MODE_X);

        auto findStatus = grid.shardRegistry()->getConfigShard()->exhaustiveFindOnConfig(
                                                            txn,
                                                            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
                                                            repl::ReadConcernLevel::kLocalReadConcern,
                                                            NamespaceString(ChunkType::ConfigNS),
                                                            BSON("ns" << ns),
                                                            BSON(ChunkType::DEPRECATED_lastmod << -1),
                                                            1);
        if (!findStatus.isOK()) {
            return findStatus.getStatus();
        }

        const auto& chunksVector = findStatus.getValue().docs;
        if (chunksVector.empty()) {
            _maxChunkVersionMap[ns] = static_cast<uint64_t>(1) << 32;
        } else {
            ChunkVersion currentMaxVersion =
                ChunkVersion::fromBSON(chunksVector.front(), ChunkType::DEPRECATED_lastmod());
            _maxChunkVersionMap[ns] = currentMaxVersion.toLong() + 1048576;
        }
    }

    return _maxChunkVersionMap[ns]++;
}

}  // namespace mongo