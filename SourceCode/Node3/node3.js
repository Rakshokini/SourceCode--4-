var udp = require('dgram');

// --------------------creating a udp server --------------------// 
var server = udp.createSocket('udp4');
let lastTermvotedfor = 0;

let currentTerm = 0
let votedFor = null
let log = [] //Message from client (Since no client logic this remains empty) 
// structure for log: { term: 0, msg: "" }
let currentRole = ""
let currentLeader = null
let nodeId = 'Node3'
let votesRecieved = []
let sentlength = [] //Sentlen format: { followerid: 0, len: 0 }
let acklength = [] //acklen format: { followerid: 0, len: 0 }
let lastTerm = null;
let voteRequest = null
let timeout = false;
let Received = 0;
let startElection = false;
let followerTimer;
let checkHeartBeat;
let ReceivedVoteREquest = false;
let currentCount = 0;
let ReceivedLeaderCount = 0;
let eligibleForElecetion = true;
let votingTerm = 0
let shutdown = false;
let Append
let vote;
let vote1;
let logInfo = [];
let leaderLogInfo = [];
let leaderCommitedlogs = [];
let followerlogInfo = [];
let leaderCommitIndex = 0;
let FollowerCommitIndex = 0;  //when to set the follower commit index
let nextIndex = [];
let logInfoReceived = false;
let MsgAckReceived = [];
let logInfoAck = false;

function init() {
    currentTerm = 0
    votedFor = null
    log = [];
    leaderLogInfo = [];
    followerlogInfo = [];
    currentRole = "Follower"
    currentLeader = null
    votesRecieved = []
    sentlength = []
    acklength = []
    lastTerm = 0;
}


//Run timer periodically for the follower mode
function FollowerTimer() {
    if (currentRole === 'Follower' && startElection === false) {
        followerTimer = setTimeout(function () {
            //Run the server timeout
            timeout = true;
            if (currentRole === 'leader' && currentLeader === nodeId) {
                clearInterval(followerTimer);
                timeout = false;
            }
        }, 397);
    }
}

//check if data is receiving from leader every T seconds
function OnrecievingAppendEntryRPC() {
    checkHeartBeat = setInterval(function () {
        Received += 1;

        if (currentRole == 'Follower' && timeout === true && currentLeader !== null && Received > ReceivedLeaderCount && startElection === false && ReceivedVoteREquest === false) {
            vote1 = setTimeout(() => {
                if (Received > ReceivedLeaderCount && currentRole == 'Follower') {

                    startElection = true;
                    RequestVoteRPC();
                    // clearInterval(checkHeartBeat);
                }
            }, 597);
        }

        else if (currentRole == 'Follower' && currentLeader === null && currentLeader !== nodeId && startElection === false && timeout === true && ReceivedLeaderCount === 0 && ReceivedVoteREquest === false) {
            console.log('First election node3');
            vote = setTimeout(() => {
                if (ReceivedLeaderCount == 0 && currentRole == 'Follower') {
                    startElection = true;
                    RequestVoteRPC();
                    // clearInterval(checkHeartBeat);
                }
            }, 0);
        }
    }
        , 200);
}

//UpdatingLeaderElection
function updateLeaderContent(responseJSON) {
    var responseMsg;
    if (currentLeader != responseJSON?.sender_name) {
        Received = 0;
        ReceivedLeaderCount = 0;
    }
    
    if(responseJSON?.key !== null && responseJSON?.value !== null && responseJSON?.term < currentTerm && responseJSON?.PrevLogTerm !== followerlogInfo?.[followerlogInfo?.length-1]?.term) {//check the last condition once
        //leader has sent heartbeat plus client message but does not match with old entry
        currentLeader = responseJSON?.sender_name;
        currentTerm = responseJSON?.term;
        log = responseJSON?.log;
        currentRole = 'Follower';
        ReceivedLeaderCount = responseJSON?.currentCount;
        eligibleForElecetion = false

        if(responseJSON?.leaderCommit> FollowerCommitIndex) {
            FollowerCommitIndex = min(responseJSON?.leaderCommit, responseJSON?.PrevLogIndex)
        }

        //send success false response
        responseMsg = {
            term: currentTerm,
            sender_name: nodeId,
            request: 'APPEND_RPC_RESPONSE',
            success: false,
        }
        let BufferMsg = Buffer.from(JSON.stringify(responseMsg))
        server.send(BufferMsg, 5555, responseJSON?.sender_name, function (error) {
            if (error) {
                console.log(error);

            } else {
                console.log('heartbeat sent to all servers!');
            }
        });
    } else if(responseJSON?.key !== null && responseJSON?.value !== null && responseJSON?.term > currentTerm && responseJSON?.PrevLogIndex === followerlogInfo?.[followerlogInfo?.length-1] && responseJSON?.PrevLogTerm === followerlogInfo?.[followerlogInfo?.length-1]?.term){
        //leader has sent heartbeat plus client message and match with old entry
        currentLeader = responseJSON?.sender_name;
        currentTerm = responseJSON?.term;
        log = responseJSON?.log;
        currentRole = 'Follower';
        ReceivedLeaderCount = responseJSON?.currentCount;
        eligibleForElecetion = false
        if(responseJSON?.leaderCommit> FollowerCommitIndex) {
            FollowerCommitIndex = min(responseJSON?.leaderCommit, responseJSON?.PrevLogIndex)
        }

        //append the leader log in follower log
        if(followerlogInfo?.length - 1 < responseJSON?.log?.length-1) {
            followerlogInfo?.push(responseJSON?.log);
        }
        
        //send success true response
        responseMsg = {
            term: currentTerm,
            sender_name: nodeId,
            request: 'APPEND_RPC_RESPONSE',
            success: true,
        }
        let BufferMsg = Buffer.from(JSON.stringify(responseMsg))
        server.send(BufferMsg, 5555, responseJSON?.sender_name, function (error) {
            if (error) {
                console.log(error);

            } else {
                console.log('heartbeat sent to all servers!');
            }
        });
    } else if(responseJSON?.key !== null && responseJSON?.value !== null && responseJSON?.term > currentTerm && responseJSON?.PrevLogTerm !== followerlogInfo?.[followerlogInfo?.length-1]?.term && responseJSON?.PrevLogIndex === followerlogInfo?.[followerlogInfo?.length-1]){
    //same index but different term so replceing the current term from the wrong one with the updated term in the log 
    currentLeader = responseJSON?.sender_name;
        currentTerm = responseJSON?.term;
        log = responseJSON?.log;
        currentRole = 'Follower';
        ReceivedLeaderCount = responseJSON?.currentCount;
        eligibleForElecetion = false


        //Replacing the mismatched term in the follower index
        for(i=followerlogInfo?.length - 1; i< responseJSON?.log?.length-1; i++) { // check this condition during evaluation
            // followerlogInfo && followerlogInfo[i]?.term = responseJSON?.log?.term;
            console.log('followerlogInfo[i]>>', followerlogInfo[i]);
        }
        //append the leader log in follower log
        if(followerlogInfo?.length - 1 < responseJSON?.log?.length-1) {
            followerlogInfo?.push(responseJSON?.log);
        }
        if(responseJSON?.leaderCommit> FollowerCommitIndex) {
            FollowerCommitIndex = min(responseJSON?.leaderCommit, responseJSON?.PrevLogIndex)
        }
    } else {
        //leader has sent the heartbeat
        currentLeader = responseJSON?.sender_name;
        currentTerm = responseJSON?.term;
        log = responseJSON?.log;
        currentRole = 'Follower';
        ReceivedLeaderCount = responseJSON?.currentCount;
        eligibleForElecetion = false
        if(responseJSON?.leaderCommit> FollowerCommitIndex) {
            FollowerCommitIndex = min(responseJSON?.leaderCommit, responseJSON?.PrevLogIndex)
        }

        //send success true response
        responseMsg = {
            term: currentTerm,
            sender_name: nodeId,
            request: 'APPEND_RPC_RESPONSE',
            success: true,
        }
        let BufferMsg = Buffer.from(JSON.stringify(responseMsg))
        server.send(BufferMsg, 5555, responseJSON?.sender_name, function (error) {
            if (error) {
                console.log(error);

            } else {
                console.log('heartbeat sent to all servers!');
            }
        });
    } 
}

//Candidate starting the election
function RequestVoteRPC() {
    votingTerm = currentTerm + 1;
    currentRole = "Candidate";
    votedFor = nodeId;
    votesRecieved.push(nodeId);
    lastTerm = 0;
    if (followerlogInfo.length > 0) {
        lastTerm = followerlogInfo?.[followerlogInfo.length - 1]?.term
    } else {
        // Send election vote to all servers
        var allServer = ['Node1', 'Node2', 'Node4', 'Node5'];
        // Edit later
        // var allServer = [8085, 8084]
        var msg = {
            sender_name: nodeId,
            request: 'VOTE_REQUEST',
            term: votingTerm,
            logLength: log.length,
            Role: currentRole
        }
        let BufferMsg = Buffer.from(JSON.stringify(msg))
        allServer.forEach(eachserver => {
            server.send(BufferMsg, 5555, eachserver, function (error) {
                if (error) {
                    console.log(error);

                } else {
                    console.log('election request is sent!');
                }
            });
        });
    }

}

function VotesCalculation(response = {}, info = {}) {
    if (currentRole === 'Candidate' && response?.term === votingTerm && response?.success === true) {
        votesRecieved.push(response?.nodeId);
        if (votesRecieved?.length >= 2) { //(nodes+1)/2 edit later
            var allServer = ['Node1', 'Node2', 'Node4', 'Node5'];
            currentRole = 'leader';
            currentLeader = nodeId;
            currentTerm = votingTerm

            clearInterval(followerTimer);
            clearInterval(checkHeartBeat);
            allServer.forEach(eachserver => {
                let followerIndex = {
                    followerName: eachserver,
                    index: followerlogInfo?.length,
                    newStoreAck: false
                }
                nextIndex?.push(followerIndex);
            });
            leaderCommitedlogs?.push(followerlogInfo);
            leaderCommitIndex = followerlogInfo?.length -1;
            leaderLogInfo?.push(followerlogInfo);
            AppendEntryRPC();
            // need to check the condition
        }
    } else if (response?.term >= currentTerm && response?.success === false) {

        votingTerm = currentTerm
        currentRole = 'Follower';

        votedFor = null;
        startElection = false
    }
}



function AppendEntryRPC() {
    Append = setInterval(function () {
        if (currentRole === 'leader') {
            var allServer = ['Node1', 'Node2', 'Node4', 'Node5'];
            currentCount = currentCount + 1

            var msg;
            // foreach(server in allServer)
            nextIndex?.forEach(eachserver => {
                if(logInfoReceived === true && eachserver?.newStoreAck === false && logInfoAck === false) {
                    msg = {
                        sender_name: nodeId,
                        request: 'APPEND_RPC',
                        term: currentTerm,
                        log: leaderLogInfo,
                        Role: currentRole,
                        currentCount: currentCount,
                        key: leaderLogInfo?.[leaderLogInfo?.length-1]?.key,
                        value: leaderLogInfo?.[leaderLogInfo?.length-1]?.value,
                        PrevLogTerm: leaderLogInfo?.[eachserver?.index -1].term,
                        PrevLogIndex: eachserver?.index -1,
                        leaderCommit: leaderCommitIndex, //check the term there are actually two namings in the document
                        success: null,
                        entry: leaderLogInfo?.[leaderLogInfo?.length-1]?.term,
                    }
                }else{
                    msg = {
                        sender_name: nodeId,
                        request: 'APPEND_RPC',
                        term: currentTerm,
                        log: leaderLogInfo,
                        Role: currentRole,
                        currentCount: currentCount,
                        key: null,
                        value: null,
                        PrevLogTerm: null,
                        PrevLogIndex: null,
                        leaderCommit: null,
                        success: null,
                        entry: []
                    }
                }
                let BufferMsg = Buffer.from(JSON.stringify(msg))
                server.send(BufferMsg, 5555, eachserver?.followerName, function (error) {
                    if (error) {
                        console.log(error);

                    } else {
                        console.log('heartbeat sent to all servers!');
                    }
                });
            });
        }
    }, 200);
}





function Leaderinfo() {
    var msg = {
        sender_name: nodeId,
        request: 'Leader_info',
        term: currentTerm,
        key: 'Leader',
        value: currentLeader
    }
    let BufferMsg = Buffer.from(JSON.stringify(msg))
    //Acknowledgement for the leader
    server.send(BufferMsg, 5555, 'Controller', function (error) {
        if (error) {
            console.log(error)

        } else {
            console.log('Data sent !');
        }
    });
}

//Received a vote request from candidate
function votingLeader(response = {}, info = {}) {
    if (response?.term > currentTerm && currentRole != "leader" && lastTermvotedfor < response?.term) {
        currentRole = "Follower";
        votedFor = response?.sender_name
        ReceivedHigherVoteReq = true;
        lastTermvotedfor = response?.term;
    }
    lastTerm = 0;
    if (leaderLogInfo.length > 0) {
        lastTerm = leaderLogInfo?.[leaderLogInfo.length - 1]?.term
    }

    // logOk Needs to be checked when client request is present

    if (response?.term > currentTerm && (votedFor === response?.sender_name)) {
        // var allServer = ['node1', 'node2', 'node3', 'node4', 'node5'];
        // Edit later
        var allServer = [votedFor]
        var msg = {


            sender_name: nodeId,
            request: 'VOTE_ACK',
            term: response?.term,
            logLength: leaderLogInfo.length,
            Role: currentRole,
            success: true
        }
        let BufferMsg = Buffer.from(JSON.stringify(msg))
        //Acknowledgement for the leader
        server.send(BufferMsg, 5555, votedFor, function (error) {
            if (error) {
                // client.close();
                console.log(error)
            } else {
                console.log('Data sent !');
            }
        });
    } else {
        ReceivedVoteREquest = false;
        var msg = {
            sender_name: nodeId,
            request: 'VOTE_ACK',
            term: response?.term,
            logLength: leaderLogInfo.length,
            Role: currentRole,
            success: false
        }
        let BufferMsg = Buffer.from(JSON.stringify(msg))
        //Acknowledgement for the leader
        server.send(BufferMsg, 5555, response?.sender_name, function (error) {
            if (error) {
                // client.close();
                console.log(error)
            } else {
                console.log('Data sent !');
            }
        });
    }
}

function StoreClientInfo(response = {}) {
    console.log('store request received: node3');
    if(currentRole === 'leader') {
        newStoreObject = {
            term: currentTerm, //term will come as null from controller so using our own term
            key: response?.key,
            value: response?.value,
        };
        logInfoReceived = true;
        console.log('store request received: at leader node3', newStoreObject);
        leaderLogInfo?.push(newStoreObject);
    } else {
        var StoreRejectMsg = {
            sender_name: nodeId,
            request: 'LEADER_INFO',
            term: null,
            key: 'LEADER',
            value: currentLeader,
        }
        console.log('store request received: at follower node3', StoreRejectMsg);

        let BufferMsg = Buffer.from(JSON.stringify(StoreRejectMsg))
        server.send(BufferMsg, 5555, response?.sender_name, function (error) {
            if (error) {
                server.close();
                console.log(error)
            } else {
                console.log('Leader Info for Store request sent!');
            }
        });
    }
}

function RetrieveRequestInfo(response = {}) {
    if(currentRole === 'leader') {
        var RetrieveAcceptMsg = {
            sender_name: nodeId,
            request: 'RETRIEVE',
            term: null,
            key: 'COMMITED_LOGS',
            value: leaderCommitedlogs,
        }
        let BufferMsg = Buffer.from(JSON.stringify(RetrieveAcceptMsg))
        server.send(BufferMsg, 5555, response?.sender_name, function (error) {
            if (error) {
                server.close();
                console.log(error)
            } else {
                console.log('Retrieve Info success request sent!');
            }
        });
    } else {
        var RetrieveRejectMsg = {
            sender_name: nodeId,
            request: 'LEADER_INFO',
            term: null,
            key: 'LEADER',
            value: currentLeader,
        }
        let BufferMsg = Buffer.from(JSON.stringify(RetrieveRejectMsg))
        server.send(BufferMsg, 5555, response?.sender_name, function (error) {
            if (error) {
                server.close();
                console.log(error)
            } else {
                console.log('Retrieve Info reject request sent!');
            }
        });
    }
}

function ReceiveBroadcastResponse(response = {}) {
    if (MsgAckReceived?.length >= 2) {
        logInfoAck = true;
        logInfoReceived = false;
        leaderCommitedlogs?.push(leaderLogInfo?.length-1);
        leaderCommitIndex +=1;
    }
    if(response?.success === true) {
        //append the acknowledgement counts
        MsgAckReceived?.push(response?.nodeId);
        nextIndex.forEach(follower => {
            if(follower?.followerName === response?.sender_name) {
                follower.index +=1;
                follower.newStoreAck = true;
            }
        });
    } else if(response?.success === false){
        nextIndex.forEach(follower => {
            if(follower?.followerName === response?.sender_name) {
                follower.index -=1;
                follower.newStoreAck = false;
            }
        });

    }
}


//Server is listening
server.on('listening', function () {
    var address = server.address();
    var port = address.port;
    init();

    //Run timer periodically for the follower mode
    FollowerTimer();

    //check if data is receiving from leader every T seconds
    OnrecievingAppendEntryRPC();
});

// When receiving message from server
server.on('message', function (msg, info) {
    let responseString = msg.toString('utf-8');
    let responseJSON = JSON.parse(responseString)
    if (shutdown == false) {
        //When receiving vote request from server
        if (responseJSON?.request?.toUpperCase() === 'VOTE_REQUEST') {
            votingLeader(responseJSON, info);
        } else if (responseJSON?.request?.toUpperCase() === 'APPEND_RPC') {
            updateLeaderContent(responseJSON);
        } else if (responseJSON?.request?.toUpperCase() === 'VOTE_ACK') {
            VotesCalculation(responseJSON, info);
        } else if (responseJSON?.request?.toUpperCase() === 'CONVERT_FOLLOWER') {
            init();

            //Run timer periodically for the follower mode
            FollowerTimer();

            //check if data is receiving from leader every T seconds
            // OnrecievingAppendEntryRPC();
        }
        else if (responseJSON?.Request?.toUpperCase() === 'LEADER_INFO') {
            Leaderinfo();
        }
        else if (responseJSON?.request?.toUpperCase() === 'CONVERT_FOLLOWER') {
            init();

            //Run timer periodically for the follower mode
            FollowerTimer();

            //check if data is receiving from leader every T seconds
            OnrecievingAppendEntryRPC();
        }
        else if (responseJSON?.request?.toUpperCase() === 'SHUTDOWN') {
            clearInterval(Append);
            shutdown = true;
        }
        else if (responseJSON?.request?.toUpperCase() === 'TIMEOUT') {
            clearTimeout(vote);
            clearTimeout(vote1);
            startElection = true;
            RequestVoteRPC();
        }
        else if (responseJSON?.request?.toUpperCase() === 'STORE') {
            StoreClientInfo(responseJSON);
        }
        else if (responseJSON?.request?.toUpperCase() === 'RETRIEVE') {
            RetrieveRequestInfo(responseJSON);
        }
        else if (responseJSON?.request?.toUpperCase() === 'APPEND_RPC_RESPONSE') {
            ReceiveBroadcastResponse(responseJSON);
        }
    }
    else {
        console.log("shutdown true");
    }
});

//================ if an error occurs
server.on('error', function (error) {
    console.log('Error: ' + error);
    server.close();
});

server.bind(5555);