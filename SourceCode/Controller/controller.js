var udp = require('dgram');
const { TIMEOUT } = require('dns');

// --------------------creating a udp server --------------------// 
var server = udp.createSocket('udp4');
let leader;
let timoutnode
let callConvertFollower = true;

function storeRequest() {
    var msg = {
        sender_name: 'controller',
        term: null,
        request: 'STORE',
        key: 'k1',
        value: 'value1'
    }

    let BufferMsg = Buffer.from(JSON.stringify(msg))
        console.log('store Request', msg);

        server.send(BufferMsg, 5555, 'Node3', function (error) {
            if (error) {
                console.log('store req error!');

            } else {
                console.log('store req is sent!');
                // if(callConvertFollower) {
                //     CONVERT_FOLLOWER();
                // }
            }
        })
}

function retrieveRequest() {
    var msg = {
        sender_name: 'controller',
        term: null,
        request: 'RETRIEVE',
        key: null,
        value: null
    }

    let BufferMsg = Buffer.from(JSON.stringify(msg))
        console.log('retrieve Request', msg);

        server.send(BufferMsg, 5555, 'Node1', function (error) {
            if (error) {
                console.log('retrieve req error!');

            } else {
                console.log('retrieve req is sent!');
                // if(callConvertFollower) {
                //     CONVERT_FOLLOWER();
                // }
            }
        })
}

function leader_info() {

    var msg = {
        sender_name: 'controller',
        request: 'LEADER_INFO',
    }
    setTimeout(function () {
        let BufferMsg = Buffer.from(JSON.stringify(msg))
        console.log('Leader Info Request', msg);

        server.send(BufferMsg, 5555, 'Node1', function (error) {
            if (error) {
                console.log('leader info req error!');

            } else {
                console.log('leader info req is sent!');
                if(callConvertFollower) {
                    CONVERT_FOLLOWER();
                }
            }
        })
    }, 2000)
}

function CONVERT_FOLLOWER() {
    callConvertFollower = false;
    var msg = {
        sender_name: 'controller',
        request: 'CONVERT_FOLLOWER',
    }

    setTimeout(function () {
        BufferMsg = Buffer.from(JSON.stringify(msg))
        console.log('convert_follower', msg);

        server.send(BufferMsg, 5555, leader, function (error) {
            if (error) {
                console.log(error);
            } else {
                console.log('convert_follower req is sent');
                leader_info();
                shutdown();
            }
        })
    }, 2000)
}

function shutdown() {
    var msg = {
        sender_name: 'controller',
        request: 'SHUTDOWN',
    }
    setTimeout(function () {
        BufferMsg = Buffer.from(JSON.stringify(msg))
        console.log('shutdown', msg);

        server.send(BufferMsg, 5555, leader, function (error) {
            if (error) {
                console.log(error);

            } else {
                console.log('shutdown req is sent');
            }

             if (leader != 'Node4') {
                timoutnode = 'Node4'
            }
          else  if (leader != 'Node2') {
                timoutnode = 'Node2'
            }
          else  if (leader != 'Node1') {
                timoutnode = 'Node1'
            }
           
            else if (leader != 'Node3') {
                timoutnode = 'Node3'
            }
           
            else if (leader != 'Node5') {
                timoutnode = 'Node5'
            }
            timeout()
        })
    }, 3000)
}
function timeout() {
    var msg = {
        sender_name: 'controller',
        request: 'TIMEOUT',
    }
    setTimeout(function () {
        BufferMsg = Buffer.from(JSON.stringify(msg))
        console.log('timeout', timoutnode);

        server.send(BufferMsg, 5555, timoutnode, function (error) {
            if (error) {
                console.log(error);
            } else {
                console.log('timeout req is sent');
            }
            // leader_info()  //change it after the code is completed
            storeRequest();
            // setTimeout(retrieveRequest(), 400); 
        })
    }, 10)
}


//Server is listening
server.on('listening', function () {
    var address = server.address();
    var port = address.port;
    console.log('Server is listening at port' + port);
    leader_info();
    
});

// When receiving message from server
server.on('message', function (msg, info) {
    let responseString = msg.toString('utf-8');
    let responseJSON = JSON.parse(responseString)
    console.log('Data Received: ', responseJSON);
    if (responseJSON?.request?.toLowerCase() == 'leader_info') {
        leader = responseJSON.value
        console.log(leader)
    }

});


//================ if an error occurs
server.on('error', function (error) {
    console.log('Error: ' + error);
    server.close();
});

server.bind(5555);