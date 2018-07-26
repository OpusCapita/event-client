const {exec} = require('child_process');
const request = require('superagent');

const COMMANDS = [
    'block', 'unblock',
    'kill', 'start'
];

function getDefaultGateway() {
    return new Promise((resolve, reject) => {
        exec('ip route | grep "default"', (err, stdout) => {
            if (err) {
                console.error('ip route failed: ', err);
                reject(err);
            }

            let ip = stdout.substring(12, stdout.length);
            ip = ip.substring(0, ip.indexOf(' '));

            resolve(ip);
        });
    });
}

async function killRabbit(node) {
    return doExec(node, 'kill');
}

async function resurrectRabbit(node) {
    return doExec(node, 'start');
}

async function blockRabbit(node) {
    return doExec(node, 'block');
}

async function unblockRabbit(node) {
    return doExec(node, 'unblock');
}

async function doExec(node, command) {
    if (COMMANDS.includes(command) !== true) {
        return 'Not implemented.';
    }

    let ip = await getDefaultGateway();

    return new Promise((resolve, reject) => {
        exec('curl ' + ip + ':666' + (5 + node) + '/' + command, (err) => {
            if (err) {
                console.error('Command ' + command + ' failed to execute ... ', err);
                reject(err);
            }
            resolve(true);
        });
    });
}


function awaitRabbitCluster({host}, username, password) {
    let maxTries = 30;

    return new Promise((resolve, reject) => {
        let interval = setInterval(async () => {
            maxTries--;

            console.log('Waiting for rabbit cluster to come up with at least 2 nodes ... ' + maxTries);

            let nodes;
            try {
                let url = `http://${host}:15672/api/nodes`;
                let response = await request
                    .get(url)
                    .auth(username, password);

                nodes = (response && response.body) ? response.body : [];
            } catch (e) {
                /* noop */
            }

            if (nodes && nodes.length >= 2) {
                clearInterval(interval);
                resolve(true);
            }

            if (maxTries <= 0) {
                clearInterval(interval);
                reject('Failed to connect to rabbit cluster');
            }

        }, 1000);
    });
};

module.exports = {
    awaitRabbitCluster,
    killRabbit,
    resurrectRabbit,
    blockRabbit,
    unblockRabbit
};
