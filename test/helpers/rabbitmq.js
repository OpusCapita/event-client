const {exec} = require('child_process');

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

module.exports = {
    killRabbit,
    resurrectRabbit,
    blockRabbit,
    unblockRabbit
};
