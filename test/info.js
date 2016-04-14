const DOCKER_MACHINE = '192.168.99.100';

exports.druidHost = `${DOCKER_MACHINE}:8082`;
exports.druidVersion = '0.9.0';

exports.mySqlHost = `${DOCKER_MACHINE}:3306`;
exports.mySqlDatabase = 'plywood_test';
exports.mySqlUser = 'root';
exports.mySqlPassword = '';
