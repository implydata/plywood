const DOCKER_MACHINE = '192.168.99.100';

exports.druidVersion = '0.9.1-rc4';
exports.druidHost = `${DOCKER_MACHINE}:8082`;

exports.mySqlVersion = '5.7.13';
exports.mySqlHost = `${DOCKER_MACHINE}:3306`;
exports.mySqlDatabase = 'datazoo';
exports.mySqlUser = 'root';
exports.mySqlPassword = '';

exports.postgresVersion = '9.5.2';
exports.postgresHost = `${DOCKER_MACHINE}:5432`;
exports.postgresDatabase = 'datazoo';
exports.postgresUser = 'root';
exports.postgresPassword = 'datazoo';
