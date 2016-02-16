# Loading test data
## General Setup
1. Install Docker
1. Run `./make-data-files`
1. If you've not set ${IMPLY_PROJECTS} in your path, replace it with the path to where imply projects live

## Druid 

### Setup

1. `docker run -v /${IMPLY_PROJECTS}/plywood/data:/opt/data -p 8081-8110:8081-8110 -p 8200:8200 -p 9095:9095 -d --name plywood-test-druid imply/imply`
2. `docker exec -it plywood-test-druid bin/post-index-task -f /opt/data/wikipedia-index.json`

### Tear down

1. `docker stop plywood-test-druid`
2. `docker rm plywood-test-druid`

### Regular use

1. `docker start plywood-test-druid`

## MySQL

### Setup

1. `docker run -v /${IMPLY_PROJECTS}/plywood/data:/opt/data -p 6603:3306 -d --name plywood-test-mysql -e MYSQL_ALLOW_EMPTY_PASSWORD='true' -d mysql/mysql-server:5.7`
2. `docker exec -it plywood-test-mysql /opt/data/import-sql`

### Tear down

1. `docker stop plywood-test-mysql`
2. `docker rm plywood-test-mysql`

### Regular use
2. `docker start plywood-test-mysql`

