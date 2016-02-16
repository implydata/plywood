# Loading test data
## General Setup
1. Install Docker
1. run `./make-data-files`

## MySQL

1. run `./make-sql` (make sure you run make-data-files first)

1. `docker pull mysql/mysql-server:5.7`

2. `docker run -v /Users/eva/Projects/plywood/data:/opt/data -p 8081-8110:8081-8110 -p 8200:8200 -p 6603:3306 -d --name plywood-test-mysql -e MYSQL_ALLOW_EMPTY_PASSWORD='true' -d mysql/mysql-server:5.7`

3. `docker exec -it plywood-test-mysql /opt/data/import-sql`

## Druid

### Setup

2. `docker pull imply/imply`
2. `docker run -v /Users/vadim/Projects/plywood/data:/opt/data -p 8081-8110:8081-8110 -p 8200:8200 -p 9095:9095 -d --name plywood-test imply/imply`
2. `docker exec -it plywood-test bin/post-index-task -f /opt/data/wikipedia-index.json`

### Tear down

1. `docker stop plywood-test`
2. `docker rm plywood-test`

### Regular use

1. `docker start plywood-test`
2. `docker start plywood-test-mysql`
