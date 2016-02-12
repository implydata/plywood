# Loading test data

## MySQL

Fill me in https://dev.mysql.com/doc/refman/5.1/en/load-data.html

## Druid

### Setup

1. Install Docker
2. `docker pull imply/imply`
2. `docker run -v /Users/vadim/Projects/plywood/data:/opt/data -p 8081-8110:8081-8110 -p 8200:8200 -p 9095:9095 -d --name plywood-test imply/imply`
2. `docker exec -it plywood-test bin/post-index-task -f /opt/data/wikipedia-index.json`

### Tear down

1. `docker stop plywood-test`
2. `docker rm plywood-test`

### Regular use

1. `docker start plywood-test`
