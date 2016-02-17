# Loading test data

## General Setup

1. [Install Docker](https://docs.docker.com/engine/installation/)
2. Run `./make-data-files`
3. If you've not set the IMPLY_PROJECTS environment variable, replace it with the path to where imply projects live

## Druid 

### Setup

```bash
docker run -v /${IMPLY_PROJECTS}/plywood/data:/opt/data -p 8081-8110:8081-8110 -p 8200:8200 -p 9095:9095 -d --name plywood-test-druid imply/imply
docker exec -it plywood-test-druid bin/post-index-task -f /opt/data/wikipedia-index.json
```

### Tear down

```bash
docker stop plywood-test-druid
docker rm plywood-test-druid
```

### Regular use

```bash
docker start plywood-test-druid
```

## MySQL

### Setup

```bash
docker run -v /${IMPLY_PROJECTS}/plywood/data:/opt/data -p 6603:3306 -d --name plywood-test-mysql -e MYSQL_ALLOW_EMPTY_PASSWORD='true' -d mysql/mysql-server:5.7
docker exec -it plywood-test-mysql /opt/data/import-sql
```

### Tear down

```bash
docker stop plywood-test-mysql
docker rm plywood-test-mysql
```

### Regular use

```bash
docker start plywood-test-mysql
```
