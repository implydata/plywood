# Loading test data

## General Setup

1. [Install Docker](https://docs.docker.com/engine/installation/)
1. Increase docker machine memory to 4GB 
  a. Open Oracle VM VirtualBox Manager (installed with docker)
  b. Under default machine -> Settings -> System -> Base Memory -> 4096MB
2. Run `./make-data-files`
3. If you've not set the IMPLY_PROJECTS environment variable, set it to the path to where Imply projects live

## Druid

### Setup

```bash
docker run -v /${IMPLY_PROJECTS}/plywood/data:/opt/data -p 8081-8110:8081-8110 -p 8200:8200 -p 9095:9095 -d --name plywood-test-druid imply/imply
docker exec -it plywood-test-druid /opt/data/druid/load-data
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
docker exec -it plywood-test-mysql /opt/data/mysql/load-data
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
