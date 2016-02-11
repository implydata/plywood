# Loading test data

## MySQL

Fill me in

## Druid

### Setup

1. Install Docker
2. `docker pull imply/imply`
2. `docker run -v /Users/vadim/Projects/plywood/data:/opt/data -p 8081-8110:8081-8110 -p 8200:8200 -p 9095:9095 -d --name plywood-test imply/imply`
2. `docker exec -it plywood-test bin/post-index-task -f /opt/data/wikipedia-index.json`

docker stop plywood-test
docker rm plywood-test

### Regular use

1. `docker start plywood-test`




  492  docker ps
  493  docker ps -a
  494  docker start imply
  495  docker-machine ip default
  496  docker -it imply /bin/bash
  497  docker exec -it imply /bin/bash
  498  docker ps
  499  docker run -p 8081-8110:8081-8110 -p 8200:8200 -p 9095:9095 -d --name imply imply/imply
  500  docker-machine env default
  501  eval $(docker-machine env default)
  502  docker ps
  503  docker run
  504  docker run --help
  505  docker
  506  docker cp
  507  docker stop impl
  508  docker stop imply
  509  docker run -v /Users/vadim/Projects/plywood/data:/opt/data -p 8081-8110:8081-8110 -p 8200:8200 -p 9095:9095 -d --name imply imply/imply
  510  docker rm imply
  511  docker run -v /Users/vadim/Projects/plywood/data:/opt/data -p 8081-8110:8081-8110 -p 8200:8200 -p 9095:9095 -d --name imply imply/imply
  512  docker exec -it /bin/bash
  513  docker exec -it imply /bin/bash
  514  docker exec -it plywood-test /bin/bash
