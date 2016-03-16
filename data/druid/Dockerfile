#FROM imply/imply:1.1.0
FROM 269875963461.dkr.ecr.us-east-1.amazonaws.com/iap:1.2.0-671d228-bard-9cd0c77-tranquility-99b4752-druid-06ee440

COPY init /tmp/init
RUN chmod +x /tmp/init
RUN /tmp/init
