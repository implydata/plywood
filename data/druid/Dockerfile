#FROM imply/imply:1.1.1
# Only imply people have access to this IAP 1.2.0 snapshot. Uncomment the line above if you need to run tests.
FROM 269875963461.dkr.ecr.us-east-1.amazonaws.com/iap:1.2.1-dc4b488-pivot-f63fba0-tranquility-e97f078-druid-6ac1750

COPY init /tmp/init
RUN chmod +x /tmp/init
RUN /tmp/init
