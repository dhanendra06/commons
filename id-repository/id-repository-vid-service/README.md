## id-repository-vid-service


[Background & Design](../../docs/design/idrepository/vid-service.md)

[Api Documentation]( https://github.com/mosip/mosip/wiki/ID-Repository-API)

Default Port and Context Path

```
server.port=8091
server.servlet.path=/idrepository/v1
```
localhost:8090/idrepository/v1/swagger-ui.html


**Application Properties**

[id-repository-dev.properties](https://github.com/mosip/mosip-configuration/blob/0.12.0/config/id-repository-dev.properties)


```
Build and Deployment commands:

docker run -it -d -p 8091:8091 -e active_profile_env={profile}  -e spring_config_label_env= {branch} -e spring_config_url_env={config_server_url} docker-registry.mosip.io:5000/id-repository-vid-service

```
