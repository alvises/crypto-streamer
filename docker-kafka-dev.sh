docker container run --name kafka --rm -p 2181:2181 -p 3030:3030 -p 8081:8081 \
                -p 8082:8082 -p 8083:8083 -p 9092:9092 \
                -e ADV_HOST=127.0.0.1 landoop/fast-data-dev