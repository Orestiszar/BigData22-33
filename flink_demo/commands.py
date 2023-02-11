docker exec -it broker kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic sales-usd

docker exec -it broker kafka-topics --create \
    --bootstrap-server localhost:9092 \
    --topic sales-euros

sudo docker exec -it broker kafka-console-consumer --from-beginning --bootstrap-server localhost:9092 --topic sales-euros
