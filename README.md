# apiKafkaAdapter
a router navigator for kafka topics

# run kafka
docker compose -f kafkaui.yaml up --build

# run app
uvicorn main:app --reload
