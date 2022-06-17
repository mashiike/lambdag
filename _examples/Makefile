prepare: clean
	docker compose up -d

build:
	PAGER="" aws stepfunctions --endpoint http://localhost:8083 create-state-machine --definition file://./definition.asl.json --name "SamleDAG" --role-arn "arn:aws:iam::012345678901:role/DummyRole"

deploy:
	PAGER="" aws stepfunctions --endpoint http://localhost:8083 update-state-machine --state-machine-arn arn:aws:states:us-east-1:123456789012:stateMachine:SamleDAG --definition file://./definition.asl.json

run/%:
	PAGER="" aws stepfunctions --endpoint http://localhost:8083 start-execution --state-machine arn:aws:states:us-east-1:123456789012:stateMachine:SamleDAG --name $* --input '{"Comment": "Insert your DAG Run config here"}'

history/%:
	PAGER="" aws stepfunctions --endpoint http://localhost:8083 get-execution-history --execution-arn arn:aws:states:us-east-1:123456789012:execution:SamleDAG:$*

clean:
	docker compose down
