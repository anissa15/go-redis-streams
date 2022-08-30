run:
	make redis 
	go run main.go

redis:
	docker stop redis || true 
	docker-compose up -d redis