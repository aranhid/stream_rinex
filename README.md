# Stream rinex
## Запуск брокера Kafka
```
docker compose up
```
## Запуск отправителя данных
```
python sendStream.py ISTP125I.22O 30 topic
```
## Запуск получателя данных
```
python processStream.py topic
```