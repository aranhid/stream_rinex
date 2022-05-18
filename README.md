# Stream rinex
## Установка Docker compose
Проверить последнюю версию на https://github.com/docker/compose/releases (на момент написания v2.5.1)
```
sudo curl -L "https://github.com/docker/compose/releases/download/v2.5.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
```
Дать права на исполнение
```
sudo chmod +x /usr/local/bin/docker-compose
```
Проверить работоспособность
```
docker-compose --version
```
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