# Stream rinex
## git clone
```
git clone https://token:glpat-Z58aBm5ns1-JmSbCXShh@git.iszf.irk.ru/vladislav_tsybulya/stream_rinex.git
```
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
docker-compose up
```
## Python venv
Обновление pip
```
python3 -m pip install --user --upgrade pip
```
```
python3 -m pip --version
```
Установка virtualenv из pip
```
python3 -m pip install --user virtualenv
```
Создание виртуальной среды
```
python3 -m venv env
```
Активация виртуальной среды
```
source env/bin/activate
```
Проверка, где находится интерпретатор
```
which python
```
Деактивация виртуальной среды
```
deactivate
```
## Запуск отправителя данных
```
python sendStream.py ISTP125I.22O 30 localhost topic
```
## Запуск получателя данных
```
python processStream.py topic
```