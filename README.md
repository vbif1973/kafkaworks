ДОКЕР и КАФКА
---------------
как зайти в консоль кафки
1) docker ps - просмотрр запущенных образов
2) docker exec -it container_id bash

создать топик:
kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic test
Windows:
----------
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic testpartitions


Просмотр топиков:
1) $KAFKA_HOME/bin/kafka-topics.sh --list --zookeeper zookeeper:2181

Продьюсер и консьюмер из консоли:
1) продьюсер
$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic test
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list kafka:9092 --topic test
Windows:
----------
kafka-console-producer.bat --broker-list localhost:9092 --topic testtopic
 

2) консьюмер
$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic test --from-beginning

в первом окне вводим сообщение - оно появляется во втором

Очистка топика:
Удалить и создать:
kafka-topics.bat --zookeeper localhost:2181 --delete --topic testtopic
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testtopic


Погуглить: seekToBeginning/seekToEnd 
    
getWatermarkOffsets 

Тест кейз с 2-мя партициями:
----------------------------
Топик: testpartitions
group_id: cliettwopartitions

1) создать топик:
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic testpartitions

2) Общая информация о группе(появляется только после запуска спринг-приложения и появления реального клиента у группы)
kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group cliettwopartitions3 --describe

Соотв. есть партиции 0 и 1

3) Публикация сообщений в разные партиции: (НЕ НУЖНО - БУДЕТ 1 ПАРТИЦИЯ)
(ввод в формате ключ,значение разделяет на партиции)
kafka-console-producer.bat --broker-list localhost:9092 --topic testpartitions

вводим сообщения:
key1,msg1
key1,msg2
key1,msg3

key2,msg4
key2,msg5
key2,msg6
key2,msg7

3) Публикация сообщений в одну партицию:
создать топик
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic testpartitions
запуск продьюсера
kafka-console-producer.bat --broker-list localhost:9092 --topic testpartitions

вводим сообщения:
msg1
msg2
msg3
msg4
msg5
msg6
msg7

enable.auto.commit - ВАЖНО!