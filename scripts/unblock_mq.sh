docker exec eventclient_rabbit1_1 rabbitmqctl set_vm_memory_high_watermark 0.4 
docker exec eventclient_rabbit2_1 rabbitmqctl set_vm_memory_high_watermark 0.4 