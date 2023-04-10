import paho.mqtt.client as mqtt
import time

BROKER_ADDRESS = "192.168.20.12"
BROKER_PORT = 1883

USERNAME = "test_user"
PASSWORD = "password"
CLIENT_ID = "a1b2c3"
TEST_TOPIC = "test/topic"

def main():
    client = mqtt_connect()
    print(f"Connected to broker {BROKER_ADDRESS}:{BROKER_PORT}")
    client.subscribe(TEST_TOPIC)
    publish(client)

def mqtt_connect():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)
    
    def on_message(client, userdata, msg):
        print(f"Received `{msg.payload.decode()}` from `{msg.topic}` topic")
    
    # Set Connecting Client ID
    client = mqtt.Client(CLIENT_ID)
    client.username_pw_set(USERNAME, PASSWORD)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER_ADDRESS, BROKER_PORT)

    return client

def publish(client):
     msg_count = 0
     topic = TEST_TOPIC
     while True:
         msg = f"messages: {msg_count}"
         result = client.publish(topic, msg)
         
         # result: [0, 1]
         status = result[0]
         if status == 0:
             print(f"Send `{msg}` to topic `{topic}`")
         else:
             print(f"Failed to send message to topic {topic}")
         msg_count += 1

         for i in range(5):
            time.sleep(0.5)
            client.loop()



if __name__ == '__main__':
    main()