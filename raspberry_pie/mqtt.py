import paho.mqtt.client as mqtt
import pygame
import os
import sys
import wave
#from subprocess import call
import vlc


host="test.mosquitto.org"
#host="test.mosquitto.org"

MQTT_TOPICS=[("music",0), ("pause",0), ("voice",0), ("sensor", 0)]

player = vlc.MediaPlayer("music1.wav")

def on_message(client, obj, msg):
    
    print("connect android")
    
    if msg.topic == "music":
        print("playing Music")
        player.play()
        
    elif msg.topic == "pause":
        print("Stopping Music")
        player.stop()
    
    elif msg.topic == "voice":
        print("write")
        f = open('sample.wav','wb')
        f.write(msg.payload)
        f.close()
        os.system('omxplayer -o alsa sample.wav')    
        
client = mqtt.Client("client_name")
print("connect to:", host)
client.connect(host, 1883)
client.subscribe(MQTT_TOPICS)
client.on_message = on_message


ms = 0

while ms == 0:
    ms = client.loop()