import time
import paho.mqtt.client as paho
import hashlib

broker="broker.hivemq.com"
count = 1
do = 0


topic="jiyoung"
qos=1
filename = "image.jpg"
data_block_size=16000
file_out=filename
fout=open(file_out,"wb") #use a different filename

def process_message(msg):
    
   print("received ")
   
   # for outfile as I'm running sender and receiver together
   global bytes_in
   if len(msg)==200: #is header or end
      print("found header")
      msg_in=msg.decode("utf-8")
      msg_in=msg_in.split(",,")
      if msg_in[0]=="end": #is it really last packet?
         in_hash_final=in_hash_md5.hexdigest()
         if in_hash_final==msg_in[2]:           
            print("File copied OK -valid hash  ",in_hash_final)
            return -1
         else:
            print("Bad file receive   ",in_hash_final)
            return -2
      else:
         if msg_in[0]!="header":
            in_hash_md5.update(msg)
            return True
         else:
            run_flag=False
            return False
   else:
      bytes_in=bytes_in+len(msg)
      in_hash_md5.update(msg)
      print("found data bytes= ",bytes_in)

      do = bytes_in
      return True


while True:

   
   
   #define callback
   def on_message(client, userdata, message):
      global run_flag
      ret=process_message(message.payload)
      if ret:
         fout.write(message.payload)
      if ret== -1:
         run_flag=False #exit receive loop
         print("complete file received")
         
         count1 =count1+1
      if ret == -2:
         run_flag=False
         print("File received error")
   


   bytes_in=0
   client= paho.Client("client-receive-001")
   ######
   client.on_message=on_message
   client.mid_value=None
   #####
   print("connecting to broker ",broker)
   
   if bytes_in != 0:
      run_flag = False
   client.connect(broker)#connect
   #client.loop_start() #start loop to process received messages
   print("subscribing ")
   client.subscribe(topic)#subscribe
   time.sleep(2)
   start=time.time()
   time_taken=time.time()-start
   in_hash_md5 = hashlib.md5()
   run_flag=True
   while run_flag:
      client.loop(00.1)  #manual loop
      pass

   if do != 0:
      run_flag=False

   client.disconnect() #disconnect
   count = count +1
   if count == 7:
      count = 1
      fout.close()
   

