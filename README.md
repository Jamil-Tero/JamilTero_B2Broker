# JamilTero_B2Broker
I divided the work into two tasks. First task is receiving messages from clients while the second task is publishing data once the total size of received messages exceeds a certain threshold. 
# Classes description: 
   MessageRepo is responsible for storing messages. 
   Publisher is responsible for publishing data using connection. 
   Manager is the main controller who uses an instance from MessageRepo and an instance from Publisher to manage the work. I am using an IBusConnection instance in the Manager constructor as dependency injection. I didn’t use dependency injection for MessageRepo , Publisher to prevent users from linking an instance of any of them with more than one publisher because this could break the workflow and result in unexpected behavior. 
   Logger is a helper class to log data actions and print them. Using this design we can receive messages and publish at the same time. Usually the publish operation takes much longer time so we don’t want to block our clients from sending messages during the publish operation.
