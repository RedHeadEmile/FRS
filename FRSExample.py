import FRSClient


# publish event function need 2 args : channel: str and message: str
# do what you want with this
def on_publish(channel: str, message: str):
    print("Message received from channel \"" + channel + "\": \"" + message + "\"")


# create object all args is optional
client = FRSClient.Client(host="127.0.0.1", port=4514, publish_callback=on_publish, debug=False)
# start the client don't forget this
client.start()

# publish message
client.publish("channel", "message")

# set a value
# Format:
# client.set_value(key, field, value)
#
client.set_value("test", "a", "lol")
client.set_value("test", "b", "lol2")

# get amm fields from key
# Format:
# print(client.get_fields(key))
#
print(client.get_fields("test"))

# get a value from key and fields
# Format:
# print(client.get_value(key, field))
#
print(client.get_value("test", "a"))
print(client.get_value("test", "b"))

# get dic {field : value } from key
# Format:
# print(client.get_values(key, *values))
#
print(client.get_values("test", "a", "b"))

# if you end code then will close client
# do while True to don't stop or all your code
while True:
    stop = input("Enter text to stop -> ")
    if len(stop) > 0:
        # close when you want stop
        client.close()
        break
