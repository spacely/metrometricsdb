from pubsub import pub

def publish_message(topic, **data):
    """
    Publishes data to a specified topic.
    
    Args:
        topic (str): The topic to publish the data to.
        data: Arbitrary keyword arguments representing the data being sent.
    """
    pub.sendMessage(topic, **data)

def subscribe_to_topic(topic, listener):
    """
    Subscribes a listener function to a specified topic.
    
    Args:
        topic (str): The topic to subscribe to.
        listener (callable): The function that will handle messages from the topic.
    """
    pub.subscribe(listener, topic)
