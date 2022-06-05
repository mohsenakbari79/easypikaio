

# EasyPikaIot

Library for easy amqp communication via pika lirmbrary

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install foobar.

```bash
pip install easypikaio
```

## Usage

```python
from easy_pika.pika_obj import PikaMassenger

# creat connection to rabittmq

PMI = PikaMassenger(username='<str>',password='<str>',host='<int:defualt(5672)>',port,exchange_name='<str>',exchange_type='<extype:defualt(direct)>')

# for create  consume

PMI.run(queue_name='<str>', routing_keys='<list>' ,callback='<function>')

# function callback
def callback(ch, method, properties, body):...
    PMI.connection._channel.basic_ack(method.delivery_tag)

# for publish  message
PMI.send_message(self,routing_key='<str>',data='<str>')


```

