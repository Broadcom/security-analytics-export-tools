#!/usr/bin/env python
'''
consumer.py

This script:
    1. Configures a RabbitMQ service to accept Security Analytics
        ICDx alert and metadata messages.
    2. Registers with RabbitMQ as a consumer.
    3. Runs indefinitely, forwarding all RabbitMQ messages
        to a Splunk service.

USAGE
    python3 consumer.py --config [file]

    Configure user arguments in alerts.ini and meta.ini.
'''

import gzip
import sys
import argparse
import configparser
import json
import time
import multiprocessing as mp
import requests
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
import urllib3
import pika

# Debug: uncomment the line below to support HTTP logging
#   and uncomment add_stderr_logger in the new_process function
#from urllib3 import add_stderr_logger


class RabbitToSplunk:
    '''The forwarder process - instantiated by main().'''

    def __init__(self, config, process_num, result_queue):
        '''Set the .ini configuration variables.'''

        self.config = config
        self.process_num = process_num
        self.result_queue = result_queue
        self.msg_count = 0
        self.last_time = int(time.time())
        self.verbose = config['verbose']
        self.debug = config['debug']
        self.session = requests.Session()
        self.session.mount('https://', HTTPAdapter())
        self.session.mount('http://', HTTPAdapter())
        print(f'Forwarding messages from {config["rabbitmq.exchange"]}.'
            f' Process_num = {process_num}')

    def start(self):
        '''
        Configure the RabbitMQ virtual host, exchange, and queues,
            and wait for callbacks. These configuration operations are
            idempotent and may run multiple times during multiple
            consumer launches without any side effects.
        '''

        if self.debug:
            print('\n*** Debug mode is enabled ***')
            time.sleep(3)

        # amqp://<user>:<password>@<server>:<port>/<vhost>
        connection_str = (
                        f'amqp://'
                        f'{self.config["rabbitmq.user"]}:'
                        f'{self.config["rabbitmq.password"]}@'
                        f'{self.config["rabbitmq.server"]}:'
                        f'{self.config["rabbitmq.amqp_port"]}/'
                        f'{self.config["rabbitmq.vhost"]}'
        )
        try:
            connection = pika.BlockingConnection(pika.URLParameters(connection_str))
            channel = connection.channel()
        except ConnectionRefusedError:
            print(f'\n*** The RabbitMQ connection was refused: ({connection_str})...aborting.\n')
            sys.exit(1)

        exchange = self.config['rabbitmq.exchange']

        # Create the exchange to receive messages. This name must match the value in the SA ICDx UI.
        channel.exchange_declare(exchange=exchange, durable=True)

        # Create the Queue to publish messages. The queue name doesn't really matter
        # since this script will connect the queue's exchange, so derive the name
        # from the exchange name by appending "_q".
        queue = exchange + "_q"
        channel.queue_declare(queue=queue)

        # Create a binding to connect the exchange to the Queue
        channel.queue_bind(queue = queue, exchange = exchange, routing_key="")

        # Limit the number of unacknowledged messages that can be sent to the consumer.
        # If the prefetch count isn't set, RabbitMQ will continue to send
        # data to the RabbitMQ consumer library, which will buffer it. That will
        # cause the consumer application to continue to consume memory if it can't
        # keep up. It's better to let the messages stay queued in RabbitMQ, where
        # it will be obvious on the RabbitMQ GUI that the consumer isn't keeping
        # up.
        channel.basic_qos(prefetch_count=10)

        # Start consuming messages
        channel.basic_consume(queue=queue, on_message_callback=self.rabbit_callback, auto_ack=False)

        channel.start_consuming()


    def add_event(self, json_data):
        '''
        Convert each element to a JSON string and add the required "event" tags
            for the HEC data (time, host, source, event).
        '''

        event = {'event': json_data}
        if 'device_time' in json_data.keys():
            # Time must be in seconds.milliseconds. Device time has milliseconds.microseconds
            event['time'] = int(json_data['device_time'])/1000
        if 'device_name' in json_data.keys():
            event['host'] = json_data['device_name']
        if 'product_name' in json_data.keys():
            event['source'] = json_data['product_name']

        event_str = json.dumps(event)

        return event_str


    def send_to_splunk(self, content_encoding, body):
        '''
        Post the message to Splunk

        Splunk requires event data to be wrapped in an "event" element.
        It also allows multiple events in a single message. When receiving
        metadata from Security Analytics, the events will be wrapped in
        a JSON list. Each item in the list needs to be wrapped in
        an "event" tag before being sent to Splunk.

        See https://docs.splunk.com/Documentation/Splunk/8.2.6/Data/HECExamples
        '''

        # Data from Security Analytics is gzipped, so it needs to be decompressed
        if content_encoding == 'gzip':
            # Unzip the data
            json_str = gzip.decompress(body)
            json_str = json_str.decode('utf-8')
        else:
            # Data isn't compressed
            json_str = body.decode('utf-8')


        if self.debug:
            print(f'\nSending data: {json_str}')

        json_data = json.loads(json_str)
        if not isinstance(json_data, list):
            # Convert to a list to allow for common wrapping code
            json_data = [json_data]

        # Wrap the data in an "event" key
        event_list = map(self.add_event, json_data)
        # Join all the data into a single message. There is no separator
        event_data = "".join(event_list)

        try:
            reply = self.session.post(self.config['splunk.url'],
                verify = False, data=event_data,
                headers = {'Authorization': 'Splunk '
                + self.config['splunk.token']})
            reply.raise_for_status()
        except requests.exceptions.RequestException as error:
            print(f'\n*** Splunk connectivity error...aborting ***\n {error}\n')
            sys.exit(1)

        if not reply.ok:
            print(f'\n*** Failure: {reply.reason} ({reply.status_code})...aborting.\n')
            sys.exit(1)

        if self.verbose:
            print('.', end='', flush=True)

        # Return the number of sessions
        return len(json_data)

    def rabbit_callback(self, channel, method, properties, body):
        '''Callback for new messages from RabbitMQ.'''

        # Send to splunk
        session_count = self.send_to_splunk(properties.content_encoding, body)
        # Acknowledge that the message has been handled
        channel.basic_ack(delivery_tag = method.delivery_tag)
        # Publish the message count to the main process for aggregation
        self.result_queue.put((self.process_num, session_count))


def get_config(config_file):
        # Grab the parameters from the config file
        config = configparser.ConfigParser()
        config_data = {}
        if len(config.read(config_file)) == 1:
            config_data['rabbitmq.user'] = config.get('rabbitmq', 'user')
            config_data['rabbitmq.password'] = config.get('rabbitmq', 'password')
            config_data['rabbitmq.server'] = config.get('rabbitmq', 'server')
            config_data['rabbitmq.amqp_port'] = config.get('rabbitmq', 'amqp_port')
            config_data['rabbitmq.http_port'] = config.get('rabbitmq', 'http_port')
            config_data['rabbitmq.vhost'] = config.get('rabbitmq', 'vhost')
            config_data['rabbitmq.exchange'] = config.get('rabbitmq', 'exchange')

            config_data['splunk.url'] = config.get('splunk','url')
            config_data['splunk.token'] = config.get('splunk','token')

            config_data['threads'] = config.getint('general', 'threads')
            config_data['verbose'] = config.getboolean('general', 'verbose')
            config_data['debug'] = config.getboolean('general', 'debug')
        else:
            print(f'\n*** Unable to open configuration file \'{config_file}\'.')
            sys.exit(1)

        return config_data

def add_vhost(config):
    '''Create the RabbitMQ virtual host required by the SA ICDx export mechanism.'''

    # curl -i -u guest:guest -H "content-type:application/json"
    #   -XPUT http://localhost:15672/api/vhosts/dx
    url = (
        f'http://'
        f'{config["rabbitmq.server"]}:'
        f'{config["rabbitmq.http_port"]}/api/vhosts/'
        f'{config["rabbitmq.vhost"]}'
    )

    # Verify RabbitMQ connectivity while configuring the virtual host.
    try:
        reply = requests.put(url, verify = False,
                        auth = HTTPBasicAuth(config['rabbitmq.user'],
                        config['rabbitmq.password']))
    except Exception as error:
        print(f'\n*** The connection was refused to RabbitMQ ({url})...aborting. Error: ${error}\n')
        sys.exit(1)

    if not reply.ok:
        print(f'\n*** Failed to add the RabbitMQ virtual host ({reply.text})...aborting.\n')
        sys.exit(1)

def test_splunk_connection(config):
    #Debug: Uncomment the line below to enable very verbose HTTP debugging
    #add_stderr_logger()

    # Disable the warning about connecting without verifying SSL
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    url=config['splunk.url']
    headers = {"Authorization": "Splunk " + config['splunk.token']}
    json_health_check = {"event": "Splunk connectivity check"}
    try:
        reply = requests.post(url, headers=headers, json=json_health_check, verify=False)
        reply.raise_for_status()
    except requests.exceptions.HTTPError as error:
        print('\n*** An HTTP error occurred while connecting to Splunk.')
        print('*** Verify the config file\'s Splunk URL path and token\n')
        print(f'{error}\n')
        sys.exit(1)
    except requests.exceptions.ConnectionError as error:
        print('\n*** An HTTP Connection error occurred while connecting to Splunk.')
        print('*** Verify the config file\'s Splunk URL IP address and port.\n')
        print(f'{error}\n')
        sys.exit(1)
    except requests.exceptions.RequestException as error:
        print('\n*** An exception occurred while connecting to Splunk.')
        print(f'{error}\n')
        sys.exit(1)

def new_process(config, process_num, result_queue):
    '''Parse the configuration file and start RabbitToSplunk() forwarding.'''
    # Python doesn't support true multi-threading due to the GIL. The way to
    # work around this limitation is to create multiple processes. This method
    # will create a new process that will run independently of the main
    # process, other than sending back results for reporting.
    try:
        # Begin forwarding
        forwarder = RabbitToSplunk(config, process_num, result_queue)
        forwarder.start()
    except KeyboardInterrupt:
        print('Interrupted. Exiting child python process.', file=sys.stderr)


def main():
    '''Get the user configuration and spawn the requested child processes.'''

    # Disable the warning about connecting without verifying SSL
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Locate the .ini config file
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="The configuration file", required=True)
    args = parser.parse_args()

    # Grab the parameters from the config file
    config  = get_config(args.config)

    # Configure the RabbitMQ virtual host
    add_vhost(config)

    # Make sure we can connect to splunk
    test_splunk_connection(config)

    # Create the requested number of processes
    result_queue = mp.Queue()
    process_list = map(lambda process_num:
                       mp.Process(target=new_process,
                       args=(config, process_num, result_queue)),
                       range(config['threads']))
    try:
        print('Waiting for messages. To exit press CTRL+C', file=sys.stderr)
        # Start the processes
        for p in process_list:
            p.start()

        # Counters for periodic status
        total_msg_count = 0
        last_total_msg_count = 0
        total_session_count = 0
        last_total_session_count = 0
        last_time = int(time.time())

        while True:
            # Wait for a result from one of the child processes
            (process_num, session_count) = result_queue.get()
            total_msg_count += 1
            total_session_count += session_count

            now = int(time.time())
            delta_time = now - last_time
            if delta_time >= 5:
                msg_per_second = (total_msg_count - last_total_msg_count) / delta_time
                sessions_per_second = (total_session_count - last_total_session_count) / delta_time
                print(f'\n[python] Exchange: {config["rabbitmq.exchange"]}, Total Messages: {total_msg_count}, '
                      f'Messages/Second: {msg_per_second:,.1f}, '
                      f'Sessions/Second: {int(sessions_per_second)}')
                last_time = now
                last_total_msg_count = total_msg_count
                last_total_session_count = total_session_count
    except KeyboardInterrupt:
        print('Interrupted', file=sys.stderr)
        for p in process_list:
            p.kill()
            p.join()
        sys.exit(1)


if __name__ == '__main__':
    '''Run while listenting for a CTRL+C.'''

    main()
