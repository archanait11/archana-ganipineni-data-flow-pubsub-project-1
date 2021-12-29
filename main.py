import json
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from google.cloud import pubsub_v1


# parse order details
def parse_json(record):
    # print(type(record))
    dict1 = dict()
    dict1['order_id'] = record['order_id']
    dict1['addresses'] = dict()
    if len(record['order_address'].split(',')) == 3:
        print('in 3 len - first if')
        if record['order_address'].split(',')[-1].split(" ")[-2] in ['AP', 'AE', 'AA']:
            print('in 3 len - first if AP AE AA')
            dict1['addresses']['order_building_number'] = None
            dict1['addresses']['order_street_name'] = record['order_address'].split(',')[0] + \
                                                      record['order_address'].split(',')[1]
            dict1['addresses']['order_city'] = record['order_address'].split(',')[-1].split(" ")[-3].strip()
            dict1['addresses']['order_state_code'] = record['order_address'].split(',')[-1].split(" ")[-2]
            dict1['addresses']['order_zip_code'] = record['order_address'].split(',')[-1].split(" ")[-1]
        else:
            print('regular address')
            dict1['addresses']['order_building_number'], order_address_name = record['order_address'].split(' ', 1)
            dict1['addresses']['order_street_name'] = order_address_name.split(',')[0]
            dict1['addresses']['order_city'] = order_address_name.split(',')[1].strip()
            dict1['addresses']['order_state_code'] = order_address_name.split(',')[2].split(' ')[1]
            dict1['addresses']['order_zip_code'] = order_address_name.split(',')[2].split(' ')[2]
        # print('order_state_code ->', dict1['addresses']['order_state_code'])
        # print('order_zip_code ->', dict1['addresses']['order_zip_code'])
    elif len(record['order_address'].split(',')) == 2:
        print('in 2 len - ')
        dict1['addresses']['order_building_number'] = None
        dict1['addresses']['order_street_name'] = record['order_address'].split(',')[0]
        dict1['addresses']['order_city'] = record['order_address'].split(',')[1].split(' ')[-3]
        dict1['addresses']['order_state_code'] = record['order_address'].split(',')[1].split(' ')[-2]
        dict1['addresses']['order_zip_code'] = record['order_address'].split(',')[1].split(' ')[-1]
    dict1['addresses']['numberOfYears'] = ''
    dict1['first_name'], dict1['last_name'] = record['customer_name'].split(' ')
    dict1['customer_ip'] = record['customer_ip']
    cost_shipping = record['cost_shipping']
    cost_tax = record['cost_tax']
    total_item_price = []
    for order_item in record['order_items']:
        total_item_price.append(order_item['price'])
    dict1['order_currency'] = record['order_currency']
    total_cost = cost_shipping + cost_tax + sum(total_item_price)
    dict1['cost_total'] = ("{:.2f}".format(round(total_cost, 2)))
    print('this record loads into db ->', dict1)
    # if isinstance(dict1['addresses']['order_building_number'], int):
    return dict1


# prepare order to write to bq and to publish
def collect_order(data):
    order = []
    order_raw = json.loads(data.decode("utf-8"))
    # print('order raw data ->', order_raw)
    # order_messages = order_raw.get('messages')
    order.append(order_raw)
    # writing order details to history tabled based on USD, GBP, EUR
    write_order_to_bq(order)
    # publish order items to stock update topic
    order_items(order_raw)


PROJECT_ID = "york-cdf-start"
PUB_TOPIC = "archana_stock_update_topic_1"

# init
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, PUB_TOPIC)


# publish order items stock update
def publish(publisher, topic, message):
    data = json.dumps(message).encode('utf-8')
    return publisher.publish(topic, data=data)


# pub callback
def pub_callback(message_future):
    # When timeout is unspecified, the exception method waits indefinitely.
    if message_future.exception(timeout=30):
        print('Publishing message on {} threw an Exception {}.'.format(message_future.exception()))
    else:
        print(message_future.result())


# create order items to update stock to publish
def order_items(data):
    # print('type of data is', type(data))
    item_id = []
    dict1 = dict()

    for key, value in data.items():
        if key == "order_id":
            dict1["order_id"] = value
        if key == "order_items":
            for item in value:
                if isinstance(item, dict):
                    for key1, value1 in item.items():
                        if key1 == "id":
                            item_id.append(value1)
    dict1["items_list"] = item_id
    # print(type(dict1))
    # print(dict1)
    # call publish function to load order items update stock
    message_future = publish(publisher, topic_path, dict1)
    message_future.add_done_callback(pub_callback)
    print("order items published", dict1)


# write to big query tables based on order currency
def write_order_to_bq(data):
    with beam.Pipeline() as p:
        # create schema for bigquery to load order data
        table_schema = bigquery.TableSchema()

        # Fields that use standard types.
        order_id_schema = bigquery.TableFieldSchema()
        order_id_schema.name = 'order_id'
        order_id_schema.type = 'integer'
        order_id_schema.mode = 'nullable'
        table_schema.fields.append(order_id_schema)

        # A nested field
        addresses_schema = bigquery.TableFieldSchema()
        addresses_schema.name = 'addresses'
        addresses_schema.type = 'record'
        addresses_schema.mode = 'nullable'

        order_building_number = bigquery.TableFieldSchema()
        order_building_number.name = 'order_building_number'
        order_building_number.type = 'integer'
        order_building_number.mode = 'nullable'
        addresses_schema.fields.append(order_building_number)

        order_street_name = bigquery.TableFieldSchema()
        order_street_name.name = 'order_street_name'
        order_street_name.type = 'string'
        order_street_name.mode = 'nullable'
        addresses_schema.fields.append(order_street_name)

        order_city = bigquery.TableFieldSchema()
        order_city.name = 'order_city'
        order_city.type = 'string'
        order_city.mode = 'nullable'
        addresses_schema.fields.append(order_city)

        order_state_code = bigquery.TableFieldSchema()
        order_state_code.name = 'order_state_code'
        order_state_code.type = 'string'
        order_state_code.mode = 'nullable'
        addresses_schema.fields.append(order_state_code)

        order_zip_code = bigquery.TableFieldSchema()
        order_zip_code.name = 'order_zip_code'
        order_zip_code.type = 'integer'
        order_zip_code.mode = 'nullable'
        addresses_schema.fields.append(order_zip_code)

        numberOfYears = bigquery.TableFieldSchema()
        numberOfYears.name = 'numberOfYears'
        numberOfYears.type = 'string'
        numberOfYears.mode = 'nullable'
        addresses_schema.fields.append(numberOfYears)
        table_schema.fields.append(addresses_schema)

        first_name_schema = bigquery.TableFieldSchema()
        first_name_schema.name = 'first_name'
        first_name_schema.type = 'string'
        first_name_schema.mode = 'nullable'
        table_schema.fields.append(first_name_schema)

        last_name_schema = bigquery.TableFieldSchema()
        last_name_schema.name = 'last_name'
        last_name_schema.type = 'string'
        last_name_schema.mode = 'nullable'
        table_schema.fields.append(last_name_schema)

        customer_ip_schema = bigquery.TableFieldSchema()
        customer_ip_schema.name = 'customer_ip'
        customer_ip_schema.type = 'string'
        customer_ip_schema.mode = 'nullable'
        table_schema.fields.append(customer_ip_schema)

        cost_total_schema = bigquery.TableFieldSchema()
        cost_total_schema.name = 'cost_total'
        cost_total_schema.type = 'string'
        cost_total_schema.mode = 'nullable'
        table_schema.fields.append(cost_total_schema)

        order_currency_schema = bigquery.TableFieldSchema()
        order_currency_schema.name = 'order_currency'
        order_currency_schema.type = 'string'
        order_currency_schema.mode = 'nullable'
        table_schema.fields.append(order_currency_schema)

        # data flow for process order data from google pubsub subscription
        order_details = (
                p
                | 'Read order details record' >> beam.Create(data)
                | 'extract columns from order details' >> beam.Map(lambda rec: parse_json(rec))
        )

        table_names = (p | beam.Create([
            ('USD', 'york-cdf-start:aganipineni_proj_1.usd_order_payment_history'),
            ('GBP', 'york-cdf-start:aganipineni_proj_1.gbp_order_payment_history'),
            ('EUR', 'york-cdf-start:aganipineni_proj_1.eur_order_payment_history')
        ]))

        table_names_dict = beam.pvalue.AsDict(table_names)

        order_details | beam.io.gcp.bigquery.WriteToBigQuery(
            table=lambda row, table_dict: table_dict[row['order_currency']],
            table_side_inputs=(table_names_dict,),
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method='STREAMING_INSERTS'
        )


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    project_id = "york-cdf-start"
    subscription_id = "archana_topic_1-sub"
    # Number of seconds the subscriber should listen for messages
    timeout = 50.0

    # subscribe to read order details
    subscriber = pubsub_v1.SubscriberClient()
    # The `subscription_path` method creates a fully qualified identifier
    # in the form `projects/{project_id}/subscriptions/{subscription_id}`
    subscription_path = subscriber.subscription_path(project_id, subscription_id)


    def callback(message: pubsub_v1.subscriber.message.Message) -> None:
        # print('Received message: {}'.format(message.data))
        collect_order(message.data)
        message.ack()


    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            order_data = streaming_pull_future.result(timeout=timeout)

        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()  # Block until the shutdown is complete.

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
