import json
import pika
import time
from flask import Flask, request, jsonify
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.trace import SpanKind
from opentelemetry.sdk.resources import Resource

# 1. Setup OpenTelemetry Tracer & OTLP Exporter ke Jaeger
provider = TracerProvider(resource=Resource.create({"service.name": "order-service"}))
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="localhost:4317", insecure=True))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("order-service")
app = Flask(__name__)

def publish_to_rabbitmq(order_data, headers):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672))
    channel = connection.channel()
    channel.exchange_declare(exchange='order_notifications_dlx', exchange_type='direct')
    channel.queue_declare(queue='order_notifications_dlq')
    channel.queue_bind(queue='order_notifications_dlq',
                       exchange='order_notifications_dlx',
                       routing_key='order_notifications')
    channel.queue_declare(queue='order_notifications',
                          arguments={'x-dead-letter-exchange': 'order_notifications_dlx'})

    # Publikasikan pesan beserta header AMQP yang memuat Trace Context
    properties = pika.BasicProperties(headers=headers, content_type='application/json')
    channel.basic_publish(
        exchange='',
        routing_key='order_notifications',
        body=json.dumps(order_data),
        properties=properties
    )
    connection.close()

@app.route('/orders', methods=['POST'])
def create_order():
    # 2. Inisialisasi Root Span untuk HTTP Handler
    with tracer.start_as_current_span("POST /orders", kind=SpanKind.SERVER) as root_span:
        order_data = request.json or {"order_id": "ORD-9982", "item": "Laptop", "amount": 15000000}
        order_id = order_data.get("order_id") # Ditambah untuk menangani Bad Request
        if not order_id:
            return jsonify({"error": "order_id is required"}), 400
        root_span.set_attribute("order.id", order_id)

        # 3. Buat Producer Span khusus u/ aksi publikasi ke Message Broker
        with tracer.start_as_current_span("publish_order_created", kind=SpanKind.PRODUCER) as pub_span:
            headers = {}
            # 4. Inject Trace Context aktif ke dalam dictionary headers 
            TraceContextTextMapPropagator().inject(carrier=headers)
            headers["published_at"] = time.time() # Ditambah untuk evaluasi 
            
            pub_span.set_attribute("messaging.system", "rabbitmq")
            pub_span.set_attribute("messaging.destination", "order_notifications")
            
            publish_to_rabbitmq(order_data, headers)
            
        return jsonify({"status": "Order created & event published", 
                        "order_id": order_data["order_id"]}), 202

if __name__ == '__main__':
    app.run(port=5000)
