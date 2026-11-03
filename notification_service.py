import json
import logging
import time
import pika
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.trace import SpanKind, StatusCode
from opentelemetry.sdk.resources import Resource

# 1. Setup OpenTelemetry Tracer & OTLP Exporter
provider = TracerProvider(resource=Resource.create({"service.name": "notification-service"}))
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="localhost:4317", insecure=True))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("notification-service")
# Setup Structured JSON u/ Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("notification-logger")

def log_structured(level, message, trace_id, span_id, extra_data=None):
    log_payload = {
        "level": level,
        "message": message,
        "trace_id": f"{trace_id:032x}" if trace_id else None,
        "span_id": f"{span_id:016x}" if span_id else None,
        "data": extra_data or {}
    }
    print(json.dumps(log_payload))

def callback(ch, method, properties, body):
    # 2. Ambil headers dari pesan AMQP (carrier)
    headers = properties.headers if properties and properties.headers else {}
    
    # 3. Extract Trace Context dari headers
    parent_context = TraceContextTextMapPropagator().extract(carrier=headers)
    
    # 4. Buat Child Span terhubung langsung ke Root Context Producer
    with tracer.start_as_current_span("process_notification", context=parent_context, kind=SpanKind.CONSUMER) as consumer_span:
        ctx = consumer_span.get_span_context()
        trace_id = ctx.trace_id
        span_id = ctx.span_id

        # Hitung waktu tunggu di antrean (queueing delay) dari header published_at
        published_at = headers.get("published_at")
        if published_at:
            consumer_span.set_attribute("messaging.queue_delay_ms", (time.time() - published_at) * 1000)

        try:
            order_event = json.loads(body)
            consumer_span.set_attribute("order.id", order_event.get("order_id"))

            # Pemicu kegagalan untuk menguji jalur error & DLQ
            if order_event.get("fail"):
                raise RuntimeError("Simulated notification failure")

            # Simulasi pemrosesan notifikasi
            time.sleep(0.5)
            log_structured("INFO", "Notification processed successfully", trace_id, span_id, order_event)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            
        except Exception as e:
            # Penanganan Fault Tolerance & Error Status
            consumer_span.record_exception(e)
            consumer_span.set_status(StatusCode.ERROR, description=str(e))
            log_structured("ERROR", f"Failed to process notification: {str(e)}", trace_id, span_id)
            # Requeue atau lempar ke Dead Letter Queue (DLQ)
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def start_consumer():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', 5672))
    channel = connection.channel()
    channel.exchange_declare(exchange='order_notifications_dlx', exchange_type='direct')
    channel.queue_declare(queue='order_notifications_dlq')
    channel.queue_bind(queue='order_notifications_dlq',
                    exchange='order_notifications_dlx',
                    routing_key='order_notifications')
    channel.queue_declare(queue='order_notifications',
                        arguments={'x-dead-letter-exchange': 'order_notifications_dlx'})
    channel.basic_consume(queue='order_notifications', on_message_callback=callback)
    logger.info("Notification Service Consumer started. Waiting for messages...")
    channel.start_consuming()

if __name__ == '__main__':
    start_consumer()
