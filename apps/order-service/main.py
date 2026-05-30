import os
import time
import httpx
from fastapi import FastAPI, Request, Response
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("order-service")

resource = Resource.create({
    "service.name": "order-service",
    "service.namespace": "ecommerce",
    "deployment.environment": "production"
})

provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

app = FastAPI(title="Order Service", version="1.0.0")
FastAPIInstrumentor.instrument_app(app)

PAYMENT_SERVICE_URL = os.getenv("PAYMENT_SERVICE_URL", "http://payment-service:8081")

@app.post("/orders")
async def create_order(request: Request):
    with tracer.start_as_current_span("create_order_business_logic") as span:
        body = await request.json()
        order_id = body.get("order_id", "unknown")
        amount = body.get("amount", 0.0)
        span.set_attribute("order.id", order_id)
        span.set_attribute("order.amount", amount)
        
        logger.info(f"Processing order {order_id} for amount {amount}", extra={
            "trace_id": format(span.get_span_context().trace_id, "032x"),
            "span_id": format(span.get_span_context().span_id, "016x")
        })
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{PAYMENT_SERVICE_URL}/charges",
                json={"order_id": order_id, "amount": amount},
                timeout=5.0
            )
            
        if response.status_code != 200:
            span.set_status(trace.StatusCode.ERROR, f"Payment service failed: {response.text}")
            logger.error(f"Order {order_id} failed payment: {response.text}", extra={
                "trace_id": format(span.get_span_context().trace_id, "032x"),
                "span_id": format(span.get_span_context().span_id, "016x")
            })
            return {"status": "FAILED", "reason": "PAYMENT_FAILURE"}
            
        return {"status": "SUCCESS", "order_id": order_id}

@app.get("/health")
def health():
    return {"status": "UP"}
