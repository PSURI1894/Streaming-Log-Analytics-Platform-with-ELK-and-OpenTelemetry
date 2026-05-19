import os
import random
import time
from fastapi import FastAPI, Request, Response, HTTPException
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("payment-service")

resource = Resource.create({
    "service.name": "payment-service",
    "service.namespace": "ecommerce",
    "deployment.environment": "production"
})

provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")))
provider.add_span_processor(processor)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer(__name__)

app = FastAPI(title="Payment Service", version="1.0.0")
FastAPIInstrumentor.instrument_app(app)

@app.post("/charges")
async def charge(request: Request):
    with tracer.start_as_current_span("process_payment") as span:
        body = await request.json()
        order_id = body.get("order_id", "unknown")
        amount = body.get("amount", 0.0)
        
        span.set_attribute("payment.amount", amount)
        span.set_attribute("payment.order_id", order_id)
        
        user_id = body.get("user_id", f"user_{random.randint(1000, 99999)}")
        
        delay = random.uniform(0.01, 0.5)
        if amount > 500:
            delay += 0.8
            
        time.sleep(delay)
        
        if random.random() < 0.10:
            span.set_status(trace.StatusCode.ERROR, "Gateway Timeout - Bank API unreachable")
            logger.error(f"Failed payment charge for order {order_id}: Bank timeout", extra={
                "trace_id": format(span.get_span_context().trace_id, "032x"),
                "span_id": format(span.get_span_context().span_id, "016x"),
                "user_id": user_id
            })
            raise HTTPException(status_code=504, detail="Bank timeout")
            
        logger.info(f"Successfully charged {amount} for order {order_id}", extra={
            "trace_id": format(span.get_span_context().trace_id, "032x"),
            "span_id": format(span.get_span_context().span_id, "016x")
        })
        return {"status": "CHARGED", "transaction_id": f"tx_{random.randint(100000, 999999)}"}

@app.get("/health")
def health():
    return {"status": "UP"}

# Work revision 2
