import urllib.request
import json
import sys

def inject_trace_context(trace_id, span_id, target_url):
    # W3C Trace Parent spec: 00-traceid-spanid-flags
    trace_parent = f"00-{trace_id}-{span_id}-01"
    headers = {
        "traceparent": trace_parent,
        "Content-Type": "application/json"
    }
    req = urllib.request.Request(target_url, data=b"{}", headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req) as response:
            res_data = response.read().decode("utf-8")
            print(f"Success! Response: {res_data}")
    except Exception as e:
        print(f"Call failed: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python trace-propagator.py <trace_id> <span_id> <target_url>")
        sys.exit(1)
    inject_trace_context(sys.argv[1], sys.argv[2], sys.argv[3])
