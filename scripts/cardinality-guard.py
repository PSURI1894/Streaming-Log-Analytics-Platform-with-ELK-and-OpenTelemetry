import sys
import requests

def verify_metrics_cardinality(prometheus_endpoint):
    print(f"Scraping Prometheus active series metadata from {prometheus_endpoint}...")
    try:
        response = requests.get(f"{prometheus_endpoint}/api/v1/status/tsdb", timeout=10)
        if response.status_code != 200:
            print("Error: Failed to fetch status.")
            return False
            
        data = response.json().get("data", {})
        top_metrics = data.get("topCardinalityMetrics", [])
        print("\n--- Top Cardinality Metrics ---")
        hazard_found = False
        for metric in top_metrics[:10]:
            metric_name = metric.get("name")
            cardinality = metric.get("value")
            print(f" - {metric_name}: {cardinality} active series")
            if cardinality > 5000:
                print(f"   [WARNING]: Cardinality exceeds safe limits (5000) for metric {metric_name}!")
                hazard_found = True
        return not hazard_found
    except Exception as e:
        print(f"Request failed: {e}")
        return False

if __name__ == "__main__":
    endpoint = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:9090"
    verify_metrics_cardinality(endpoint)

# Work revision 3
