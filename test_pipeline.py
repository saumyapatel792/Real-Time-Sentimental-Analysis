import sys
import json
import urllib.request

try:
    sys.stdout.reconfigure(encoding='utf-8')
except Exception:
    pass

samples = [
    "This movie was fantastic!",
    "I really enjoyed this product.",
    "The movie was boring.",
    "This was a terrible experience."
]

print("=" * 65)
print(f"{'Input Text':<35} | {'Sentiment Output'}")
print("=" * 65)

for text in samples:
    req = urllib.request.Request(
        "http://localhost:8050/api/predict",
        data=json.dumps({"text": text}).encode('utf-8'),
        headers={"Content-Type": "application/json"}
    )
    resp = urllib.request.urlopen(req)
    data = json.loads(resp.read().decode('utf-8'))
    result = data["result"]
    print(f"{text:<35} | {result['label']} (Polarity: {result['polarity']}, Conf: {result['confidence']}%)")

print("=" * 65)
print("Pipeline verification successful!")
