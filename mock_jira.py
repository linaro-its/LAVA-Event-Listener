"""Simple mock Jira server for testing the LAVA event listener."""

import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler

TICKETS = {}
NEXT_ID = 1


class JiraHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        global NEXT_ID
        content_length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(content_length)) if content_length else {}

        if self.path == "/rest/api/2/issue":
            key = f"TEST-{NEXT_ID}"
            NEXT_ID += 1
            summary = body.get("fields", {}).get("summary", "")
            TICKETS[key] = {"status": "Open", "summary": summary, "comments": []}
            print(f"  CREATED ticket {key}: {summary}")
            self.send_response(201)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"key": key}).encode())

        elif "/comment" in self.path:
            key = self.path.split("/")[5]
            comment = body.get("body", "")
            if key in TICKETS:
                TICKETS[key]["comments"].append(comment)
                print(f"  COMMENT on {key}: {comment}")
            self.send_response(201)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"id": "1"}')

        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        if self.path.startswith("/rest/api/2/issue/"):
            key = self.path.split("/")[5].split("?")[0]
            if key in TICKETS:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                resp = {"fields": {"status": {"name": TICKETS[key]["status"]}}}
                self.wfile.write(json.dumps(resp).encode())
            else:
                self.send_response(404)
                self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        print(f"  [Mock Jira] {args[0]}")


if __name__ == "__main__":
    server = HTTPServer(("127.0.0.1", 8089), JiraHandler)
    print("Mock Jira server running on http://127.0.0.1:8089")
    server.serve_forever()
