"""Simple mock Jira server for testing the LAVA event listener.

Simulates both the JSM API (/rest/servicedeskapi/) and the standard
Jira API (/rest/api/2/) endpoints used by the listener.
"""

import json
from http.server import HTTPServer, BaseHTTPRequestHandler

TICKETS = {}
NEXT_ID = 1


class JiraHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        global NEXT_ID
        content_length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(content_length)) if content_length else {}

        # JSM create request
        if self.path == "/rest/servicedeskapi/request":
            key = f"TEST-{NEXT_ID}"
            NEXT_ID += 1
            summary = body.get("requestFieldValues", {}).get("summary", "")
            TICKETS[key] = {"status": "Open", "statusCategory": "NEW", "summary": summary, "comments": []}
            print(f"  CREATED ticket {key}: {summary}")
            self._json_response(201, {"issueKey": key, "issueId": str(NEXT_ID - 1)})

        # JSM add comment
        elif self.path.startswith("/rest/servicedeskapi/request/") and self.path.endswith("/comment"):
            key = self.path.split("/")[4]
            comment = body.get("body", "")
            if key in TICKETS:
                TICKETS[key]["comments"].append(comment)
                print(f"  COMMENT on {key}: {comment}")
            self._json_response(201, {"id": "1"})

        else:
            self.send_response(404)
            self.end_headers()

    def do_PUT(self):
        # Test helper: close a ticket
        if self.path.startswith("/test/close-ticket/"):
            key = self.path.split("/")[-1]
            if key in TICKETS:
                TICKETS[key]["status"] = "Completed"
                TICKETS[key]["statusCategory"] = "DONE"
                print(f"  CLOSED ticket {key}")
                self._json_response(200, {"ok": True})
            else:
                self.send_response(404)
                self.end_headers()
            return
        self.send_response(404)
        self.end_headers()

    def do_GET(self):
        # JSM list service desks
        if self.path == "/rest/servicedeskapi/servicedesk":
            self._json_response(200, {
                "values": [{"id": 1, "projectKey": "TEST", "projectName": "Test Project"}],
            })

        # JSM list request types
        elif "/requesttype" in self.path and "/servicedeskapi/servicedesk/" in self.path:
            self._json_response(200, {
                "values": [
                    {"id": 10, "name": "Service Request"},
                    {"id": 11, "name": "Incident"},
                ],
            })

        # JSM get request status
        elif self.path.startswith("/rest/servicedeskapi/request/") and "/comment" not in self.path:
            key = self.path.split("/")[4]
            if key in TICKETS:
                self._json_response(200, {
                    "issueKey": key,
                    "currentStatus": {
                        "status": TICKETS[key]["status"],
                        "statusCategory": TICKETS[key]["statusCategory"],
                    },
                })
            else:
                self.send_response(404)
                self.end_headers()

        else:
            self.send_response(404)
            self.end_headers()

    def _json_response(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def log_message(self, format, *args):
        print(f"  [Mock Jira] {args[0]}")


if __name__ == "__main__":
    server = HTTPServer(("127.0.0.1", 8089), JiraHandler)
    print("Mock Jira server running on http://127.0.0.1:8089")
    server.serve_forever()
