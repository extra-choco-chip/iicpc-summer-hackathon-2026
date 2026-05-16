import http.server
import socketserver
import json

PORT = 8080

class TradingHandler(http.server.SimpleHTTPRequestHandler):
    def do_POST(self):
        # We are ignoring the actual order data for now.
        # Just send a blazing fast "200 OK" receipt.
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        response = {"status": "order_received", "latency_check": "success"}
        self.wfile.write(json.dumps(response).encode())

# Start the server
with socketserver.TCPServer(("", PORT), TradingHandler) as httpd:
    print(f"Sandbox Engine locked in and listening on port {PORT}...")
    httpd.serve_forever()