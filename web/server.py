import asyncio
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from uvicorn import Config, Server
import multipart


class HTTPServer:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.server = FastAPI()
        self.setup_routes()
        asyncio.get_event_loop().create_task(self.run_server())

    def setup_routes(self):
        @self.server.post("/submit")
        async def handle_post(request: Request):
            form = await request.form()
            msg = form.get("msg")
            return JSONResponse(content={"message_received": msg})

    async def run_server(self):
        config = Config(app=self.server, host=self.host, port=self.port, reload=False)
        server = Server(config)
        await server.serve()