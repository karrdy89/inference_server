from fastapi import FastAPI
import uvicorn
from router import InferenceRouter

app = FastAPI()

inference_router = InferenceRouter()
app.include_router(inference_router.router)

uv_conf = {"app": "start_server:app",
           "host": "0.0.0.0",
           "port": 10001}

uv_conf = uvicorn.Config(**uv_conf)


class UvicornServer(uvicorn.Server):
    def install_signal_handlers(self):
        pass

# init deploy state
# init triton server


if __name__ == "__main__":
    server = UvicornServer(config=uv_conf)
    server.run()
