from fastapi import FastAPI
from starlette.middleware.httpsredirect import HTTPSRedirectMiddleware
import uvicorn

import request_util
from router import InferenceRouter
from service_state import ServiceState
from _constants import ROOT_DIR, SYSTEM_ENV, RequestPath

app = FastAPI()


for url in SYSTEM_ENV.TRITON_SERVER_URLS:
    url = url + RequestPath.TRITON_HEALTH_CHECK_API
    code, msg = request_util.get(url=url)
    if code != 0:
        raise RuntimeError(f"triton server '{url}' not ready. health check failed")


inference_router = InferenceRouter(app=app)
app.include_router(inference_router.router)

uv_conf = {"app": "start_server:app",
           "host": "0.0.0.0",
           "port": 7600,
           "log_config": ROOT_DIR+"/base_config/uv_log_config.ini"}


if SYSTEM_ENV.SSL_KEY and SYSTEM_ENV.SSL_CERT:
    app.add_middleware(HTTPSRedirectMiddleware)
    base_cert_path = ROOT_DIR + "/cert/"
    uv_conf["ssl_keyfile"] = base_cert_path + SYSTEM_ENV.SSL_KEY
    uv_conf["ssl_certfile"] = base_cert_path + SYSTEM_ENV.SSL_CERT
    if SYSTEM_ENV.SSL_CA_CERT:
        uv_conf["ssl_ca_certs"] = base_cert_path + SYSTEM_ENV.SSL_CERT

uv_conf = uvicorn.Config(**uv_conf)


class UvicornServer(uvicorn.Server):
    def install_signal_handlers(self):
        pass


if __name__ == "__main__":
    server = UvicornServer(config=uv_conf)
    service_state = ServiceState()
    server.run()
