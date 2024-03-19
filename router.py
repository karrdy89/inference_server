from fastapi import APIRouter, Depends, FastAPI, Query
from starlette.middleware.httpsredirect import HTTPSRedirectMiddleware


from state_manager import StateManager
import request_vo as req_vo
import response_vo as res_vo
from validator_request import validate_token, validator_load_model, validator_unload_model, validator_load_ensemble, \
    validator_create_endpoint, validator_remove_endpoint, validator_update_model_latest
from _types import TaskResultCode, RequestResult
from _constants import SYSTEM_ENV, PropPath


app = FastAPI()
router = APIRouter()

state_manager: StateManager = StateManager(app)


@router.get("/endpoint/list", response_model=res_vo.ListEndpoint)
def list_endpoint(project: str = Query(default=None), auth: None = Depends(validate_token)):
    result_msg = res_vo.ListEndpoint(CODE=RequestResult.SUCCESS, ERROR_MSG='', ENDPOINTS=[])
    endpoint_list = []
    projects = {}
    for path, desc in state_manager.routing_table_desc.items():
        split_path = path.split('/')
        t_project = split_path[1]
        t_service_name = '/'.join(split_path[2:-1])
        r_desc = desc.dict()
        r_desc["NAME"] = t_service_name
        r_desc["REGION"] = SYSTEM_ENV.DISCOVER_REGION
        if t_project in projects:
            projects[t_project]["SERVICES"].append(r_desc)
        else:
            projects[t_project] = {"SERVICES": [r_desc]}
    if project is None:
        for project_id, services in projects.items():
            endpoint_list.append({"PROJECT": project_id, "SERVICES": services["SERVICES"]})
        result_msg.ENDPOINTS = endpoint_list
        return result_msg
    else:
        if project in projects:
            result_msg.ENDPOINTS = projects[project]["SERVICES"]
            return result_msg
        else:
            result_msg.CODE = RequestResult.FAIL
            result_msg.ERROR_MSG = f"project '{project} not exist'"
            return result_msg


@router.get("/status/loaded_models", response_model=res_vo.LoadedModels)
def get_loaded_models():
    result_msg = res_vo.LoadedModels(CODE=RequestResult.SUCCESS, ERROR_MSG='', LOADED_MODELS={})
    result_loaded_models = {}
    for model_key, version_state in state_manager.loaded_models.items():
        result_loaded_models[model_key] = {
            "VERSIONS": [{"VERSION": version, "REF": ref} for version, ref in version_state.states.items()],
            "LATEST": version_state.latest}
    result_msg.LOADED_MODELS = result_loaded_models
    return result_msg


@router.post("/model/load", response_model=res_vo.Base)
def load_model(req_body: req_vo.LoadModel = Depends(validator_load_model)):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.load_model(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post(PropPath.LOAD_MODEL, response_model=res_vo.Base)
def prop_load_model(req_body: req_vo.PropLoadModel):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.prop_load_model(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post(PropPath.ROLLBACK_LOAD_MODEL, response_model=res_vo.Base)
def prop_rollback_load_model(req_body: req_vo.PropLoadModel):
    state_manager.prop_rollback_load_model(req_body)
    return res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')


@router.post("/model/unload", response_model=res_vo.Base)
def unload_model(req_body: req_vo.UnloadModel = Depends(validator_unload_model)):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.unload_model(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post(PropPath.UNLOAD_MODEL, response_model=res_vo.Base)
def prop_unload_model(req_body: req_vo.PropUnloadModel):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.prop_unload_model(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post("/ensemble/load", response_model=res_vo.Base)
def load_ensemble(req_body: req_vo.LoadEnsemble = Depends(validator_load_ensemble)):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.load_ensemble(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post(PropPath.LOAD_ENSEMBLE, response_model=res_vo.Base)
def prop_load_ensemble(req_body: req_vo.PropLoadEnsemble):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.prop_load_ensemble(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post(PropPath.ROLLBACK_LOAD_ENSEMBLE, response_model=res_vo.Base)
def prop_rollback_load_ensemble(model_info: req_vo.PropLoadEnsemble):
    state_manager.prop_rollback_load_ensemble(model_info)
    return res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')


@router.post("/ensemble/unload", response_model=res_vo.Base)
def unload_ensemble(req_body: req_vo.LoadEnsemble = Depends(validator_load_ensemble)):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.unload_ensemble(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post(PropPath.UNLOAD_ENSEMBLE, response_model=res_vo.Base)
def prop_unload_ensemble(req_body: req_vo.PropLoadEnsemble):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.prop_unload_ensemble(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post("/endpoint/create", response_model=res_vo.Base)
def create_endpoint(req_body: req_vo.CreateEndpoint = Depends(validator_create_endpoint)):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.create_endpoint(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post(PropPath.CREATE_ENDPOINT, response_model=res_vo.Base)
def prop_create_endpoint(req_body: req_vo.PropCreateEndpoint):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.prop_create_endpoint(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post(PropPath.OVERWRITE_ENDPOINT, response_model=res_vo.Base)
def prop_rollback_create_endpoint(req_body: req_vo.OverwriteEndpoints):
    state_manager.prop_overwrite_endpoint(req_body)
    return res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')


@router.post("/endpoint/remove", response_model=res_vo.Base)
def remove_endpoint(req_body: req_vo.RemoveEndpoint = Depends(validator_remove_endpoint)):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.remove_endpoint(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post(PropPath.REMOVE_ENDPOINT, response_model=res_vo.Base)
def prop_remove_endpoint(req_body: req_vo.PropRemoveEndpoint):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.prop_remove_endpoint(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post("/endpoint/update", response_model=res_vo.Base)
def update_endpoint(req_body: req_vo.UpdateEndpoint):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.update_endpoint(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post(PropPath.UPDATE_ENDPOINT, response_model=res_vo.Base)
def prop_update_endpoint(req_body: req_vo.PropCreateEndpoint):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.prop_create_endpoint(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post("/model/update_latest", response_model=res_vo.Base)
def update_model_latest(req_body: req_vo.ModelLatest = Depends(validator_update_model_latest)):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.update_model_latest(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post(PropPath.UPDATE_MODEL, response_model=res_vo.Base)
def prop_update_model_latest(req_body: req_vo.PropUpdateModelLatest):
    result_msg = res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.prop_update_model_latest(req_body)
    if task_result.CODE == TaskResultCode.DONE:
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.post(PropPath.ROLLBACK_UPDATE_MODEL, response_model=res_vo.Base)
def prop_rollback_update_model(req_body: req_vo.OverwriteModelState):
    state_manager.overwrite_model_state(req_body)
    return res_vo.Base(CODE=RequestResult.SUCCESS, ERROR_MSG='')


@router.get(PropPath.JOIN_CLUSTER, response_model=res_vo.RspInferenceServiceState)
def append_cluster(url: str = Query()):
    result_msg = res_vo.RspInferenceServiceState(CODE=RequestResult.SUCCESS, ERROR_MSG='')
    task_result = state_manager.append_cluster(url)
    if task_result.CODE == TaskResultCode.DONE:
        result_msg.SERVICE_STATE = task_result.RESULT_VALUE
        return result_msg
    else:
        result_msg.CODE = RequestResult.FAIL
        result_msg.ERROR_MSG = task_result.MESSAGE
        return result_msg


@router.get(PropPath.ACQUIRE_TASK_HANDLE, response_model=res_vo.Agreement)
def get_agreement(url: str = Query()):
    agreement = state_manager.acquire_handle(url)
    return res_vo.Agreement(CODE=RequestResult.SUCCESS, ERROR_MSG='', AGREEMENT=agreement)


app.include_router(router)
if SYSTEM_ENV.SSL_KEY and SYSTEM_ENV.SSL_CERT:
    app.add_middleware(HTTPSRedirectMiddleware)
