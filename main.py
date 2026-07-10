from datetime import date, datetime, timedelta
import hashlib
import hmac
import json
import math
import os
from pathlib import Path
import statistics

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
import httpx
import stripe
from urllib.parse import urlencode


app = FastAPI(title="VaultFlow Backend", version="2026.07.06")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BUILD_VERSION = "vaultflow-fastapi-2026-07-10-ai-storage-health"


def clean_env_value(name, fallback=""):
    raw = os.environ.get(name)
    if raw is None:
        raw = fallback
    value = str(raw).strip()
    for _ in range(2):
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1].strip()
    return value


def looks_like_client_id(value):
    return bool(value) and len(value) == 24 and all(ch in "0123456789abcdefABCDEF" for ch in value)

STRIPE_SECRET = clean_env_value("STRIPE_SECRET_KEY")
STRIPE_PUBLISHABLE_KEY = clean_env_value("STRIPE_PUBLISHABLE_KEY")
STRIPE_WEBHOOK = clean_env_value("STRIPE_WEBHOOK_SECRET")
PRICE_PRIME = clean_env_value("STRIPE_PRICE_PRIME")
PRICE_VAULT = clean_env_value("STRIPE_PRICE_VAULT")
OPENAI_API_KEY = clean_env_value("OPENAI_API_KEY")
OPENAI_MODEL = clean_env_value("OPENAI_MODEL", "gpt-4o-mini") or "gpt-4o-mini"
OPENAI_BASE_URL = clean_env_value("OPENAI_BASE_URL", "https://api.openai.com").rstrip("/") or "https://api.openai.com"
OPENAI_TIMEOUT_SECONDS = float(clean_env_value("OPENAI_TIMEOUT_SECONDS", "45") or "45")
DOC_VAULT_ENCRYPTION_KEY = clean_env_value("DOC_VAULT_ENCRYPTION_KEY")
DOC_VAULT_STORAGE_PROVIDER = clean_env_value("DOC_VAULT_STORAGE_PROVIDER")
DOC_VAULT_BUCKET = clean_env_value("DOC_VAULT_BUCKET")
DOC_VAULT_SIGNING_BASE_URL = clean_env_value("DOC_VAULT_SIGNING_BASE_URL")
DOC_VAULT_SIGNED_URL_TTL_MINUTES = int(clean_env_value("DOC_VAULT_SIGNED_URL_TTL_MINUTES", "15") or "15")
MAX_VAULT_DOCUMENTS = int(clean_env_value("MAX_VAULT_DOCUMENTS", "5000") or "5000")
APP_STORAGE_PROVIDER = clean_env_value("APP_STORAGE_PROVIDER")
APP_DATA_DIR = clean_env_value("APP_DATA_DIR") or clean_env_value("RAILWAY_VOLUME_MOUNT_PATH")
DATABASE_URL = clean_env_value("DATABASE_URL")
REDIS_URL = clean_env_value("REDIS_URL")
ENABLE_TRANSFER_RAIL = clean_env_value("ENABLE_TRANSFER_RAIL", "false").lower() == "true"
TRANSFER_PROVIDER = clean_env_value("TRANSFER_PROVIDER")
TRANSFER_WEBHOOK_URL = clean_env_value("TRANSFER_WEBHOOK_URL")
PLAID_TRANSFER_CREATE_ENABLED = clean_env_value("PLAID_TRANSFER_CREATE_ENABLED", "false").lower() == "true"
PLAID_TRANSFER_NETWORK = clean_env_value("PLAID_TRANSFER_NETWORK", "ach") or "ach"
PLAID_TRANSFER_ACH_CLASS = clean_env_value("PLAID_TRANSFER_ACH_CLASS", "ppd") or "ppd"
PLAID_TRANSFER_TYPE = clean_env_value("PLAID_TRANSFER_TYPE", "debit") or "debit"
PLAID_TRANSFER_MAX_AMOUNT = float(clean_env_value("PLAID_TRANSFER_MAX_AMOUNT", "1000") or "1000")
REPORT_EXPORTS_ENABLED = clean_env_value("REPORT_EXPORTS_ENABLED", "true").lower() != "false"
CAPITAL_WAITLIST_WEBHOOK_URL = clean_env_value("CAPITAL_WAITLIST_WEBHOOK_URL")
OWNER_USERNAME = clean_env_value("OWNER_USERNAME", "deion").lower() or "deion"
OWNER_EMAIL = clean_env_value("OWNER_EMAIL", "deion@vaultflow.owner") or "deion@vaultflow.owner"
OWNER_ACCESS_CODE = clean_env_value("OWNER_ACCESS_CODE")

PLAID_CLIENT_ID = clean_env_value("PLAID_CLIENT_ID")
PLAID_SECRET = clean_env_value("PLAID_SECRET")
PLAID_ENV = clean_env_value("PLAID_ENV", "sandbox") or "sandbox"
PLAID_CLIENT_NAME = clean_env_value("PLAID_CLIENT_NAME", "VaultFlow") or "VaultFlow"
PLAID_PRODUCTS = clean_env_value("PLAID_PRODUCTS", "auth,transactions")
PLAID_COUNTRY_CODES = clean_env_value("PLAID_COUNTRY_CODES", "US")
PLAID_BANK_INCOME_DAYS = int(os.environ.get("PLAID_BANK_INCOME_DAYS", "120") or "120")

ALPACA_KEY_ID = clean_env_value("ALPACA_KEY_ID")
ALPACA_SECRET_KEY = clean_env_value("ALPACA_SECRET_KEY")
ALPACA_TRADING_BASE_URL = clean_env_value("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets")
ALPACA_DATA_BASE_URL = clean_env_value("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")
ALPACA_DATA_FEED = clean_env_value("ALPACA_DATA_FEED", "iex")
ENABLE_ALPACA_PAPER_ORDERS = clean_env_value("ENABLE_ALPACA_PAPER_ORDERS", "false").lower() == "true"
ENABLE_LIVE_TRADING = clean_env_value("ENABLE_LIVE_TRADING", "false").lower() == "true"
LIVE_TRADE_MAX_NOTIONAL = float(clean_env_value("LIVE_TRADE_MAX_NOTIONAL", "1000") or "1000")

stripe.api_key = STRIPE_SECRET

PLAID_BASE = f"https://{PLAID_ENV}.plaid.com"
income_users = {}
bank_sessions = {}
capital_waitlist = []
transfer_requests = []
report_exports = []
vault_documents = []
app_state_store = {}


def split_env(value, fallback):
    raw = value or fallback
    return [item.strip() for item in raw.split(",") if item.strip()]


def requested_products(data):
    allowed = {"auth", "transactions", "investments", "identity", "liabilities"}
    products = data.get("products")
    if isinstance(products, str):
        products = split_env(products, "")
    if isinstance(products, list):
        cleaned = [item for item in products if item in allowed]
        if cleaned:
            return cleaned
    return split_env(PLAID_PRODUCTS, "auth,transactions")


def plaid_configured():
    return bool(PLAID_CLIENT_ID and PLAID_SECRET)


def plaid_health_payload():
    configured = plaid_configured()
    secret_may_be_client_id = bool(PLAID_SECRET and PLAID_SECRET == PLAID_CLIENT_ID)
    secret_shape_warning = bool(secret_may_be_client_id or looks_like_client_id(PLAID_SECRET) or len(PLAID_SECRET) < 25)
    return {
        "success": configured,
        "configured": configured,
        "build_version": BUILD_VERSION,
        "plaid_env": PLAID_ENV,
        "client_id_present": bool(PLAID_CLIENT_ID),
        "secret_present": bool(PLAID_SECRET),
        "client_id_preview": f"{PLAID_CLIENT_ID[:6]}...{PLAID_CLIENT_ID[-4:]}" if PLAID_CLIENT_ID else "",
        "client_id_shape_ok": looks_like_client_id(PLAID_CLIENT_ID),
        "secret_shape_ok": bool(PLAID_SECRET and not secret_shape_warning),
        "secret_may_be_client_id": secret_may_be_client_id,
        "products": split_env(PLAID_PRODUCTS, "auth,transactions"),
        "country_codes": split_env(PLAID_COUNTRY_CODES, "US"),
        "income_link_supported": True,
        "investment_holdings_supported": True,
        "detail": (
            "Plaid backend is configured."
            if configured
            else "Set PLAID_CLIENT_ID, PLAID_SECRET, and PLAID_ENV on this Railway service."
        ),
    }


def plaid_error_detail(result):
    code = result.get("error_code") or ""
    message = result.get("error_message") or result.get("display_message") or "Plaid request failed."
    lower_message = message.lower()
    if "secret must be a properly formatted" in lower_message:
        return (
            "Plaid rejected PLAID_SECRET because it is not formatted like a Plaid secret. In Railway, replace "
            "PLAID_SECRET with the Sandbox Secret from Plaid Dashboard, not the Client ID, publishable key, "
            "masked dots, quotes, or spaces. Then redeploy the backend."
        )
    if code in {"INVALID_API_KEYS", "INVALID_CLIENT_ID", "INVALID_SECRET"} or "client_id" in message.lower():
        return (
            "Plaid rejected the backend key pair. In Railway, make sure PLAID_CLIENT_ID is the Plaid Client ID, "
            "PLAID_SECRET is the matching Sandbox Secret, and PLAID_ENV is sandbox."
        )
    return message


def plaid_setup_required(detail):
    lower_detail = str(detail or "").lower()
    setup_markers = [
        "user_token",
        "user-token",
        "income requires",
        "income is enabled",
        "request user-token",
        "payroll income",
        "bank income",
    ]
    return any(marker in lower_detail for marker in setup_markers)


def plaid_error_response(result, status_code=400):
    detail = plaid_error_detail(result)
    setup_required = plaid_setup_required(detail)
    return JSONResponse(
        status_code=status_code,
        content={
            "success": False,
            "detail": detail,
            "setup_required": setup_required,
            "product": "plaid_income" if setup_required else "plaid",
            "action": (
                "Enable Plaid Income user-token access for this Plaid app, then retry ADP/payroll Link."
                if setup_required
                else "Fix the Railway Plaid variables, redeploy, then retry VaultFlow's live check."
            ),
            "plaid_error": {
                "error_type": result.get("error_type"),
                "error_code": result.get("error_code"),
                "request_id": result.get("request_id"),
            },
        },
    )


async def plaid_post(path, payload):
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(f"{PLAID_BASE}{path}", json=payload)
    try:
        result = response.json()
    except Exception:
        result = {"error_message": response.text or "Plaid returned a non-JSON response."}
    return response.status_code, result


def require_plaid_config():
    if not plaid_configured():
        raise HTTPException(
            status_code=500,
            detail="Plaid backend variables are missing. Set PLAID_CLIENT_ID and PLAID_SECRET in Railway.",
        )


def is_plaid_transfer_provider():
    return (TRANSFER_PROVIDER or "").strip().lower() in {"plaid", "plaid_transfer", "plaid-transfer", "transfer"}


def safe_json_response_body(response):
    try:
        return json.loads(response.body.decode())
    except Exception:
        return {}


def create_bank_session(user_id, access_token, item_id=""):
    session_id = "bank_" + hashlib.sha256(
        f"{datetime.utcnow().isoformat()}:{user_id}:{item_id}:{access_token}".encode()
    ).hexdigest()[:24]
    bank_sessions[session_id] = {
        "access_token": access_token,
        "item_id": item_id,
        "user_id": str(user_id or "default-user")[:120],
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    if len(bank_sessions) > 500:
        for key in list(bank_sessions.keys())[:-500]:
            bank_sessions.pop(key, None)
    return session_id


def resolve_access_token(raw_token):
    token = str(raw_token or "").strip()
    if token.startswith("bank_"):
        session = bank_sessions.get(token)
        if not session:
            raise HTTPException(
                status_code=400,
                detail="Bank session expired on the backend. Reconnect Plaid Link before using this account.",
            )
        return session["access_token"]
    return token


def trim_collection(items, limit):
    if limit <= 0:
        return
    del items[limit:]


FILE_STORAGE_PROVIDERS = {"file", "local", "local_file", "railway_volume", "volume", "filesystem"}


def safe_storage_name(value, fallback="item"):
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value or ""))
    cleaned = cleaned.strip("._")[:180]
    return cleaned or fallback


def file_storage_requested():
    provider = (APP_STORAGE_PROVIDER or DOC_VAULT_STORAGE_PROVIDER or "").strip().lower()
    return bool(APP_DATA_DIR or provider in FILE_STORAGE_PROVIDERS)


def file_storage_dir():
    return Path(APP_DATA_DIR or "/data/vaultflow").expanduser()


def file_storage_health():
    if not file_storage_requested():
        return {
            "success": False,
            "requested": False,
            "provider": APP_STORAGE_PROVIDER or DOC_VAULT_STORAGE_PROVIDER or "",
            "data_dir": APP_DATA_DIR or "",
            "detail": "File storage is not configured.",
        }
    data_dir = file_storage_dir()
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
        probe = data_dir / ".vaultflow-storage-check.json"
        payload = {"ok": True, "checked_at": datetime.utcnow().isoformat(timespec="seconds") + "Z"}
        probe.write_text(json.dumps(payload), encoding="utf-8")
        loaded = json.loads(probe.read_text(encoding="utf-8"))
        try:
            probe.unlink()
        except Exception:
            pass
        return {
            "success": bool(loaded.get("ok")),
            "requested": True,
            "provider": APP_STORAGE_PROVIDER or DOC_VAULT_STORAGE_PROVIDER or "file",
            "data_dir": str(data_dir),
            "detail": "File storage is configured and writable.",
        }
    except Exception as exc:
        return {
            "success": False,
            "requested": True,
            "provider": APP_STORAGE_PROVIDER or DOC_VAULT_STORAGE_PROVIDER or "file",
            "data_dir": str(data_dir),
            "detail": f"File storage is configured but not writable: {exc}",
        }


def storage_file_path(name):
    return file_storage_dir() / "state" / f"{safe_storage_name(name)}.json"


def storage_read_json(name, fallback):
    if not file_storage_health()["success"]:
        return fallback
    path = storage_file_path(name)
    if not path.exists():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return fallback


def storage_write_json(name, value):
    if not file_storage_health()["success"]:
        return False
    try:
        path = storage_file_path(name)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, indent=2, default=str), encoding="utf-8")
        return True
    except Exception:
        return False


def hydrate_persistent_state():
    if not file_storage_health()["success"]:
        return
    stored_lists = {
        "capital_waitlist": capital_waitlist,
        "transfer_requests": transfer_requests,
        "report_exports": report_exports,
        "vault_documents": vault_documents,
    }
    for name, target in stored_lists.items():
        stored = storage_read_json(name, [])
        if isinstance(stored, list):
            target[:] = stored
    stored_state = storage_read_json("app_state_store", {})
    if isinstance(stored_state, dict):
        app_state_store.update(stored_state)


def persist_state(name, value):
    storage_write_json(name, value)


def vault_objects_dir():
    return file_storage_dir() / "vault_objects"


def backend_vault_storage_ready():
    return bool(DOC_VAULT_ENCRYPTION_KEY and file_storage_health()["success"])


def vault_object_token(user_id, doc_id, expires):
    message = f"{user_id}:{doc_id}:{expires}:vault-object".encode()
    return hmac.new(DOC_VAULT_ENCRYPTION_KEY.encode(), message, hashlib.sha256).hexdigest()


def verify_vault_object_request(request, doc_id):
    if not backend_vault_storage_ready():
        raise HTTPException(status_code=503, detail="Secure vault file storage is not configured.")
    user_id = str(request.query_params.get("user_id") or "guest")[:120]
    filename = str(request.query_params.get("filename") or "document")[:180]
    content_type = str(request.query_params.get("content_type") or "application/octet-stream")[:120]
    try:
        expires = int(request.query_params.get("expires") or 0)
    except Exception:
        expires = 0
    if expires < int(datetime.utcnow().timestamp()):
        raise HTTPException(status_code=403, detail="Signed vault URL expired.")
    token = str(request.query_params.get("token") or "")
    expected = vault_object_token(user_id, doc_id, expires)
    if not hmac.compare_digest(token, expected):
        raise HTTPException(status_code=403, detail="Invalid signed vault URL.")
    return user_id, filename, content_type, expires


def vault_object_path(user_id, doc_id):
    safe_user = safe_storage_name(user_id, "guest")
    safe_doc = safe_storage_name(doc_id, "document")
    return vault_objects_dir() / safe_user / safe_doc


hydrate_persistent_state()


def storage_scaling_payload():
    file_health = file_storage_health()
    database_ready = bool(DATABASE_URL)
    persistent_ready = bool(database_ready or file_health["success"])
    cache_ready = bool(REDIS_URL)
    if database_ready:
        storage_provider = "database"
    elif file_health["success"]:
        storage_provider = file_health["provider"] or "file"
    else:
        storage_provider = APP_STORAGE_PROVIDER or DOC_VAULT_STORAGE_PROVIDER or "memory-preview"
    return {
        "success": persistent_ready,
        "configured": persistent_ready,
        "build_version": BUILD_VERSION,
        "storage_provider": storage_provider,
        "database_configured": database_ready,
        "redis_configured": cache_ready,
        "app_storage_provider_configured": bool(APP_STORAGE_PROVIDER),
        "file_storage_requested": file_health["requested"],
        "file_storage_ready": file_health["success"],
        "file_storage_dir": file_health["data_dir"],
        "memory_preview": not persistent_ready,
        "limits": {
            "bank_sessions": 500,
            "transfer_requests": 200,
            "report_exports": 200,
            "vault_documents": MAX_VAULT_DOCUMENTS,
        },
        "detail": (
            "Persistent database storage is configured."
            if database_ready
            else (
                "Railway/file storage is configured and writable."
                if file_health["success"]
                else "Add DATABASE_URL, or set APP_STORAGE_PROVIDER=file and APP_DATA_DIR=/data/vaultflow with a Railway volume. Current backend memory is preview-only."
            )
        ),
    }


def stripe_health_payload():
    secret_ready = bool(STRIPE_SECRET)
    prime_ready = bool(PRICE_PRIME)
    vault_ready = bool(PRICE_VAULT)
    publishable_ready = bool(STRIPE_PUBLISHABLE_KEY)
    fully_ready = bool(secret_ready and prime_ready and vault_ready)
    if fully_ready:
        detail = "Stripe checkout backend is configured for Prime and Vault subscriptions."
    elif secret_ready and (prime_ready or vault_ready):
        detail = "Stripe backend is partially configured. Add both STRIPE_PRICE_PRIME and STRIPE_PRICE_VAULT before launch."
    else:
        detail = "Add STRIPE_SECRET_KEY, STRIPE_PRICE_PRIME, and STRIPE_PRICE_VAULT in Railway before live checkout."
    return {
        "success": fully_ready,
        "configured": fully_ready,
        "build_version": BUILD_VERSION,
        "setup_required": not fully_ready,
        "secret_configured": secret_ready,
        "publishable_key_configured": publishable_ready,
        "prime_price_configured": prime_ready,
        "vault_price_configured": vault_ready,
        "webhook_configured": bool(STRIPE_WEBHOOK),
        "detail": detail,
    }


def transfer_health_payload():
    provider = (TRANSFER_PROVIDER or "").strip().lower()
    provider_ready = bool(provider and provider not in {"review", "demo", "none", "disabled", "off"})
    plaid_transfer_ready = bool(is_plaid_transfer_provider() and plaid_configured())
    webhook_ready = bool(TRANSFER_WEBHOOK_URL)
    handoff_ready = bool(webhook_ready or plaid_transfer_ready)
    ready = bool(ENABLE_TRANSFER_RAIL and provider_ready and handoff_ready)
    if ready and plaid_transfer_ready:
        detail = (
            "Transfer rail is enabled for Plaid Transfer. Run Plaid approval status to confirm the Transfer product "
            "is approved before creating live transfers."
        )
    elif ready:
        detail = "Transfer rail is enabled and a provider handoff webhook is configured. Reviewed auto-transfer requests can be handed to the approved provider workflow."
    elif not ENABLE_TRANSFER_RAIL:
        detail = "Real transfer execution is guarded off. Set ENABLE_TRANSFER_RAIL=true only after ACH/Plaid Transfer or another processor approves your account."
    elif not provider_ready:
        detail = "Transfer rail is enabled, but TRANSFER_PROVIDER is missing. Add an approved provider name such as plaid_transfer after approval."
    elif is_plaid_transfer_provider() and not plaid_configured():
        detail = "Transfer rail is set to Plaid Transfer, but Plaid backend variables are missing."
    else:
        detail = "Transfer rail is enabled, but no Plaid Transfer configuration or provider webhook is ready."
    return {
        "success": ready,
        "configured": ready,
        "build_version": BUILD_VERSION,
        "enabled": ENABLE_TRANSFER_RAIL,
        "setup_required": not ready,
        "provider": provider or "not-configured",
        "provider_configured": provider_ready,
        "webhook_configured": webhook_ready,
        "plaid_transfer_provider": is_plaid_transfer_provider(),
        "plaid_transfer_configured": plaid_transfer_ready,
        "plaid_transfer_create_enabled": PLAID_TRANSFER_CREATE_ENABLED,
        "plaid_transfer_network": PLAID_TRANSFER_NETWORK,
        "plaid_transfer_ach_class": PLAID_TRANSFER_ACH_CLASS,
        "plaid_transfer_type": PLAID_TRANSFER_TYPE,
        "plaid_transfer_max_amount": PLAID_TRANSFER_MAX_AMOUNT,
        "bank_sessions": len(bank_sessions),
        "approval_required": not ready,
        "mode": "plaid-transfer-ready" if ready and plaid_transfer_ready else ("provider-handoff-ready" if ready else "guarded-review"),
        "review_authorization_required": True,
        "queued_requests": len(transfer_requests),
        "detail": detail,
    }


async def plaid_transfer_configuration_status():
    if not plaid_configured():
        return {
            "success": False,
            "approved": False,
            "configured": False,
            "setup_required": True,
            "detail": "Set PLAID_CLIENT_ID, PLAID_SECRET, and PLAID_ENV before checking Plaid Transfer approval.",
        }
    status_code, result = await plaid_post(
        "/transfer/configuration/get",
        {"client_id": PLAID_CLIENT_ID, "secret": PLAID_SECRET},
    )
    if status_code < 400:
        summary = {
            key: result.get(key)
            for key in ["request_id", "maximum_amount", "minimum_amount", "supported_networks", "enabled"]
            if key in result
        }
        return {
            "success": True,
            "approved": True,
            "configured": True,
            "setup_required": False,
            "provider": "plaid_transfer",
            "create_enabled": PLAID_TRANSFER_CREATE_ENABLED,
            "network": PLAID_TRANSFER_NETWORK,
            "ach_class": PLAID_TRANSFER_ACH_CLASS,
            "transfer_type": PLAID_TRANSFER_TYPE,
            "max_amount": PLAID_TRANSFER_MAX_AMOUNT,
            "configuration": summary,
            "detail": "Plaid Transfer configuration endpoint accepted this backend key pair.",
        }
    detail = plaid_error_detail(result)
    lower = detail.lower()
    setup_required = any(
        marker in lower
        for marker in ["not enabled", "not authorized", "permission", "approval", "product", "transfer"]
    )
    return {
        "success": False,
        "approved": False,
        "configured": False,
        "setup_required": setup_required,
        "provider": "plaid_transfer",
        "create_enabled": PLAID_TRANSFER_CREATE_ENABLED,
        "network": PLAID_TRANSFER_NETWORK,
        "ach_class": PLAID_TRANSFER_ACH_CLASS,
        "transfer_type": PLAID_TRANSFER_TYPE,
        "max_amount": PLAID_TRANSFER_MAX_AMOUNT,
        "detail": detail,
        "plaid_error": {
            "error_type": result.get("error_type"),
            "error_code": result.get("error_code"),
            "request_id": result.get("request_id"),
        },
    }


async def plaid_income_status(user_id):
    if not plaid_configured():
        return {
            "success": False,
            "approved": False,
            "setup_required": True,
            "detail": "Set Plaid backend variables before checking ADP/payroll Income access.",
        }
    user_ref, error = await get_income_user_reference(user_id)
    if user_ref:
        return {
            "success": True,
            "approved": True,
            "setup_required": False,
            "detail": "Plaid Income user-token flow is available for ADP/payroll Link.",
        }
    body = safe_json_response_body(error) if error else {}
    return {
        "success": False,
        "approved": False,
        "setup_required": bool(body.get("setup_required", True)),
        "detail": body.get("detail") or "Plaid Income access could not be confirmed.",
        "plaid_error": body.get("plaid_error") or {},
    }


def doc_vault_health_payload():
    encryption_ready = bool(DOC_VAULT_ENCRYPTION_KEY)
    file_health = file_storage_health()
    external_storage_ready = bool(DOC_VAULT_STORAGE_PROVIDER and (DOC_VAULT_BUCKET or DOC_VAULT_SIGNING_BASE_URL))
    backend_file_ready = bool(file_health["success"])
    storage_ready = bool(external_storage_ready or backend_file_ready)
    signed_urls_ready = bool(encryption_ready and storage_ready)
    scaling = storage_scaling_payload()
    if signed_urls_ready and external_storage_ready:
        detail = "Secure vault encryption and external signed URL settings are configured."
    elif signed_urls_ready:
        detail = "Secure vault encryption and backend file signed URLs are configured."
    elif not encryption_ready:
        detail = "Add DOC_VAULT_ENCRYPTION_KEY before production document uploads."
    else:
        detail = "Add DOC_VAULT_STORAGE_PROVIDER plus DOC_VAULT_BUCKET or DOC_VAULT_SIGNING_BASE_URL, or set APP_STORAGE_PROVIDER=file and APP_DATA_DIR=/data/vaultflow with a Railway volume."
    return {
        "success": signed_urls_ready,
        "configured": signed_urls_ready,
        "build_version": BUILD_VERSION,
        "setup_required": not signed_urls_ready,
        "encryption_key_configured": encryption_ready,
        "storage_provider_configured": bool(DOC_VAULT_STORAGE_PROVIDER),
        "backend_file_storage_ready": backend_file_ready,
        "external_signed_url_storage_ready": external_storage_ready,
        "bucket_configured": bool(DOC_VAULT_BUCKET),
        "signing_base_url_configured": bool(DOC_VAULT_SIGNING_BASE_URL),
        "signed_urls_ready": signed_urls_ready,
        "signed_url_ttl_minutes": DOC_VAULT_SIGNED_URL_TTL_MINUTES,
        "max_documents": MAX_VAULT_DOCUMENTS,
        "registered_documents": len(vault_documents),
        "scalable_storage_ready": scaling["success"],
        "storage_mode": scaling["storage_provider"],
        "database_configured": scaling["database_configured"],
        "detail": detail,
    }


def reports_health_payload():
    return {
        "success": REPORT_EXPORTS_ENABLED,
        "configured": REPORT_EXPORTS_ENABLED,
        "build_version": BUILD_VERSION,
        "setup_required": not REPORT_EXPORTS_ENABLED,
        "secure_attachments_ready": doc_vault_health_payload()["success"],
        "registered_exports": len(report_exports),
        "detail": (
            "Financial report export route is ready. Secure vault files attach when signed URLs are configured."
            if REPORT_EXPORTS_ENABLED
            else "REPORT_EXPORTS_ENABLED=false is set in Railway."
        ),
    }


def remember_transfer_record(record):
    transfer_requests.insert(0, record)
    del transfer_requests[200:]
    persist_state("transfer_requests", transfer_requests)


def payroll_health_payload():
    configured = plaid_configured()
    return {
        "success": configured,
        "configured": configured,
        "build_version": BUILD_VERSION,
        "setup_required": not configured,
        "route_available": True,
        "income_link_supported": True,
        "adp_supported_through_plaid_income": True,
        "detail": (
            "Payroll/ADP route is deployed. A real PASS requires Plaid Income user-token access and a successful Link token."
            if configured
            else "Set Plaid backend variables before ADP/payroll income can connect."
        ),
    }


def owner_health_payload():
    configured = bool(OWNER_ACCESS_CODE)
    return {
        "success": configured,
        "configured": configured,
        "build_version": BUILD_VERSION,
        "auth_route_available": True,
        "setup_required": not configured,
        "owner_username_configured": bool(OWNER_USERNAME),
        "owner_email_configured": bool(OWNER_EMAIL),
        "detail": (
            "Owner authentication is configured. Use the owner username plus OWNER_ACCESS_CODE from Railway."
            if configured
            else "Add OWNER_ACCESS_CODE in Railway variables, then redeploy/restart before using the owner account."
        ),
    }


def frontend_file_path():
    root = Path(__file__).resolve().parent
    for candidate in [root / "app" / "index.html", root / "index.html"]:
        if candidate.exists():
            return candidate
    return None


@app.get("/")
def root():
    frontend = frontend_file_path()
    if frontend:
        return HTMLResponse(frontend.read_text(encoding="utf-8"))
    return api_root()


@app.get("/app")
def app_frontend():
    frontend = frontend_file_path()
    if not frontend:
        raise HTTPException(status_code=404, detail="VaultFlow frontend file is missing from this deployment.")
    return HTMLResponse(frontend.read_text(encoding="utf-8"))


@app.get("/api")
def api_root():
    return {
        "status": "VaultFlow backend running",
        "build_version": BUILD_VERSION,
        "plaid_env": PLAID_ENV,
        "routes": [
            "GET /plaid/health",
            "GET /plaid/approval-status",
            "GET /scaling/health",
            "GET /billing/health",
            "GET /bank/transfer/health",
            "GET /vault/health",
            "GET /reports/health",
            "GET /owner/health",
            "GET /health",
            "POST /owner/auth",
            "POST /owner/health",
            "POST /plaid/approval-status",
            "POST /plaid/create-link-token",
            "POST /plaid/create-income-link-token",
            "POST /plaid/exchange-token",
            "POST /plaid/transactions",
            "POST /plaid/balance",
            "POST /bank/transfer",
            "POST /bank/transfer/history",
            "GET /payroll/health",
            "POST /payroll/health",
            "POST /scaling/health",
            "POST /vault/sign-url",
            "POST /vault/register",
            "POST /vault/list",
            "POST /vault/remove",
            "POST /reports/export",
            "POST /app/state",
            "POST /investments/holdings",
            "POST /plaid/investments/holdings",
            "POST /market/signals",
            "POST /trading/connect",
            "POST /trading/order",
            "GET /capital/health",
            "POST /capital/waitlist",
            "GET /live/readiness",
            "POST /live/readiness",
        ],
    }


@app.post("/owner/auth")
async def owner_auth(request: Request):
    try:
        data = await request.json()
    except Exception:
        data = {}

    username = str(data.get("username") or data.get("email") or "").strip().lower()
    code = str(data.get("code") or data.get("owner_code") or data.get("password") or "").strip()

    if not OWNER_ACCESS_CODE:
        return JSONResponse(
            status_code=503,
            content={
                "success": False,
                "setup_required": True,
                "detail": "Set OWNER_ACCESS_CODE in Railway variables to enable production owner login.",
            },
        )

    owner_ok = hmac.compare_digest(username, OWNER_USERNAME)
    code_ok = hmac.compare_digest(code, OWNER_ACCESS_CODE)
    if not (owner_ok and code_ok):
        return JSONResponse(
            status_code=401,
            content={"success": False, "detail": "Owner username or private code was not accepted."},
        )

    stamp = datetime.utcnow().isoformat()
    session_id = "owner_" + hashlib.sha256(f"{stamp}:{username}".encode("utf-8")).hexdigest()[:18]
    return {
        "success": True,
        "owner": True,
        "owner_name": OWNER_USERNAME,
        "owner_id": "owner-console",
        "email": OWNER_EMAIL,
        "session_id": session_id,
        "detail": "Owner access approved.",
    }


@app.get("/owner/health")
def owner_health_get():
    return owner_health_payload()


@app.post("/owner/health")
def owner_health_post():
    return owner_health_payload()


@app.get("/health")
def health_get():
    readiness = live_readiness()
    return {
        "success": True,
        "status": "ok",
        "build_version": BUILD_VERSION,
        "live_ready": readiness["live_ready"],
        "plaid_configured": readiness["plaid_backend"]["success"],
        "billing_configured": readiness["billing"]["success"],
        "transfer_configured": readiness["transfer"]["success"],
        "vault_configured": readiness["doc_vault"]["success"],
        "reports_configured": readiness["report_exports"]["success"],
        "owner_auth_configured": readiness["owner_auth"]["success"],
        "ai_configured": readiness["ai_configured"],
        "alpaca_configured": readiness["alpaca_configured"],
    }


@app.get("/plaid/health")
def plaid_health_get():
    return plaid_health_payload()


@app.post("/plaid/health")
def plaid_health_post():
    return plaid_health_payload()


async def plaid_approval_status_payload(user_id="approval-check"):
    plaid_backend = plaid_health_payload()
    income = await plaid_income_status(user_id)
    transfer = await plaid_transfer_configuration_status()
    approved_products = []
    if plaid_backend["success"]:
        approved_products.append("bank_link")
    if income.get("approved"):
        approved_products.append("income_adp_payroll")
    if transfer.get("approved"):
        approved_products.append("plaid_transfer")
    return {
        "success": bool(plaid_backend["success"]),
        "build_version": BUILD_VERSION,
        "plaid_backend": plaid_backend,
        "income": income,
        "transfer": transfer,
        "approved_products": approved_products,
        "setup_required": bool(
            (not plaid_backend["success"]) or income.get("setup_required") or transfer.get("setup_required")
        ),
        "detail": (
            "Plaid bank Link is configured. Review Income and Transfer statuses for separate product approvals."
            if plaid_backend["success"]
            else plaid_backend["detail"]
        ),
    }


@app.get("/plaid/approval-status")
async def plaid_approval_status_get():
    return await plaid_approval_status_payload("approval-check")


@app.post("/plaid/approval-status")
async def plaid_approval_status_post(request: Request):
    data = await request.json()
    user_id = data.get("user_id") or data.get("client_user_id") or "approval-check"
    return await plaid_approval_status_payload(str(user_id))


@app.get("/billing/health")
def billing_health_get():
    return stripe_health_payload()


@app.post("/billing/health")
def billing_health_post():
    return stripe_health_payload()


@app.get("/bank/transfer/health")
def bank_transfer_health_get():
    return transfer_health_payload()


@app.post("/bank/transfer/health")
def bank_transfer_health_post():
    return transfer_health_payload()


@app.get("/vault/health")
def vault_health_get():
    return doc_vault_health_payload()


@app.post("/vault/health")
def vault_health_post():
    return doc_vault_health_payload()


@app.get("/reports/health")
def reports_health_get():
    return reports_health_payload()


@app.post("/reports/health")
def reports_health_post():
    return reports_health_payload()


@app.get("/scaling/health")
def scaling_health_get():
    return storage_scaling_payload()


@app.post("/scaling/health")
def scaling_health_post():
    return storage_scaling_payload()


@app.get("/payroll/health")
def payroll_health_get():
    return payroll_health_payload()


@app.post("/payroll/health")
def payroll_health_post():
    return payroll_health_payload()


@app.get("/live/readiness")
@app.post("/live/readiness")
def live_readiness():
    billing = stripe_health_payload()
    transfer = transfer_health_payload()
    vault = doc_vault_health_payload()
    reports = reports_health_payload()
    payroll = payroll_health_payload()
    scaling = storage_scaling_payload()
    owner_auth = owner_health_payload()
    critical_ready = all(
        [
            plaid_health_payload()["success"],
            billing["success"],
            transfer["success"],
            vault["success"],
            reports["success"],
            payroll["success"],
            owner_auth["success"],
            bool(OPENAI_API_KEY),
            bool(ALPACA_KEY_ID and ALPACA_SECRET_KEY),
        ]
    )
    return {
        "success": True,
        "live_ready": critical_ready,
        "build_version": BUILD_VERSION,
        "plaid_backend": plaid_health_payload(),
        "billing": billing,
        "payment_checkout_configured": billing["success"],
        "payroll": payroll,
        "owner_auth": owner_auth,
        "owner_auth_configured": owner_auth["success"],
        "plaid_income_link_supported": True,
        "investment_holdings_supported": True,
        "stripe_configured": billing["success"],
        "ai_configured": bool(OPENAI_API_KEY),
        "ai_model": OPENAI_MODEL if OPENAI_API_KEY else "",
        "alpaca_configured": bool(ALPACA_KEY_ID and ALPACA_SECRET_KEY),
        "alpaca_orders_enabled": ENABLE_ALPACA_PAPER_ORDERS or ENABLE_LIVE_TRADING,
        "alpaca_live_trading_enabled": ENABLE_LIVE_TRADING,
        "transfer": transfer,
        "transfer_enabled": transfer["success"],
        "doc_vault": vault,
        "doc_vault_key_configured": vault["encryption_key_configured"],
        "doc_vault_signed_urls_ready": vault["signed_urls_ready"],
        "report_exports": reports,
        "report_exports_enabled": reports["success"],
        "scaling": scaling,
        "scalable_storage_ready": scaling["success"],
        "capital_waitlist_enabled": True,
        "capital_webhook_configured": bool(CAPITAL_WAITLIST_WEBHOOK_URL),
        "detail": "Backend readiness route is live.",
    }


@app.post("/ai/health")
def ai_health():
    configured = bool(OPENAI_API_KEY)
    return {
        "success": True,
        "configured": configured,
        "route_available": True,
        "mode": "openai" if configured else "local-fallback",
        "model": OPENAI_MODEL if configured else "",
        "base_url": OPENAI_BASE_URL if configured else "",
        "setup_required": not configured,
        "detail": (
            "OpenAI backend key is configured."
            if configured
            else "Add OPENAI_API_KEY and optionally OPENAI_MODEL in Railway to enable real AI answers."
        ),
    }


def openai_error_detail(result):
    if isinstance(result, dict):
        error = result.get("error")
        if isinstance(error, dict):
            return error.get("message") or str(error)
        if error:
            return str(error)
    return "OpenAI request failed."


def parse_openai_response_text(result):
    if not isinstance(result, dict):
        return ""
    if result.get("output_text"):
        return str(result.get("output_text") or "").strip()
    for item in result.get("output") or []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content") or []:
            if isinstance(content, dict) and content.get("text"):
                return str(content.get("text") or "").strip()
    return ""


@app.post("/ai/chat")
async def ai_chat(request: Request):
    if not OPENAI_API_KEY:
        return JSONResponse(
            status_code=503,
            content={
                "success": False,
                "configured": False,
                "mode": "local-fallback",
                "detail": "Add OPENAI_API_KEY in Railway to enable real AI answers.",
                "answer": "I can help with the local VaultFlow guide, but real AI answers need OPENAI_API_KEY configured on the backend.",
            },
        )
    data = await request.json()
    question = str(data.get("message") or data.get("question") or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="message is required.")
    snapshot = data.get("snapshot") or data.get("context") or {}
    history = data.get("history") or []
    system_prompt = (
        "You are VaultFlow's AI assistant for veterans, merchant mariners, contractors, overtime workers, "
        "travel nurses, real estate investors, and other people with irregular or high-variable income. Answer "
        "normal everyday questions clearly, and answer finance questions as an educational financial coach. "
        "Help users organize, plan, simulate, and understand tradeoffs. Be practical, concise, and friendly. "
        "Ask users to verify numbers before acting. Never claim VaultFlow is a bank, broker, lender, investment "
        "adviser, tax adviser, credit repair company, or money transmitter. Never claim to provide legal, tax, "
        "investment, lending, or trading advice."
    )
    prior = []
    if isinstance(history, list):
        for item in history[-8:]:
            raw_role = str(item.get("from") or item.get("role") or "").lower()
            role = "assistant" if raw_role in {"ai", "assistant", "bot"} else "user"
            text = str(item.get("text") or "")[:900]
            if text:
                prior.append({"role": role, "content": text})
    user_prompt = f"User question: {question}\n\nSafe VaultFlow context JSON: {snapshot}"
    messages = [{"role": "system", "content": system_prompt}] + prior + [{"role": "user", "content": user_prompt}]
    async with httpx.AsyncClient(timeout=OPENAI_TIMEOUT_SECONDS) as client:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
        response = await client.post(
            f"{OPENAI_BASE_URL}/v1/chat/completions",
            headers=headers,
            json={
                "model": OPENAI_MODEL,
                "messages": messages,
                "temperature": 0.4,
                "max_tokens": 700,
            },
        )
        try:
            result = response.json()
        except Exception:
            result = {"error": {"message": response.text or "OpenAI returned a non-JSON response."}}
        if response.status_code < 400:
            answer = result.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
            if answer:
                return {"success": True, "configured": True, "mode": "openai-chat", "model": OPENAI_MODEL, "answer": answer}
        chat_error = openai_error_detail(result)
        response = await client.post(
            f"{OPENAI_BASE_URL}/v1/responses",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": OPENAI_MODEL,
                "input": messages,
                "temperature": 0.4,
                "max_output_tokens": 700,
            },
        )
    try:
        result = response.json()
    except Exception:
        result = {"error": {"message": response.text or "OpenAI returned a non-JSON response."}}
    if response.status_code >= 400:
        return JSONResponse(
            status_code=response.status_code,
            content={
                "success": False,
                "configured": True,
                "mode": "openai-error",
                "detail": f"Chat completions failed: {chat_error}. Responses API failed: {openai_error_detail(result)}",
            },
        )
    answer = parse_openai_response_text(result)
    if not answer:
        return JSONResponse(
            status_code=502,
            content={
                "success": False,
                "configured": True,
                "mode": "openai-error",
                "detail": "OpenAI responded but no answer text was returned.",
            },
        )
    return {"success": True, "configured": True, "mode": "openai-responses", "model": OPENAI_MODEL, "answer": answer}


@app.post("/vault/sign-url")
async def vault_sign_url(request: Request):
    data = await request.json()
    health = doc_vault_health_payload()
    filename = str(data.get("filename") or "document")[:180]
    doc_id = str(data.get("doc_id") or hashlib.sha256(filename.encode()).hexdigest()[:18])
    user_id = str(data.get("user_id") or "guest")[:120]
    content_type = str(data.get("type") or data.get("content_type") or "application/octet-stream")[:120]
    size = max(0, int(float(data.get("size") or 0)))
    if not health["success"]:
        return {**health, "success": False}
    expires = int((datetime.utcnow() + timedelta(minutes=DOC_VAULT_SIGNED_URL_TTL_MINUTES)).timestamp())
    if backend_vault_storage_ready() and not DOC_VAULT_SIGNING_BASE_URL:
        safe_doc_id = safe_storage_name(doc_id, "document")
        token = vault_object_token(user_id, safe_doc_id, expires)
        query = urlencode(
            {
                "user_id": user_id,
                "filename": filename,
                "content_type": content_type,
                "expires": expires,
                "token": token,
            }
        )
        base_url = str(request.base_url).rstrip("/")
        storage_key = f"vault_objects/{safe_storage_name(user_id, 'guest')}/{safe_doc_id}"
        return {
            "success": True,
            "configured": True,
            "doc_id": doc_id,
            "storage_key": storage_key,
            "download_url": f"{base_url}/vault/object/{safe_doc_id}?{query}",
            "upload_url": f"{base_url}/vault/object/{safe_doc_id}?{query}",
            "upload_method": "PUT",
            "required_headers": {"Content-Type": "application/octet-stream", "x-vaultflow-doc-id": doc_id},
            "storage_provider": "vaultflow-backend-file",
            "expires_at": datetime.utcfromtimestamp(expires).isoformat(timespec="seconds") + "Z",
            "ttl_minutes": DOC_VAULT_SIGNED_URL_TTL_MINUTES,
            "detail": f"Expiring backend signed URLs created for {filename}.",
        }
    signing_base = (DOC_VAULT_SIGNING_BASE_URL or f"https://vaultflow-vault/{DOC_VAULT_BUCKET}").rstrip("/")
    storage_key = f"{user_id}/{doc_id}/{filename}".replace(" ", "_")
    message = f"{user_id}:{doc_id}:{filename}:{size}:{content_type}:{expires}".encode()
    token = hmac.new(DOC_VAULT_ENCRYPTION_KEY.encode(), message, hashlib.sha256).hexdigest()
    return {
        "success": True,
        "configured": True,
        "doc_id": doc_id,
        "storage_key": storage_key,
        "download_url": f"{signing_base}/download/{storage_key}?expires={expires}&token={token}",
        "upload_url": f"{signing_base}/upload/{storage_key}?expires={expires}&token={token}",
        "upload_method": "PUT",
        "required_headers": {"Content-Type": content_type, "x-vaultflow-doc-id": doc_id},
        "storage_provider": DOC_VAULT_STORAGE_PROVIDER or "signed-url-provider",
        "expires_at": datetime.utcfromtimestamp(expires).isoformat(timespec="seconds") + "Z",
        "ttl_minutes": DOC_VAULT_SIGNED_URL_TTL_MINUTES,
        "detail": f"Expiring signed URLs created for {filename}.",
    }


@app.put("/vault/object/{doc_id}")
async def vault_object_upload(doc_id: str, request: Request):
    safe_doc = safe_storage_name(doc_id, "document")
    user_id, filename, content_type, expires = verify_vault_object_request(request, safe_doc)
    body = await request.body()
    if not body:
        raise HTTPException(status_code=400, detail="No file bytes were uploaded.")
    target = vault_object_path(user_id, safe_doc)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)
    for doc in vault_documents:
        if doc.get("doc_id") == safe_doc:
            doc["object_uploaded"] = True
            doc["object_size"] = len(body)
            doc["storage_key"] = f"vault_objects/{safe_storage_name(user_id, 'guest')}/{safe_doc}"
            doc["uploaded_at"] = datetime.utcnow().isoformat(timespec="seconds") + "Z"
            doc["backend_content_type"] = content_type
            doc["expires_at"] = datetime.utcfromtimestamp(expires).isoformat(timespec="seconds") + "Z"
            break
    persist_state("vault_documents", vault_documents)
    return {
        "success": True,
        "uploaded": True,
        "doc_id": safe_doc,
        "filename": filename,
        "bytes": len(body),
        "storage_key": f"vault_objects/{safe_storage_name(user_id, 'guest')}/{safe_doc}",
        "detail": "Encrypted document bytes were uploaded to secure backend file storage.",
    }


@app.get("/vault/object/{doc_id}")
def vault_object_download(doc_id: str, request: Request):
    safe_doc = safe_storage_name(doc_id, "document")
    user_id, filename, content_type, _expires = verify_vault_object_request(request, safe_doc)
    target = vault_object_path(user_id, safe_doc)
    if not target.exists():
        raise HTTPException(status_code=404, detail="Vault object was not found in backend storage.")
    return FileResponse(target, media_type=content_type or "application/octet-stream", filename=filename)


@app.post("/vault/register")
async def vault_register(request: Request):
    data = await request.json()
    health = doc_vault_health_payload()
    filename = str(data.get("filename") or "document")[:180]
    doc_id = str(data.get("doc_id") or hashlib.sha256(f"{filename}:{datetime.utcnow().isoformat()}".encode()).hexdigest()[:18])
    size = max(0, int(float(data.get("size") or 0)))
    content_type = str(data.get("type") or data.get("content_type") or "document")[:120]
    digest_source = f"{doc_id}:{filename}:{size}:{content_type}:{data.get('user_id') or 'guest'}"
    manifest_digest = hmac.new(
        (DOC_VAULT_ENCRYPTION_KEY or "vaultflow-local-manifest").encode(),
        digest_source.encode(),
        hashlib.sha256,
    ).hexdigest()
    record = {
        "doc_id": doc_id,
        "user_id": str(data.get("user_id") or "guest")[:120],
        "filename": filename,
        "type": content_type,
        "size": size,
        "category": str(data.get("category") or "document")[:80],
        "include_in_report": bool(data.get("include_in_report", True)),
        "file_hash": str(data.get("file_hash") or data.get("sha256") or "")[:128],
        "encrypted_client_side": bool(data.get("encrypted") or data.get("encrypted_client_side")),
        "storage_key": str(data.get("storage_key") or "")[:260],
        "source": str(data.get("source") or "vaultflow-web")[:80],
        "manifest_digest": manifest_digest,
        "signed_urls_ready": health["success"],
        "registered_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    vault_documents.insert(0, record)
    trim_collection(vault_documents, MAX_VAULT_DOCUMENTS)
    persist_state("vault_documents", vault_documents)
    return {
        "success": True,
        "registered": True,
        "encrypted_manifest_ready": bool(DOC_VAULT_ENCRYPTION_KEY),
        "signed_urls_ready": health["success"],
        "scalable_storage_ready": health["scalable_storage_ready"],
        "detail": (
            "Document registered in the secure vault manifest. Use the signed upload URL for production file bytes."
            if health["success"]
            else "Document registered locally. Add vault encryption/storage env vars before production file-byte uploads."
        ),
        **record,
    }


@app.post("/vault/list")
async def vault_list(request: Request):
    data = await request.json()
    stored = storage_read_json("vault_documents", None)
    if isinstance(stored, list):
        vault_documents[:] = stored
    user_id = str(data.get("user_id") or "").strip()
    docs = vault_documents
    if user_id:
        docs = [doc for doc in vault_documents if doc.get("user_id") == user_id]
    return {"success": True, "count": len(docs), "documents": docs[:200], "health": doc_vault_health_payload()}


@app.post("/vault/remove")
async def vault_remove(request: Request):
    data = await request.json()
    doc_id = str(data.get("doc_id") or "").strip()
    user_id = str(data.get("user_id") or "").strip()
    if not doc_id:
        raise HTTPException(status_code=400, detail="doc_id is required.")
    before = len(vault_documents)
    vault_documents[:] = [
        doc for doc in vault_documents
        if not (doc.get("doc_id") == doc_id and (not user_id or doc.get("user_id") == user_id))
    ]
    removed = before - len(vault_documents)
    if user_id:
        object_path = vault_object_path(user_id, safe_storage_name(doc_id, "document"))
        if object_path.exists():
            try:
                object_path.unlink()
            except Exception:
                pass
    persist_state("vault_documents", vault_documents)
    return {
        "success": True,
        "removed": removed,
        "doc_id": doc_id,
        "detail": "Document manifest removed. Delete the actual object in your storage provider if file bytes were uploaded.",
    }


@app.post("/reports/export")
async def reports_export(request: Request):
    if not REPORT_EXPORTS_ENABLED:
        return JSONResponse(
            status_code=503,
            content={"success": False, "detail": "REPORT_EXPORTS_ENABLED=false is set in Railway."},
        )
    data = await request.json()
    documents = data.get("documents") or []
    doc_manifest = [
        {
            "id": str(doc.get("id") or doc.get("doc_id") or "")[:120],
            "name": str(doc.get("name") or doc.get("filename") or "document")[:180],
            "size": int(float(doc.get("size") or 0)),
            "type": str(doc.get("type") or "document")[:120],
            "category": str(doc.get("category") or "document")[:80],
            "include_in_report": bool(doc.get("includeInReport", doc.get("include_in_report", True))),
            "signed_url_ready": bool(doc.get("signedUrlReady") or doc.get("signed_url_ready")),
        }
        for doc in documents[:100]
        if isinstance(doc, dict)
    ]
    report_id = "vf_report_" + hashlib.sha256(
        f"{datetime.utcnow().isoformat()}:{data.get('user_id') or 'guest'}".encode()
    ).hexdigest()[:14]
    manifest_digest = hashlib.sha256(str(doc_manifest).encode()).hexdigest()[:18]
    record = {
        "report_id": report_id,
        "user_id": str(data.get("user_id") or "guest")[:120],
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "doc_count": len(doc_manifest),
        "paystub_count": len([doc for doc in doc_manifest if doc["category"] == "paystub"]),
        "manifest_digest": manifest_digest,
        "vault_signed_urls_ready": doc_vault_health_payload()["success"],
    }
    report_exports.insert(0, record)
    del report_exports[200:]
    persist_state("report_exports", report_exports)
    return {
        "success": True,
        "report_id": report_id,
        "detail": "Report export registered. The browser print/PDF and CSV exports are ready.",
        **record,
    }


@app.post("/bank/transfer")
async def bank_transfer(request: Request):
    data = await request.json()
    health = transfer_health_payload()
    try:
        amount = round(float(data.get("amount") or 0), 2)
    except Exception:
        amount = 0
    if amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be greater than zero.")
    if not data.get("access_token"):
        raise HTTPException(status_code=400, detail="access_token is required.")
    resolved_access_token = resolve_access_token(data.get("access_token"))
    if not data.get("review_authorized"):
        return {
            "success": False,
            "guarded": True,
            "review_only": True,
            "detail": "A reviewed authorization is required before VaultFlow can queue an auto-transfer request.",
            "health": health,
        }
    transfer_id = "vf_transfer_" + hashlib.sha256(
        f"{datetime.utcnow().isoformat()}:{data.get('user_id') or 'guest'}:{amount}".encode()
    ).hexdigest()[:14]
    if not health["success"]:
        record = {
            "transfer_id": transfer_id,
            "user_id": str(data.get("user_id") or "guest")[:120],
            "destination": str(data.get("destination") or "Vault")[:120],
            "amount": amount,
            "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "provider": health["provider"],
            "status": "review_only_guarded",
            "autopilot": bool(data.get("autopilot") or data.get("auto_transfer")),
        }
        remember_transfer_record(record)
        return {
            "success": False,
            "transfer_id": transfer_id,
            "guarded": True,
            "review_only": True,
            "status": "review_only_guarded",
            "detail": health["detail"] + " The reviewed request was saved as a guarded queue entry; no money moved.",
            "health": health,
        }
    if is_plaid_transfer_provider():
        account_id = str(data.get("account_id") or "").strip()
        if not account_id:
            return {
                "success": False,
                "guarded": True,
                "review_only": True,
                "status": "missing_account_id",
                "detail": "Reconnect the bank through Plaid and choose a checking account before creating a Plaid Transfer.",
                "health": health,
            }
        if amount > PLAID_TRANSFER_MAX_AMOUNT:
            return {
                "success": False,
                "guarded": True,
                "review_only": True,
                "status": "amount_over_transfer_limit",
                "detail": f"Amount exceeds PLAID_TRANSFER_MAX_AMOUNT (${PLAID_TRANSFER_MAX_AMOUNT:,.2f}). Lower the amount or raise the backend limit after review.",
                "health": health,
            }
        legal_name = str(
            data.get("legal_name")
            or data.get("user_name")
            or data.get("customer_name")
            or "VaultFlow User"
        )[:120]
        authorization_payload = {
            "client_id": PLAID_CLIENT_ID,
            "secret": PLAID_SECRET,
            "access_token": resolved_access_token,
            "account_id": account_id,
            "type": str(data.get("transfer_type") or PLAID_TRANSFER_TYPE),
            "network": str(data.get("network") or PLAID_TRANSFER_NETWORK),
            "amount": f"{amount:.2f}",
            "ach_class": str(data.get("ach_class") or PLAID_TRANSFER_ACH_CLASS),
            "user": {"legal_name": legal_name},
        }
        status_code, auth_result = await plaid_post("/transfer/authorization/create", authorization_payload)
        if status_code >= 400:
            record = {
                "transfer_id": transfer_id,
                "user_id": str(data.get("user_id") or "guest")[:120],
                "destination": str(data.get("destination") or "Vault")[:120],
                "amount": amount,
                "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "provider": "plaid_transfer",
                "status": "plaid_authorization_failed",
                "autopilot": bool(data.get("autopilot") or data.get("auto_transfer")),
                "plaid_request_id": auth_result.get("request_id"),
            }
            remember_transfer_record(record)
            return {
                "success": False,
                "guarded": True,
                "review_only": True,
                "transfer_id": transfer_id,
                "status": "plaid_authorization_failed",
                "detail": plaid_error_detail(auth_result),
                "plaid_error": {
                    "error_type": auth_result.get("error_type"),
                    "error_code": auth_result.get("error_code"),
                    "request_id": auth_result.get("request_id"),
                },
            }
        authorization = auth_result.get("authorization") or auth_result
        authorization_id = authorization.get("id") or auth_result.get("authorization_id")
        decision = str(authorization.get("decision") or auth_result.get("decision") or "").lower()
        if not authorization_id or (decision and decision not in {"approved", "allowed", "approve"}):
            record = {
                "transfer_id": transfer_id,
                "user_id": str(data.get("user_id") or "guest")[:120],
                "destination": str(data.get("destination") or "Vault")[:120],
                "amount": amount,
                "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "provider": "plaid_transfer",
                "status": "plaid_authorization_not_approved",
                "decision": decision or "unknown",
                "autopilot": bool(data.get("autopilot") or data.get("auto_transfer")),
                "plaid_request_id": auth_result.get("request_id"),
            }
            remember_transfer_record(record)
            return {
                "success": False,
                "guarded": True,
                "review_only": True,
                "transfer_id": transfer_id,
                "status": "plaid_authorization_not_approved",
                "decision": decision or "unknown",
                "detail": "Plaid did not approve this transfer authorization, so no money moved.",
            }
        record = {
            "transfer_id": transfer_id,
            "user_id": str(data.get("user_id") or "guest")[:120],
            "destination": str(data.get("destination") or "Vault")[:120],
            "amount": amount,
            "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "provider": "plaid_transfer",
            "status": "plaid_authorized_review_only",
            "authorization_id": authorization_id,
            "decision": decision or "approved",
            "autopilot": bool(data.get("autopilot") or data.get("auto_transfer")),
            "source": str(data.get("source") or "vaultflow")[:80],
        }
        if not PLAID_TRANSFER_CREATE_ENABLED or not data.get("execute_live_transfer"):
            remember_transfer_record(record)
            return {
                "success": False,
                "guarded": True,
                "review_only": True,
                "transfer_id": transfer_id,
                "authorization_id": authorization_id,
                "status": "plaid_authorized_review_only",
                "detail": (
                    "Plaid approved the transfer authorization. Live transfer creation is still guarded off, "
                    "so no money moved. Enable PLAID_TRANSFER_CREATE_ENABLED and check the live-transfer box "
                    "to create the transfer."
                ),
            }
        create_payload = {
            "client_id": PLAID_CLIENT_ID,
            "secret": PLAID_SECRET,
            "authorization_id": authorization_id,
            "description": str(data.get("description") or f"VaultFlow {data.get('destination') or 'Transfer'}")[:80],
            "metadata": {
                "vaultflow_transfer_id": transfer_id,
                "destination": str(data.get("destination") or "Vault")[:120],
            },
        }
        status_code, create_result = await plaid_post("/transfer/create", create_payload)
        if status_code >= 400:
            record["status"] = "plaid_create_failed"
            record["plaid_request_id"] = create_result.get("request_id")
            remember_transfer_record(record)
            return {
                "success": False,
                "guarded": True,
                "review_only": True,
                "transfer_id": transfer_id,
                "authorization_id": authorization_id,
                "status": "plaid_create_failed",
                "detail": plaid_error_detail(create_result),
                "plaid_error": {
                    "error_type": create_result.get("error_type"),
                    "error_code": create_result.get("error_code"),
                    "request_id": create_result.get("request_id"),
                },
            }
        transfer = create_result.get("transfer") or create_result
        record["status"] = str(transfer.get("status") or "plaid_transfer_created")
        record["plaid_transfer_id"] = transfer.get("id") or create_result.get("transfer_id")
        record["plaid_request_id"] = create_result.get("request_id")
        remember_transfer_record(record)
        return {
            "success": True,
            "provider": "plaid_transfer",
            "transfer_id": transfer_id,
            "plaid_transfer_id": record["plaid_transfer_id"],
            "authorization_id": authorization_id,
            "status": record["status"],
            "detail": "Plaid Transfer was created. Confirm settlement and final status in the Plaid dashboard.",
        }
    record = {
        "transfer_id": transfer_id,
        "user_id": str(data.get("user_id") or "guest")[:120],
        "destination": str(data.get("destination") or "Vault")[:120],
        "amount": amount,
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "provider": health["provider"],
        "status": "queued_for_provider_handoff",
        "autopilot": bool(data.get("autopilot") or data.get("auto_transfer")),
        "source": str(data.get("source") or "vaultflow")[:80],
    }
    webhook_sent = False
    webhook_error = ""
    if TRANSFER_WEBHOOK_URL:
        try:
            async with httpx.AsyncClient(timeout=12) as client:
                response = await client.post(TRANSFER_WEBHOOK_URL, json=record)
            webhook_sent = response.status_code < 400
            if not webhook_sent:
                webhook_error = f"Provider handoff returned HTTP {response.status_code}."
        except Exception as exc:
            webhook_error = str(exc)
    remember_transfer_record({**record, "webhook_sent": webhook_sent, "webhook_error": webhook_error})
    return {
        "success": webhook_sent,
        "transfer_id": transfer_id,
        "status": "queued_for_provider_handoff" if webhook_sent else "handoff_failed",
        "webhook_sent": webhook_sent,
        "webhook_error": webhook_error,
        "detail": (
            "Transfer request was handed to the approved provider workflow. Confirm settlement in the provider dashboard."
            if webhook_sent
            else "Transfer request was built, but provider handoff failed. No money moved."
        ),
    }


@app.post("/bank/transfer/history")
def bank_transfer_history():
    stored = storage_read_json("transfer_requests", None)
    if isinstance(stored, list):
        transfer_requests[:] = stored
    return {"success": True, "count": len(transfer_requests), "transfers": transfer_requests[:50]}


@app.post("/app/state")
async def app_state(request: Request):
    data = await request.json()
    user_id = str(data.get("user_id") or "guest")[:120]
    app_state_store[user_id] = {
        "state": data.get("state") or {},
        "updated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }
    if len(app_state_store) > 1000:
        for key in list(app_state_store.keys())[:-1000]:
            app_state_store.pop(key, None)
    persist_state("app_state_store", app_state_store)
    return {
        "success": True,
        "stored": True,
        "persistent": storage_scaling_payload()["success"],
        "detail": "VaultFlow app state saved for this backend session.",
    }


@app.get("/capital/health")
def capital_health_get():
    return {
        "success": True,
        "configured": True,
        "build_version": BUILD_VERSION,
        "webhook_configured": bool(CAPITAL_WAITLIST_WEBHOOK_URL),
        "mode": "waitlist-marketplace",
        "detail": "VaultFlow Capital backend route is live. Waitlist mode is enabled; no loan decisions are made here.",
    }


@app.post("/capital/health")
def capital_health_post():
    return capital_health_get()


@app.post("/capital/waitlist")
async def capital_waitlist_join(request: Request):
    data = await request.json()
    name = str(data.get("name") or "").strip()[:120]
    email = str(data.get("email") or "").strip()[:160]
    capital_type = str(data.get("type") or data.get("capital_type") or "General capital").strip()[:80]
    goal = str(data.get("goal") or "").strip()[:500]
    source = str(data.get("source") or "vaultflow-capital").strip()[:80]
    consent = bool(data.get("consent"))
    try:
        need = max(0, min(1000000, int(float(data.get("need") or data.get("amount") or 0))))
    except Exception:
        need = 0

    if not email or "@" not in email or "." not in email.split("@")[-1]:
        raise HTTPException(status_code=400, detail="A valid email is required.")
    if not consent:
        raise HTTPException(status_code=400, detail="Consent is required before joining the Capital waitlist.")

    lead = {
        "name": name,
        "email": email,
        "type": capital_type,
        "need": need,
        "goal": goal,
        "source": source,
        "bridge_connected": bool(data.get("bridgeConnected") or data.get("bridge_connected")),
        "created_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "compliance_mode": "waitlist_only_no_credit_decision",
    }
    capital_waitlist.insert(0, lead)
    del capital_waitlist[200:]
    persist_state("capital_waitlist", capital_waitlist)

    webhook_sent = False
    webhook_error = ""
    if CAPITAL_WAITLIST_WEBHOOK_URL:
        try:
            async with httpx.AsyncClient(timeout=12) as client:
                response = await client.post(CAPITAL_WAITLIST_WEBHOOK_URL, json=lead)
            webhook_sent = response.status_code < 400
            if not webhook_sent:
                webhook_error = f"Webhook returned HTTP {response.status_code}."
        except Exception as exc:
            webhook_error = str(exc)

    return {
        "success": True,
        "stored": True,
        "webhook_sent": webhook_sent,
        "webhook_configured": bool(CAPITAL_WAITLIST_WEBHOOK_URL),
        "webhook_error": webhook_error,
        "detail": "Capital waitlist entry saved. This is not a loan application, approval, or offer.",
    }


@app.post("/create-subscription")
async def create_subscription(request: Request):
    try:
        data = await request.json()
        email = data.get("email")
        pm_id = data.get("payment_method_id")
        plan = data.get("plan", "prime")
        price = PRICE_PRIME if plan == "prime" else PRICE_VAULT
        if not STRIPE_SECRET or not price:
            raise HTTPException(status_code=400, detail="Stripe keys or price IDs are missing in Railway.")
        customer = stripe.Customer.create(
            email=email,
            payment_method=pm_id,
            invoice_settings={"default_payment_method": pm_id},
        )
        sub = stripe.Subscription.create(
            customer=customer.id,
            items=[{"price": price}],
            expand=["latest_invoice.payment_intent"],
        )
        return {"success": True, "subscription_id": sub.id, "status": sub.status}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@app.post("/plaid/create-link-token")
async def create_link_token(request: Request):
    require_plaid_config()
    data = await request.json()
    user_id = data.get("user_id") or data.get("client_user_id") or "default-user"
    payload = {
        "client_id": PLAID_CLIENT_ID,
        "secret": PLAID_SECRET,
        "client_name": PLAID_CLIENT_NAME,
        "country_codes": split_env(PLAID_COUNTRY_CODES, "US"),
        "language": "en",
        "user": {"client_user_id": str(user_id)},
        "products": requested_products(data),
    }
    status_code, result = await plaid_post("/link/token/create", payload)
    if "link_token" in result:
        return {
            "success": True,
            "link_token": result["link_token"],
            "expiration": result.get("expiration"),
            "request_id": result.get("request_id"),
        }
    return plaid_error_response(result, status_code)


async def get_income_user_reference(user_id):
    if user_id in income_users:
        return income_users[user_id], None
    payload = {
        "client_id": PLAID_CLIENT_ID,
        "secret": PLAID_SECRET,
        "client_user_id": str(user_id),
    }
    status_code, result = await plaid_post("/user/create", payload)
    if result.get("user_token"):
        income_users[user_id] = {"user_token": result["user_token"]}
        return income_users[user_id], None
    if result.get("user_id"):
        result["error_message"] = (
            "Plaid Income requires a user_token, but this Plaid app is returning only user_id from /user/create. "
            "Plaid requires newer integrations to request user-token access for Bank/Payroll Income through "
            "Plaid support, an account manager, or the Dashboard. Normal bank and investment Link still work."
        )
        return None, plaid_error_response(result, status_code)
    if result.get("request_id") and not (result.get("error_message") or result.get("error_code")):
        result["error_message"] = (
            "Plaid created a user response without user_id or user_token. Check whether Income is enabled "
            "for this Plaid app and whether the User API flow matches the current Plaid dashboard account."
        )
        return None, plaid_error_response(result, status_code)
    return None, plaid_error_response(result, status_code)


@app.post("/plaid/create-income-link-token")
async def create_income_link_token(request: Request):
    require_plaid_config()
    data = await request.json()
    user_id = data.get("user_id") or data.get("client_user_id") or "default-user"
    source_types = data.get("income_source_types") or ["payroll"]
    source_types = [item for item in source_types if item in {"payroll", "bank"}] or ["payroll"]
    user_ref, error = await get_income_user_reference(user_id)
    if error:
        return error

    income_verification = {"income_source_types": source_types}
    if "payroll" in source_types:
        income_verification["payroll_income"] = {"flow_types": ["payroll_digital_income"]}
    if "bank" in source_types:
        income_verification["bank_income"] = {"days_requested": PLAID_BANK_INCOME_DAYS}

    payload = {
        "client_id": PLAID_CLIENT_ID,
        "secret": PLAID_SECRET,
        "client_name": PLAID_CLIENT_NAME,
        "country_codes": split_env(PLAID_COUNTRY_CODES, "US"),
        "language": "en",
        "products": ["income_verification"],
        "income_verification": income_verification,
    }
    payload.update(user_ref)

    status_code, result = await plaid_post("/link/token/create", payload)
    if "link_token" in result:
        return {
            "success": True,
            "link_token": result["link_token"],
            "expiration": result.get("expiration"),
            "request_id": result.get("request_id"),
            "income_source_types": source_types,
        }
    return plaid_error_response(result, status_code)


@app.post("/plaid/income/payroll")
async def payroll_income(request: Request):
    require_plaid_config()
    data = await request.json()
    user_id = data.get("user_id") or "default-user"
    user_ref = income_users.get(user_id)
    if not user_ref:
        raise HTTPException(status_code=400, detail="Connect ADP/payroll first with /plaid/create-income-link-token.")
    payload = {"client_id": PLAID_CLIENT_ID, "secret": PLAID_SECRET}
    payload.update(user_ref)
    status_code, result = await plaid_post("/credit/payroll_income/get", payload)
    if status_code < 400:
        return {"success": True, "items": result.get("items", []), "request_id": result.get("request_id")}
    return plaid_error_response(result, status_code)


@app.post("/plaid/exchange-token")
async def exchange_token(request: Request):
    require_plaid_config()
    data = await request.json()
    public_token = data.get("public_token")
    if not public_token:
        raise HTTPException(status_code=400, detail="public_token is required.")
    status_code, result = await plaid_post(
        "/item/public_token/exchange",
        {"client_id": PLAID_CLIENT_ID, "secret": PLAID_SECRET, "public_token": public_token},
    )
    if "access_token" in result:
        session_id = create_bank_session(user_id, result["access_token"], result.get("item_id"))
        return {
            "success": True,
            "access_token": session_id,
            "bank_session_id": session_id,
            "item_id": result.get("item_id"),
            "request_id": result.get("request_id"),
            "session_storage": "backend-memory",
            "detail": "Plaid token exchanged and stored as an opaque backend bank session.",
        }
    return plaid_error_response(result, status_code)


@app.post("/plaid/transactions")
async def get_transactions(request: Request):
    require_plaid_config()
    data = await request.json()
    access_token = data.get("access_token")
    if not access_token:
        raise HTTPException(status_code=400, detail="access_token is required.")
    access_token = resolve_access_token(access_token)
    today = date.today()
    start = data.get("start_date") or (today - timedelta(days=120)).isoformat()
    end = data.get("end_date") or today.isoformat()
    status_code, result = await plaid_post(
        "/transactions/get",
        {
            "client_id": PLAID_CLIENT_ID,
            "secret": PLAID_SECRET,
            "access_token": access_token,
            "start_date": start,
            "end_date": end,
            "options": {"count": int(data.get("count", 100))},
        },
    )
    if "transactions" in result:
        transactions = result["transactions"]
        accounts = result.get("accounts", [])
        balance = 0
        if accounts:
            balance_data = accounts[0].get("balances", {})
            balance = balance_data.get("current") or balance_data.get("available") or 0
        paychecks = [
            txn for txn in transactions
            if txn.get("amount", 0) < -500 and any(
                word in (txn.get("name", "") + " " + txn.get("merchant_name", "")).lower()
                for word in ["payroll", "salary", "adp", "gusto", "paychex", "deposit"]
            )
        ]
        return {
            "success": True,
            "transactions": transactions[:100],
            "paychecks": paychecks[:10],
            "accounts": accounts,
            "balance": balance,
            "request_id": result.get("request_id"),
        }
    return plaid_error_response(result, status_code)


@app.post("/plaid/balance")
async def get_balance(request: Request):
    require_plaid_config()
    data = await request.json()
    access_token = data.get("access_token")
    if not access_token:
        raise HTTPException(status_code=400, detail="access_token is required.")
    access_token = resolve_access_token(access_token)
    status_code, result = await plaid_post(
        "/accounts/balance/get",
        {"client_id": PLAID_CLIENT_ID, "secret": PLAID_SECRET, "access_token": access_token},
    )
    if "accounts" in result:
        return {"success": True, "accounts": result["accounts"], "request_id": result.get("request_id")}
    return plaid_error_response(result, status_code)


def normalize_investment_holding(holding, securities_by_id):
    security = securities_by_id.get(holding.get("security_id"), {})
    quantity = float(holding.get("quantity") or 0)
    price = float(holding.get("institution_price") or 0)
    value = float(holding.get("institution_value") or quantity * price or 0)
    return {
        "symbol": security.get("ticker_symbol") or security.get("sedol") or security.get("cusip") or "N/A",
        "name": security.get("name") or "Investment holding",
        "quantity": quantity,
        "price": price,
        "value": value,
        "type": security.get("type") or security.get("market_identifier_code") or "Security",
        "account_id": holding.get("account_id"),
        "security_id": holding.get("security_id"),
        "cost_basis": holding.get("cost_basis"),
        "iso_currency_code": holding.get("iso_currency_code") or security.get("iso_currency_code"),
    }


async def investment_holdings_payload(request):
    require_plaid_config()
    data = await request.json()
    access_token = data.get("access_token")
    if not access_token:
        raise HTTPException(status_code=400, detail="access_token is required.")
    access_token = resolve_access_token(access_token)
    status_code, result = await plaid_post(
        "/investments/holdings/get",
        {"client_id": PLAID_CLIENT_ID, "secret": PLAID_SECRET, "access_token": access_token},
    )
    if "holdings" in result:
        securities_by_id = {item.get("security_id"): item for item in result.get("securities", [])}
        holdings = [normalize_investment_holding(item, securities_by_id) for item in result.get("holdings", [])]
        return {
            "success": True,
            "holdings": holdings,
            "accounts": result.get("accounts", []),
            "securities": result.get("securities", []),
            "total": sum(item.get("value") or 0 for item in holdings),
            "request_id": result.get("request_id"),
        }
    return plaid_error_response(result, status_code)


@app.post("/investments/holdings")
async def investments_holdings(request: Request):
    return await investment_holdings_payload(request)


@app.post("/plaid/investments/holdings")
async def plaid_investments_holdings(request: Request):
    return await investment_holdings_payload(request)


@app.post("/market/signals")
async def market_signals(request: Request):
    data = await request.json()
    raw_symbols = data.get("symbols")
    if isinstance(raw_symbols, list) and raw_symbols:
        symbols = [str(item).upper().strip() for item in raw_symbols if str(item).strip()][:8]
    else:
        symbols = [str(data.get("symbol") or "SPY").upper().strip()]
    history_by_symbol = data.get("history_by_symbol") if isinstance(data.get("history_by_symbol"), dict) else {}

    def fallback_prices(symbol):
        seed = sum((index + 3) * ord(char) for index, char in enumerate(symbol))
        base = 90 + seed % 240
        prices = []
        price = float(base)
        for index in range(60):
            drift = 0.18 + ((seed % 9) - 4) * 0.015
            wave = math.sin((index + seed % 13) / 4) * 0.7
            price = max(8, price + drift + wave)
            prices.append(round(price, 2))
        return prices

    async def build_signal(symbol, supplied_prices=None):
        source = "client-prices"
        prices = supplied_prices
        bars_used = 0
        if not prices and ALPACA_KEY_ID and ALPACA_SECRET_KEY:
            try:
                async with httpx.AsyncClient(timeout=20) as client:
                    response = await client.get(
                        f"{ALPACA_DATA_BASE_URL}/v2/stocks/{symbol}/bars",
                        headers={
                            "APCA-API-KEY-ID": ALPACA_KEY_ID,
                            "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
                        },
                        params={"timeframe": "1Day", "limit": 60, "feed": ALPACA_DATA_FEED, "adjustment": "raw"},
                    )
                alpaca_data = response.json()
                bars = alpaca_data.get("bars") or []
                prices = [float(bar.get("c")) for bar in bars if bar.get("c") is not None]
                bars_used = len(prices)
                if prices:
                    source = "alpaca-market-data"
            except Exception:
                prices = None
        if not prices:
            source = "backend-local"
            prices = fallback_prices(symbol)
        prices = [float(price) for price in prices if isinstance(price, (int, float))]
        if len(prices) < 5:
            prices = fallback_prices(symbol)
            source = "backend-local"
        short = statistics.mean(prices[-5:])
        long = statistics.mean(prices[-min(20, len(prices)):])
        returns = [(prices[i] - prices[i - 1]) / prices[i - 1] for i in range(1, len(prices)) if prices[i - 1]]
        volatility = statistics.pstdev(returns) * math.sqrt(252) if len(returns) > 1 else 0
        momentum = (prices[-1] - prices[0]) / prices[0]
        score = max(0, min(100, 50 + (short - long) * 3 + momentum * 100 - volatility * 20))
        action = "BUY" if score >= 62 else "SELL" if score <= 38 else "HOLD"
        last_price = prices[-1]
        risk_pct = max(0.015, min(0.08, volatility / math.sqrt(252) * 2 if volatility else 0.03))
        return {
            "symbol": symbol,
            "s": symbol,
            "action": action,
            "g": action,
            "label": action,
            "score": round(score, 1),
            "confidence": round(score, 1),
            "source": source,
            "bars_used": bars_used or len(prices),
            "last_price": round(last_price, 2),
            "stop": round(last_price * (1 - risk_pct), 2),
            "target": round(last_price * (1 + risk_pct * 1.8), 2),
            "metrics": {
                "short_average": round(short, 2),
                "long_average": round(long, 2),
                "momentum": round(momentum, 4),
                "annualized_volatility": round(volatility, 4),
                "risk_pct": round(risk_pct, 4),
            },
        }

    signals = []
    for symbol in symbols:
        supplied = history_by_symbol.get(symbol)
        if len(symbols) == 1:
            supplied = supplied or data.get("prices") or data.get("history")
        signals.append(await build_signal(symbol, supplied))
    primary = signals[0]
    return {
        "success": True,
        "symbol": primary["symbol"],
        "action": primary["action"],
        "confidence": primary["confidence"],
        "source": primary["source"],
        "signals": signals,
        "metrics": primary["metrics"],
        "disclaimer": "Educational signal only. Review risk before placing real trades.",
    }


@app.post("/trading/connect")
def trading_connect():
    configured = bool(ALPACA_KEY_ID and ALPACA_SECRET_KEY)
    live_endpoint = "paper-api" not in ALPACA_TRADING_BASE_URL
    live_guarded = live_endpoint and not ENABLE_LIVE_TRADING
    return {
        "success": configured,
        "configured": configured,
        "mode": (
            "live-ready"
            if configured and ENABLE_LIVE_TRADING and live_endpoint
            else "live-endpoint-guarded"
            if configured and live_guarded
            else "paper-ready"
            if configured and ENABLE_ALPACA_PAPER_ORDERS
            else "paper-keys-only"
            if configured
            else "missing-keys"
        ),
        "paper_orders_enabled": ENABLE_ALPACA_PAPER_ORDERS,
        "live_trading_enabled": ENABLE_LIVE_TRADING,
        "live_endpoint_guarded": live_guarded,
        "live_order_confirmation_required": bool(configured and ENABLE_LIVE_TRADING and live_endpoint),
        "live_trade_max_notional": LIVE_TRADE_MAX_NOTIONAL,
        "base_url": ALPACA_TRADING_BASE_URL,
        "data_base_url": ALPACA_DATA_BASE_URL,
        "data_feed": ALPACA_DATA_FEED,
        "detail": (
            "Alpaca live endpoint is configured and live trading is explicitly enabled."
            if configured and ENABLE_LIVE_TRADING and live_endpoint
            else "Alpaca base URL looks live, but ENABLE_LIVE_TRADING is false. Orders are guarded to prevent accidental real trades."
            if configured and live_guarded
            else "Alpaca keys are configured. Paper order submission is enabled and notional dollar orders are supported."
            if configured and ENABLE_ALPACA_PAPER_ORDERS
            else "Alpaca keys are configured. Add ENABLE_ALPACA_PAPER_ORDERS=true in Railway to submit paper notional orders."
            if configured
            else "Add ALPACA_KEY_ID and ALPACA_SECRET_KEY in Railway."
        ),
    }


def build_alpaca_order_payload(data):
    symbol = str(data.get("symbol") or "SPY").strip().upper()
    side = str(data.get("side") or "buy").strip().lower()
    order_type = str(data.get("type") or "market").strip().lower()
    time_in_force = str(data.get("time_in_force") or "day").strip().lower()
    if not symbol or not symbol.replace(".", "").replace("-", "").isalnum():
        raise HTTPException(status_code=400, detail="A valid trading symbol is required.")
    if side not in {"buy", "sell"}:
        raise HTTPException(status_code=400, detail="side must be buy or sell.")
    allowed_types = {"market", "limit", "stop", "stop_limit", "trailing_stop"}
    if order_type not in allowed_types:
        raise HTTPException(status_code=400, detail="Unsupported Alpaca order type.")
    allowed_tif = {"day", "gtc", "opg", "cls", "ioc", "fok"}
    if time_in_force not in allowed_tif:
        raise HTTPException(status_code=400, detail="Unsupported time_in_force.")

    payload = {
        "symbol": symbol,
        "side": side,
        "type": order_type,
        "time_in_force": time_in_force,
    }
    qty = data.get("qty") or data.get("quantity")
    notional = data.get("notional") or data.get("amount")
    if qty not in {None, ""}:
        qty_value = float(qty)
        if qty_value <= 0:
            raise HTTPException(status_code=400, detail="qty must be greater than zero.")
        payload["qty"] = str(qty_value).rstrip("0").rstrip(".")
    elif notional not in {None, ""}:
        notional_value = round(float(notional), 2)
        if notional_value <= 0:
            raise HTTPException(status_code=400, detail="notional must be greater than zero.")
        if order_type != "market":
            raise HTTPException(status_code=400, detail="Alpaca notional orders must use market type.")
        payload["notional"] = f"{notional_value:.2f}"
    else:
        payload["qty"] = "1"

    if order_type in {"limit", "stop_limit"}:
        limit_price = data.get("limit_price")
        if limit_price in {None, ""}:
            raise HTTPException(status_code=400, detail="limit_price is required for limit orders.")
        payload["limit_price"] = str(round(float(limit_price), 4))
    if order_type in {"stop", "stop_limit"}:
        stop_price = data.get("stop_price")
        if stop_price in {None, ""}:
            raise HTTPException(status_code=400, detail="stop_price is required for stop orders.")
        payload["stop_price"] = str(round(float(stop_price), 4))
    client_order_id = str(data.get("client_order_id") or "").strip()
    if client_order_id:
        payload["client_order_id"] = client_order_id[:48]
    return payload


@app.post("/trading/order")
async def trading_order(request: Request):
    data = await request.json()
    live_endpoint = "paper-api" not in ALPACA_TRADING_BASE_URL
    if live_endpoint and not ENABLE_LIVE_TRADING:
        return {
            "success": False,
            "guarded": True,
            "detail": "Alpaca base URL looks live, but ENABLE_LIVE_TRADING is false. Switch ALPACA_TRADING_BASE_URL to paper-api or explicitly enable live trading after compliance review.",
        }
    if not (ENABLE_ALPACA_PAPER_ORDERS or ENABLE_LIVE_TRADING):
        return {
            "success": False,
            "guarded": True,
            "detail": "Order placement is disabled. Turn on ENABLE_ALPACA_PAPER_ORDERS only after you confirm paper trading.",
        }
    if not (ALPACA_KEY_ID and ALPACA_SECRET_KEY):
        raise HTTPException(status_code=400, detail="Alpaca keys are missing.")
    order_payload = build_alpaca_order_payload(data)
    if live_endpoint:
        if not data.get("live_authorized"):
            return {
                "success": False,
                "guarded": True,
                "requires_live_authorization": True,
                "detail": "Live Alpaca trading is enabled, but this order needs explicit live-order authorization from the user.",
            }
        if "notional" in order_payload and LIVE_TRADE_MAX_NOTIONAL > 0:
            notional_value = float(order_payload["notional"])
            if notional_value > LIVE_TRADE_MAX_NOTIONAL:
                return {
                    "success": False,
                    "guarded": True,
                    "detail": f"Live order blocked by max notional risk limit (${LIVE_TRADE_MAX_NOTIONAL:,.2f}). Lower the amount or update LIVE_TRADE_MAX_NOTIONAL after review.",
                }
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"{ALPACA_TRADING_BASE_URL}/v2/orders",
            headers={
                "APCA-API-KEY-ID": ALPACA_KEY_ID,
                "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
            },
            json=order_payload,
        )
    try:
        result = response.json()
    except Exception:
        result = {"detail": response.text}
    if response.status_code >= 400:
        return JSONResponse(status_code=response.status_code, content={"success": False, "detail": result})
    return {
        "success": True,
        "mode": "live" if live_endpoint else "paper",
        "order_id": result.get("id"),
        "status": result.get("status") or "submitted",
        "order": result,
        "submitted": order_payload,
    }


@app.post("/webhook")
async def webhook(request: Request):
    payload = await request.body()
    sig = request.headers.get("stripe-signature")
    try:
        event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK)
        if event["type"] == "invoice.payment_succeeded":
            print("Payment succeeded")
        return {"status": "ok"}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
