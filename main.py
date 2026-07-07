from datetime import date, timedelta
import math
import os
import statistics

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import httpx
import stripe


app = FastAPI(title="VaultFlow Backend", version="2026.07.06")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BUILD_VERSION = "vaultflow-fastapi-2026-07-06-plaid-health"

STRIPE_SECRET = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
PRICE_PRIME = os.environ.get("STRIPE_PRICE_PRIME", "")
PRICE_VAULT = os.environ.get("STRIPE_PRICE_VAULT", "")

PLAID_CLIENT_ID = os.environ.get("PLAID_CLIENT_ID", "").strip()
PLAID_SECRET = os.environ.get("PLAID_SECRET", "").strip()
PLAID_ENV = os.environ.get("PLAID_ENV", "sandbox").strip() or "sandbox"
PLAID_CLIENT_NAME = os.environ.get("PLAID_CLIENT_NAME", "VaultFlow").strip() or "VaultFlow"
PLAID_PRODUCTS = os.environ.get("PLAID_PRODUCTS", "auth,transactions")
PLAID_COUNTRY_CODES = os.environ.get("PLAID_COUNTRY_CODES", "US")
PLAID_BANK_INCOME_DAYS = int(os.environ.get("PLAID_BANK_INCOME_DAYS", "120") or "120")

ALPACA_KEY_ID = os.environ.get("ALPACA_KEY_ID", "").strip()
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "").strip()
ALPACA_TRADING_BASE_URL = os.environ.get("ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets")
ALPACA_DATA_FEED = os.environ.get("ALPACA_DATA_FEED", "iex")
ENABLE_ALPACA_PAPER_ORDERS = os.environ.get("ENABLE_ALPACA_PAPER_ORDERS", "false").lower() == "true"
ENABLE_LIVE_TRADING = os.environ.get("ENABLE_LIVE_TRADING", "false").lower() == "true"

stripe.api_key = STRIPE_SECRET

PLAID_BASE = f"https://{PLAID_ENV}.plaid.com"
income_users = {}


def split_env(value, fallback):
    raw = value or fallback
    return [item.strip() for item in raw.split(",") if item.strip()]


def plaid_configured():
    return bool(PLAID_CLIENT_ID and PLAID_SECRET)


def plaid_health_payload():
    configured = plaid_configured()
    return {
        "success": configured,
        "configured": configured,
        "build_version": BUILD_VERSION,
        "plaid_env": PLAID_ENV,
        "client_id_present": bool(PLAID_CLIENT_ID),
        "secret_present": bool(PLAID_SECRET),
        "client_id_preview": f"{PLAID_CLIENT_ID[:6]}...{PLAID_CLIENT_ID[-4:]}" if PLAID_CLIENT_ID else "",
        "products": split_env(PLAID_PRODUCTS, "auth,transactions"),
        "country_codes": split_env(PLAID_COUNTRY_CODES, "US"),
        "income_link_supported": True,
        "detail": (
            "Plaid backend is configured."
            if configured
            else "Set PLAID_CLIENT_ID, PLAID_SECRET, and PLAID_ENV on this Railway service."
        ),
    }


def plaid_error_detail(result):
    code = result.get("error_code") or ""
    message = result.get("error_message") or result.get("display_message") or "Plaid request failed."
    if code in {"INVALID_API_KEYS", "INVALID_CLIENT_ID", "INVALID_SECRET"} or "client_id" in message.lower():
        return (
            "Plaid rejected the backend key pair. In Railway, make sure PLAID_CLIENT_ID is the Plaid Client ID, "
            "PLAID_SECRET is the matching Sandbox Secret, and PLAID_ENV is sandbox."
        )
    return message


def plaid_error_response(result, status_code=400):
    return JSONResponse(
        status_code=status_code,
        content={
            "success": False,
            "detail": plaid_error_detail(result),
            "action": "Fix the Railway Plaid variables, redeploy, then retry VaultFlow's live check.",
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


@app.get("/")
def root():
    return {
        "status": "VaultFlow backend running",
        "build_version": BUILD_VERSION,
        "plaid_env": PLAID_ENV,
        "routes": [
            "GET /plaid/health",
            "POST /plaid/create-link-token",
            "POST /plaid/create-income-link-token",
            "POST /plaid/exchange-token",
            "POST /plaid/transactions",
            "POST /plaid/balance",
            "POST /market/signals",
            "POST /trading/connect",
            "POST /trading/order",
            "POST /live/readiness",
        ],
    }


@app.get("/plaid/health")
def plaid_health_get():
    return plaid_health_payload()


@app.post("/plaid/health")
def plaid_health_post():
    return plaid_health_payload()


@app.post("/live/readiness")
def live_readiness():
    return {
        "success": True,
        "build_version": BUILD_VERSION,
        "plaid_backend": plaid_health_payload(),
        "plaid_income_link_supported": True,
        "stripe_configured": bool(STRIPE_SECRET),
        "alpaca_configured": bool(ALPACA_KEY_ID and ALPACA_SECRET_KEY),
        "alpaca_orders_enabled": ENABLE_ALPACA_PAPER_ORDERS or ENABLE_LIVE_TRADING,
        "detail": "Backend readiness route is live.",
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
        "products": split_env(PLAID_PRODUCTS, "auth,transactions"),
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


async def get_income_user_token(user_id):
    if user_id in income_users:
        return income_users[user_id]
    payload = {
        "client_id": PLAID_CLIENT_ID,
        "secret": PLAID_SECRET,
        "client_user_id": str(user_id),
    }
    status_code, result = await plaid_post("/user/create", payload)
    if "user_token" not in result:
        return None, plaid_error_response(result, status_code)
    income_users[user_id] = result["user_token"]
    return result["user_token"], None


@app.post("/plaid/create-income-link-token")
async def create_income_link_token(request: Request):
    require_plaid_config()
    data = await request.json()
    user_id = data.get("user_id") or data.get("client_user_id") or "default-user"
    source_types = data.get("income_source_types") or ["payroll"]
    source_types = [item for item in source_types if item in {"payroll", "bank"}] or ["payroll"]
    user_token, error = await get_income_user_token(user_id)
    if error:
        return error

    payload = {
        "client_id": PLAID_CLIENT_ID,
        "secret": PLAID_SECRET,
        "client_name": PLAID_CLIENT_NAME,
        "country_codes": split_env(PLAID_COUNTRY_CODES, "US"),
        "language": "en",
        "user_token": user_token,
        "products": ["income_verification"],
        "income_verification": {"income_source_types": source_types},
    }
    if "payroll" in source_types:
        payload["payroll_income"] = {"flow_types": ["payroll_digital_income"]}
    if "bank" in source_types:
        payload["bank_income"] = {"days_requested": PLAID_BANK_INCOME_DAYS}

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
    user_token = income_users.get(user_id)
    if not user_token:
        raise HTTPException(status_code=400, detail="Connect ADP/payroll first with /plaid/create-income-link-token.")
    payload = {"client_id": PLAID_CLIENT_ID, "secret": PLAID_SECRET, "user_token": user_token}
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
        return {
            "success": True,
            "access_token": result["access_token"],
            "item_id": result.get("item_id"),
            "request_id": result.get("request_id"),
        }
    return plaid_error_response(result, status_code)


@app.post("/plaid/transactions")
async def get_transactions(request: Request):
    require_plaid_config()
    data = await request.json()
    access_token = data.get("access_token")
    if not access_token:
        raise HTTPException(status_code=400, detail="access_token is required.")
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
            "accounts": result.get("accounts", []),
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
    status_code, result = await plaid_post(
        "/accounts/balance/get",
        {"client_id": PLAID_CLIENT_ID, "secret": PLAID_SECRET, "access_token": access_token},
    )
    if "accounts" in result:
        return {"success": True, "accounts": result["accounts"], "request_id": result.get("request_id")}
    return plaid_error_response(result, status_code)


@app.post("/market/signals")
async def market_signals(request: Request):
    data = await request.json()
    symbol = (data.get("symbol") or "SPY").upper()
    prices = data.get("prices") or data.get("history") or [498, 501, 503, 500, 506, 509, 512, 511, 515, 518]
    prices = [float(price) for price in prices if isinstance(price, (int, float))]
    if len(prices) < 5:
        raise HTTPException(status_code=400, detail="At least five price points are required.")
    short = statistics.mean(prices[-5:])
    long = statistics.mean(prices[-min(20, len(prices)):])
    returns = [(prices[i] - prices[i - 1]) / prices[i - 1] for i in range(1, len(prices)) if prices[i - 1]]
    volatility = statistics.pstdev(returns) * math.sqrt(252) if len(returns) > 1 else 0
    momentum = (prices[-1] - prices[0]) / prices[0]
    score = max(0, min(100, 50 + (short - long) * 3 + momentum * 100 - volatility * 20))
    action = "BUY" if score >= 62 else "SELL" if score <= 38 else "HOLD"
    return {
        "success": True,
        "symbol": symbol,
        "action": action,
        "confidence": round(score, 1),
        "metrics": {
            "short_average": round(short, 2),
            "long_average": round(long, 2),
            "momentum": round(momentum, 4),
            "annualized_volatility": round(volatility, 4),
        },
        "disclaimer": "Educational signal only. Review risk before placing real trades.",
    }


@app.post("/trading/connect")
def trading_connect():
    return {
        "success": bool(ALPACA_KEY_ID and ALPACA_SECRET_KEY),
        "configured": bool(ALPACA_KEY_ID and ALPACA_SECRET_KEY),
        "paper_orders_enabled": ENABLE_ALPACA_PAPER_ORDERS,
        "live_trading_enabled": ENABLE_LIVE_TRADING,
        "base_url": ALPACA_TRADING_BASE_URL,
        "data_feed": ALPACA_DATA_FEED,
        "detail": (
            "Alpaca keys are configured."
            if ALPACA_KEY_ID and ALPACA_SECRET_KEY
            else "Add ALPACA_KEY_ID and ALPACA_SECRET_KEY in Railway."
        ),
    }


@app.post("/trading/order")
async def trading_order(request: Request):
    if not (ENABLE_ALPACA_PAPER_ORDERS or ENABLE_LIVE_TRADING):
        return {
            "success": False,
            "guarded": True,
            "detail": "Real order placement is disabled. Turn on ENABLE_ALPACA_PAPER_ORDERS only after you confirm paper trading.",
        }
    if not (ALPACA_KEY_ID and ALPACA_SECRET_KEY):
        raise HTTPException(status_code=400, detail="Alpaca keys are missing.")
    data = await request.json()
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(
            f"{ALPACA_TRADING_BASE_URL}/v2/orders",
            headers={
                "APCA-API-KEY-ID": ALPACA_KEY_ID,
                "APCA-API-SECRET-KEY": ALPACA_SECRET_KEY,
            },
            json={
                "symbol": (data.get("symbol") or "SPY").upper(),
                "qty": str(data.get("qty") or data.get("quantity") or 1),
                "side": data.get("side", "buy"),
                "type": data.get("type", "market"),
                "time_in_force": data.get("time_in_force", "day"),
            },
        )
    try:
        result = response.json()
    except Exception:
        result = {"detail": response.text}
    if response.status_code >= 400:
        return JSONResponse(status_code=response.status_code, content={"success": False, "detail": result})
    return {"success": True, "order": result}


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
