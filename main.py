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

BUILD_VERSION = "vaultflow-fastapi-2026-07-08-ai-alpaca-market-data"


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

STRIPE_SECRET = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
PRICE_PRIME = os.environ.get("STRIPE_PRICE_PRIME", "")
PRICE_VAULT = os.environ.get("STRIPE_PRICE_VAULT", "")
OPENAI_API_KEY = clean_env_value("OPENAI_API_KEY")
OPENAI_MODEL = clean_env_value("OPENAI_MODEL", "gpt-4o-mini") or "gpt-4o-mini"
DOC_VAULT_ENCRYPTION_KEY = clean_env_value("DOC_VAULT_ENCRYPTION_KEY")
ENABLE_TRANSFER_RAIL = clean_env_value("ENABLE_TRANSFER_RAIL", "false").lower() == "true"

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

stripe.api_key = STRIPE_SECRET

PLAID_BASE = f"https://{PLAID_ENV}.plaid.com"
income_users = {}


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
            "POST /investments/holdings",
            "POST /plaid/investments/holdings",
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
        "investment_holdings_supported": True,
        "stripe_configured": bool(STRIPE_SECRET),
        "ai_configured": bool(OPENAI_API_KEY),
        "ai_model": OPENAI_MODEL if OPENAI_API_KEY else "",
        "alpaca_configured": bool(ALPACA_KEY_ID and ALPACA_SECRET_KEY),
        "alpaca_orders_enabled": ENABLE_ALPACA_PAPER_ORDERS or ENABLE_LIVE_TRADING,
        "transfer_enabled": ENABLE_TRANSFER_RAIL,
        "doc_vault_key_configured": bool(DOC_VAULT_ENCRYPTION_KEY),
        "detail": "Backend readiness route is live.",
    }


@app.post("/ai/health")
def ai_health():
    return {
        "success": True,
        "configured": bool(OPENAI_API_KEY),
        "model": OPENAI_MODEL if OPENAI_API_KEY else "",
        "detail": (
            "OpenAI backend key is configured."
            if OPENAI_API_KEY
            else "Add OPENAI_API_KEY and optionally OPENAI_MODEL in Railway to enable real AI answers."
        ),
    }


@app.post("/ai/chat")
async def ai_chat(request: Request):
    if not OPENAI_API_KEY:
        return JSONResponse(
            status_code=503,
            content={
                "success": False,
                "configured": False,
                "detail": "Add OPENAI_API_KEY in Railway to enable real AI answers.",
            },
        )
    data = await request.json()
    question = str(data.get("message") or data.get("question") or "").strip()
    if not question:
        raise HTTPException(status_code=400, detail="message is required.")
    snapshot = data.get("snapshot") or data.get("context") or {}
    history = data.get("history") or []
    system_prompt = (
        "You are VaultFlow's AI assistant. Answer normal everyday questions clearly, and answer finance "
        "questions as an educational financial coach. Be practical, concise, and friendly. Ask users to verify "
        "numbers before acting. Never claim to provide legal, tax, investment, lending, or trading advice."
    )
    prior = []
    if isinstance(history, list):
        for item in history[-8:]:
            role = "assistant" if item.get("from") == "ai" else "user"
            text = str(item.get("text") or "")[:900]
            if text:
                prior.append({"role": role, "content": text})
    user_prompt = f"User question: {question}\n\nSafe VaultFlow context JSON: {snapshot}"
    messages = [{"role": "system", "content": system_prompt}] + prior + [{"role": "user", "content": user_prompt}]
    async with httpx.AsyncClient(timeout=45) as client:
        response = await client.post(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"},
            json={
                "model": OPENAI_MODEL,
                "messages": messages,
                "temperature": 0.4,
                "max_tokens": 500,
            },
        )
    result = response.json()
    if response.status_code >= 400:
        return JSONResponse(
            status_code=response.status_code,
            content={
                "success": False,
                "configured": True,
                "mode": "openai-error",
                "detail": result.get("error", {}).get("message") or "OpenAI request failed.",
            },
        )
    answer = result.get("choices", [{}])[0].get("message", {}).get("content", "").strip()
    return {"success": True, "configured": True, "mode": "openai-responses", "model": OPENAI_MODEL, "answer": answer}


@app.post("/vault/sign-url")
async def vault_sign_url(request: Request):
    data = await request.json()
    filename = data.get("filename") or "document"
    if not DOC_VAULT_ENCRYPTION_KEY:
        return {
            "success": False,
            "configured": False,
            "detail": "Add DOC_VAULT_ENCRYPTION_KEY and a real storage provider before production document uploads.",
        }
    return {
        "success": False,
        "configured": True,
        "detail": f"Encryption key is present, but secure object storage is not connected yet for {filename}.",
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
    return {
        "success": configured,
        "configured": configured,
        "mode": "paper-ready" if configured and ENABLE_ALPACA_PAPER_ORDERS else "paper-keys-only" if configured else "missing-keys",
        "paper_orders_enabled": ENABLE_ALPACA_PAPER_ORDERS,
        "live_trading_enabled": ENABLE_LIVE_TRADING,
        "base_url": ALPACA_TRADING_BASE_URL,
        "data_base_url": ALPACA_DATA_BASE_URL,
        "data_feed": ALPACA_DATA_FEED,
        "detail": (
            "Alpaca keys are configured. Paper order submission is enabled."
            if configured and ENABLE_ALPACA_PAPER_ORDERS
            else "Alpaca keys are configured. Add ENABLE_ALPACA_PAPER_ORDERS=true in Railway to submit paper orders."
            if configured
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
