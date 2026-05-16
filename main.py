from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import stripe
import os
import json
import httpx

app = FastAPI()

app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# CONFIG
STRIPE_SECRET = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
PRICE_PRIME = os.environ.get("STRIPE_PRICE_PRIME", "")
PRICE_VAULT = os.environ.get("STRIPE_PRICE_VAULT", "")
PLAID_CLIENT_ID = os.environ.get("PLAID_CLIENT_ID", "")
PLAID_SECRET = os.environ.get("PLAID_SECRET", "")
PLAID_ENV = os.environ.get("PLAID_ENV", "sandbox")
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")

stripe.api_key = STRIPE_SECRET

PLAID_BASE = f"https://{PLAID_ENV}.plaid.com"

# ===== HEALTH CHECK =====
@app.get("/")
def root():
    return {"status": "VaultFlow backend running", "plaid_env": PLAID_ENV}

# ===== STRIPE SUBSCRIPTION =====
@app.post("/create-subscription")
async def create_subscription(request: Request):
    try:
        data = await request.json()
        email = data.get("email")
        pm_id = data.get("payment_method_id")
        plan = data.get("plan", "prime")
        price = PRICE_PRIME if plan == "prime" else PRICE_VAULT
        customer = stripe.Customer.create(
            email=email,
            payment_method=pm_id,
            invoice_settings={"default_payment_method": pm_id}
        )
        sub = stripe.Subscription.create(
            customer=customer.id,
            items=[{"price": price}],
            expand=["latest_invoice.payment_intent"]
        )
        return {"success": True, "subscription_id": sub.id, "status": sub.status}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ===== PLAID - CREATE LINK TOKEN =====
@app.post("/plaid/create-link-token")
async def create_link_token(request: Request):
    try:
        data = await request.json()
        user_id = data.get("user_id", "default-user")
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{PLAID_BASE}/link/token/create",
                json={
                    "client_id": PLAID_CLIENT_ID,
                    "secret": PLAID_SECRET,
                    "client_name": "VaultFlow",
                    "country_codes": ["US"],
                    "language": "en",
                    "user": {"client_user_id": user_id},
                    "products": ["transactions", "auth"],
                }
            )
        result = resp.json()
        if "link_token" in result:
            return {"link_token": result["link_token"]}
        raise HTTPException(status_code=400, detail=result.get("error_message", "Failed to create link token"))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ===== PLAID - EXCHANGE TOKEN =====
@app.post("/plaid/exchange-token")
async def exchange_token(request: Request):
    try:
        data = await request.json()
        public_token = data.get("public_token")
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{PLAID_BASE}/item/public_token/exchange",
                json={
                    "client_id": PLAID_CLIENT_ID,
                    "secret": PLAID_SECRET,
                    "public_token": public_token
                }
            )
        result = resp.json()
        if "access_token" in result:
            access_token = result["access_token"]
            return {"success": True, "access_token": access_token}
        raise HTTPException(status_code=400, detail=result.get("error_message", "Exchange failed"))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ===== PLAID - GET TRANSACTIONS =====
@app.post("/plaid/transactions")
async def get_transactions(request: Request):
    try:
        data = await request.json()
        access_token = data.get("access_token")
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{PLAID_BASE}/transactions/get",
                json={
                    "client_id": PLAID_CLIENT_ID,
                    "secret": PLAID_SECRET,
                    "access_token": access_token,
                    "start_date": "2024-01-01",
                    "end_date": "2025-12-31",
                    "options": {"count": 100}
                }
            )
        result = resp.json()
        if "transactions" in result:
            txns = result["transactions"]
            # Detect paychecks (large deposits over $1000)
            paychecks = [t for t in txns if t.get("amount", 0) < -1000]
            return {
                "success": True,
                "transactions": txns[:20],
                "paychecks": paychecks[:5],
                "balance": result.get("accounts", [{}])[0].get("balances", {}).get("current", 0)
            }
        raise HTTPException(status_code=400, detail=result.get("error_message", "Failed to get transactions"))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ===== PLAID - GET BALANCE =====
@app.post("/plaid/balance")
async def get_balance(request: Request):
    try:
        data = await request.json()
        access_token = data.get("access_token")
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{PLAID_BASE}/accounts/balance/get",
                json={
                    "client_id": PLAID_CLIENT_ID,
                    "secret": PLAID_SECRET,
                    "access_token": access_token
                }
            )
        result = resp.json()
        if "accounts" in result:
            accounts = result["accounts"]
            return {"success": True, "accounts": accounts}
        raise HTTPException(status_code=400, detail="Failed to get balance")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

# ===== STRIPE WEBHOOK =====
@app.post("/webhook")
async def webhook(request: Request):
    payload = await request.body()
    sig = request.headers.get("stripe-signature")
    try:
        event = stripe.Webhook.construct_event(payload, sig, STRIPE_WEBHOOK)
        if event["type"] == "invoice.payment_succeeded":
            email = event["data"]["object"]["customer_email"]
            print(f"Payment succeeded for {email}")
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
