from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import stripe
import os

app = FastAPI()
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_methods=["*"],allow_headers=["*"])

stripe.api_key = os.environ.get("STRIPE_SECRET_KEY","")
WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET","")
PRICE_PRIME = os.environ.get("STRIPE_PRICE_PRIME","")
PRICE_VAULT = os.environ.get("STRIPE_PRICE_VAULT","")

@app.get("/")
def root():
    return {"status":"VaultFlow backend running"}

@app.post("/create-subscription")
async def create_subscription(request: Request):
    try:
        data = await request.json()
        email = data.get("email")
        pm_id = data.get("payment_method_id")
        plan = data.get("plan","prime")
        price = PRICE_PRIME if plan=="prime" else PRICE_VAULT
        customer = stripe.Customer.create(email=email,payment_method=pm_id,invoice_settings={"default_payment_method":pm_id})
        sub = stripe.Subscription.create(customer=customer.id,items=[{"price":price}],expand=["latest_invoice.payment_intent"])
        return {"success":True,"subscription_id":sub.id,"status":sub.status}
    except Exception as e:
        raise HTTPException(status_code=400,detail=str(e))

@app.post("/webhook")
async def webhook(request: Request):
    payload = await request.body()
    sig = request.headers.get("stripe-signature")
    try:
        event = stripe.Webhook.construct_event(payload,sig,WEBHOOK_SECRET)
        if event["type"]=="invoice.payment_succeeded":
            print("Payment succeeded:",event["data"]["object"]["customer_email"])
        return {"status":"ok"}
    except Exception as e:
        raise HTTPException(status_code=400,detail=str(e))
