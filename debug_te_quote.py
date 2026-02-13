"""
Quick diagnostic: call TE shipment/quote with both commercial and residential
types to compare raw API responses vs what the GUI shows.
"""
import json, sys, os
sys.path.insert(0, os.path.dirname(__file__))

# Set dummy env vars to allow config.py to load without errors
if not os.getenv("LINE_TOKEN"):
    os.environ["LINE_TOKEN"] = "dummy"

from services.te_api_service import call_api as te_call_api

def to_inches(cm):
    return round(cm / 2.54, 2)

# --- Test package (adjust to match your test case) ---
# Use the same package you quoted in the TE GUI
packages = [
    {
        "weight": 8.50,  # kg
        "dimension": {
            "length": to_inches(46),
            "width": to_inches(35),
            "height": to_inches(33),
        },
        "insurance": 100,
    }
]

FROM_POSTAL = "M4J2A1"
TO_POSTAL = "V6X1Z7"

for init_type, dest_type in [("commercial", "commercial"), ("residential", "commercial"), ("residential", "residential")]:
    payload = {
        "initiation": {
            "region_id": "CA",
            "postalcode": FROM_POSTAL,
            "type": init_type,
        },
        "destination": {
            "region_id": "CA",
            "postalcode": TO_POSTAL,
            "type": dest_type,
        },
        "package": {"type": "parcel", "packages": packages},
        "option": {"memo": "Parcel"},
    }

    print(f"\n{'='*80}")
    print(f"  INIT TYPE: {init_type} -> DEST TYPE: {dest_type}")
    print(f"{'='*80}")

    result = te_call_api("shipment/quote", payload)

    if not result or result.get("status") != 1:
        print(f"  ERROR: {result}")
        continue

    for carrier in result.get("response", []):
        vendor = carrier.get("name", "?")
        for svc in carrier.get("services", []):
            name = svc.get("name", "?")
            freight = svc.get("freight", 0)
            charge = svc.get("charge", 0)
            eta = svc.get("eta", "?")

            tax_details = svc.get("tax_details", [])
            charge_details = svc.get("charge_details", [])

            tax_sum = sum(float(t.get("price", 0)) for t in tax_details)
            surcharge_sum = sum(float(d.get("price", 0)) for d in charge_details)

            print(f"\n  {vendor} - {name}")
            print(f"    freight (API):  {freight}")
            print(f"    charge (API):   {charge}  <-- Grand Total from API")
            print(f"    charge_details: {charge_details}")
            print(f"    tax_details:    {tax_details}")
            print(f"    surcharge sum:  {surcharge_sum}")
            print(f"    tax sum:        {tax_sum}")
            print(f"    freight+sur+tax={float(freight)+surcharge_sum+tax_sum:.2f}")
            print(f"    ETA:            {eta}")
