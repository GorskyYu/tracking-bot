"""Debug: compare TE API raw response for commercial vs residential."""
import json
from services.te_api_service import call_api as te_call_api

to_inches = lambda cm: round(cm / 2.54, 2)

packages = [{"weight": 8.50, "dimension": {"length": to_inches(46), "width": to_inches(35), "height": to_inches(33)}, "insurance": 100}]

for addr_type in ["commercial", "residential"]:
    payload = {
        "initiation": {"region_id": "CA", "postalcode": "M4J2A1", "type": addr_type},
        "destination": {"region_id": "CA", "postalcode": "V6X1Z7", "type": addr_type},
        "package": {"type": "parcel", "packages": packages},
        "option": {"memo": "Parcel"},
    }
    print(f"\n=== {addr_type} ===")
    result = te_call_api("shipment/quote", payload)
    if result and result.get("status") == 1:
        for carrier in result.get("response", []):
            vendor = carrier.get("name", "?")
            for svc in carrier.get("services", []):
                name = svc.get("name", "?")
                freight = svc.get("freight", 0)
                charge = svc.get("charge", 0)
                tax_details = svc.get("tax_details", [])
                charge_details = svc.get("charge_details", [])
                tax_sum = sum(float(t.get("price", 0)) for t in tax_details)
                sur_sum = sum(float(d.get("price", 0)) for d in charge_details)
                print(f"  {vendor} - {name}")
                print(f"    freight={freight}, charge={charge}")
                print(f"    charge_details={json.dumps(charge_details)}")
                print(f"    tax_details={json.dumps(tax_details)}")
                print(f"    sur_sum={sur_sum}, tax_sum={tax_sum}, calc={float(freight)+sur_sum+tax_sum:.2f}")
                skip = {"name","freight","charge","tax_details","charge_details","eta"}
                other = {k:v for k,v in svc.items() if k not in skip}
                if other:
                    print(f"    other_keys={json.dumps(other)}")
    else:
        print(f"  ERROR: {result}")
