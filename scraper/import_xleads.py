"""
Xleads Skip Trace Import → GHL
Updates existing contacts and adds to workflow
"""
import csv, sys, json, time, re, os
import requests
from datetime import datetime

GHL_API_KEY     = "pit-43fab20f-2ff3-438f-b074-2a7a78276896"
GHL_LOCATION_ID = "DCHwcJVyBixvcmK0DO37"
GHL_WORKFLOW_ID = "48fa268e-a09a-4515-92ad-1ff60bbfd08d"
HEADERS = {
    "Authorization": f"Bearer {GHL_API_KEY}",
    "Content-Type": "application/json",
    "Version": "2021-07-28"
}

def clean_phone(p):
    if not p: return ""
    digits = re.sub(r"\D", "", str(p))
    if len(digits) == 10: return "+1" + digits
    if len(digits) == 11 and digits[0] == "1": return "+" + digits
    return ""

def find_existing_contact(phone, email):
    """Search for existing contact by phone or email"""
    if phone:
        res = requests.get(
            f"https://services.leadconnectorhq.com/contacts/search/duplicate",
            headers=HEADERS,
            params={"locationId": GHL_LOCATION_ID, "number": phone}
        )
        if res.ok:
            data = res.json()
            if data.get("contact"):
                return data["contact"]["id"]
    if email:
        res = requests.get(
            f"https://services.leadconnectorhq.com/contacts/search/duplicate",
            headers=HEADERS,
            params={"locationId": GHL_LOCATION_ID, "email": email}
        )
        if res.ok:
            data = res.json()
            if data.get("contact"):
                return data["contact"]["id"]
    return None

def push_contact(row):
    first = (row.get("FirstName") or "").strip()
    last  = (row.get("LastName") or "").strip()

    # Only mobile phones, skip DNC and litigators
    phone = ""
    for i in range(1, 4):
        p_type      = str(row.get(f"Contact1Phone_{i}_Type") or "").strip().lower()
        p_val       = clean_phone(row.get(f"Contact1Phone_{i}") or "")
        p_dnc       = str(row.get(f"Contact1Phone_{i}_DNC") or "").lower()
        p_litigator = str(row.get(f"Contact1Phone_{i}_Litigator") or "").lower()
        if p_val and p_type == "mobile" and p_dnc != "true" and p_litigator != "true":
            phone = p_val
            break

    email      = (row.get("Contact1Email_1") or "").strip()
    prop_addr  = (row.get("PropertyAddress") or "").strip()
    prop_city  = (row.get("PropertyCity") or "").strip()
    prop_state = (row.get("PropertyState") or "").strip()
    prop_zip   = str(row.get("PropertyPostalCode") or "").strip()

    if not phone and not email:
        return False, "No mobile phone or email"

    contact_payload = {
        "locationId": GHL_LOCATION_ID,
        "firstName":  first,
        "lastName":   last,
        "phone":      phone,
        "email":      email,
        "address1":   prop_addr,
        "city":       prop_city,
        "state":      prop_state,
        "postalCode": prop_zip,
        "tags":       ["Motivated Seller", "Hillsborough Leads"],
        "source":     "TrustCircle Scraper",
    }

    # Check if contact exists
    existing_id = find_existing_contact(phone, email)

    if existing_id:
        # Update existing contact
        res = requests.put(
            f"https://services.leadconnectorhq.com/contacts/{existing_id}",
            headers=HEADERS, json=contact_payload
        )
        contact_id = existing_id
    else:
        # Create new contact
        res = requests.post(
            "https://services.leadconnectorhq.com/contacts/",
            headers=HEADERS, json=contact_payload
        )
        if not res.ok:
            return False, res.text
        contact_id = res.json().get("contact", {}).get("id")

    if not contact_id:
        return False, "No contact ID"

    # Add to workflow
    requests.post(
        f"https://services.leadconnectorhq.com/contacts/{contact_id}/workflow/{GHL_WORKFLOW_ID}",
        headers=HEADERS, json={}
    )

    return True, contact_id

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 import_xleads.py /path/to/file.csv")
        sys.exit(1)

    csv_file = sys.argv[1]
    if not os.path.exists(csv_file):
        print(f"File not found: {csv_file}")
        sys.exit(1)

    print(f"Processing: {csv_file}")
    with open(csv_file, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    print(f"Total rows: {len(rows)}")
    added = skipped = failed = 0

    for i, row in enumerate(rows):
        try:
            ok, result = push_contact(row)
            if ok:
                added += 1
                if added % 10 == 0:
                    print(f"  {added} pushed, {skipped} skipped, {failed} failed...")
            else:
                if "No mobile" in str(result):
                    skipped += 1
                else:
                    failed += 1
                    print(f"  Failed {row.get('FirstName')} {row.get('LastName')}: {str(result)[:80]}")
            time.sleep(0.3)
        except Exception as e:
            failed += 1
            print(f"  Error row {i}: {e}")

    print(f"\n✅ Done! {added} pushed to GHL workflow, {skipped} skipped (no mobile), {failed} failed")

if __name__ == "__main__":
    main()
