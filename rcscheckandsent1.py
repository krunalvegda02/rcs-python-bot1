import threading
import requests
import json
import uuid
import time
import os
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timezone
import urllib3
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import math

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Global variables
fail = 0
count = 0
token = 0
ASSISTANT_ID = 0
CLIENT_SECRET = 0
phonelist = []
retrylist = []
maincampainid = 0
payload = ""
phone_numbers = []
phone_to_message_id = {}
current_index = 0
lock = threading.Lock()
capable_numbers = []
non_capable_numbers = []
missing_numbers = []

# Database configuration
MONGODB_URI = "mongodb+srv://sikarwarvishal75_db_user:Gama%40123@cluster0.whqwih.mongodb.net/rcs?retryWrites=true&w=majority"
DATABASE_NAME = "rcs"

# Constants
MAX_API_LIMIT = 10000
MIN_BATCH_SIZE = 500
CONCURRENCY = 30


def get_utc_now():
    """Get current UTC datetime"""
    return datetime.now(timezone.utc)


def mark_campaign_running(campaign_id):
    """Open MongoDB connection, mark campaign as Running, then close connection"""
    try:
        # Open connection
        client = MongoClient(MONGODB_URI)
        db = client[DATABASE_NAME]

        # Convert campaign_id to ObjectId if it's a string
        if isinstance(campaign_id, str):
            campaign_id = ObjectId(campaign_id)

        # Mark campaign as Running
        result = db.campaigns.update_one(
            {"_id": campaign_id},
            {
                "$set": {
                    "status": "running",
                    "updatedAt": get_utc_now()
                }
            }
        )

        if result.modified_count > 0:
            print("✅ Campaign marked as 'Running'")
        else:
            print("ℹ️ Campaign already 'Running' or not updated")

        # Close connection
        client.close()
        return True

    except Exception as e:
        print(f"❌ Error marking campaign as Running: {e}")
        return False


def mark_campaign_completed(campaign_id):
    """Open MongoDB connection, mark campaign as completed, then close connection"""
    try:
        # Open connection
        client = MongoClient(MONGODB_URI)
        db = client[DATABASE_NAME]

        # Convert campaign_id to ObjectId if it's a string
        if isinstance(campaign_id, str):
            campaign_id = ObjectId(campaign_id)

        # Mark campaign as completed
        result = db.campaigns.update_one(
            {"_id": campaign_id},
            {
                "$set": {
                    "status": "completed",
                    "updatedAt": get_utc_now(),
                    "completedAt": get_utc_now()
                }
            }
        )

        if result.modified_count > 0:
            print("✅ Campaign marked as 'completed'")
        else:
            print("ℹ️ Campaign already 'completed' or not updated")

        # Close connection
        client.close()
        return True

    except Exception as e:
        print(f"❌ Error marking campaign as completed: {e}")
        return False


def update_campaign_stats(campaign_id, capable_count, non_capable_count, missing_count=0):
    """Update campaign with capability stats"""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[DATABASE_NAME]

        if isinstance(campaign_id, str):
            campaign_id = ObjectId(campaign_id)

        result = db.campaigns.update_one(
            {"_id": campaign_id},
            {
                "$set": {
                    "rcsCapableCount": capable_count,
                    "nonRcsCapableCount": non_capable_count,
                    "missingCount": missing_count,
                    "updatedAt": get_utc_now()
                }
            }
        )

        if result.modified_count > 0:
            print(
                f"📊 Campaign stats updated: {capable_count} capable, {non_capable_count} non-capable, {missing_count} missing")

        client.close()
        return True
    except Exception as e:
        print(f"❌ Error updating campaign stats: {e}")
        return False


def get_campaign_data():
    """Get campaign data from MongoDB and close connection immediately"""
    global payload

    try:
        # Open connection
        client = MongoClient(MONGODB_URI)
        db = client[DATABASE_NAME]

        # Find ONLY pending campaign with botId "bot2"
        print("🔍 Looking for pending campaign with botId 'bot2'...")
        campaign = db.campaigns.find_one({
            "botId": "bot2",
            "status": "pending"
        })

        if not campaign:
            print("❌ No pending campaign found with botId 'bot2'")
            client.close()
            return None

        campaign_id = campaign["_id"]
        campaign_name = campaign.get("name", "Unnamed")

        print(f"✅ Found pending campaign: {campaign_name}")
        print(f"   ID: {campaign_id}")

        payload = campaign["payload"]

        # Parse payload if it's a string
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
                print("   ✅ Parsed payload")
            except json.JSONDecodeError as e:
                print(f"   ❌ Failed to parse JSON: {e}")
                client.close()
                return None

        user_id = campaign.get("userId")

        # Get user's jioConfig
        user = db.users.find_one({"_id": user_id})
        if not user or "jioConfig" not in user:
            print("❌ User or jioConfig not found")
            client.close()
            return None

        jio_config = user["jioConfig"]
        client_id = jio_config.get("clientId")
        client_secret = jio_config.get("clientSecret")

        print(f"   👤 User: {user.get('name', 'Unknown')}")
        print(f"   🔑 Client ID: {client_id[:10]}...")

        # Get phone numbers and messageIds from contact_campaign_messages
        phone_numbers_list = []
        message_ids_list = []
        phone_message_map = {}

        print(f"   🔍 Fetching contacts for this campaign...")

        contacts_cursor = db.contact_campaign_messages.find({
            "campaignId": campaign_id
        })

        contacts_count = 0
        for contact in contacts_cursor:
            contacts_count += 1
            phone = contact.get("recipientPhoneNumber")
            if phone:
                phone_numbers_list.append(phone)
                message_id = contact.get("messageId")
                if message_id:
                    message_ids_list.append(message_id)
                    phone_message_map[phone] = message_id

        print(f"   📊 Found {contacts_count} contact records")

        # Remove duplicates
        unique_phones = list(set(phone_numbers_list))

        if unique_phones:
            print(f"   📱 Unique phone numbers: {len(unique_phones)}")
            print(f"   📨 Message IDs found: {len(message_ids_list)}")
        else:
            print(f"   ⚠️ No phone numbers found for this campaign")

        # Close connection immediately after fetching data
        client.close()

        return {
            "success": True,
            "campaign_id": str(campaign_id),
            "campaign_name": campaign_name,
            "client_id": client_id,
            "client_secret": client_secret,
            "phone_numbers": unique_phones,
            "message_ids": message_ids_list,
            "phone_message_map": phone_message_map,
            "total_contacts": len(unique_phones),
            "total_records": contacts_count
        }

    except Exception as e:
        print(f"❌ Error retrieving campaign data: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


def get_token(ASSISTANT_ID, CLIENT_SECRET):
    """Get authentication token from Jio API"""
    url = "https://tgs.businessmessaging.jio.com/v1/oauth/token"
    params = {
        "grant_type": "client_credentials",
        "client_id": ASSISTANT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "read"
    }

    print(f"  🔑 Getting authentication token...")
    response = requests.get(url, params=params, verify=False, timeout=10)
    if response.status_code == 200:
        token = response.json()["access_token"]
        print(f"  ✅ Token generated")
        return token
    else:
        print(f"  ❌ Failed to get token: {response.status_code}")
        print(f"  Response: {response.text}")
        return None


def format_phone_number(phone):
    """Format phone number to E.164 format"""
    # Remove all non-digit characters
    digits = ''.join(filter(str.isdigit, str(phone)))

    if len(digits) == 10:
        return f"+91{digits}"
    elif digits.startswith('91') and len(digits) == 12:
        return f"+{digits}"
    elif digits.startswith('91') and len(digits) == 11:
        return f"+{digits}"
    else:
        return f"+91{digits[-10:]}"  # Take last 10 digits


def create_smart_batches(phone_numbers, min_batch_size=500, max_batch_size=10000):
    """Create smart batches ensuring all chunks are between min_batch_size and max_batch_size"""
    total_numbers = len(phone_numbers)

    if total_numbers <= max_batch_size:
        return [phone_numbers]

    # Calculate number of batches needed
    num_batches = math.ceil(total_numbers / max_batch_size)

    # Calculate batch size to ensure all batches are at least min_batch_size
    if num_batches > 1 and total_numbers / num_batches < min_batch_size:
        num_batches = total_numbers // min_batch_size
        if total_numbers % min_batch_size > 0:
            num_batches += 1

    batches = []
    batch_size = total_numbers // num_batches
    remainder = total_numbers % num_batches

    start = 0
    for i in range(num_batches):
        current_batch_size = batch_size + (1 if i < remainder else 0)
        end = start + current_batch_size
        batches.append(phone_numbers[start:end])
        start = end

    return batches


def check_single_user_capability(phone_number, request_id):
    """Check RCS capability for a single user - GET /v1/messaging/users/:userPhoneNumber/capabilities"""
    global token

    formatted_phone = format_phone_number(phone_number)
    url = f"https://api.businessmessaging.jio.com/v1/messaging/users/{formatted_phone}/capabilities"
    params = {
        "requestId": request_id
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, params=params, verify=False, timeout=5)

        if response.status_code == 200:
            # Success means user is reachable through JBM
            return {"phone": phone_number, "capable": True}
        elif response.status_code == 404:
            # 404 means "User can't be reached through JBM"
            return {"phone": phone_number, "capable": False}
        else:
            # Any other status code, treat as non-capable
            return {"phone": phone_number, "capable": False}
    except Exception as e:
        return {"phone": phone_number, "capable": False}


def check_batch_capability(phone_numbers_batch, batch_index, total_batches):
    """Check RCS capability for a batch of users - POST /v1/messaging/usersBatchGet"""
    global token

    url = "https://api.businessmessaging.jio.com/v1/messaging/usersBatchGet"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Format phone numbers to E.164 format
    formatted_numbers = [format_phone_number(num) for num in phone_numbers_batch]

    data = {
        "phoneNumbers": formatted_numbers
    }

    try:
        start_time = time.time()
        response = requests.post(url, headers=headers, json=data, verify=False, timeout=30)
        batch_time = time.time() - start_time

        if response.status_code == 200:
            result = response.json()
            reachable_users = result.get("reachableUsers", [])

            # Extract phone numbers and remove +91 prefix for consistency
            reachable_phones = []
            for phone in reachable_users:
                if phone.startswith("+91"):
                    reachable_phones.append(phone[3:])  # Remove +91 prefix
                else:
                    reachable_phones.append(phone)

            print(
                f"   ✅ Batch {batch_index}/{total_batches}: {len(reachable_phones)}/{len(phone_numbers_batch)} reachable ({batch_time:.1f}s)")

            return {
                "reachable_phones": reachable_phones,
                "batch_phones": phone_numbers_batch,  # Store original batch phones
                "total_random_sample": result.get("totalRandomSampleUserCount", 0),
                "reachable_random_sample": result.get("reachableRandomSampleUserCount", 0),
                "success": True
            }
        else:
            print(f"   ❌ Batch {batch_index}/{total_batches} failed: HTTP {response.status_code} ({batch_time:.1f}s)")
            return {
                "reachable_phones": [],
                "batch_phones": phone_numbers_batch,
                "error": f"HTTP {response.status_code}",
                "success": False
            }
    except Exception as e:
        print(f"   ❌ Batch {batch_index}/{total_batches} exception: {str(e)}")
        return {
            "reachable_phones": [],
            "batch_phones": phone_numbers_batch,
            "error": str(e),
            "success": False
        }


def print_all_non_capable_numbers(non_capable_numbers):
    """Print all non-capable numbers in a readable format"""
    print(f"\n📋 ALL NON-RCS CAPABLE NUMBERS ({len(non_capable_numbers)} total):")
    print("=" * 80)

    # Sort numbers for easier reading
    non_capable_sorted = sorted(non_capable_numbers)

    # Print in columns for better readability
    cols = 5
    for i in range(0, len(non_capable_sorted), cols):
        row = non_capable_sorted[i:i + cols]
        row_str = "  ".join(f"{num:>12}" for num in row)
        # print(row_str)

    print("=" * 80)

    # Also save to a file for reference
    # try:
    #     with open(f"non_capable_numbers_{int(time.time())}.txt", "w") as f:
    #         for num in non_capable_sorted:
    #             f.write(f"{num}\n")
    #     print(f"📄 Non-capable numbers saved to: non_capable_numbers_{int(time.time())}.txt")
    # except Exception as e:
    #     print(f"⚠️ Could not save to file: {e}")


def check_rcs_capabilities(phone_numbers):
    """Check RCS capabilities for all phone numbers using smart batching"""
    global token, capable_numbers, non_capable_numbers, missing_numbers

    print(f"\n🔍 CHECKING RCS CAPABILITIES")
    print(f"   📱 Total numbers to check: {len(phone_numbers)}")

    capable_numbers = []
    non_capable_numbers = []
    missing_numbers = []
    all_processed_numbers = set()

    if len(phone_numbers) < 500:
        # Single user capability check for <500 numbers
        print("   ⚡ Using single user capability check (less than 500 numbers)")

        request_id = str(uuid.uuid4())
        results = []

        # Use ThreadPoolExecutor for parallel checking
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            for phone in phone_numbers:
                future = executor.submit(check_single_user_capability, phone, request_id)
                futures.append(future)

            for i, future in enumerate(as_completed(futures)):
                results.append(future.result())
                if (i + 1) % 100 == 0:
                    print(f"   📊 Processed {i + 1}/{len(phone_numbers)} numbers...")

        # Process results
        for result in results:
            phone_str = str(result["phone"])
            all_processed_numbers.add(phone_str)
            if result["capable"]:
                capable_numbers.append(result["phone"])
            else:
                non_capable_numbers.append(result["phone"])

    else:
        # Batch capability check for ≥500 numbers
        print("   ⚡ Using batch capability check (500 or more numbers)")

        # Create smart batches
        batches = create_smart_batches(phone_numbers, MIN_BATCH_SIZE, MAX_API_LIMIT)
        print(f"   📦 Created {len(batches)} batches: {[len(b) for b in batches]}")

        batch_results = []

        # Process batches with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
            futures = []
            for i, batch in enumerate(batches):
                future = executor.submit(check_batch_capability, batch, i + 1, len(batches))
                futures.append(future)

            for future in as_completed(futures):
                batch_results.append(future.result())

        # Process all batch results
        successful_batches = 0
        failed_batches = 0

        for result in batch_results:
            batch_phones = result.get("batch_phones", [])
            batch_phones_set = set([str(p) for p in batch_phones])
            all_processed_numbers.update(batch_phones_set)

            if result.get("success"):
                successful_batches += 1
                reachable_set = set(result["reachable_phones"])

                # Add capable numbers
                capable_numbers.extend(list(reachable_set))

                # Calculate non-capable numbers for this batch
                non_capable_batch = list(batch_phones_set - reachable_set)
                non_capable_numbers.extend(non_capable_batch)

                if result.get("total_random_sample", 0) > 0:
                    reach_rate = (result.get("reachable_random_sample", 0) / result.get("total_random_sample", 1)) * 100
                    print(f"   📈 Batch estimated reach rate: {reach_rate:.1f}%")
            else:
                failed_batches += 1
                print(f"   ⚠️ Batch failed: {result.get('error', 'Unknown error')}")
                # Consider all in failed batch as non-capable
                non_capable_numbers.extend(list(batch_phones_set))

        print(f"   📊 Batch summary: {successful_batches} successful, {failed_batches} failed")

        # If we have failed batches, fall back to single checks for those numbers
        if failed_batches > 0:
            print(f"   🔄 Falling back to single checks for failed batches...")

            # Collect all numbers that need single checking
            numbers_to_check_single = []
            for result in batch_results:
                if not result.get("success"):
                    numbers_to_check_single.extend(result.get("batch_phones", []))

            if numbers_to_check_single:
                request_id = str(uuid.uuid4())
                single_results = []

                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = []
                    for phone in numbers_to_check_single:
                        future = executor.submit(check_single_user_capability, phone, request_id)
                        futures.append(future)

                    for future in as_completed(futures):
                        single_results.append(future.result())

                # Remove these numbers from non_capable (they were added as non-capable when batch failed)
                numbers_to_remove = set([str(p) for p in numbers_to_check_single])
                non_capable_numbers = [p for p in non_capable_numbers if str(p) not in numbers_to_remove]

                # Process single check results
                for res in single_results:
                    phone_str = str(res["phone"])
                    all_processed_numbers.add(phone_str)
                    if res["capable"]:
                        capable_numbers.append(res["phone"])
                    else:
                        non_capable_numbers.append(res["phone"])

    # Remove duplicates
    capable_numbers = list(set(capable_numbers))
    non_capable_numbers = list(set(non_capable_numbers))

    # Calculate missing numbers
    all_input_set = set([str(p) for p in phone_numbers])
    processed_set = all_processed_numbers
    missing_set = all_input_set - processed_set
    missing_numbers = list(missing_set)

    total_processed = len(processed_set)
    missing_count = len(missing_numbers)

    print(f"\n📊 CAPABILITY CHECK RESULTS")
    print(f"   📱 Total input numbers: {len(phone_numbers)}")
    print(f"   🔄 Processed numbers: {total_processed}")
    print(f"   ⚠️  Missing numbers: {missing_count}")
    print(f"   ✅ RCS Capable: {len(capable_numbers)}")
    print(f"   ❌ Non-RCS Capable: {len(non_capable_numbers)}")

    # Verify the math
    total_accounted = len(capable_numbers) + len(non_capable_numbers) + missing_count
    if total_accounted != len(phone_numbers):
        print(
            f"   ⚠️  Math discrepancy: {len(capable_numbers)} + {len(non_capable_numbers)} + {missing_count} = {total_accounted}, expected {len(phone_numbers)}")

    if missing_count > 0:
        print(f"   📱 Sample missing numbers: {missing_numbers[:5] if missing_numbers else 'None'}")

    if capable_numbers:
        print(f"   📱 Sample capable numbers: {capable_numbers[:5]}")

    # Print all non-capable numbers if there are any
    if non_capable_numbers and len(non_capable_numbers) > 0:
        print(f"   📱 Sample non-capable numbers: {non_capable_numbers[:5]}")
        # Print all non-capable numbers
        print_all_non_capable_numbers(non_capable_numbers)

    return capable_numbers, non_capable_numbers, missing_numbers


def send_message1(phone_number, message_id=None):
    """Send a single message - helper function"""
    global token, ASSISTANT_ID, payload

    try:
        if not message_id:
            message_id = f"msg-{uuid.uuid4().hex[:8]}"

        url = f"https://api.businessmessaging.jio.com/v1/messaging/users/+91{phone_number}/assistantMessages/async"
        url += f"?messageId={message_id}&assistantId={ASSISTANT_ID}"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        data = {
            "messageTrafficType": "PROMOTION",
            "content": payload['content']
        }

        # Fast request with timeout
        response = requests.post(url, headers=headers, json=data, verify=False, timeout=5)
        return response.status_code

    except Exception:
        return 500  # Return error code for any exception


def send_message(phone_number, message_id=None):
    """Main message sending function with FAST retry mechanism"""
    global count, phonelist, fail, retrylist

    try:
        if not message_id:
            message_id = f"msg-{uuid.uuid4().hex[:8]}"

        url = f"https://api.businessmessaging.jio.com/v1/messaging/users/+91{phone_number}/assistantMessages/async"
        url += f"?messageId={message_id}&assistantId={ASSISTANT_ID}"

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        data = {
            "messageTrafficType": "PROMOTION",
            "content": payload['content']
        }

        # First attempt
        try:
            response = requests.post(url, headers=headers, json=data, verify=False, timeout=5)
        except:
            response = type('obj', (object,), {'status_code': 500})()

        if response.status_code == 201:
            count += 1
            phonelist.append(phone_number)
            return

        # FAST RETRY LOGIC: Immediate 200 retries
        retrylist.append(phone_number)

        for retry_count in range(100):
            x = send_message1(phone_number, message_id)
            if x == 201:
                count += 1
                phonelist.append(phone_number)
                break
            if retry_count == 99:
                fail += 1

    except Exception:
        # FAST RETRY on exception
        retrylist.append(phone_number)

        for retry_count in range(100):
            x = send_message1(phone_number, message_id)
            if x == 201:
                count += 1
                phonelist.append(phone_number)
                break
            if retry_count == 99:
                fail += 1


def worker_thread():
    """Worker function for message sending (only for capable numbers)"""
    global capable_numbers, phone_to_message_id, current_index, lock

    while True:
        with lock:
            if current_index >= len(capable_numbers):
                break
            number = capable_numbers[current_index]
            current_index += 1

        # Get the messageId for this phone number
        message_id = phone_to_message_id.get(number)

        # Send message with FAST retry logic
        send_message(number, message_id)


def main():
    """Main function to orchestrate the campaign sending"""
    global phone_numbers, token, ASSISTANT_ID, CLIENT_SECRET, phone_to_message_id, maincampainid
    global current_index, payload, count, fail, phonelist, retrylist
    global capable_numbers, non_capable_numbers, missing_numbers

    print("=" * 60)
    print("🚀 STARTING CAMPAIGN PROCESSING")
    print("=" * 60)

    # STEP 1: Get campaign data (connection opens and closes inside function)
    data = get_campaign_data()

    if data and data.get("success"):
        print("\n" + "=" * 60)
        print("✅ CAMPAIGN LOADED")
        print("=" * 60)
        print(f"📋 Campaign: {data['campaign_name']}")
        print(f"📝 ID: {data['campaign_id']}")
        print(f"👥 Contacts: {data['total_contacts']}")

        maincampainid = data['campaign_id']
        ASSISTANT_ID = data['client_id']
        CLIENT_SECRET = data['client_secret']

        if data['total_contacts'] == 0:
            print("\n❌ No contacts to send. Marking campaign as completed.")
            # STEP 5: Mark as completed (separate connection)
            mark_campaign_completed(maincampainid)
            return

        # STEP 2: Mark campaign as Running (separate connection)
        print("\n🔄 Marking campaign as 'Running'...")
        if not mark_campaign_running(maincampainid):
            print("❌ Failed to mark campaign as Running. Aborting.")
            return

        # Get authentication token
        print(f"\n🔐 Authenticating...")
        token = get_token(ASSISTANT_ID, CLIENT_SECRET)
        if not token:
            print("❌ Authentication failed. Aborting campaign.")
            return

        # Store phone numbers and messageId mapping
        phone_numbers = data['phone_numbers']
        phone_to_message_id = data.get('phone_message_map', {})

        # STEP 3: Check RCS capabilities
        capable_numbers, non_capable_numbers, missing_numbers = check_rcs_capabilities(phone_numbers)

        # Update campaign with capability stats
        update_campaign_stats(maincampainid, len(capable_numbers), len(non_capable_numbers), len(missing_numbers))

        if len(capable_numbers) == 0:
            print("\n❌ No RCS capable contacts found. Marking campaign as completed.")
            mark_campaign_completed(maincampainid)
            return {
                "campaign_id": maincampainid,
                "campaign_name": data['campaign_name'],
                "sent": 0,
                "failed": 0,
                "total": len(phone_numbers),
                "capable": 0,
                "non_capable": len(non_capable_numbers),
                "missing": len(missing_numbers),
                "retried": 0,
                "processing_time": 0
            }

        # Reset counters
        current_index = 0
        count = 0
        fail = 0
        phonelist.clear()
        retrylist.clear()

        print(f"\n🎯 STARTING MESSAGE SENDING")
        print(f"📤 RCS capable contacts to process: {len(capable_numbers)}")

        # Use 100 workers as requested
        num_threads = 100
        print(f"🧵 Using {num_threads} workers")
        print("-" * 40)

        # Create and start threads
        threads = []
        start_time = time.time()

        for i in range(num_threads):
            t = threading.Thread(target=worker_thread, name=f"Worker-{i + 1}")
            t.start()
            threads.append(t)

        # Wait for all threads to complete
        print(f"\n⏳ Processing {len(capable_numbers)} capable contacts...")
        for t in threads:
            t.join()

        processing_time = time.time() - start_time

        print("\n" + "=" * 60)
        print("✅ ALL MESSAGES SENT")
        print("=" * 60)
        print(f"📊 Results: {count} sent, {fail} failed, {len(retrylist)} retried")
        print(
            f"📱 Capability: {len(capable_numbers)} capable, {len(non_capable_numbers)} non-capable, {len(missing_numbers)} missing")
        print(f"⏱️  Time: {processing_time:.2f} seconds")

        # STEP 4: Mark campaign as completed (separate connection)
        print("\n🔄 Marking campaign as 'completed'...")
        mark_campaign_completed(maincampainid)

        return {
            "campaign_id": maincampainid,
            "campaign_name": data['campaign_name'],
            "sent": count,
            "failed": fail,
            "total": len(phone_numbers),
            "capable": len(capable_numbers),
            "non_capable": len(non_capable_numbers),
            "missing": len(missing_numbers),
            "retried": len(retrylist),
            "processing_time": processing_time
        }

    else:
        print("❌ No pending campaign available for processing")
        return None


if __name__ == "__main__":
    while True:
        print("\n" + "=" * 60)
        print("🔄 CHECKING FOR NEW PENDING CAMPAIGNS")
        print("=" * 60)

        result = main()

        if result:
            print(f"\n📊 FINAL RESULTS:")
            print(f"   Campaign: {result['campaign_name']}")
            print(f"   📱 Total Contacts: {result['total']}")
            print(f"   ✅ RCS Capable: {result['capable']}")
            print(f"   ❌ Non-RCS Capable: {result['non_capable']}")
            print(f"   ⚠️  Missing: {result['missing']}")
            print(f"   📤 Messages Sent: {result['sent']}")
            print(f"   ❌ Failed: {result['failed']}")
            print(f"   🔄 Retried: {result['retried']}")
            print(f"   📈 RCS Reach Rate: {(result['capable'] / result['total'] * 100):.1f}%" if result[
                                                                                                    'total'] > 0 else "   📈 RCS Reach Rate: N/A")
            print(f"   📈 Send Success Rate: {(result['sent'] / result['capable'] * 100):.1f}%" if result[
                                                                                                      'capable'] > 0 else "   📈 Send Success Rate: N/A")
            print(f"   ⏱️  Total Time: {result['processing_time']:.2f} seconds")
            print(f"   🚀 Send Rate: {result['sent'] / result['processing_time']:.2f} messages/sec" if result[
                                                                                                          'processing_time'] > 0 else "   🚀 Send Rate: N/A")

            # Verify total
            total_accounted = result['capable'] + result['non_capable'] + result['missing']
            if total_accounted != result['total']:
                print(
                    f"   ⚠️  Math check: {result['capable']} + {result['non_capable']} + {result['missing']} = {total_accounted}, expected {result['total']}")

        print(f"\n🔄 Next check in 30 seconds...")
        print("=" * 60)
        # time.sleep(30)