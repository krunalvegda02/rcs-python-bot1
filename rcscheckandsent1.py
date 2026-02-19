# import threading
# import requests
# import json
# import uuid
# import time
# import os
# from pymongo import MongoClient
# from bson import ObjectId
# from datetime import datetime, timezone
# import urllib3
# import warnings
# from concurrent.futures import ThreadPoolExecutor, as_completed
# import math

# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# # Global variables
# fail = 0
# count = 0
# token = 0
# token_generated_time = 0
# ASSISTANT_ID = 0
# CLIENT_SECRET = 0
# phonelist = []
# retrylist = []
# maincampainid = 0
# payload = ""
# phone_numbers = []
# phone_to_message_id = {}
# current_index = 0
# lock = threading.Lock()
# capable_numbers = []
# non_capable_numbers = []
# missing_numbers = []
# should_refresh_token = False
# token_refresh_lock = threading.Lock()

# # Database configuration
# MONGODB_URI = "mongodb+srv://sikarwarvishal75_db_user:Gama%40123@cluster0.whqwih.mongodb.net/rcs?retryWrites=true&w=majority"
# DATABASE_NAME = "rcs"

# # Constants
# MAX_API_LIMIT = 10000
# MIN_BATCH_SIZE = 500
# CONCURRENCY = 30
# TOKEN_REFRESH_INTERVAL = 2400  # Refresh token every 40 minutes (2400 seconds)
# TOKEN_EXPIRY_TIME = 3600  # Token expires in 1 hour (3600 seconds)


# def get_utc_now():
#     """Get current UTC datetime"""
#     return datetime.now(timezone.utc)


# def mark_campaign_running(campaign_id):
#     """Open MongoDB connection, mark campaign as Running, then close connection"""
#     try:
#         # Open connection
#         client = MongoClient(MONGODB_URI)
#         db = client[DATABASE_NAME]

#         # Convert campaign_id to ObjectId if it's a string
#         if isinstance(campaign_id, str):
#             campaign_id = ObjectId(campaign_id)

#         # Mark campaign as Running
#         result = db.campaigns.update_one(
#             {"_id": campaign_id},
#             {
#                 "$set": {
#                     "status": "running",
#                     "updatedAt": get_utc_now()
#                 }
#             }
#         )

#         if result.modified_count > 0:
#             print("✅ Campaign marked as 'Running'")
#         else:
#             print("ℹ️ Campaign already 'Running' or not updated")

#         # Close connection
#         client.close()
#         return True

#     except Exception as e:
#         print(f"❌ Error marking campaign as Running: {e}")
#         return False


# def mark_campaign_completed(campaign_id):
#     """Open MongoDB connection, mark campaign as completed, then close connection"""
#     try:
#         # Open connection
#         client = MongoClient(MONGODB_URI)
#         db = client[DATABASE_NAME]

#         # Convert campaign_id to ObjectId if it's a string
#         if isinstance(campaign_id, str):
#             campaign_id = ObjectId(campaign_id)

#         # Mark campaign as completed
#         result = db.campaigns.update_one(
#             {"_id": campaign_id},
#             {
#                 "$set": {
#                     "status": "completed",
#                     "updatedAt": get_utc_now(),
#                     "completedAt": get_utc_now()
#                 }
#             }
#         )

#         if result.modified_count > 0:
#             print("✅ Campaign marked as 'completed'")
#         else:
#             print("ℹ️ Campaign already 'completed' or not updated")

#         # Close connection
#         client.close()
#         return True

#     except Exception as e:
#         print(f"❌ Error marking campaign as completed: {e}")
#         return False


# def update_campaign_stats(campaign_id, capable_count, non_capable_count, missing_count=0):
#     """Update campaign with capability stats"""
#     try:
#         client = MongoClient(MONGODB_URI)
#         db = client[DATABASE_NAME]

#         if isinstance(campaign_id, str):
#             campaign_id = ObjectId(campaign_id)

#         result = db.campaigns.update_one(
#             {"_id": campaign_id},
#             {
#                 "$set": {
#                     "rcsCapableCount": capable_count,
#                     "nonRcsCapableCount": non_capable_count,
#                     "missingCount": missing_count,
#                     "updatedAt": get_utc_now()
#                 }
#             }
#         )

#         if result.modified_count > 0:
#             print(
#                 f"📊 Campaign stats updated: {capable_count} capable, {non_capable_count} non-capable, {missing_count} missing")

#         client.close()
#         return True
#     except Exception as e:
#         print(f"❌ Error updating campaign stats: {e}")
#         return False


# def get_campaign_data():
#     """Get campaign data from MongoDB and close connection immediately"""
#     global payload

#     try:
#         # Open connection
#         client = MongoClient(MONGODB_URI)
#         db = client[DATABASE_NAME]

#         # Find ONLY pending campaign with botId "bot2"
#         print("🔍 Looking for pending campaign with botId 'bot2'...")
#         campaign = db.campaigns.find_one({
#             "botId": "bot2",
#             "status": "pending"
#         })

#         if not campaign:
#             print("❌ No pending campaign found with botId 'bot2'")
#             client.close()
#             return None

#         campaign_id = campaign["_id"]
#         campaign_name = campaign.get("name", "Unnamed")

#         print(f"✅ Found pending campaign: {campaign_name}")
#         print(f"   ID: {campaign_id}")

#         payload = campaign["payload"]

#         # Parse payload if it's a string
#         if isinstance(payload, str):
#             try:
#                 payload = json.loads(payload)
#                 print("   ✅ Parsed payload")
#             except json.JSONDecodeError as e:
#                 print(f"   ❌ Failed to parse JSON: {e}")
#                 client.close()
#                 return None

#         user_id = campaign.get("userId")

#         # Get user's jioConfig
#         user = db.users.find_one({"_id": user_id})
#         if not user or "jioConfig" not in user:
#             print("❌ User or jioConfig not found")
#             client.close()
#             return None

#         jio_config = user["jioConfig"]
#         client_id = jio_config.get("clientId")
#         client_secret = jio_config.get("clientSecret")

#         print(f"   👤 User: {user.get('name', 'Unknown')}")
#         print(f"   🔑 Client ID: {client_id[:10]}...")

#         # Get phone numbers and messageIds from contact_campaign_messages
#         phone_numbers_list = []
#         message_ids_list = []
#         phone_message_map = {}

#         print(f"   🔍 Fetching contacts for this campaign...")

#         contacts_cursor = db.contact_campaign_messages.find({
#             "campaignId": campaign_id
#         })

#         contacts_count = 0
#         for contact in contacts_cursor:
#             contacts_count += 1
#             phone = contact.get("recipientPhoneNumber")
#             if phone:
#                 phone_numbers_list.append(phone)
#                 message_id = contact.get("messageId")
#                 if message_id:
#                     message_ids_list.append(message_id)
#                     phone_message_map[phone] = message_id

#         print(f"   📊 Found {contacts_count} contact records")

#         # Remove duplicates
#         unique_phones = list(set(phone_numbers_list))

#         if unique_phones:
#             print(f"   📱 Unique phone numbers: {len(unique_phones)}")
#             print(f"   📨 Message IDs found: {len(message_ids_list)}")
#         else:
#             print(f"   ⚠️ No phone numbers found for this campaign")

#         # Close connection immediately after fetching data
#         client.close()

#         return {
#             "success": True,
#             "campaign_id": str(campaign_id),
#             "campaign_name": campaign_name,
#             "client_id": client_id,
#             "client_secret": client_secret,
#             "phone_numbers": unique_phones,
#             "message_ids": message_ids_list,
#             "phone_message_map": phone_message_map,
#             "total_contacts": len(unique_phones),
#             "total_records": contacts_count
#         }

#     except Exception as e:
#         print(f"❌ Error retrieving campaign data: {e}")
#         import traceback
#         traceback.print_exc()
#         return {"success": False, "error": str(e)}


# def get_token(ASSISTANT_ID, CLIENT_SECRET):
#     """Get authentication token from Jio API"""
#     url = "https://tgs.businessmessaging.jio.com/v1/oauth/token"
#     params = {
#         "grant_type": "client_credentials",
#         "client_id": ASSISTANT_ID,
#         "client_secret": CLIENT_SECRET,
#         "scope": "read"
#     }

#     print(f"  🔑 Getting authentication token...")
#     try:
#         response = requests.get(url, params=params, verify=False, timeout=10)
#         if response.status_code == 200:
#             token = response.json()["access_token"]
#             print(f"  ✅ Token generated")
#             return token
#         else:
#             print(f"  ❌ Failed to get token: {response.status_code}")
#             print(f"  Response: {response.text}")
#             return None
#     except Exception as e:
#         print(f"  ❌ Exception getting token: {e}")
#         return None


# def refresh_token_if_needed():
#     """Check if token needs refresh and refresh if necessary"""
#     global token, token_generated_time, ASSISTANT_ID, CLIENT_SECRET, should_refresh_token

#     with token_refresh_lock:
#         current_time = time.time()

#         # Check if token needs refresh (every 40 minutes)
#         if token and (current_time - token_generated_time) >= TOKEN_REFRESH_INTERVAL:
#             print(f"🔄 Token expired or about to expire. Refreshing...")
#             new_token = get_token(ASSISTANT_ID, CLIENT_SECRET)
#             if new_token:
#                 token = new_token
#                 token_generated_time = current_time
#                 print(f"✅ Token refreshed successfully")
#                 should_refresh_token = False
#                 return True
#             else:
#                 print(f"❌ Failed to refresh token")
#                 return False

#         # Also check if token is None (first time)
#         if not token:
#             new_token = get_token(ASSISTANT_ID, CLIENT_SECRET)
#             if new_token:
#                 token = new_token
#                 token_generated_time = current_time
#                 return True
#             else:
#                 return False

#     return True


# def format_phone_number(phone):
#     """Format phone number to E.164 format"""
#     # Remove all non-digit characters
#     digits = ''.join(filter(str.isdigit, str(phone)))

#     if len(digits) == 10:
#         return f"+91{digits}"
#     elif digits.startswith('91') and len(digits) == 12:
#         return f"+{digits}"
#     elif digits.startswith('91') and len(digits) == 11:
#         return f"+{digits}"
#     else:
#         return f"+91{digits[-10:]}"  # Take last 10 digits


# def create_smart_batches(phone_numbers, min_batch_size=500, max_batch_size=10000):
#     """Create smart batches ensuring all chunks are between min_batch_size and max_batch_size"""
#     total_numbers = len(phone_numbers)

#     if total_numbers <= max_batch_size:
#         return [phone_numbers]

#     # Calculate number of batches needed
#     num_batches = math.ceil(total_numbers / max_batch_size)

#     # Calculate batch size to ensure all batches are at least min_batch_size
#     if num_batches > 1 and total_numbers / num_batches < min_batch_size:
#         num_batches = total_numbers // min_batch_size
#         if total_numbers % min_batch_size > 0:
#             num_batches += 1

#     batches = []
#     batch_size = total_numbers // num_batches
#     remainder = total_numbers % num_batches

#     start = 0
#     for i in range(num_batches):
#         current_batch_size = batch_size + (1 if i < remainder else 0)
#         end = start + current_batch_size
#         batches.append(phone_numbers[start:end])
#         start = end

#     return batches


# def check_single_user_capability(phone_number, request_id):
#     """Check RCS capability for a single user - GET /v1/messaging/users/:userPhoneNumber/capabilities"""
#     global token

#     # Check and refresh token if needed
#     if not refresh_token_if_needed():
#         return {"phone": phone_number, "capable": False, "error": "Token refresh failed"}

#     formatted_phone = format_phone_number(phone_number)
#     url = f"https://api.businessmessaging.jio.com/v1/messaging/users/{formatted_phone}/capabilities"
#     params = {
#         "requestId": request_id
#     }

#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Content-Type": "application/json"
#     }

#     try:
#         response = requests.get(url, headers=headers, params=params, verify=False, timeout=5)

#         if response.status_code == 200:
#             # Success means user is reachable through JBM
#             return {"phone": phone_number, "capable": True}
#         elif response.status_code == 404:
#             # 404 means "User can't be reached through JBM"
#             return {"phone": phone_number, "capable": False}
#         else:
#             # Any other status code, treat as non-capable
#             return {"phone": phone_number, "capable": False}
#     except Exception as e:
#         return {"phone": phone_number, "capable": False}


# def check_batch_capability(phone_numbers_batch, batch_index, total_batches):
#     """Check RCS capability for a batch of users - POST /v1/messaging/usersBatchGet"""
#     global token

#     # Check and refresh token if needed
#     if not refresh_token_if_needed():
#         return {
#             "reachable_phones": [],
#             "batch_phones": phone_numbers_batch,
#             "error": "Token refresh failed",
#             "success": False
#         }

#     url = "https://api.businessmessaging.jio.com/v1/messaging/usersBatchGet"

#     headers = {
#         "Authorization": f"Bearer {token}",
#         "Content-Type": "application/json"
#     }

#     # Format phone numbers to E.164 format
#     formatted_numbers = [format_phone_number(num) for num in phone_numbers_batch]

#     data = {
#         "phoneNumbers": formatted_numbers
#     }

#     try:
#         start_time = time.time()
#         response = requests.post(url, headers=headers, json=data, verify=False, timeout=30)
#         batch_time = time.time() - start_time

#         if response.status_code == 200:
#             result = response.json()
#             reachable_users = result.get("reachableUsers", [])

#             # Extract phone numbers and remove +91 prefix for consistency
#             reachable_phones = []
#             for phone in reachable_users:
#                 if phone.startswith("+91"):
#                     reachable_phones.append(phone[3:])  # Remove +91 prefix
#                 else:
#                     reachable_phones.append(phone)

#             print(
#                 f"   ✅ Batch {batch_index}/{total_batches}: {len(reachable_phones)}/{len(phone_numbers_batch)} reachable ({batch_time:.1f}s)")

#             return {
#                 "reachable_phones": reachable_phones,
#                 "batch_phones": phone_numbers_batch,  # Store original batch phones
#                 "total_random_sample": result.get("totalRandomSampleUserCount", 0),
#                 "reachable_random_sample": result.get("reachableRandomSampleUserCount", 0),
#                 "success": True
#             }
#         else:
#             print(f"   ❌ Batch {batch_index}/{total_batches} failed: HTTP {response.status_code} ({batch_time:.1f}s)")
#             return {
#                 "reachable_phones": [],
#                 "batch_phones": phone_numbers_batch,
#                 "error": f"HTTP {response.status_code}",
#                 "success": False
#             }
#     except Exception as e:
#         print(f"   ❌ Batch {batch_index}/{total_batches} exception: {str(e)}")
#         return {
#             "reachable_phones": [],
#             "batch_phones": phone_numbers_batch,
#             "error": str(e),
#             "success": False
#         }


# def print_all_non_capable_numbers(non_capable_numbers):
#     """Print all non-capable numbers in a readable format"""
#     print(f"\n📋 ALL NON-RCS CAPABLE NUMBERS ({len(non_capable_numbers)} total):")
#     print("=" * 80)

#     # Sort numbers for easier reading
#     non_capable_sorted = sorted(non_capable_numbers)

#     # Print in columns for better readability
#     cols = 5
#     for i in range(0, len(non_capable_sorted), cols):
#         row = non_capable_sorted[i:i + cols]
#         row_str = "  ".join(f"{num:>12}" for num in row)
#         # print(row_str)

#     print("=" * 80)

#     # Also save to a file for reference
#     # try:
#     #     with open(f"non_capable_numbers_{int(time.time())}.txt", "w") as f:
#     #         for num in non_capable_sorted:
#     #             f.write(f"{num}\n")
#     #     print(f"📄 Non-capable numbers saved to: non_capable_numbers_{int(time.time())}.txt")
#     # except Exception as e:
#     #     print(f"⚠️ Could not save to file: {e}")


# def check_rcs_capabilities(phone_numbers):
#     """Check RCS capabilities for all phone numbers using smart batching"""
#     global token, capable_numbers, non_capable_numbers, missing_numbers

#     print(f"\n🔍 CHECKING RCS CAPABILITIES")
#     print(f"   📱 Total numbers to check: {len(phone_numbers)}")

#     capable_numbers = []
#     non_capable_numbers = []
#     missing_numbers = []
#     all_processed_numbers = set()

#     if len(phone_numbers) < 500:
#         # Single user capability check for <500 numbers
#         print("   ⚡ Using single user capability check (less than 500 numbers)")

#         request_id = str(uuid.uuid4())
#         results = []

#         # Use ThreadPoolExecutor for parallel checking
#         with ThreadPoolExecutor(max_workers=20) as executor:
#             futures = []
#             for phone in phone_numbers:
#                 future = executor.submit(check_single_user_capability, phone, request_id)
#                 futures.append(future)

#             for i, future in enumerate(as_completed(futures)):
#                 results.append(future.result())
#                 if (i + 1) % 100 == 0:
#                     print(f"   📊 Processed {i + 1}/{len(phone_numbers)} numbers...")

#         # Process results
#         for result in results:
#             phone_str = str(result["phone"])
#             all_processed_numbers.add(phone_str)
#             if result["capable"]:
#                 capable_numbers.append(result["phone"])
#             else:
#                 non_capable_numbers.append(result["phone"])

#     else:
#         # Batch capability check for ≥500 numbers
#         print("   ⚡ Using batch capability check (500 or more numbers)")

#         # Create smart batches
#         batches = create_smart_batches(phone_numbers, MIN_BATCH_SIZE, MAX_API_LIMIT)
#         print(f"   📦 Created {len(batches)} batches: {[len(b) for b in batches]}")

#         batch_results = []

#         # Process batches with ThreadPoolExecutor
#         with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
#             futures = []
#             for i, batch in enumerate(batches):
#                 future = executor.submit(check_batch_capability, batch, i + 1, len(batches))
#                 futures.append(future)

#             for future in as_completed(futures):
#                 batch_results.append(future.result())

#         # Process all batch results
#         successful_batches = 0
#         failed_batches = 0

#         for result in batch_results:
#             batch_phones = result.get("batch_phones", [])
#             batch_phones_set = set([str(p) for p in batch_phones])
#             all_processed_numbers.update(batch_phones_set)

#             if result.get("success"):
#                 successful_batches += 1
#                 reachable_set = set(result["reachable_phones"])

#                 # Add capable numbers
#                 capable_numbers.extend(list(reachable_set))

#                 # Calculate non-capable numbers for this batch
#                 non_capable_batch = list(batch_phones_set - reachable_set)
#                 non_capable_numbers.extend(non_capable_batch)

#                 if result.get("total_random_sample", 0) > 0:
#                     reach_rate = (result.get("reachable_random_sample", 0) / result.get("total_random_sample", 1)) * 100
#                     print(f"   📈 Batch estimated reach rate: {reach_rate:.1f}%")
#             else:
#                 failed_batches += 1
#                 print(f"   ⚠️ Batch failed: {result.get('error', 'Unknown error')}")
#                 # Consider all in failed batch as non-capable
#                 non_capable_numbers.extend(list(batch_phones_set))

#         print(f"   📊 Batch summary: {successful_batches} successful, {failed_batches} failed")

#         # If we have failed batches, fall back to single checks for those numbers
#         if failed_batches > 0:
#             print(f"   🔄 Falling back to single checks for failed batches...")

#             # Collect all numbers that need single checking
#             numbers_to_check_single = []
#             for result in batch_results:
#                 if not result.get("success"):
#                     numbers_to_check_single.extend(result.get("batch_phones", []))

#             if numbers_to_check_single:
#                 request_id = str(uuid.uuid4())
#                 single_results = []

#                 with ThreadPoolExecutor(max_workers=10) as executor:
#                     futures = []
#                     for phone in numbers_to_check_single:
#                         future = executor.submit(check_single_user_capability, phone, request_id)
#                         futures.append(future)

#                     for future in as_completed(futures):
#                         single_results.append(future.result())

#                 # Remove these numbers from non_capable (they were added as non-capable when batch failed)
#                 numbers_to_remove = set([str(p) for p in numbers_to_check_single])
#                 non_capable_numbers = [p for p in non_capable_numbers if str(p) not in numbers_to_remove]

#                 # Process single check results
#                 for res in single_results:
#                     phone_str = str(res["phone"])
#                     all_processed_numbers.add(phone_str)
#                     if res["capable"]:
#                         capable_numbers.append(res["phone"])
#                     else:
#                         non_capable_numbers.append(res["phone"])

#     # Remove duplicates
#     capable_numbers = list(set(capable_numbers))
#     non_capable_numbers = list(set(non_capable_numbers))

#     # Calculate missing numbers
#     all_input_set = set([str(p) for p in phone_numbers])
#     processed_set = all_processed_numbers
#     missing_set = all_input_set - processed_set
#     missing_numbers = list(missing_set)

#     total_processed = len(processed_set)
#     missing_count = len(missing_numbers)

#     print(f"\n📊 CAPABILITY CHECK RESULTS")
#     print(f"   📱 Total input numbers: {len(phone_numbers)}")
#     print(f"   🔄 Processed numbers: {total_processed}")
#     print(f"   ⚠️  Missing numbers: {missing_count}")
#     print(f"   ✅ RCS Capable: {len(capable_numbers)}")
#     print(f"   ❌ Non-RCS Capable: {len(non_capable_numbers)}")

#     # Verify the math
#     total_accounted = len(capable_numbers) + len(non_capable_numbers) + missing_count
#     if total_accounted != len(phone_numbers):
#         print(
#             f"   ⚠️  Math discrepancy: {len(capable_numbers)} + {len(non_capable_numbers)} + {missing_count} = {total_accounted}, expected {len(phone_numbers)}")

#     if missing_count > 0:
#         print(f"   📱 Sample missing numbers: {missing_numbers[:5] if missing_numbers else 'None'}")

#     if capable_numbers:
#         print(f"   📱 Sample capable numbers: {capable_numbers[:5]}")

#     # Print all non-capable numbers if there are any
#     if non_capable_numbers and len(non_capable_numbers) > 0:
#         print(f"   📱 Sample non-capable numbers: {non_capable_numbers[:5]}")
#         # Print all non-capable numbers
#         print_all_non_capable_numbers(non_capable_numbers)

#     return capable_numbers, non_capable_numbers, missing_numbers


# def send_message1(phone_number, message_id=None):
#     """Send a single message - helper function"""
#     global token, ASSISTANT_ID, payload

#     # Check and refresh token if needed
#     if not refresh_token_if_needed():
#         return 401  # Unauthorized

#     try:
#         if not message_id:
#             message_id = f"msg-{uuid.uuid4().hex[:8]}"

#         url = f"https://api.businessmessaging.jio.com/v1/messaging/users/+91{phone_number}/assistantMessages/async"
#         url += f"?messageId={message_id}&assistantId={ASSISTANT_ID}"

#         headers = {
#             "Authorization": f"Bearer {token}",
#             "Content-Type": "application/json"
#         }

#         data = {
#             "messageTrafficType": "PROMOTION",
#             "content": payload['content']
#         }

#         # Fast request with timeout
#         response = requests.post(url, headers=headers, json=data, verify=False, timeout=5)
#         return response.status_code

#     except Exception:
#         return 500  # Return error code for any exception


# def send_message(phone_number, message_id=None):
#     """Main message sending function with FAST retry mechanism"""
#     global count, phonelist, fail, retrylist

#     try:
#         if not message_id:
#             message_id = f"msg-{uuid.uuid4().hex[:8]}"

#         url = f"https://api.businessmessaging.jio.com/v1/messaging/users/+91{phone_number}/assistantMessages/async"
#         url += f"?messageId={message_id}&assistantId={ASSISTANT_ID}"

#         headers = {
#             "Authorization": f"Bearer {token}",
#             "Content-Type": "application/json"
#         }

#         data = {
#             "messageTrafficType": "PROMOTION",
#             "content": payload['content']
#         }

#         # Check token before first attempt
#         if not refresh_token_if_needed():
#             retrylist.append(phone_number)
#             fail += 1
#             return

#         # First attempt
#         try:
#             response = requests.post(url, headers=headers, json=data, verify=False, timeout=5)
#         except:
#             response = type('obj', (object,), {'status_code': 500})()

#         if response.status_code == 201:
#             count += 1
#             phonelist.append(phone_number)
#             return

#         # Check if it's a token error
#         if response.status_code == 401:
#             print(f"   🔄 Token expired, refreshing and retrying...")
#             if refresh_token_if_needed():
#                 # Retry with new token
#                 try:
#                     response = requests.post(url, headers=headers, json=data, verify=False, timeout=5)
#                     if response.status_code == 201:
#                         count += 1
#                         phonelist.append(phone_number)
#                         return
#                 except:
#                     pass

#         # FAST RETRY LOGIC: Immediate 200 retries
#         retrylist.append(phone_number)

#         for retry_count in range(100):
#             x = send_message1(phone_number, message_id)
#             if x == 201:
#                 count += 1
#                 phonelist.append(phone_number)
#                 break
#             if retry_count == 99:
#                 fail += 1

#     except Exception:
#         # FAST RETRY on exception
#         retrylist.append(phone_number)

#         for retry_count in range(100):
#             x = send_message1(phone_number, message_id)
#             if x == 201:
#                 count += 1
#                 phonelist.append(phone_number)
#                 break
#             if retry_count == 99:
#                 fail += 1


# def worker_thread():
#     """Worker function for message sending (only for capable numbers)"""
#     global capable_numbers, phone_to_message_id, current_index, lock

#     # Each worker will refresh token periodically
#     worker_start_time = time.time()

#     while True:
#         with lock:
#             if current_index >= len(capable_numbers):
#                 break
#             number = capable_numbers[current_index]
#             current_index += 1

#         # Refresh token every 1000 messages or 30 minutes for this worker
#         if (current_index % 1000 == 0) or (time.time() - worker_start_time > 1800):
#             refresh_token_if_needed()
#             worker_start_time = time.time()

#         # Get the messageId for this phone number
#         message_id = phone_to_message_id.get(number)

#         # Send message with FAST retry logic
#         send_message(number, message_id)


# def periodic_token_refresher():
#     """Background thread to refresh token every 40 minutes"""
#     global should_refresh_token

#     while True:
#         time.sleep(TOKEN_REFRESH_INTERVAL - 300)  # Check 5 minutes before expiry
#         should_refresh_token = True
#         print("⏰ Token refresh scheduled in 5 minutes...")


# def main():
#     """Main function to orchestrate the campaign sending"""
#     global phone_numbers, token, token_generated_time, ASSISTANT_ID, CLIENT_SECRET, phone_to_message_id, maincampainid
#     global current_index, payload, count, fail, phonelist, retrylist
#     global capable_numbers, non_capable_numbers, missing_numbers

#     print("=" * 60)
#     print("🚀 STARTING CAMPAIGN PROCESSING")
#     print("=" * 60)

#     # STEP 1: Get campaign data (connection opens and closes inside function)
#     data = get_campaign_data()

#     if data and data.get("success"):
#         print("\n" + "=" * 60)
#         print("✅ CAMPAIGN LOADED")
#         print("=" * 60)
#         print(f"📋 Campaign: {data['campaign_name']}")
#         print(f"📝 ID: {data['campaign_id']}")
#         print(f"👥 Contacts: {data['total_contacts']}")

#         maincampainid = data['campaign_id']
#         ASSISTANT_ID = data['client_id']
#         CLIENT_SECRET = data['client_secret']

#         if data['total_contacts'] == 0:
#             print("\n❌ No contacts to send. Marking campaign as completed.")
#             # STEP 5: Mark as completed (separate connection)
#             mark_campaign_completed(maincampainid)
#             return

#         # STEP 2: Mark campaign as Running (separate connection)
#         print("\n🔄 Marking campaign as 'Running'...")
#         if not mark_campaign_running(maincampainid):
#             print("❌ Failed to mark campaign as Running. Aborting.")
#             return

#         # Get initial authentication token
#         print(f"\n🔐 Authenticating...")
#         token = get_token(ASSISTANT_ID, CLIENT_SECRET)
#         if not token:
#             print("❌ Authentication failed. Aborting campaign.")
#             return
#         token_generated_time = time.time()

#         # Start background token refresher thread
#         token_refresher_thread = threading.Thread(target=periodic_token_refresher, daemon=True)
#         token_refresher_thread.start()
#         print(f"🔄 Background token refresher started (will refresh every {TOKEN_REFRESH_INTERVAL // 60} minutes)")

#         # Store phone numbers and messageId mapping
#         phone_numbers = data['phone_numbers']
#         phone_to_message_id = data.get('phone_message_map', {})

#         # STEP 3: Check RCS capabilities
#         capable_numbers, non_capable_numbers, missing_numbers = check_rcs_capabilities(phone_numbers)

#         # Update campaign with capability stats
#         update_campaign_stats(maincampainid, len(capable_numbers), len(non_capable_numbers), len(missing_numbers))

#         if len(capable_numbers) == 0:
#             print("\n❌ No RCS capable contacts found. Marking campaign as completed.")
#             mark_campaign_completed(maincampainid)
#             return {
#                 "campaign_id": maincampainid,
#                 "campaign_name": data['campaign_name'],
#                 "sent": 0,
#                 "failed": 0,
#                 "total": len(phone_numbers),
#                 "capable": 0,
#                 "non_capable": len(non_capable_numbers),
#                 "missing": len(missing_numbers),
#                 "retried": 0,
#                 "processing_time": 0
#             }

#         # Reset counters
#         current_index = 0
#         count = 0
#         fail = 0
#         phonelist.clear()
#         retrylist.clear()

#         print(f"\n🎯 STARTING MESSAGE SENDING")
#         print(f"📤 RCS capable contacts to process: {len(capable_numbers)}")
#         print(f"⏰ Token will auto-refresh every {TOKEN_REFRESH_INTERVAL // 60} minutes")

#         # Use 100 workers as requested
#         num_threads = 100
#         print(f"🧵 Using {num_threads} workers")
#         print("-" * 40)

#         # Create and start threads
#         threads = []
#         start_time = time.time()

#         for i in range(num_threads):
#             t = threading.Thread(target=worker_thread, name=f"Worker-{i + 1}")
#             t.start()
#             threads.append(t)

#         # Wait for all threads to complete
#         print(f"\n⏳ Processing {len(capable_numbers)} capable contacts...")
#         for t in threads:
#             t.join()

#         processing_time = time.time() - start_time

#         print("\n" + "=" * 60)
#         print("✅ ALL MESSAGES SENT")
#         print("=" * 60)
#         print(f"📊 Results: {count} sent, {fail} failed, {len(retrylist)} retried")
#         print(
#             f"📱 Capability: {len(capable_numbers)} capable, {len(non_capable_numbers)} non-capable, {len(missing_numbers)} missing")
#         print(f"⏱️  Total Time: {processing_time:.2f} seconds")
#         print(f"⏰ Token refreshed every {TOKEN_REFRESH_INTERVAL // 60} minutes")

#         # STEP 4: Mark campaign as completed (separate connection)
#         print("\n🔄 Marking campaign as 'completed'...")
#         mark_campaign_completed(maincampainid)

#         return {
#             "campaign_id": maincampainid,
#             "campaign_name": data['campaign_name'],
#             "sent": count,
#             "failed": fail,
#             "total": len(phone_numbers),
#             "capable": len(capable_numbers),
#             "non_capable": len(non_capable_numbers),
#             "missing": len(missing_numbers),
#             "retried": len(retrylist),
#             "processing_time": processing_time
#         }

#     else:
#         print("❌ No pending campaign available for processing")
#         return None


# if __name__ == "__main__":
#     while True:
#         print("\n" + "=" * 60)
#         print("🔄 CHECKING FOR NEW PENDING CAMPAIGNS")
#         print("=" * 60)

#         result = main()

#         if result:
#             print(f"\n📊 FINAL RESULTS:")
#             print(f"   Campaign: {result['campaign_name']}")
#             print(f"   📱 Total Contacts: {result['total']}")
#             print(f"   ✅ RCS Capable: {result['capable']}")
#             print(f"   ❌ Non-RCS Capable: {result['non_capable']}")
#             print(f"   ⚠️  Missing: {result['missing']}")
#             print(f"   📤 Messages Sent: {result['sent']}")
#             print(f"   ❌ Failed: {result['failed']}")
#             print(f"   🔄 Retried: {result['retried']}")
#             print(f"   📈 RCS Reach Rate: {(result['capable'] / result['total'] * 100):.1f}%" if result[
#                                                                                                     'total'] > 0 else "   📈 RCS Reach Rate: N/A")
#             print(f"   📈 Send Success Rate: {(result['sent'] / result['capable'] * 100):.1f}%" if result[
#                                                                                                       'capable'] > 0 else "   📈 Send Success Rate: N/A")
#             print(f"   ⏱️  Total Time: {result['processing_time']:.2f} seconds")
#             print(f"   🚀 Send Rate: {result['sent'] / result['processing_time']:.2f} messages/sec" if result[
#                                                                                                           'processing_time'] > 0 else "   🚀 Send Rate: N/A")

#             # Verify total
#             total_accounted = result['capable'] + result['non_capable'] + result['missing']
#             if total_accounted != result['total']:
#                 print(
#                     f"   ⚠️  Math check: {result['capable']} + {result['non_capable']} + {result['missing']} = {total_accounted}, expected {result['total']}")

#         print(f"\n🔄 Next check in 30 seconds...")
#         print("=" * 60)
#         # time.sleep(30)













































import threading
import requests
import json
import uuid
import time
import os
import signal
import sys
import traceback
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, timezone
import urllib3
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import math
import logging
from queue import Queue
from threading import Event
import atexit

# Disable warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot_monitor.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Suppress pymongo logging to avoid connection pool noise
logging.getLogger('pymongo').setLevel(logging.CRITICAL)

# Global variables with proper initialization
class BotState:
    def __init__(self):
        self.fail = 0
        self.count = 0
        self.token = None
        self.token_generated_time = 0
        self.assistant_id = None
        self.client_secret = None
        self.phonelist = []
        self.retrylist = []
        self.maincampainid = 0
        self.payload = ""
        self.phone_numbers = []
        self.phone_to_message_id = {}
        self.current_index = 0
        self.capable_numbers = []
        self.non_capable_numbers = []
        self.missing_numbers = []
        self.should_refresh_token = False
        self.is_running = True
        self.processing_campaign = False
        self.last_heartbeat = time.time()
        self.active_workers = 0
        self.lock = threading.Lock()
        self.token_refresh_lock = threading.Lock()
        self.worker_errors = Queue()

        # ---- Multi-config support ----
        self.is_multi_config = False
        self.jio_configs = []            # list of {clientId, clientSecret, assistantId}
        self.config_tokens = {}          # {configIndex: token_string}
        self.config_token_times = {}     # {configIndex: timestamp}
        self.phone_to_config_index = {}  # {phone: configIndex}
        self.config_token_locks = {}     # {configIndex: Lock}

bot_state = BotState()

# Database configuration
MONGODB_URI = "mongodb+srv://sikarwarvishal75_db_user:Gama%40123@cluster0.whqwih.mongodb.net/rcs?retryWrites=true&w=majority"
DATABASE_NAME = "rcs"

# Constants
MAX_API_LIMIT = 10000
MIN_BATCH_SIZE = 500
CONCURRENCY = 30
TOKEN_REFRESH_INTERVAL = 2400  # Refresh token every 40 minutes
TOKEN_EXPIRY_TIME = 3600  # Token expires in 1 hour
HEARTBEAT_INTERVAL = 60  # Send heartbeat every minute
MAX_WORKER_IDLE_TIME = 300  # 5 minutes max idle time for workers
MAX_RETRIES = 3  # Max retries for failed operations
REQUEST_TIMEOUT = 30  # Default request timeout

# MongoDB connection pool
mongo_client = None
mongo_lock = threading.Lock()


def get_mongo_client():
    """Get or create MongoDB client with connection pooling"""
    global mongo_client
    with mongo_lock:
        if mongo_client is None:
            try:
                mongo_client = MongoClient(
                    MONGODB_URI,
                    maxPoolSize=50,
                    minPoolSize=10,
                    maxIdleTimeMS=45000,
                    connectTimeoutMS=30000,
                    socketTimeoutMS=45000,
                    serverSelectionTimeoutMS=30000,
                    retryWrites=True,
                    retryReads=True
                )
                # Test connection
                mongo_client.admin.command('ping')
                logger.info("✅ MongoDB connection pool established")
            except Exception as e:
                logger.error(f"❌ Failed to connect to MongoDB: {e}")
                raise
        return mongo_client

def close_mongo_connections():
    """Close MongoDB connections gracefully"""
    global mongo_client
    with mongo_lock:
        if mongo_client:
            mongo_client.close()
            mongo_client = None
            logger.info("MongoDB connections closed")

# Register cleanup on exit
atexit.register(close_mongo_connections)

def signal_handler(sig, frame):
    """Handle shutdown signals gracefully"""
    logger.info("🛑 Shutdown signal received, cleaning up...")
    bot_state.is_running = False
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def update_heartbeat():
    """Update heartbeat timestamp"""
    bot_state.last_heartbeat = time.time()

def check_health():
    """Check if bot is healthy"""
    current_time = time.time()
    
    # Check if stuck (no heartbeat for too long)
    if current_time - bot_state.last_heartbeat > HEARTBEAT_INTERVAL * 3:
        logger.error(f"🚨 Bot appears stuck! Last heartbeat: {bot_state.last_heartbeat}")
        return False
    
    # Check if workers are alive during campaign
    if bot_state.processing_campaign and bot_state.active_workers == 0:
        if bot_state.current_index < len(bot_state.capable_numbers):
            logger.error("🚨 No active workers but numbers remaining to process!")
            return False
    
    # Check for worker errors
    if not bot_state.worker_errors.empty():
        try:
            error_info = bot_state.worker_errors.get_nowait()
            logger.error(f"Worker error reported: {error_info}")
        except:
            pass
    
    return True

def health_monitor():
    """Background thread to monitor bot health"""
    while bot_state.is_running:
        time.sleep(HEARTBEAT_INTERVAL)
        if not check_health():
            logger.warning("⚠️ Health check failed, but continuing monitoring")
        else:
            logger.debug(f"✅ Health check passed. Active workers: {bot_state.active_workers}")

def get_utc_now():
    """Get current UTC datetime"""
    return datetime.now(timezone.utc)

def safe_mongo_operation(operation_func, *args, **kwargs):
    """Execute MongoDB operation with retries"""
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            client = get_mongo_client()
            db = client[DATABASE_NAME]
            return operation_func(db, *args, **kwargs)
        except Exception as e:
            logger.warning(f"MongoDB operation failed (attempt {attempt + 1}/{max_attempts}): {e}")
            if attempt == max_attempts - 1:
                logger.error(f"MongoDB operation failed after {max_attempts} attempts")
                raise
            time.sleep(2 ** attempt)  # Exponential backoff

def mark_campaign_running(campaign_id):
    """Mark campaign as Running with retry logic"""
    def _update(db):
        cid = ObjectId(campaign_id) if isinstance(campaign_id, str) else campaign_id
        
        result = db.campaigns.update_one(
            {"_id": cid},
            {"$set": {"status": "running", "updatedAt": get_utc_now()}}
        )
        return result.modified_count > 0
    
    try:
        success = safe_mongo_operation(_update)
        if success:
            logger.info(f"✅ Campaign {campaign_id} marked as 'Running'")
        return success
    except Exception as e:
        logger.error(f"❌ Error marking campaign as Running: {e}")
        return False

def mark_campaign_completed(campaign_id):
    """Mark campaign as completed with retry logic"""
    def _update(db):
        cid = ObjectId(campaign_id) if isinstance(campaign_id, str) else campaign_id
        
        result = db.campaigns.update_one(
            {"_id": cid},
            {
                "$set": {
                    "status": "completed",
                    "updatedAt": get_utc_now(),
                    "completedAt": get_utc_now()
                }
            }
        )
        return result.modified_count > 0
    
    try:
        success = safe_mongo_operation(_update)
        if success:
            logger.info(f"✅ Campaign {campaign_id} marked as 'completed'")
        return success
    except Exception as e:
        logger.error(f"❌ Error marking campaign as completed: {e}")
        return False

def update_campaign_stats(campaign_id, capable_count, non_capable_count, missing_count=0):
    """Update campaign stats with retry logic"""
    def _update(db):
        cid = ObjectId(campaign_id) if isinstance(campaign_id, str) else campaign_id
        
        result = db.campaigns.update_one(
            {"_id": cid},
            {
                "$set": {
                    "rcsCapableCount": capable_count,
                    "nonRcsCapableCount": non_capable_count,
                    "missingCount": missing_count,
                    "updatedAt": get_utc_now()
                }
            }
        )
        return result.modified_count > 0
    
    try:
        success = safe_mongo_operation(_update)
        if success:
            logger.info(f"📊 Campaign stats updated: {capable_count} capable, {non_capable_count} non-capable")
        return success
    except Exception as e:
        logger.error(f"❌ Error updating campaign stats: {e}")
        return False

def get_campaign_data():
    """Get campaign data with connection pooling — single DB round-trip for all data"""
    try:
        client = get_mongo_client()
        db = client[DATABASE_NAME]

        logger.info("🔍 Looking for pending campaign with botId 'bot2'...")
        campaign = db.campaigns.find_one({
            "botId": "bot2",
            "status": "pending"
        })

        if not campaign:
            logger.info("❌ No pending campaign found with botId 'bot2'")
            return None

        campaign_id = campaign["_id"]
        campaign_name = campaign.get("name", "Unnamed")

        logger.info(f"✅ Found pending campaign: {campaign_name} (ID: {campaign_id})")

        payload = campaign["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)

        user_id = campaign.get("userId")
        # Fetch user with jioConfigs secrets included
        user = db.users.find_one(
            {"_id": user_id},
            {"jioConfig": 1, "isMultiConfig": 1, "jioConfigs": 1}
        )

        if not user:
            logger.error("❌ User not found")
            return None

        # Determine single vs multi config
        is_multi = user.get("isMultiConfig", False)
        jio_configs_list = []

        if is_multi and user.get("jioConfigs") and len(user["jioConfigs"]) > 0:
            jio_configs_list = user["jioConfigs"]
            logger.info(f"🔀 Multi-config mode: {len(jio_configs_list)} configs")
            client_id = jio_configs_list[0].get("clientId")
            client_secret = jio_configs_list[0].get("clientSecret")
        else:
            is_multi = False
            jio_config = user.get("jioConfig")
            if not jio_config:
                logger.error("❌ jioConfig not found")
                return None
            client_id = jio_config.get("clientId")
            client_secret = jio_config.get("clientSecret")

        # Get phone numbers, messageIds, and configIndex in ONE query
        phone_numbers_list = []
        phone_message_map = {}
        phone_config_map = {}  # phone -> configIndex

        contacts_cursor = db.contact_campaign_messages.find(
            {"campaignId": campaign_id},
            {"recipientPhoneNumber": 1, "messageId": 1, "configIndex": 1}  # only needed fields
        )

        contacts_count = 0
        for contact in contacts_cursor:
            contacts_count += 1
            phone = contact.get("recipientPhoneNumber")
            if phone:
                phone_numbers_list.append(phone)
                message_id = contact.get("messageId")
                if message_id:
                    phone_message_map[phone] = message_id
                config_idx = contact.get("configIndex")
                if config_idx is not None:
                    phone_config_map[phone] = config_idx

        # Remove duplicates
        unique_phones = list(set(phone_numbers_list))

        logger.info(f"📊 Found {contacts_count} contact records, {len(unique_phones)} unique numbers")

        result = {
            "success": True,
            "campaign_id": str(campaign_id),
            "campaign_name": campaign_name,
            "client_id": client_id,
            "client_secret": client_secret,
            "phone_numbers": unique_phones,
            "phone_message_map": phone_message_map,
            "payload": payload,
            "total_contacts": len(unique_phones),
            "total_records": contacts_count,
            "is_multi_config": is_multi,
            "jio_configs": jio_configs_list,
            "phone_config_map": phone_config_map
        }

        return result

    except Exception as e:
        logger.error(f"❌ Error retrieving campaign data: {e}")
        traceback.print_exc()
        return {"success": False, "error": str(e)}

def get_token(assistant_id, client_secret):
    """Get token with timeout and retry"""
    url = "https://tgs.businessmessaging.jio.com/v1/oauth/token"
    params = {
        "grant_type": "client_credentials",
        "client_id": assistant_id,
        "client_secret": client_secret,
        "scope": "read"
    }

    logger.info(f"🔑 Getting authentication token...")
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(
                url, 
                params=params, 
                verify=False, 
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                token = response.json()["access_token"]
                logger.info(f"✅ Token generated successfully")
                return token
            else:
                logger.warning(f"❌ Failed to get token (attempt {attempt + 1}): {response.status_code}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error(f"Response: {response.text}")
        except requests.exceptions.Timeout:
            logger.warning(f"⏰ Token request timeout (attempt {attempt + 1})")
        except Exception as e:
            logger.warning(f"❌ Exception getting token (attempt {attempt + 1}): {e}")
        
        if attempt < MAX_RETRIES - 1:
            time.sleep(2 ** attempt)
    
    return None

def refresh_token_if_needed(config_index=None):
    """Thread-safe token refresh with retry logic.
    For multi-config: pass config_index to refresh that specific config's token.
    For single-config: pass None (default).
    """
    if config_index is not None and bot_state.is_multi_config:
        # --- Multi-config: per-config token refresh ---
        lock = bot_state.config_token_locks.get(config_index)
        if not lock:
            return False

        with lock:
            current_time = time.time()
            last_time = bot_state.config_token_times.get(config_index, 0)
            current_token = bot_state.config_tokens.get(config_index)

            should_refresh = (
                not current_token or
                (current_time - last_time) >= TOKEN_REFRESH_INTERVAL
            )

            if should_refresh:
                cfg = bot_state.jio_configs[config_index]
                logger.info(f"🔄 Refreshing token for config {config_index} ({cfg.get('label', '')})...")
                new_token = get_token(cfg["clientId"], cfg["clientSecret"])

                if new_token:
                    bot_state.config_tokens[config_index] = new_token
                    bot_state.config_token_times[config_index] = current_time
                    return True
                else:
                    logger.error(f"❌ Failed to refresh token for config {config_index}")
                    return False

            return True
    else:
        # --- Single-config (original behaviour) ---
        with bot_state.token_refresh_lock:
            current_time = time.time()

            should_refresh = (
                not bot_state.token or
                (current_time - bot_state.token_generated_time) >= TOKEN_REFRESH_INTERVAL
            )

            if should_refresh:
                logger.info("🔄 Refreshing authentication token...")
                new_token = get_token(bot_state.assistant_id, bot_state.client_secret)

                if new_token:
                    bot_state.token = new_token
                    bot_state.token_generated_time = current_time
                    bot_state.should_refresh_token = False
                    logger.info("✅ Token refreshed successfully")
                    return True
                else:
                    logger.error("❌ Failed to refresh token")
                    return False

            return True

def format_phone_number(phone):
    """Format phone number to E.164 format with error handling"""
    try:
        digits = ''.join(filter(str.isdigit, str(phone)))
        
        if not digits:
            return None
            
        if len(digits) == 10:
            return f"+91{digits}"
        elif digits.startswith('91') and len(digits) == 12:
            return f"+{digits}"
        elif digits.startswith('91') and len(digits) == 11:
            return f"+{digits}"
        else:
            return f"+91{digits[-10:]}"
    except Exception as e:
        logger.error(f"Error formatting phone {phone}: {e}")
        return None

def get_token_for_message(phone_number):
    """Get the correct token + assistantId for a phone number.
    Multi-config: looks up configIndex, returns that config's token.
    Single-config: returns the global token.
    No DB calls — everything is in memory.
    """
    if bot_state.is_multi_config:
        config_index = bot_state.phone_to_config_index.get(phone_number, 0)
        refresh_token_if_needed(config_index)
        token = bot_state.config_tokens.get(config_index)
        cfg = bot_state.jio_configs[config_index]
        return token, cfg.get("assistantId", bot_state.assistant_id)
    else:
        refresh_token_if_needed()
        return bot_state.token, bot_state.assistant_id

def check_single_user_capability(phone_number, request_id):
    """Check single user capability with timeout"""
    try:
        token, _ = get_token_for_message(phone_number)
        if not token:
            return {"phone": phone_number, "capable": False, "error": "Token refresh failed"}

        formatted_phone = format_phone_number(phone_number)
        if not formatted_phone:
            return {"phone": phone_number, "capable": False, "error": "Invalid phone format"}

        url = f"https://api.businessmessaging.jio.com/v1/messaging/users/{formatted_phone}/capabilities"
        params = {"requestId": request_id}

        headers = {"Authorization": f"Bearer {token}"}

        response = requests.get(
            url, 
            headers=headers, 
            params=params, 
            verify=False, 
            timeout=5
        )

        if response.status_code == 200:
            return {"phone": phone_number, "capable": True}
        else:
            return {"phone": phone_number, "capable": False}
            
    except requests.exceptions.Timeout:
        logger.debug(f"Timeout checking {phone_number}")
        return {"phone": phone_number, "capable": False, "error": "timeout"}
    except Exception as e:
        logger.debug(f"Error checking {phone_number}: {e}")
        return {"phone": phone_number, "capable": False, "error": str(e)}

def check_rcs_capabilities(phone_numbers):
    """Check RCS capabilities with improved error handling"""
    logger.info(f"\n🔍 CHECKING RCS CAPABILITIES for {len(phone_numbers)} numbers")

    capable = []
    non_capable = []
    
    if len(phone_numbers) < 500:
        # Single user checks
        request_id = str(uuid.uuid4())
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(check_single_user_capability, phone, request_id) 
                      for phone in phone_numbers]
            
            for i, future in enumerate(as_completed(futures)):
                result = future.result()
                if result.get("capable"):
                    capable.append(result["phone"])
                else:
                    non_capable.append(result["phone"])
                
                if (i + 1) % 100 == 0:
                    logger.info(f"📊 Processed {i + 1}/{len(phone_numbers)} numbers...")
    else:
        # Batch capability check
        logger.info("⚡ Using batch capability check")
        
        # Create batches
        batch_size = 1000
        batches = [phone_numbers[i:i + batch_size] 
                  for i in range(0, len(phone_numbers), batch_size)]
        
        logger.info(f"📦 Created {len(batches)} batches")
        
        for i, batch in enumerate(batches):
            if not refresh_token_if_needed():
                logger.error("Token refresh failed, stopping capability check")
                break
                
            result = check_batch_capability(batch, i + 1, len(batches))
            
            if result.get("success"):
                capable.extend(result["reachable_phones"])
                # Determine non-capable from batch
                batch_phones_set = set([str(p) for p in batch])
                reachable_set = set(result["reachable_phones"])
                non_capable.extend(list(batch_phones_set - reachable_set))
            else:
                # If batch fails, mark all as non-capable
                non_capable.extend(batch)

    # Remove duplicates
    capable = list(set(capable))
    non_capable = list(set(non_capable))
    
    logger.info(f"📊 Results: {len(capable)} capable, {len(non_capable)} non-capable")
    
    return capable, non_capable, []

def check_batch_capability(phone_numbers_batch, batch_index, total_batches):
    """Check batch capability with timeout"""
    try:
        if not refresh_token_if_needed():
            return {"success": False, "batch_phones": phone_numbers_batch}

        url = "https://api.businessmessaging.jio.com/v1/messaging/usersBatchGet"
        headers = {"Authorization": f"Bearer {bot_state.token}"}

        # Format phone numbers
        formatted_numbers = []
        valid_numbers = []
        for num in phone_numbers_batch:
            formatted = format_phone_number(num)
            if formatted:
                formatted_numbers.append(formatted)
                valid_numbers.append(num)

        if not formatted_numbers:
            return {"success": False, "batch_phones": phone_numbers_batch}

        data = {"phoneNumbers": formatted_numbers}

        response = requests.post(
            url, 
            headers=headers, 
            json=data, 
            verify=False, 
            timeout=30
        )

        if response.status_code == 200:
            result = response.json()
            reachable_users = result.get("reachableUsers", [])
            
            # Extract phone numbers without +91
            reachable_phones = []
            for phone in reachable_users:
                if phone.startswith("+91"):
                    reachable_phones.append(phone[3:])
                else:
                    reachable_phones.append(phone)

            logger.info(f"✅ Batch {batch_index}/{total_batches}: {len(reachable_phones)}/{len(valid_numbers)} reachable")
            
            return {
                "success": True,
                "reachable_phones": reachable_phones,
                "batch_phones": valid_numbers
            }
        else:
            logger.warning(f"❌ Batch {batch_index} failed: HTTP {response.status_code}")
            return {"success": False, "batch_phones": phone_numbers_batch}
            
    except Exception as e:
        logger.error(f"Batch {batch_index} exception: {e}")
        return {"success": False, "batch_phones": phone_numbers_batch}

def send_message_with_retry(phone_number, message_id=None, max_retries=100):
    """Send single message with retry logic and timeout.
    Automatically uses the correct config/token for this phone number.
    Zero DB calls — all lookups are in-memory.
    """
    config_index = bot_state.phone_to_config_index.get(phone_number) if bot_state.is_multi_config else None

    for attempt in range(max_retries):
        try:
            token, assistant_id = get_token_for_message(phone_number)
            if not token:
                if attempt == max_retries - 1:
                    return False
                time.sleep(0.1)
                continue

            # Generate message ID if not provided
            if not message_id:
                message_id = f"msg-{uuid.uuid4().hex[:8]}"
            else:
                message_id = str(message_id)

            # Build URL with properly formatted phone
            phone_clean = str(phone_number).strip()
            if not phone_clean.startswith('+91'):
                phone_clean = f"+91{phone_clean}"
            
            url = f"https://api.businessmessaging.jio.com/v1/messaging/users/{phone_clean}/assistantMessages/async"
            url += f"?messageId={message_id}&assistantId={assistant_id}"

            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }

            # Check if payload exists
            if not bot_state.payload:
                logger.error(f"❌ No payload for {phone_number}")
                return False
            
            # Extract the actual content to send
            if isinstance(bot_state.payload, dict):
                if 'content' in bot_state.payload:
                    content_to_send = bot_state.payload['content']
                else:
                    content_to_send = bot_state.payload
            else:
                content_to_send = bot_state.payload

            # Ensure content_to_send is serializable
            if isinstance(content_to_send, str):
                try:
                    content_to_send = json.loads(content_to_send)
                except:
                    pass

            data = {
                "messageTrafficType": "PROMOTION",
                "content": content_to_send
            }

            response = requests.post(
                url, 
                headers=headers, 
                json=data, 
                verify=False, 
                timeout=10
            )

            if response.status_code == 201:
                logger.debug(f"✅ Successfully sent to {phone_number}")
                return True
            elif response.status_code == 401:
                logger.debug(f"🔄 Token expired for {phone_number}, refreshing...")
                refresh_token_if_needed(config_index)
            elif response.status_code == 400:
                logger.error(f"❌ Bad request for {phone_number}: {response.text}")
                logger.error(f"Data sent: {json.dumps(data, indent=2)[:500]}")
                return False
            else:
                logger.debug(f"⚠️ Got status {response.status_code} for {phone_number}, retry {attempt + 1}")
                
        except requests.exceptions.Timeout:
            logger.debug(f"⏰ Timeout for {phone_number}, retry {attempt + 1}")
        except requests.exceptions.ConnectionError:
            logger.debug(f"🔌 Connection error for {phone_number}, retry {attempt + 1}")
        except Exception as e:
            logger.debug(f"❌ Exception for {phone_number}: {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to send to {phone_number} after {max_retries} attempts: {e}")
            else:
                time.sleep(0.05)
    
    return False

def worker_thread(worker_id):
    """Worker function with heartbeat and error handling"""
    logger.info(f"🧵 Worker-{worker_id} started")
    last_activity = time.time()
    
    with bot_state.lock:
        bot_state.active_workers += 1
    
    try:
        while bot_state.is_running:
            # Check if we should continue
            if not bot_state.processing_campaign:
                time.sleep(1)
                continue
                
            # Get next number
            with bot_state.lock:
                if bot_state.current_index >= len(bot_state.capable_numbers):
                    break
                number = bot_state.capable_numbers[bot_state.current_index]
                bot_state.current_index += 1
                last_activity = time.time()
            
            # Check for idle timeout
            if time.time() - last_activity > MAX_WORKER_IDLE_TIME:
                logger.warning(f"Worker-{worker_id} idle for too long, but continuing")
            
            # Refresh token periodically
            if bot_state.current_index % 500 == 0:
                refresh_token_if_needed()
            
            # Send message
            message_id = bot_state.phone_to_message_id.get(number)
            success = send_message_with_retry(number, message_id)
            
            with bot_state.lock:
                if success:
                    bot_state.count += 1
                    bot_state.phonelist.append(number)
                else:
                    bot_state.fail += 1
                    bot_state.retrylist.append(number)
            
            # Small yield to prevent CPU overload
            time.sleep(0.001)
            
    except Exception as e:
        logger.error(f"Worker-{worker_id} crashed: {e}")
        traceback.print_exc()
        bot_state.worker_errors.put(f"Worker-{worker_id}: {str(e)}")
    finally:
        with bot_state.lock:
            bot_state.active_workers -= 1
        logger.info(f"🧵 Worker-{worker_id} stopped")

def process_campaign():
    """Main campaign processing function"""
    global bot_state
    
    update_heartbeat()
    
    # Get campaign data
    data = get_campaign_data()
    
    if not data or not data.get("success"):
        logger.info("No pending campaign found")
        return None
    
    if data.get('total_contacts', 0) == 0:
        logger.info("No contacts to send. Marking campaign as completed.")
        mark_campaign_completed(data['campaign_id'])
        return None
    
    # Set campaign state
    bot_state.processing_campaign = True
    bot_state.maincampainid = data['campaign_id']
    bot_state.assistant_id = data['client_id']
    bot_state.client_secret = data['client_secret']
    bot_state.phone_numbers = data['phone_numbers']
    bot_state.phone_to_message_id = data.get('phone_message_map', {})
    
    # FIX: Properly handle payload
    payload_data = data.get('payload')
    if payload_data:
        if isinstance(payload_data, str):
            try:
                bot_state.payload = json.loads(payload_data)
                logger.info(f"✅ Payload loaded from string (type: {type(bot_state.payload)})")
            except json.JSONDecodeError as e:
                logger.error(f"❌ Failed to parse payload JSON: {e}")
                bot_state.payload = payload_data
        else:
            bot_state.payload = payload_data
            logger.info(f"✅ Payload loaded from dict (type: {type(bot_state.payload)})")
        
        # Debug: Show payload structure
        if isinstance(bot_state.payload, dict):
            logger.info(f"🔍 Payload keys: {list(bot_state.payload.keys())}")
            if 'content' in bot_state.payload:
                logger.info(f"🔍 Content type: {type(bot_state.payload['content'])}")
        else:
            logger.info(f"🔍 Payload is not a dict, it's: {type(bot_state.payload)}")
    else:
        logger.error("❌ No payload found in campaign data")
        bot_state.payload = {}

    # Multi-config setup (all in-memory, no extra DB calls)
    bot_state.is_multi_config = data.get('is_multi_config', False)
    bot_state.jio_configs = data.get('jio_configs', [])
    bot_state.phone_to_config_index = data.get('phone_config_map', {})
    bot_state.config_tokens = {}
    bot_state.config_token_times = {}
    bot_state.config_token_locks = {}
    
    logger.info(f"📋 Processing campaign: {data['campaign_name']} ({len(bot_state.phone_numbers)} contacts)")
    if bot_state.is_multi_config:
        logger.info(f"🔀 Multi-config mode: {len(bot_state.jio_configs)} configs")
    
    # Mark as running
    if not mark_campaign_running(bot_state.maincampainid):
        logger.error("Failed to mark campaign as Running")
        bot_state.processing_campaign = False
        return None
    
    # Close MongoDB connection - we don't need it for sending messages
    logger.info("🔌 Closing MongoDB connection (not needed for message sending)")
    close_mongo_connections()
    
    # Get tokens — one per config in multi-config, or single token
    if bot_state.is_multi_config and len(bot_state.jio_configs) > 0:
        logger.info(f"🔐 Getting tokens for {len(bot_state.jio_configs)} configs...")
        all_tokens_ok = True
        for i, cfg in enumerate(bot_state.jio_configs):
            bot_state.config_token_locks[i] = threading.Lock()
            token = get_token(cfg["clientId"], cfg["clientSecret"])
            if token:
                bot_state.config_tokens[i] = token
                bot_state.config_token_times[i] = time.time()
                logger.info(f"  ✅ Config {i} ({cfg.get('label', 'Bot ' + str(i+1))}): token OK")
            else:
                logger.error(f"  ❌ Config {i}: token FAILED")
                all_tokens_ok = False
        if not any(bot_state.config_tokens.values()):
            logger.error("All config tokens failed")
            mark_campaign_completed(bot_state.maincampainid)
            bot_state.processing_campaign = False
            return None
    else:
        logger.info("🔐 Getting initial token...")
        bot_state.token = get_token(bot_state.assistant_id, bot_state.client_secret)
        if not bot_state.token:
            logger.error("Authentication failed")
            mark_campaign_completed(bot_state.maincampainid)
            bot_state.processing_campaign = False
            return None
        bot_state.token_generated_time = time.time()
    
    # Check capabilities
    capable, non_capable, missing = check_rcs_capabilities(bot_state.phone_numbers)
    bot_state.capable_numbers = capable
    bot_state.non_capable_numbers = non_capable
    bot_state.missing_numbers = missing
    
    # Update stats
    update_campaign_stats(
        bot_state.maincampainid,
        len(capable),
        len(non_capable),
        len(missing)
    )
    
    if len(capable) == 0:
        logger.info("No RCS capable contacts found")
        mark_campaign_completed(bot_state.maincampainid)
        bot_state.processing_campaign = False
        return None
    
    # Reset counters
    bot_state.current_index = 0
    bot_state.count = 0
    bot_state.fail = 0
    bot_state.phonelist.clear()
    bot_state.retrylist.clear()
    
    logger.info(f"🎯 Starting message sending to {len(capable)} capable contacts")
    
    # Start workers
    num_workers = 100
    threads = []
    start_time = time.time()
    
    for i in range(num_workers):
        t = threading.Thread(target=worker_thread, args=(i + 1,), daemon=True)
        t.start()
        threads.append(t)
    
    # Monitor workers
    while bot_state.active_workers > 0 and bot_state.is_running:
        time.sleep(5)
        update_heartbeat()
        
        # Log progress
        with bot_state.lock:
            progress = (bot_state.current_index / len(capable)) * 100
            logger.info(f"📊 Progress: {bot_state.current_index}/{len(capable)} ({progress:.1f}%) - "
                       f"Sent: {bot_state.count}, Failed: {bot_state.fail}")
        
        # Check if workers are stuck (increased to 6 hours)
        if time.time() - start_time > 10800:  # 6 hour timeout
            logger.warning("Campaign taking too long (>6 hours), forcing completion")
            break
    
    # Wait for workers to finish
    for t in threads:
        t.join(timeout=30)
    
    processing_time = time.time() - start_time
    
    # Final results
    logger.info("=" * 60)
    logger.info("✅ CAMPAIGN COMPLETED")
    logger.info("=" * 60)
    logger.info(f"📊 Sent: {bot_state.count}, Failed: {bot_state.fail}")
    logger.info(f"⏱️  Time: {processing_time:.2f}s, Rate: {bot_state.count/processing_time:.2f}/s")
    
    # Mark as completed
    mark_campaign_completed(bot_state.maincampainid)
    bot_state.processing_campaign = False
    
    return {
        "campaign_id": bot_state.maincampainid,
        "sent": bot_state.count,
        "failed": bot_state.fail,
        "capable": len(capable),
        "non_capable": len(non_capable),
        "processing_time": processing_time
    }

def main_loop():
    """Main loop with health monitoring"""
    logger.info("=" * 60)
    logger.info("🚀 BOT STARTED - Monitoring for campaigns")
    logger.info("=" * 60)
    
    # Start health monitor
    monitor_thread = threading.Thread(target=health_monitor, daemon=True)
    monitor_thread.start()
    
    while bot_state.is_running:
        try:
            update_heartbeat()
            
            if not bot_state.processing_campaign:
                logger.info("🔄 Checking for pending campaigns...")
                result = process_campaign()
                
                if result:
                    logger.info(f"📊 Campaign completed: {result}")
                else:
                    logger.info("No campaign to process")
            
            # Wait before next check
            for _ in range(30):
                if not bot_state.is_running:
                    break
                time.sleep(1)
                update_heartbeat()
                
        except KeyboardInterrupt:
            logger.info("🛑 Bot stopped by user")
            break
        except Exception as e:
            logger.error(f"❌ Unexpected error in main loop: {e}")
            traceback.print_exc()
            time.sleep(60)  # Wait before retry
    
    logger.info("👋 Bot shutting down...")
    close_mongo_connections()

if __name__ == "__main__":
    main_loop()
