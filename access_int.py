"""
================================================================================
ARISTA CLOUDVISION BULK IMPORTER - VERSION 1.0 (NO VLAN VALIDATION)
================================================================================

DESCRIPTION:
    A high-speed version of the importer that skips switch configuration 
    scraping and VLAN verification. Use this for lab environments (e.g., cEOS) 
    or when pushing updates to a known-good environment.

HOW IT WORKS:
    1. Discovery: Connects to CV and maps inventory hostnames to Serial/UUIDs.
    2. Provisioning: Immediately builds the Studio payload from the CSV.
    3. Execution: Creates a Workspace and triggers the 'Build' process.

USAGE:
    1. Ensure the 'CSV' folder exists and contains your interface mapping files.
    2. Ensure that prequisite libraries are installed by running `pip install -r requirements.txt`.
    3. Update 'CV_ADDR' and 'CV_TOKEN' below.
    4. Run: access_int.py

CSV STRUCTURE REQUIREMENTS:
    The CSV must contain the following headers:
    - New_Switch:   The hostname of the switch (as seen in CloudVision).
    - Port:         The port number (e.g., 1, 5, 12).
    - Mode:         'Access' or 'Trunk'.
    - Port Profile: The name of the profile (e.g., A18, A18-V510, UPLINK).
    - Access:       The data/native VLAN ID.
    - Voice:        (Optional) The voice VLAN ID.
    - Description:  (Optional) Interface description.

MAPPING LOGIC:
    - If Mode is 'Trunk': Uses generic 'TRUNK_DEFAULT' profile (Allow All).
    - If Mode is 'Access' and Voice VLAN exists: Uses 'trunk phone' mode.
    - If Mode is 'Access' and NO Voice VLAN: Uses 'access' mode.

================================================================================
"""
import sys

# --- DEPENDENCY CHECK ---
missing_modules = []
try: import pandas
except ImportError: missing_modules.append("pandas")
try: import arista.workspace.v1
except ImportError: missing_modules.append("arista-cv-apis")

if missing_modules:
    print("\n" + "!"*60)
    print("  MISSING DEPENDENCIES DETECTED")
    print("!"*60)
    print(f"  The following Python modules are required but not installed:")
    for mod in missing_modules:
        print(f"    - {mod}")
    print("\n  [ACTION REQUIRED]")
    print("  Please run the following command to install them:")
    print("  pip install -r requirements_fast.txt")
    print("!"*60 + "\n")
    sys.exit(1)
    
import grpc
import uuid
import json
import pandas as pd
import os
import re
import urllib3
from google.protobuf import wrappers_pb2 as wrappers
from arista.workspace.v1 import services as workspace_services
from arista.workspace.v1 import workspace_pb2
from arista.studio.v1 import services as studio_services
from arista.studio.v1 import studio_pb2
from arista.inventory.v1 import services as inventory_services
from arista.tag.v2 import services as tag_services
from arista.tag.v2 import tag_pb2

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 1. CONFIGURATION
# ==========================================
CV_TOKEN = "TOKEN"  # Replace with actual service token
CV_ADDR = "CVP IP"  # Replace with CloudVision IP or hostname
CSV_FOLDER_NAME = "CSV"              
ACCESS_STUDIO_ID = "studio-campus-access-interfaces"

# ==========================================
# 2. UI HELPERS
# ==========================================
def print_header(text):
    print(f"\n{'='*60}\n  {text}\n{'='*60}")

def print_step(text):
    print(f"  [i] {text}...", end=" ", flush=True)

def print_done(text="Done"):
    print(f"{text}")

def print_fail(text):
    print(f"  [!] {text}")

# ==========================================
# 3. CONNECTION HELPERS
# ==========================================
def get_grpc_channel():
    channel_creds = grpc.ssl_channel_credentials()
    call_creds = grpc.access_token_call_credentials(CV_TOKEN)
    conn_creds = grpc.composite_channel_credentials(channel_creds, call_creds)
    return grpc.secure_channel(CV_ADDR, conn_creds)

# ==========================================
# 4. MAPPING HELPERS
# ==========================================
def get_grpc_inventory_map(channel):
    print_step("Mapping UUIDs (gRPC)")
    stub = inventory_services.DeviceServiceStub(channel)
    mapping = {}
    try:
        for resp in stub.GetAll(inventory_services.DeviceStreamRequest()):
            dev = resp.value
            if dev.hostname.value and dev.key.device_id.value:
                mapping[dev.hostname.value] = dev.key.device_id.value
    except grpc.RpcError: pass
    print_done(f"Done ({len(mapping)} devices)")
    return mapping

def get_topology_tags(channel):
    print_step("Mapping Topology Tags")
    stub = tag_services.TagAssignmentServiceStub(channel)
    req = tag_services.TagAssignmentStreamRequest(
        partial_eq_filter=[tag_pb2.TagAssignment(key=tag_pb2.TagAssignmentKey(element_type=tag_pb2.ELEMENT_TYPE_DEVICE))]
    )
    topo_map = {} 
    relevant = {"Campus", "Campus-Pod", "Access-Pod"} 
    try:
        for resp in stub.GetAll(req):
            assign = resp.value
            d_id = assign.key.device_id.value
            lbl = assign.key.label.value
            val = assign.key.value.value
            if lbl in relevant:
                if d_id not in topo_map: topo_map[d_id] = {}
                topo_map[d_id][lbl] = val
    except grpc.RpcError: pass
    print_done("Done")
    return topo_map

# ==========================================
# 5. PORT PROFILE BUILDER
# ==========================================
def build_profile_object(row):
    mode_col = str(row.get('Mode', '')).strip().lower()
    
    if mode_col == "trunk":
        return {
            "name": "TRUNK_DEFAULT",
            "mode": "trunk",
            "enabled": "Yes",
            "vlans": {},
            "spanningTree": {"portfast": "edge"}
        }

    profile_name = str(row.get('Port Profile', '')).strip()
    if not profile_name or profile_name.lower() == "nan": return None

    voice_vlan_raw = str(row.get('Voice', '')).strip()
    access_vlan_raw = str(row.get('Access', '')).strip()

    voice_vlan = int(voice_vlan_raw) if voice_vlan_raw.isdigit() else None
    access_vlan = int(access_vlan_raw) if access_vlan_raw.isdigit() else None
    
    if not access_vlan:
        m = re.search(r'A(\d+)', profile_name)
        if m: access_vlan = int(m.group(1))
    if not voice_vlan:
        m = re.search(r'V(\d+)', profile_name)
        if m: voice_vlan = int(m.group(1))

    if voice_vlan or "-" in profile_name or "V" in profile_name:
        mode = "trunk phone"
    else:
        mode = "access"

    profile_obj = {
        "name": profile_name,
        "mode": mode,
        "enabled": "Yes", 
        "vlans": {}, 
        "spanningTree": {"portfast": "edge"}
    }

    if mode == "access" and access_vlan:
        profile_obj["vlans"]["vlans"] = str(access_vlan)
    elif mode == "trunk phone":
        if access_vlan: profile_obj["vlans"]["nativeVlan"] = access_vlan
        if voice_vlan: profile_obj["vlans"]["phoneVlan"] = voice_vlan

    return profile_obj

def get_or_create_node(list_obj, query, child_key):
    for item in list_obj:
        if item.get("tags", {}).get("query") == query: return item
    node = {"tags": {"query": query}, "inputs": {child_key: []}}
    list_obj.append(node)
    return node

def build_sparse_tree(dev_uuid, tags, if_name, profile_name, desc, root_config):
    campus, cpod, apod = tags.get("Campus"), tags.get("Campus-Pod"), tags.get("Access-Pod")
    if not (campus and cpod and apod): return False 

    if "campus" not in root_config: root_config["campus"] = []
    campus_node = get_or_create_node(root_config["campus"], f"Campus:{campus}", "campusPod")
    cpod_node = get_or_create_node(campus_node["inputs"]["campusPod"], f"Campus-Pod:{cpod}", "accessPod")
    apod_node = get_or_create_node(cpod_node["inputs"]["accessPod"], f"Access-Pod:{apod}", "interfaces")
    
    if_tag = f"interface:{if_name}@{dev_uuid}"
    target_if = next((i for i in apod_node["inputs"]["interfaces"] if i.get("tags", {}).get("query") == if_tag), None)
    
    if not target_if:
        target_if = {"tags": {"query": if_tag}, "inputs": {"adapterDetails": {}}}
        apod_node["inputs"]["interfaces"].append(target_if)
    
    target_if["inputs"]["adapterDetails"].update({
        "portProfile": profile_name, 
        "enabled": "Yes", 
        "description": desc
    })
    return True

# ==========================================
# 6. MAIN EXECUTION
# ==========================================
def select_csv_file():
    if not os.path.exists(CSV_FOLDER_NAME): return None
    files = [f for f in os.listdir(CSV_FOLDER_NAME) if f.lower().endswith('.csv')]
    if not files: return None
    print("\n--- Available Files ---")
    for idx, f in enumerate(files): print(f" {idx + 1}. {f}")
    while True:
        try:
            choice = int(input("Select file #: "))
            if 1 <= choice <= len(files): return os.path.join(CSV_FOLDER_NAME, files[choice - 1])
        except ValueError: pass

def main():
    print_header("FAST-TRACK BULK IMPORTER (VLAN VALIDATION DISABLED)")
    csv_file = select_csv_file()
    if not csv_file: return
    
    print_step(f"Reading {os.path.basename(csv_file)}")
    df = pd.read_csv(csv_file).fillna("")
    print_done(f"({len(df)} rows)")

    grpc_channel = get_grpc_channel()

    uuid_map = get_grpc_inventory_map(grpc_channel)
    topo_map = get_topology_tags(grpc_channel)

    print_header("PROVISIONING")
    print_step("Building Payload")
    final_payload = {"portProfiles": [], "campus": []}
    
    profs = {o['name']: o for _, row in df.iterrows() if (o := build_profile_object(row))}
    final_payload["portProfiles"] = list(profs.values())

    count = 0
    for _, row in df.iterrows():
        host, raw_p = str(row['New_Switch']).strip(), str(row['Port']).strip()
        port = f"Ethernet{raw_p}" if raw_p.isdigit() else raw_p
        mode_col = str(row.get('Mode', '')).strip().lower()
        
        if host in uuid_map:
            prof_name = "TRUNK_DEFAULT" if mode_col == "trunk" else str(row.get('Port Profile', '')).strip()
            if build_sparse_tree(uuid_map[host], topo_map.get(uuid_map[host], {}), port, prof_name, row['Description'], final_payload):
                count += 1
    print_done(f"Staged {count} interfaces")

    print_step("Pushing to Workspace")
    ws_config_stub = workspace_services.WorkspaceConfigServiceStub(grpc_channel)
    ws_id = str(uuid.uuid4())
    ws_name = f"FastImport_{ws_id[:4]}"
    
    try:
        ws_config_stub.Set(workspace_services.WorkspaceConfigSetRequest(value=workspace_pb2.WorkspaceConfig(
            key=workspace_pb2.WorkspaceKey(workspace_id=wrappers.StringValue(value=ws_id)),
            display_name=wrappers.StringValue(value=ws_name)
        )))

        inputs_stub = studio_services.InputsConfigServiceStub(grpc_channel)
        key = studio_pb2.InputsKey(
            workspace_id=wrappers.StringValue(value=ws_id), 
            studio_id=wrappers.StringValue(value=ACCESS_STUDIO_ID),
            path=studio_pb2.fmp_dot_wrappers__pb2.RepeatedString(values=[])
        )
        inputs_stub.Set(studio_services.InputsConfigSetRequest(value=studio_pb2.InputsConfig(
            key=key, inputs=wrappers.StringValue(value=json.dumps(final_payload))
        )))
        print_done("OK")

        print_step("Triggering Build")
        ws_config_stub.Set(workspace_services.WorkspaceConfigSetRequest(
            value=workspace_pb2.WorkspaceConfig(
                key=workspace_pb2.WorkspaceKey(workspace_id=wrappers.StringValue(value=ws_id)),
                request=1,
                request_params=workspace_pb2.RequestParams(request_id=wrappers.StringValue(value=str(uuid.uuid4())))
            )
        ))
        print_done("Started")
        
        print(f"\n  LINK: https://{CV_ADDR}/cv/provisioning/workspaces?ws={ws_id}")
    except grpc.RpcError as e:
        print_fail(f"RPC Error: {e.details()}")

if __name__ == "__main__":
    main()