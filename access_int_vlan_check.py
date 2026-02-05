"""
================================================================================
ARISTA CLOUDVISION BULK IMPORTER - VERSION 2.0
================================================================================

DESCRIPTION:
    This production-ready script automates the provisioning of Campus Access 
    Interfaces in CloudVision Studios.

    It reads a CSV of port mappings, validates that all required VLANs exist 
    in the topology studio, merges new configuration with existing 
    Studio data, creates a new workspace, and builds the changes.
    Changes need to be reviewed and accepted then pushed via a Change Control.

ENHANCEMENT IN v1.3:
    - Smart Pod Discovery: Finds Access-Pods WITH or WITHOUT existing interfaces
    - Tag-Based Fallback: When pod not found in studio, queries device tags
    - Auto Pod Creation: Creates pod structure for empty Access-Pods
    - Hierarchy Detection: Determines campus/campusPod location from device tags
    
    This solves the "Pod not found" error for Access-Pods that exist but have
    0 configured interfaces (which don't appear in studio committed state).

HOW IT WORKS:
    1. Discovery: Maps device hostnames to UUIDs via CloudVision device tags.
       Groups interfaces by their Access-Pod tag value.
    
    2. VLAN Validation: Queries the AVD Campus Topology studio to ensure all
       required VLANs (Access + Voice) are defined. Aborts if missing.
    
    3. Pod Location (TWO-STAGE):
       - PRIMARY: Queries studio inputs for pods with interfaces
       - FALLBACK: If not found, queries device tags to verify pod exists
       - Auto-creates pod structure if devices exist but no studio config
    
    4. Port Profiles: Auto-generates smart profiles (Trunk/Phone/Access) and
       creates any missing profiles in the workspace.
    
    5. Interface Writing: Writes interfaces sequentially starting from the
       existing interface count (or 0 for new pods). Uses SetSome to write
       each interface individually.
    
    6. Workspace Build: Creates workspace, writes all data, triggers build.
       User must manually review, submit, and execute via Change Control.

USAGE:
    1. Ensure the 'CSV' folder exists and contains your interface mapping files.
    2. Install dependencies: `pip install cloudvision grpcio pandas cvprac --break-system-packages`
    3. Update 'CV_ADDR' and 'CV_TOKEN' in the Configuration section below.
    4. Run: python arista_importer_enhanced.py
    5. Select your CSV file from the menu.
    6. Review output, confirm import, open workspace URL to submit changes.

CSV STRUCTURE REQUIREMENTS:
    The CSV must contain the following headers:
    - New_Switch:   The hostname of the switch (as seen in CloudVision).
    - Port:         The port number (e.g., 1, 5, 12).
    - Mode:         'access' or 'trunk'.
    - Port Profile: The name of the profile (e.g., A18-V510, TRUNK_DEFAULT).
    - Access:       The data/native VLAN ID.
    - Voice:        (Optional) The voice VLAN ID for phones.
    - Description:  (Optional) Interface description.

MAPPING LOGIC:
    - If Mode is 'trunk': Uses generic 'TRUNK_DEFAULT' profile (Allow All).
    - If Mode is 'access' and Voice VLAN exists: Uses 'trunk phone' mode.
    - If Mode is 'access' and NO Voice VLAN: Uses 'access' mode.

================================================================================
"""

import sys, csv, grpc, json, uuid, ssl, time
from collections import defaultdict
from google.protobuf.json_format import Parse

try:
    import pandas as pd
    from cvprac.cvp_client import CvpClient
    from google.protobuf import wrappers_pb2 as wrappers
    from arista.workspace.v1 import services as workspace_services
    from arista.workspace.v1 import workspace_pb2
    from arista.studio.v1 import services as studio_services
    from arista.studio.v1 import studio_pb2
    from arista.studio.v1.studio_pb2 import fmp_dot_wrappers__pb2
    from arista.inventory.v1 import services as inventory_services
    from arista.tag.v2 import services as tag_services
    from arista.tag.v2 import tag_pb2
except ImportError as e:
    print(f"\n[!] Missing Dependency: {e.name}")
    sys.exit(1)
 
CV_TOKEN = "CVP_TOKEN"
CV_ADDR = "CVP_HOSTNAME"
INTERFACE_STUDIO_ID = "studio-campus-access-interfaces"

def print_header(text):
    print(f"\n{'='*80}\n  {text}\n{'='*80}")

def print_step(text):
    print(f"  [i] {text}...", end=" ", flush=True)

def print_done(text="Done"):
    print(f"{text}")

def get_grpc_channel():
    cert = ssl.get_server_certificate((CV_ADDR, 443))
    creds = grpc.ssl_channel_credentials(root_certificates=cert.encode())
    return grpc.secure_channel(f"{CV_ADDR}:443", grpc.composite_channel_credentials(
        creds, grpc.access_token_call_credentials(CV_TOKEN)))

def get_inventory_map(channel):
    stub = inventory_services.DeviceServiceStub(channel)
    mapping = {}
    for resp in stub.GetAll(inventory_services.DeviceStreamRequest()):
        if resp.value.hostname.value:
            mapping[resp.value.hostname.value] = resp.value.key.device_id.value
    return mapping

def clean_int_str(s):
    return s.replace('.0', '') if s else s

def build_profile_object(row):
    import re
    
    mode = str(row.get('Mode', '')).strip().lower()
    if mode == "trunk":
        return {"name": "TRUNK_DEFAULT", "mode": "trunk", "enabled": "Yes", "vlans": {}, "spanningTree": {"portfast": "edge"}}
    
    pname = str(row.get('Port Profile', '')).strip()
    if not pname or pname.lower() == "nan": 
        return None
    
    v_raw = clean_int_str(str(row.get('Voice', '')).strip())
    a_raw = clean_int_str(str(row.get('Access', '')).strip())
    vv = int(v_raw) if v_raw and v_raw.isdigit() else None
    av = int(a_raw) if a_raw and a_raw.isdigit() else None
    
    if not av:
        m = re.search(r'A(\d+)', pname)
        if m: 
            av = int(m.group(1))
    if not vv:
        m = re.search(r'V(\d+)', pname)
        if m: 
            vv = int(m.group(1))
    
    p_mode = "trunk phone" if (vv or "V" in pname) else "access"
    p_obj = {
        "name": pname, 
        "mode": p_mode, 
        "enabled": "Yes", 
        "vlans": {}, 
        "spanningTree": {"portfast": "edge"}
    }
    
    if p_mode == "access" and av:
        p_obj["vlans"]["vlans"] = str(av)
    elif p_mode == "trunk phone":
        if av: 
            p_obj["vlans"]["nativeVlan"] = av
        if vv: 
            p_obj["vlans"]["phoneVlan"] = vv
    
    return p_obj

def get_device_tags(channel):
    stub = tag_services.TagAssignmentServiceStub(channel)
    device_tags = defaultdict(dict)
    
    for resp in stub.GetAll(tag_services.TagAssignmentStreamRequest()):
        device_id = resp.value.key.device_id.value
        label = resp.value.key.label.value
        value = resp.value.key.value.value
        device_tags[device_id][label] = value
    
    return device_tags

def find_devices_by_hostname(device_tags, hostname):
    matches = []
    for device_id, tags in device_tags.items():
        if tags.get('hostname') == hostname:
            matches.append((device_id, tags))
    return matches

def verify_pod_exists_in_tags(channel, pod_name):
    tag_stub = tag_services.TagAssignmentServiceStub(channel)
    
    device_ids = []
    
    try:
        for resp in tag_stub.GetAll(tag_services.TagAssignmentStreamRequest()):
            label = resp.value.key.label.value
            value = resp.value.key.value.value
            device_id = resp.value.key.device_id.value
            
            if label == "Access-Pod" and value == pod_name and device_id:
                if device_id not in device_ids:
                    device_ids.append(device_id)
    
    except grpc.RpcError as e:
        print(f"\n      Warning: Tag query failed: {e}")
        return False, 0, None
    
    exists = len(device_ids) > 0
    sample = device_ids[0] if device_ids else None
    
    return exists, len(device_ids), sample

def find_pod_hierarchy_from_device(channel, device_id, existing_data):
    tag_stub = tag_services.TagAssignmentServiceStub(channel)
    
    campus_name = None
    campusPod_name = None
    
    try:
        for resp in tag_stub.GetAll(tag_services.TagAssignmentStreamRequest()):
            resp_device_id = resp.value.key.device_id.value
            
            if resp_device_id == device_id:
                label = resp.value.key.label.value
                value = resp.value.key.value.value
                
                if label == "Campus":
                    campus_name = value
                elif label == "Campus-Pod":
                    campusPod_name = value
                
                if campus_name and campusPod_name:
                    break
    
    except grpc.RpcError as e:
        print(f"\n      Warning: Failed to query device tags: {e}")
        return None
    
    if not campus_name or not campusPod_name:
        return None
    
    campus_list = existing_data.get("campus", [])
    
    for c_idx, campus in enumerate(campus_list):
        campus_tag = campus.get("tags", {}).get("query", "")
        if campus_tag != f"Campus:{campus_name}":
            continue
        
        cpods = campus.get("inputs", {}).get("campusPod", [])
        for cp_idx, cpod in enumerate(cpods):
            cpod_tag = cpod.get("tags", {}).get("query", "")
            if cpod_tag != f"Campus-Pod:{campusPod_name}":
                continue
            
            apods = cpod.get("inputs", {}).get("accessPod", [])
            max_ap_idx = len(apods) - 1 if apods else -1
            next_ap_idx = max_ap_idx + 1
            
            return c_idx, cp_idx, next_ap_idx
    
    return None

def get_existing_studio_data(channel):
    
    stub = studio_services.InputsServiceStub(channel)
    
    req = studio_services.InputsStreamRequest(
        partial_eq_filter=[studio_pb2.Inputs(
            key=studio_pb2.InputsKey(
                studio_id=wrappers.StringValue(value=INTERFACE_STUDIO_ID),
                workspace_id=wrappers.StringValue(value=""),
                path=fmp_dot_wrappers__pb2.RepeatedString(values=[])
            )
        )]
    )
    
    
    for resp in stub.GetAll(req):
        if not resp.value.key.path.values:
            return json.loads(resp.value.inputs.value)
    
    return {}

def get_existing_port_profiles(existing_data):
    existing_profiles = set()
    
    top_level_profiles = existing_data.get("portProfiles", [])
    for pp in top_level_profiles:
        if pp and pp.get("name"):
            existing_profiles.add(pp["name"])
    
    campus_list = existing_data.get("campus", [])
    
    for campus in campus_list:
        for pp in campus.get("inputs", {}).get("portProfiles", []):
            if pp and pp.get("name"):
                existing_profiles.add(pp["name"])

        for cpod in campus.get("inputs", {}).get("campusPod", []):
            for pp in cpod.get("inputs", {}).get("portProfiles", []):
                if pp and pp.get("name"):
                    existing_profiles.add(pp["name"])
            
            for apod in cpod.get("inputs", {}).get("accessPod", []):
                for pp in apod.get("inputs", {}).get("portProfiles", []):
                    if pp and pp.get("name"):
                        existing_profiles.add(pp["name"])
    
    return existing_profiles

def create_workspace(channel):
    ws_id = str(uuid.uuid4())
    stub = workspace_services.WorkspaceConfigServiceStub(channel)
    stub.Set(workspace_services.WorkspaceConfigSetRequest(value=workspace_pb2.WorkspaceConfig(
        key=workspace_pb2.WorkspaceKey(workspace_id=wrappers.StringValue(value=ws_id)),
        display_name=wrappers.StringValue(value=f"Import_{ws_id[:8]}")
    )))
    return ws_id

def get_vlans_from_topology_studio(channel):
    
    TOPOLOGY_STUDIO_ID = "studio-avd-campus-fabric"
    
    stub = studio_services.InputsServiceStub(channel)
    
    req = studio_services.InputsStreamRequest(
        partial_eq_filter=[studio_pb2.Inputs(
            key=studio_pb2.InputsKey(
                studio_id=wrappers.StringValue(value=TOPOLOGY_STUDIO_ID),
                workspace_id=wrappers.StringValue(value=""),
                path=fmp_dot_wrappers__pb2.RepeatedString(values=[])
            )
        )]
    )
    
    vlans = set()
    
    for resp in stub.GetAll(req):
        if not resp.value.key.path.values:
            topology_data = json.loads(resp.value.inputs.value)
            
            campus_services = topology_data.get("campusServices", [])
            
            for service_entry in campus_services:
                service_group = service_entry.get("inputs", {}).get("campusServicesGroup", {})
                campus_pods_services = service_group.get("campusPodsServices", [])
                
                for cpod_service in campus_pods_services:
                    services = cpod_service.get("inputs", {}).get("services", {})
                    svis = services.get("svis", [])
                    
                    for svi in svis:
                        vlan_id = svi.get("id")
                        if vlan_id:
                            vlans.add(int(vlan_id))
    
    return vlans

def validate_vlans_in_topology(csv_data, topology_vlans):
    import re
    
    print_step("Validating VLANs against topology design")

    required_vlans = set()
    
    for row in csv_data:
        mode_col = str(row.get('Mode', '')).strip().lower()
        if mode_col == "trunk":
            continue
        
        for col in ['Access', 'Voice']:
            val = clean_int_str(str(row.get(col, '')).strip())
            if val and val.isdigit():
                vlan = int(val)
                if vlan != 1: 
                    required_vlans.add(vlan)
        
        profile = str(row.get('Port Profile', ''))
        for m in re.findall(r'[AV](\d+)', profile):
            vlan = int(m)
            if vlan != 1: 
                required_vlans.add(vlan)
    
    missing = required_vlans - topology_vlans
    
    if missing:
        print_done("FAILED")
        print("\n" + "!"*80)
        print("  CRITICAL VALIDATION FAILURE: MISSING VLANS IN TOPOLOGY")
        print("!"*80)
        print(f"\n  The following VLANs are not defined in the AVD Campus Topology studio:")
        print(f"  {', '.join([str(v) for v in sorted(missing)])}")
        print(f"\n  Please add these VLANs to the topology before configuring interfaces.")
        print("!"*80)
        return False
    
    print_done(f"Passed ({len(required_vlans)} VLANs verified)")
    return True

def main():
    import os
    
    print_header("ARISTA IMPORTER - ENHANCED VERSION")
    print("  ENHANCEMENT: Tag-based fallback for pods with 0 interfaces")
    
    csv_dir = "./CSV"
    CSV_FILE = None
    
    if os.path.exists(csv_dir):
        csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
        if csv_files:
            print(f"\nAvailable CSV files in {csv_dir}:")
            for i, f in enumerate(csv_files, 1):
                print(f"  [{i}] {f}")
            
            choice = input(f"\nSelect CSV file [1-{len(csv_files)}]: ").strip()
            try:
                csv_idx = int(choice) - 1
                if 0 <= csv_idx < len(csv_files):
                    CSV_FILE = os.path.join(csv_dir, csv_files[csv_idx])
                else:
                    print("Invalid choice")
                    return
            except:
                print("Invalid input")
                return
        else:
            print(f"\nNo CSV files found in {csv_dir}")
            CSV_FILE = input("Enter CSV file path: ").strip()
    else:
        print(f"\nCSV directory not found: {csv_dir}")
        CSV_FILE = input("Enter CSV file path: ").strip()
    
    if not os.path.exists(CSV_FILE):
        print(f"File not found: {CSV_FILE}")
        return
    
    print(f"\nUsing CSV: {CSV_FILE}\n")
    
    grpc_channel = get_grpc_channel()
    
    print_header("PHASE 1: DISCOVERY")
    print_step("Reading CSV")
    with open(CSV_FILE) as f:
        reader = csv.DictReader(f)
        csv_data = [row for row in reader]
    print_done(f"({len(csv_data)} rows)")
    
    print_step("Getting device tags from CloudVision")
    device_tags = get_device_tags(grpc_channel)
    print_done(f"({len(device_tags)} devices)")
    
    print("\n    Mapping CSV hostnames to devices:")
    hostname_to_device = {}
    for row in csv_data[:5]:
        hostname = str(row['New_Switch']).strip()
        if hostname not in hostname_to_device:
            matches = find_devices_by_hostname(device_tags, hostname)
            if matches:
                device_id, tags = matches[0]
                pod_name = tags.get('Access-Pod', 'Unknown')
                hostname_to_device[hostname] = (device_id, pod_name)
                print(f"      {hostname} → {device_id} (Pod: {pod_name})")
            else:
                print(f"      {hostname} → NOT FOUND")
    
    if not hostname_to_device:
        print("\n[!] ERROR: No CSV hostnames found in CloudVision tags")
        return
    
    print_header("PHASE 2: PROCESSING CSV")
    interfaces_by_pod = defaultdict(list)
    
    for row in csv_data:
        switch = str(row['New_Switch']).strip()
        port = str(row['Port']).strip()
        profile = str(row.get('Port Profile', 'TRUNK_DEFAULT')).strip()
        desc = str(row.get('Description', '')).strip()
        
        matches = find_devices_by_hostname(device_tags, switch)
        if not matches:
            continue
        
        device_id, tags = matches[0]
        pod_name = tags.get('Access-Pod', 'Unknown')
        
        interface_data = {
            "tags": {"query": f"interface:Ethernet{port}@{device_id}"},
            "inputs": {
                "adapterDetails": {
                    "portProfile": profile,
                    "enabled": "Yes",
                    "description": desc if desc else None,
                    "portChannel": {},
                    "vlans": {"vlans": None}
                }
            }
        }
        
        interfaces_by_pod[pod_name].append({
            "device_id": device_id,
            "interface_data": interface_data
        })
    
    print_done(f"Grouped {sum(len(v) for v in interfaces_by_pod.values())} interfaces into {len(interfaces_by_pod)} pods")
    
    for pod_name, interfaces in interfaces_by_pod.items():
        print(f"    {pod_name}: {len(interfaces)} interfaces")
    
    print_header("PHASE 2.5: VLAN VALIDATION")
    
    print_step("Reading VLANs from topology studio")
    topology_vlans = get_vlans_from_topology_studio(grpc_channel)
    print_done(f"({len(topology_vlans)} VLANs defined)")
    
    if not validate_vlans_in_topology(csv_data, topology_vlans):
        print("\n    Aborting due to missing VLANs")
        return
    
    print_header("PHASE 3: LOCATE POD IN STUDIO")
    
    print_step("Reading interface studio")
    existing_data = get_existing_studio_data(grpc_channel)
    print_done()

    pod_name = list(interfaces_by_pod.keys())[0]
    
    print_step(f"Finding studio pod: Access-Pod:{pod_name}")
    
    found_pods = []
    campus_list = existing_data.get("campus", [])
    
    for c, campus in enumerate(campus_list):
        cpods = campus.get("inputs", {}).get("campusPod", [])
        for cp, cpod in enumerate(cpods):
            apods = cpod.get("inputs", {}).get("accessPod", [])
            for ap, apod in enumerate(apods):
                apod_tag = apod.get("tags", {}).get("query", "")
                interface_count = len(apod.get("inputs", {}).get("interfaces", []))
                
                if apod_tag == f"Access-Pod:{pod_name}":
                    found_pods.append({
                        "location": (c, cp, ap),
                        "pod_name": pod_name,
                        "total_interfaces": interface_count,
                        "tag": apod_tag
                    })
    
    if not found_pods:
        print_done("Not found in studio")
        
        print(f"\n    ⚠ Pod 'Access-Pod:{pod_name}' not found in studio inputs")
        print(f"    This usually means the pod has 0 configured interfaces")
        
        print_step("Checking if pod exists via device tags")
        exists, device_count, sample_device = verify_pod_exists_in_tags(grpc_channel, pod_name)
        
        if not exists:
            print_done("FAILED")
            print(f"\n[!] ERROR: Pod '{pod_name}' does not exist in device tags either!")
            print(f"    No devices have the tag 'Access-Pod:{pod_name}'")
            print(f"    Please verify the pod name is correct.")
            return
        
        print_done(f"Found ({device_count} devices)")
        
        print(f"\n    ✓ Pod exists in device tags: {device_count} devices tagged")
        print(f"    ⚙ Pod has 0 configured interfaces - will start from interface index 0")
        
        print_step("Determining pod hierarchy from device tags")
        hierarchy = find_pod_hierarchy_from_device(grpc_channel, sample_device, existing_data)
        
        if not hierarchy:
            print_done("FAILED")
            print(f"\n[!] ERROR: Could not determine campus/campusPod location for this pod")
            print(f"    Tried querying Campus and Campus-Pod tags from device {sample_device}")
            print(f"    Please verify device tags are correct.")
            return
        
        c_idx, cp_idx, ap_idx = hierarchy
        print_done(f"campus/{c_idx}/campusPod/{cp_idx}/accessPod/{ap_idx}")
        
        found_pods = [{
            "location": (c_idx, cp_idx, ap_idx),
            "pod_name": pod_name,
            "total_interfaces": 0,
            "tag": f"Access-Pod:{pod_name}",
            "needs_creation": True 
        }]
        
        print(f"\n    ✓ Will create pod at: campus/{c_idx}/campusPod/{cp_idx}/accessPod/{ap_idx}")
        print(f"    ✓ Starting interface count: 0")
    else:
        print_done(f"({len(found_pods)} found)")
    
    print(f"\n    Pods with exact tag 'Access-Pod:{pod_name}':")
    for pod in found_pods:
        c, cp, ap = pod["location"]
        status = " [WILL CREATE]" if pod.get("needs_creation") else ""
        print(f"      campus/{c}/campusPod/{cp}/accessPod/{ap}: {pod['tag']} ({pod['total_interfaces']} interfaces){status}")
    
    if len(found_pods) > 1:
        print(f"\n    ⚠ Multiple pods found with the same tag!")
        for i, pod in enumerate(found_pods, 1):
            c, cp, ap = pod["location"]
            print(f"    [{i}] campus/{c}/campusPod/{cp}/accessPod/{ap}")
            print(f"        Existing interfaces: {pod['total_interfaces']}")
        
        choice = input(f"\n    Which pod? [1-{len(found_pods)}]: ").strip()
        try:
            pod_idx = int(choice) - 1
            if pod_idx < 0 or pod_idx >= len(found_pods):
                print("Invalid choice")
                return
        except:
            print("Invalid input")
            return
    else:
        pod_idx = 0
    
    found_pod = found_pods[pod_idx]
    
    c_idx, cp_idx, ap_idx = found_pod["location"]
    existing_count = found_pod["total_interfaces"]
    needs_creation = found_pod.get("needs_creation", False)
    
    print(f"\n    ✓ Target: {found_pod['pod_name']}")
    print(f"    ✓ Location: campus/{c_idx}/campusPod/{cp_idx}/accessPod/{ap_idx}")
    print(f"    ✓ Current interfaces: {existing_count}")
    print(f"    ✓ Will add: {len(interfaces_by_pod[pod_name])} interfaces")
    
    if existing_count > 0:
        print(f"\n    ⚠ WARNING: This pod already has {existing_count} interfaces!")
        print(f"    ⚠ New interfaces will be ADDED (not replaced)")
        confirm = input(f"    Continue? (y/n): ").strip().lower()
        if confirm != 'y':
            print("    Aborted")
            return
    elif needs_creation:
        print(f"\n    ℹ This pod will be created with its first interfaces")
        confirm = input(f"    Continue? (y/n): ").strip().lower()
        if confirm != 'y':
            print("    Aborted")
            return
    
    print_header("PHASE 3.5: WORKSPACE")
    
    print_step("Creating workspace")
    ws_id = create_workspace(grpc_channel)
    print_done(f"({ws_id[:8]})")

    print_step("Subscribing to workspace")
    try:
        ws_stub = workspace_services.WorkspaceServiceStub(grpc_channel)
        sub_req = workspace_services.WorkspaceStreamRequest(
            partial_eq_filter=[workspace_pb2.Workspace(
                key=workspace_pb2.WorkspaceKey(workspace_id=wrappers.StringValue(value=ws_id))
            )]
        )
        subscription = ws_stub.Subscribe(sub_req)
        try:
            next(subscription)
        except:
            pass
    except Exception as e:
        print(f"(warning: {e})")
    print_done()

    if needs_creation:
        print_header("PHASE 3.6: CREATE POD STRUCTURE")
        
        print_step(f"Creating Access-Pod structure at accessPod/{ap_idx}")
        
        config_stub = studio_services.InputsConfigServiceStub(grpc_channel)
        
        pod_structure = {
            "tags": {"query": f"Access-Pod:{pod_name}"},
            "inputs": {
                "interfaces": []
        }
        
        path_values = [
            "campus", str(c_idx), "inputs",
            "campusPod", str(cp_idx), "inputs",
            "accessPod", str(ap_idx)
        ]
        
        json_request = json.dumps({
            "values": [{
                "remove": False,
                "inputs": json.dumps(pod_structure),
                "key": {
                    "studioId": INTERFACE_STUDIO_ID,
                    "workspaceId": ws_id,
                    "path": {"values": path_values}
                }
            }]
        })
        
        req = Parse(json_request, studio_services.InputsConfigSetSomeRequest(), False)
        
        for response in config_stub.SetSome(req, timeout=30):
            pass
        
        print_done()
    
    print_header("PHASE 3.7: PORT PROFILES")
    
    print_step("Building port profiles from CSV")
    profiles_map = {}
    for row in csv_data:
        profile_obj = build_profile_object(row)
        if profile_obj and profile_obj['name'] not in profiles_map:
            profiles_map[profile_obj['name']] = profile_obj
    
    print_done(f"({len(profiles_map)} unique profiles)")
    
    if profiles_map:
        print_step("Checking existing profiles in studio")
        existing_profiles = get_existing_port_profiles(existing_data)
        print_done(f"({len(existing_profiles)} existing)")
        
        new_profiles = set(profiles_map.keys()) - existing_profiles
        
        if new_profiles:
            print(f"\n    Creating {len(new_profiles)} new port profiles:")
            for pname in sorted(new_profiles)[:5]:
                print(f"      {pname}")
            if len(new_profiles) > 5:
                print(f"      ... and {len(new_profiles)-5} more")
            
            config_stub = studio_services.InputsConfigServiceStub(grpc_channel)
            profile_idx = len(existing_profiles) 
            
            for profile_name in sorted(new_profiles):
                profile_obj = profiles_map[profile_name]
                
                path_values = [
                    "portProfiles", str(profile_idx)
                ]
                
                json_request = json.dumps({
                    "values": [{
                        "remove": False,
                        "inputs": json.dumps(profile_obj),
                        "key": {
                            "studioId": INTERFACE_STUDIO_ID,
                            "workspaceId": ws_id,
                            "path": {"values": path_values}
                        }
                    }]
                })
                
                req = Parse(json_request, studio_services.InputsConfigSetSomeRequest(), False)
                
                for response in config_stub.SetSome(req, timeout=30):
                    pass
                
                profile_idx += 1
            
            print(f"    ✓ Created {len(new_profiles)} port profiles")
        else:
            print(f"    ✓ All profiles already exist")
    
    print_header("PHASE 4: WRITE INTERFACES")
    
    total_written = 0
    config_stub = studio_services.InputsConfigServiceStub(grpc_channel)
    
    for target_pod_name, interfaces in interfaces_by_pod.items():
        print(f"\n  Writing {len(interfaces)} interfaces to {target_pod_name}")
        print(f"    Target: campus/{c_idx}/campusPod/{cp_idx}/accessPod/{ap_idx}")
        print(f"    Starting from interface index: {existing_count}")
        
        devices = {}
        for iface in interfaces:
            dev = iface["device_id"]
            devices[dev] = devices.get(dev, 0) + 1
        print(f"    Devices:")
        for dev_id, count in sorted(devices.items()):
            print(f"      {dev_id}: {count} interfaces")
        
        print(f"\n    Writing...")
        
        for idx, iface in enumerate(interfaces):
            interface_data = iface["interface_data"]
            
            interface_idx = existing_count + idx
            
            path_values = [
                "campus", str(c_idx), "inputs",
                "campusPod", str(cp_idx), "inputs",
                "accessPod", str(ap_idx), "inputs",
                "interfaces", str(interface_idx)
            ]
            
            json_request = json.dumps({
                "values": [{
                    "remove": False,
                    "inputs": json.dumps(interface_data),
                    "key": {
                        "studioId": INTERFACE_STUDIO_ID,
                        "workspaceId": ws_id,
                        "path": {"values": path_values}
                    }
                }]
            })
            
            req = Parse(json_request, studio_services.InputsConfigSetSomeRequest(), False)
            
            for response in config_stub.SetSome(req, timeout=30):
                pass
            
            total_written += 1
        
        print(f"    ✓ Complete ({len(interfaces)} interfaces)")
    
    print(f"\n  Total: {total_written} interfaces written")
    
    print_header("PHASE 5: BUILD WORKSPACE")
    
    print_step("Triggering build")
    ws_stub = workspace_services.WorkspaceConfigServiceStub(grpc_channel)
    ws_stub.Set(workspace_services.WorkspaceConfigSetRequest(value=workspace_pb2.WorkspaceConfig(
        key=workspace_pb2.WorkspaceKey(workspace_id=wrappers.StringValue(value=ws_id)), 
        request=1,
        request_params=workspace_pb2.RequestParams(request_id=wrappers.StringValue(value=str(uuid.uuid4())))
    )))
    print_done()
    
    print_header("SUCCESS")
    print(f"  Workspace: https://{CV_ADDR}/cv/provisioning/workspaces?ws={ws_id}")
    print(f"  Interfaces written: {total_written}")
    print(f"\n  The workspace has been automatically built!")
    print(f"  Open the workspace URL to review and submit changes.")

if __name__ == "__main__":
    main()