"""
================================================================================
ARISTA CLOUDVISION BULK IMPORTER - VERISION 1.4 (MERGE + VLAN CHECK)
================================================================================

DESCRIPTION:
    This production-ready script automates the provisioning of Campus Access 
    Interfaces in CloudVision.

    It reads a CSV of port mappings, validates that all required VLANs exist 
    on the target switches, merges the new configuration with 
    any existing Studio data, creates a new workspace and builds the changes.
    Changes need to be reviewed and accepted then pushed via a Change Control

HOW IT WORKS:
    1. Discovery: Maps device hostnames to UUIDs and learns the full Studio 
       Topology (scanning both Fabric and Interface studios).
    2. Validation: Scrapes the running configuration of every target switch 
       via the CVP API to build a real-time VLAN database. Aborts if missing 
       VLANs are detected.
    3. Structure Merge: Downloads the current Interface Studio configuration 
       and merges it with the Fabric Skeleton to ensure no existing ports 
       are overwritten (Read-Modify-Write).
    4. Provisioning: Injects CSV data into the merged tree and auto-generates 
       smart Port Profiles (Trunk/Phone/Access).
    5. Execution: Creates a Workspace, pushes the atomic payload, and 
       triggers the Build.

USAGE:
    1. Ensure the 'CSV' folder exists and contains your interface mapping files.
    2. Install dependencies: `pip install -r requirements.txt`
    3. Update 'CV_ADDR' and 'CV_TOKEN' in the Configuration section.
    4. Run: python access_int_vlan_check_on-prem.py
    5. Select your file from the menu.

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

import sys, os, grpc, uuid, json, re, ssl, urllib3, time
from collections import defaultdict

try:
    import pandas as pd
    from cvprac.cvp_client import CvpClient
    from google.protobuf import wrappers_pb2 as wrappers
    from arista.workspace.v1 import services as workspace_services
    from arista.workspace.v1 import workspace_pb2
    from arista.studio.v1 import services as studio_services
    from arista.studio.v1 import studio_pb2
    from arista.inventory.v1 import services as inventory_services
    from arista.tag.v2 import services as tag_services
except ImportError as e:
    print(f"\n[!] Missing Dependency: {e.name}")
    print("    Run: pip install -r requirements.txt")
    sys.exit(1)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# CONFIGURATION
# ==========================================
CV_TOKEN = "TOKEN"  # Replace with actual service token
CV_ADDR = "CVP_IP" 
INTERFACE_STUDIO_ID = "studio-campus-access-interfaces"
FABRIC_STUDIO_ID = "studio-avd-campus-fabric"
CSV_FOLDER_NAME = "CSV"

# ==========================================
# UI HELPERS
# ==========================================
def print_header(text):
    print(f"\n{'='*80}\n  {text}\n{'='*80}")

def print_step(text):
    print(f"  [i] {text}...", end=" ", flush=True)

def print_done(text="Done"):
    print(f"{text}")

def print_fail(text):
    print(f"\n  [!] {text}")

def select_csv_file():
    if not os.path.exists(CSV_FOLDER_NAME): return None
    files = [f for f in os.listdir(CSV_FOLDER_NAME) if f.lower().endswith('.csv')]
    if not files: return None
    print("\n--- Available Files ---")
    for idx, f in enumerate(files): print(f" {idx + 1}. {f}")
    try:
        choice = int(input("\nSelect file #: "))
        if 1 <= choice <= len(files): return os.path.join(CSV_FOLDER_NAME, files[choice - 1])
    except: pass
    return None

def clean_int_str(val):
    if "." in val: return val.split(".")[0]
    return val

# ==========================================
# CONNECTIONS
# ==========================================
def get_grpc_channel():
    cert = ssl.get_server_certificate((CV_ADDR, 443))
    creds = grpc.ssl_channel_credentials(root_certificates=cert.encode())
    return grpc.secure_channel(f"{CV_ADDR}:443", grpc.composite_channel_credentials(creds, grpc.access_token_call_credentials(CV_TOKEN)))

def get_cvp_client():
    clnt = CvpClient()
    try:
        clnt.connect(nodes=[CV_ADDR], username='', password='', api_token=CV_TOKEN, is_cvaas=False)
        return clnt
    except Exception as e:
        print_fail(f"REST Connection failed: {e}")
        sys.exit(1)

# ==========================================
# DISCOVERY & MAPPING
# ==========================================
def get_inventory_map(channel):
    stub = inventory_services.DeviceServiceStub(channel)
    mapping = {}
    try:
        for resp in stub.GetAll(inventory_services.DeviceStreamRequest()):
            if resp.value.hostname.value:
                mapping[resp.value.hostname.value] = resp.value.key.device_id.value
    except: pass
    return mapping

def get_rest_inventory_map(clnt):
    try:
        inv = clnt.api.get_inventory()
        return {d['hostname']: d['systemMacAddress'] for d in inv if 'systemMacAddress' in d}
    except: return {}

def get_pod_map(channel):
    stub = tag_services.TagAssignmentServiceStub(channel)
    req = tag_services.TagAssignmentStreamRequest()
    mapping = {}
    for resp in stub.GetAll(req):
        if resp.value.key.label.value == "Access-Pod":
            mapping[resp.value.key.device_id.value] = resp.value.key.value.value
    return mapping

def get_topology_tags(channel):
    stub = tag_services.TagAssignmentServiceStub(channel)
    req = tag_services.TagAssignmentStreamRequest()
    topo_map = defaultdict(dict)
    relevant = {"Campus", "Campus-Pod", "Access-Pod"}
    for resp in stub.GetAll(req):
        if resp.value.key.label.value in relevant:
            topo_map[resp.value.key.device_id.value][resp.value.key.label.value] = resp.value.key.value.value
    return topo_map

# ==========================================
# VLAN VALIDATION
# ==========================================
def build_switch_config_db(clnt, target_hostnames, rest_map):
    print_step(f"Scraping VLAN DB from {len(target_hostnames)} switches")
    config_db = {}
    
    for host in target_hostnames:
        if host not in rest_map: continue
        mac = rest_map[host]
        try:
            config_text = clnt.api.get_device_configuration(mac)
            vlans = set()
            matches = re.findall(r'^vlan\s+(\d+)', config_text, re.MULTILINE)
            for m in matches: vlans.add(int(m))
            config_db[host] = vlans
        except: 
            pass
    print_done()
    return config_db

def validate_requirements(df, config_db, topo_map, uuid_map):
    print_step("Validating VLAN existence")
    missing_by_location = defaultdict(set)
    has_errors = False
    
    for _, row in df.iterrows():
        host = str(row['New_Switch']).strip()
        
        mode_col = str(row.get('Mode', '')).strip().lower()
        if mode_col == "trunk": continue

        required = set()
        for col in ['Access', 'Voice']:
            val = clean_int_str(str(row.get(col, '')).strip())
            if val.isdigit(): required.add(int(val))
            
        profile = str(row.get('Port Profile', ''))
        for m in re.findall(r'[AV](\d+)', profile): required.add(int(m))
        
        if not required: continue
        if host not in config_db: continue
            
        deployed = config_db[host]
        missing = required - deployed
        
        if missing:
            has_errors = True
            loc_str = "Unknown Location"
            if host in uuid_map:
                dev_id = uuid_map[host]
                if dev_id in topo_map:
                    tags = topo_map[dev_id]
                    c = tags.get('Campus', '?')
                    p = tags.get('Campus-Pod', '?')
                    loc_str = f"{host} (Campus: {c} / Pod: {p})"
            
            for v in missing: missing_by_location[loc_str].add(v)

    if has_errors:
        print_done("FAILED")
        print("\n" + "!"*80)
        print("  CRITICAL VALIDATION FAILURE: MISSING VLANS")
        print("!"*80)
        for loc, vls in sorted(missing_by_location.items()):
            v_list = ', '.join([str(v) for v in sorted(list(vls))])
            print(f"  NODE: {loc}")
            print(f"  MISSING: {v_list}")
            print(f"  {'-'*40}")
        return False
    
    print_done("Passed")
    return True

# ==========================================
# STRUCTURAL LOGIC
# ==========================================
def build_simple_structure_from_fabric(channel):
    print_step("Reading Fabric Structure")
    stub = studio_services.InputsServiceStub(channel)
    req = studio_services.InputsStreamRequest(
        partial_eq_filter=[studio_pb2.Inputs(key=studio_pb2.InputsKey(studio_id=wrappers.StringValue(value=FABRIC_STUDIO_ID)))]
    )
    for resp in stub.GetAll(req):
        if not resp.value.key.path.values:
            fabric_data = json.loads(resp.value.inputs.value)
            print_done()

            simple_campus_list = []
            for fabric_campus in fabric_data.get("campus", []):
                simple_campus = {"tags": fabric_campus.get("tags"), "inputs": {"campusPod": []}}
                for fabric_cpod in fabric_campus.get("inputs", {}).get("campusDetails", {}).get("campusPod", []):
                    simple_cpod = {"tags": fabric_cpod.get("tags"), "inputs": {"accessPod": []}}
                    for fabric_apod in fabric_cpod.get("inputs", {}).get("campusPodFacts", {}).get("accessPods", []):
                        simple_apod = {"tags": fabric_apod.get("tags"), "inputs": {"interfaces": []}}
                        simple_cpod["inputs"]["accessPod"].append(simple_apod)
                    simple_campus["inputs"]["campusPod"].append(simple_cpod)
                simple_campus_list.append(simple_campus)
            return simple_campus_list
    print_done("(No fabric found)")
    return []

def get_existing_studio_data(channel, ws_id=""):
    stub = studio_services.InputsServiceStub(channel)
    key = studio_pb2.InputsKey(studio_id=wrappers.StringValue(value=INTERFACE_STUDIO_ID), path=studio_pb2.fmp_dot_wrappers__pb2.RepeatedString(values=[]))
    if ws_id: key.workspace_id.value = ws_id
        
    req = studio_services.InputsStreamRequest(partial_eq_filter=[studio_pb2.Inputs(key=key)])
    for resp in stub.GetAll(req):
        if not resp.value.key.path.values:
            return json.loads(resp.value.inputs.value)
    return None

def merge_structures(existing_campus, fabric_campus):
    import copy
    merged = copy.deepcopy(existing_campus)
    existing_campus_tags = {c.get("tags", {}).get("query"): c_idx for c_idx, c in enumerate(merged)}
    
    for fabric_c in fabric_campus:
        f_c_tag = fabric_c.get("tags", {}).get("query")
        if f_c_tag in existing_campus_tags:
            c_idx = existing_campus_tags[f_c_tag]
            existing_c = merged[c_idx]
            ex_cpod_tags = {cp.get("tags", {}).get("query"): idx for idx, cp in enumerate(existing_c.get("inputs", {}).get("campusPod", []))}
            
            for fabric_cp in fabric_c.get("inputs", {}).get("campusPod", []):
                f_cp_tag = fabric_cp.get("tags", {}).get("query")
                if f_cp_tag in ex_cpod_tags:
                    cp_idx = ex_cpod_tags[f_cp_tag]
                    existing_cp = existing_c["inputs"]["campusPod"][cp_idx]
                    ex_apod_tags = {ap.get("tags", {}).get("query"): idx for idx, ap in enumerate(existing_cp.get("inputs", {}).get("accessPod", []))}
                    
                    for fabric_ap in fabric_cp.get("inputs", {}).get("accessPod", []):
                        f_ap_tag = fabric_ap.get("tags", {}).get("query")
                        if f_ap_tag not in ex_apod_tags:
                            existing_cp["inputs"]["accessPod"].append(copy.deepcopy(fabric_ap))
                else:
                    existing_c["inputs"]["campusPod"].append(copy.deepcopy(fabric_cp))
        else:
            merged.append(copy.deepcopy(fabric_c))
    return merged

def build_pod_location_map(campus_list):
    loc_map = {}
    for c_idx, c in enumerate(campus_list):
        for cp_idx, cp in enumerate(c.get("inputs", {}).get("campusPod", [])):
            for ap_idx, ap in enumerate(cp.get("inputs", {}).get("accessPod", [])):
                q = ap.get("tags", {}).get("query", "")
                if "Access-Pod:" in q:
                    name = q.split("Access-Pod:")[-1].strip()
                    loc_map[name] = (c_idx, cp_idx, ap_idx)
    return loc_map

# ==========================================
# MAIN EXECUTION
# ==========================================
def main():
    print_header("ARISTA IMPORTER V72 (FIELD-LEVEL MERGE)")
    
    csv_file = select_csv_file()
    if not csv_file: return
    
    grpc_channel = get_grpc_channel()
    rest_client = get_cvp_client()

    print_header("PHASE 1: DISCOVERY")
    
    uuid_map = get_inventory_map(grpc_channel)
    rest_map = get_rest_inventory_map(rest_client)
    pod_map = get_pod_map(grpc_channel)
    topo_map = get_topology_tags(grpc_channel)
    
    print_step("Reading CSV")
    df = pd.read_csv(csv_file).fillna("")
    print_done(f"({len(df)} rows)")

    unique_hosts = set(df['New_Switch'].dropna().unique())
    config_db = build_switch_config_db(rest_client, unique_hosts, rest_map)
    
    if not validate_requirements(df, config_db, topo_map, uuid_map):
        print_fail("Aborting due to VLAN errors.")
        return

    print_header("PHASE 2: PROCESSING")
    
    interface_payloads = defaultdict(list)
    new_profiles = {}
    
    count = 0
    
    def build_profile_object(row):
        mode = str(row.get('Mode', '')).strip().lower()
        if mode == "trunk": return {"name": "TRUNK_DEFAULT", "mode": "trunk", "enabled": "Yes", "vlans": {}, "spanningTree": {"portfast": "edge"}}
        
        pname = str(row.get('Port Profile', '')).strip()
        if not pname or pname.lower() == "nan": return None
        
        v_raw = clean_int_str(str(row.get('Voice', '')).strip())
        a_raw = clean_int_str(str(row.get('Access', '')).strip())
        vv = int(v_raw) if v_raw.isdigit() else None
        av = int(a_raw) if a_raw.isdigit() else None
        
        if not av:
             m = re.search(r'A(\d+)', pname)
             if m: av = int(m.group(1))
        if not vv:
             m = re.search(r'V(\d+)', pname)
             if m: vv = int(m.group(1))
             
        p_mode = "trunk phone" if (vv or "V" in pname) else "access"
        p_obj = {"name": pname, "mode": p_mode, "enabled": "Yes", "vlans": {}, "spanningTree": {"portfast": "edge"}}
        if p_mode == "access" and av: p_obj["vlans"]["vlans"] = str(av)
        elif p_mode == "trunk phone":
             if av: p_obj["vlans"]["nativeVlan"] = av
             if vv: p_obj["vlans"]["phoneVlan"] = vv
        return p_obj

    for _, row in df.iterrows():
        host = str(row['New_Switch']).strip()
        u_id = uuid_map.get(host)
        pod_name = pod_map.get(u_id)
        
        if u_id and pod_name:
            p_obj = build_profile_object(row)
            if p_obj: new_profiles[p_obj['name']] = p_obj
            
            p_raw = clean_int_str(str(row['Port']).strip())
            p_name = f"Ethernet{p_raw}" if p_raw.isdigit() else p_raw
            prof_name = str(row.get('Port Profile', 'TRUNK_DEFAULT')).strip()
            
            new_port = {
                "tags": {"query": f"interface:{p_name}@{u_id}"},
                "inputs": {
                    "adapterDetails": {
                        "portProfile": prof_name,
                        "description": str(row.get('Description', '')),
                        "enabled": "Yes"
                    }
                }
            }
            interface_payloads[pod_name].append(new_port)
            count += 1
            
    print_done(f"Staged {count} interfaces")

    fabric_struct = build_simple_structure_from_fabric(grpc_channel)
    existing_data = get_existing_studio_data(grpc_channel)
    
    if existing_data and existing_data.get("campus"):
        print_step("Merging Existing + Fabric Structures")
        working_campus = merge_structures(existing_data.get("campus"), fabric_struct)
        print_done()
    else:
        print_step("Initializing Structure from Fabric")
        import copy
        working_campus = copy.deepcopy(fabric_struct)
        print_done()

    print_step("Injecting Configs (Field-Level Merge)")
    loc_map = build_pod_location_map(working_campus)
    
    injected = 0
    updated = 0
    added = 0
    
    for pod_name, new_ifaces in interface_payloads.items():
        if pod_name not in loc_map:
            print(f"\n  [!] Warning: Pod '{pod_name}' found in Tags but missing in Studio Topology")
            continue
        
        c, cp, ap = loc_map[pod_name]
        target_pod = working_campus[c]["inputs"]["campusPod"][cp]["inputs"]["accessPod"][ap]
        
        current_list = target_pod.get("inputs", {}).get("interfaces", [])
        if_map = {i["tags"]["query"]: i for i in current_list}
        
        for ni in new_ifaces:
            tag_query = ni["tags"]["query"]
            
            if tag_query in if_map:
                existing_iface = if_map[tag_query]
                new_details = ni.get("inputs", {}).get("adapterDetails", {})
                
                if "inputs" not in existing_iface:
                    existing_iface["inputs"] = {}
                if "adapterDetails" not in existing_iface["inputs"]:
                    existing_iface["inputs"]["adapterDetails"] = {}
                
                existing_details = existing_iface["inputs"]["adapterDetails"]
                if "portProfile" in new_details:
                    existing_details["portProfile"] = new_details["portProfile"]
                if "description" in new_details:
                    existing_details["description"] = new_details["description"]
                if "enabled" in new_details:
                    existing_details["enabled"] = new_details["enabled"]
                
                updated += 1
            else:
                if_map[tag_query] = ni
                added += 1
            
        target_pod["inputs"]["interfaces"] = list(if_map.values())
        injected += 1
        
    print_done(f"({injected} pods, {updated} updated, {added} added)")

    print_step("Merging port profiles")
    if existing_data:
        ex_profs = {p["name"]: p for p in existing_data.get("portProfiles", [])}
        new_count = 0
        for pname, pobj in new_profiles.items():
            if pname not in ex_profs:
                ex_profs[pname] = pobj
                new_count += 1
        final_profiles = list(ex_profs.values())
        print_done(f"({new_count} new, {len(ex_profs)} total)")
    else:
        final_profiles = list(new_profiles.values())
        print_done(f"({len(final_profiles)} new)")

    print_header("PHASE 3: PUSH")
    ws_id = str(uuid.uuid4())
    print_step(f"Creating Workspace {ws_id}")
    ws_stub = workspace_services.WorkspaceConfigServiceStub(grpc_channel)
    ws_stub.Set(workspace_services.WorkspaceConfigSetRequest(value=workspace_pb2.WorkspaceConfig(
        key=workspace_pb2.WorkspaceKey(workspace_id=wrappers.StringValue(value=ws_id)),
        display_name=wrappers.StringValue(value=f"Imp_V72_{ws_id[:4]}")
    )))
    print_done()

    print_step("Filtering campus structure")
    pods_with_csv_data = set(interface_payloads.keys())
    
    filtered_campus = []
    for campus in working_campus:
        filtered_c = {"tags": campus["tags"], "inputs": {"campusPod": []}}
        
        for cpod in campus.get("inputs", {}).get("campusPod", []):
            filtered_cp = {"tags": cpod["tags"], "inputs": {"accessPod": []}}
            
            for apod in cpod.get("inputs", {}).get("accessPod", []):
                ap_tag = apod.get("tags", {}).get("query", "")
                if "Access-Pod:" in ap_tag:
                    pod_name = ap_tag.split("Access-Pod:")[-1].strip()
                    
                    if pod_name in pods_with_csv_data:
                        filtered_cp["inputs"]["accessPod"].append(apod)
            
            if filtered_cp["inputs"]["accessPod"]:
                filtered_c["inputs"]["campusPod"].append(filtered_cp)
        
        if filtered_c["inputs"]["campusPod"]:
            filtered_campus.append(filtered_c)
    
    print_done(f"({len(pods_with_csv_data)} pods)")

    print_step("Uploading Configuration")
    cfg_stub = studio_services.InputsConfigServiceStub(grpc_channel)
    final_data = {"campus": filtered_campus, "portProfiles": final_profiles}
    
    cfg_stub.Set(studio_services.InputsConfigSetRequest(value=studio_pb2.InputsConfig(
        key=studio_pb2.InputsKey(
            workspace_id=wrappers.StringValue(value=ws_id),
            studio_id=wrappers.StringValue(value=INTERFACE_STUDIO_ID),
            path=studio_pb2.fmp_dot_wrappers__pb2.RepeatedString(values=[])
        ),
        inputs=wrappers.StringValue(value=json.dumps(final_data))
    )))
    print_done()

    print_step("Triggering Build")
    ws_stub.Set(workspace_services.WorkspaceConfigSetRequest(value=workspace_pb2.WorkspaceConfig(
        key=workspace_pb2.WorkspaceKey(workspace_id=wrappers.StringValue(value=ws_id)), 
        request=1, 
        request_params=workspace_pb2.RequestParams(request_id=wrappers.StringValue(value=str(uuid.uuid4())))
    )))
    print_done()

    print_header("SUCCESS")
    print(f"  Workspace: https://{CV_ADDR}/cv/provisioning/workspaces?ws={ws_id}")
    print(f"  Updated: {updated} existing interfaces")
    print(f"  Added: {added} new interfaces")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()