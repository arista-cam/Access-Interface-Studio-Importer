# Arista CloudVision Bulk Interface Importer

This project provides a pair of Python automation scripts designed to bulk-provision campus access interfaces in Arista CloudVision using the **Campus Access Interfaces** Studio.

### The Two Versions:
1.  **`access_int_vlan_check.py`:**
    * Connects to individual switches via REST (cvprac) to verify that all VLANs defined in your CSV actually exist on the devices.
    * Aborts if a missing VLAN is detected to prevent configuration errors.
2.  **`access_int.py`:**
    * Skips all VLAN validation and config scraping.
    * Ideal for lab testing or when you are 100% sure the underlying fabric is ready.

---

## 🛠️ Prerequisites

* **Python 3.8+**
* **CloudVision Service Account Token** (with API write access).
* **Arista CV APIs** installed on your local machine.

### Installation

Clone the repo and install the dependencies based on which version you intend to use:

```bash
pip install -r requirements.txt
```
### 📊 CSV Structure

The scripts expect a `.csv` file located in a folder named `/CSV`. The file must follow this header format:

| New_Switch | Port | Mode | Port Profile | Access | Voice | Description |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Leaf-01 | 1 | Access | A10 | 10 | | User Port |
| Leaf-01 | 2 | Access | A10-V20 | 10 | 20 | Phone + PC |
| Leaf-02 | 48 | Trunk | UPLINK | 1 | | Core Link |

---

### 📖 Usage

1.  Place your interface mapping CSVs into the `/CSV` directory.
2.  Open the script and update the `CV_ADDR` and `CV_TOKEN` variables.
3.  Run the script: `acces_int_vlan_check.py`
4.  Select the desired CSV file from the interactive menu.
5.  Follow the link provided at the end to view your generated **CloudVision Workspace**.

---

### ⚠️ Troubleshooting: cEOS & Labs

Arista Studios often perform capability checks on device models. Because `cEOS` (containerized EOS) does not technically support hardware features like "Phone VLANs" or "PoE," the Studio may refuse to generate configuration commands.

**The Fix:**
Inside your CloudVision Studio, go to **Advanced Settings > Custom Platform Settings** and add the following YAML to force feature support for your lab:

```yaml
custom_platform_settings:
  - platforms:
      - cEOS.*
    feature_support:
      phone: true
      poe: true
