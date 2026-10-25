import streamlit as st
import json
import os
import time
import statistics
import socket
from pathlib import Path
import plotly.express as px

# ---------------------------
# Latency Measurement Helpers
# ---------------------------
def measure_tcp_latency(host: str, port: int = 443, trials: int = 5, timeout: float = 2.0):
    results = []
    for _ in range(trials):
        start = time.time()
        try:
            with socket.create_connection((host, port), timeout=timeout):
                elapsed = (time.time() - start) * 1000
                results.append(elapsed)
        except:
            results.append(float("inf"))
        time.sleep(0.1)
    finite = [r for r in results if r != float("inf")]
    return statistics.median(finite) if finite else float("inf")

def replicate_locally(file_path: str, datacenter_name: str, base_dir: str = "./replicated"):
    dest_dir = Path(base_dir) / datacenter_name
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / Path(file_path).name
    with open(file_path, "rb") as src, open(dest_path, "wb") as dst:
        dst.write(src.read())
    return str(dest_path)

# ---------------------------
# Streamlit UI
# ---------------------------
st.set_page_config(page_title="Latency-Aware Cloud Data Replication", layout="wide")

st.title("🌐 Latency-Aware Cloud Data Replication System")
st.markdown("Measure latency to multiple datacenters and replicate data to the fastest one automatically.")

# File upload section
uploaded_file = st.file_uploader("📁 Upload a file to replicate", type=None)
config_file = st.file_uploader("⚙️ Upload datacenter configuration (JSON)", type=["json"])

if uploaded_file and config_file:
    # Save files temporarily
    file_path = Path("uploaded") / uploaded_file.name
    file_path.parent.mkdir(exist_ok=True)
    with open(file_path, "wb") as f:
        f.write(uploaded_file.read())

    datacenters = json.load(config_file)
    if not isinstance(datacenters, list):
        st.error("❌ Invalid JSON format. Must be a list of datacenter objects.")
        st.stop()

    st.success(f"✅ File and configuration loaded successfully!")
    st.write("Datacenters to test:")
    st.json(datacenters)

    if st.button("🚀 Start Latency Test and Replicate"):
        results = []
        with st.spinner("Measuring latency..."):
            for dc in datacenters:
                name = dc.get("name", "Unknown")
                host = dc.get("host")
                if not host:
                    results.append({"Datacenter": name, "Latency (ms)": float("inf")})
                    continue
                latency = measure_tcp_latency(host)
                results.append({"Datacenter": name, "Latency (ms)": latency})

        df_data = {r["Datacenter"]: r["Latency (ms)"] for r in results}
        best_dc = min(results, key=lambda x: x["Latency (ms)"])
        best_name = best_dc["Datacenter"]
        best_latency = best_dc["Latency (ms)"]

        # Plot latency chart
        fig = px.bar(
            results,
            x="Datacenter",
            y="Latency (ms)",
            title="Datacenter Latency Comparison",
            color="Latency (ms)",
            color_continuous_scale="Blues"
        )
        st.plotly_chart(fig, use_container_width=True)

        # Replicate file
        if best_latency == float("inf"):
            st.error("❌ All datacenters unreachable. Replication aborted.")
        else:
            st.info(f"🌍 Fastest Datacenter: **{best_name}** ({best_latency:.2f} ms)")
            replicated_path = replicate_locally(file_path, best_name)
            st.success(f"✅ File replicated successfully to `{replicated_path}`")

else:
    st.warning("⚠️ Please upload both a file and a datacenter configuration JSON to continue.")
