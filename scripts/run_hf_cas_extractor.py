"""
Streamlit app for Hugging Face Transformers CAS extraction (Prompts 9 + 11).
Run: streamlit run scripts/run_hf_cas_extractor.py
Requires: pip install -r requirements_hf.txt (or see repo root)
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

# Run from repo root
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import streamlit as st

from utils.hf_cas_extractor import HFCASExtractor, validate_cas


def _ensure_extractor() -> HFCASExtractor:
    if "hf_extractor" not in st.session_state:
        st.session_state.hf_extractor = HFCASExtractor()
    return st.session_state.hf_extractor


def _ensure_extractions_count():
    if "hf_extractions" not in st.session_state:
        st.session_state.hf_extractions = 0


# --- Prompt 9: Model Manager UI ---
def render_model_manager(extractor: HFCASExtractor) -> None:
    st.markdown("### 🤗 Hugging Face Model Manager")
    st.markdown("All models run **100% locally** — no API calls, no data leaves your computer.")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Device", extractor.device.upper())
    with col2:
        try:
            import torch
            if torch.cuda.is_available():
                gpu_mem = torch.cuda.get_device_properties(0).total_memory / (1024**3)
                st.metric("GPU Memory", f"{gpu_mem:.1f} GB")
            else:
                st.metric("GPU Memory", "N/A")
        except Exception:
            st.metric("GPU Memory", "N/A")
    with col3:
        try:
            import psutil
            ram = psutil.virtual_memory().total / (1024**3)
            st.metric("System RAM", f"{ram:.1f} GB")
        except Exception:
            st.metric("System RAM", "—")
    st.divider()

    st.markdown("#### 🎯 Recommended Model")
    recommended = extractor.recommend_model_for_hardware()
    col1, col2 = st.columns([3, 1])
    with col1:
        st.info(f"**{recommended['name']}**\n\n{recommended['description']}")
    with col2:
        if st.button("📥 Load Recommended", type="primary"):
            with st.spinner(f"Loading {recommended['name']}... (first download may take a few minutes)"):
                ok = extractor.load_model(
                    recommended["hf_id"],
                    recommended.get("quantization", "none"),
                )
                if ok:
                    st.success("Model loaded successfully!")
                    st.rerun()
                else:
                    st.error("Load failed. Check console and install requirements_hf.txt.")
    st.divider()

    st.markdown("#### 🔧 Manual Model Selection")
    model_options = {
        "SmolLM2-1.7B (4GB RAM)": "HuggingFaceTB/SmolLM2-1.7B-Instruct",
        "Phi-3-mini (6GB RAM)": "microsoft/Phi-3-mini-4k-instruct",
        "Qwen2.5-7B (10GB RAM, 4bit)": "Qwen/Qwen2.5-7B-Instruct",
        "Mistral-7B (10GB RAM, 4bit)": "mistralai/Mistral-7B-Instruct-v0.3",
        "Custom HF Model ID": "custom",
    }
    selected = st.selectbox("Choose model", list(model_options.keys()))
    if selected == "Custom HF Model ID":
        model_id = st.text_input("Enter Hugging Face model ID", "microsoft/phi-2")
    else:
        model_id = model_options[selected]
    quant = st.radio(
        "Quantization (for GPU)",
        ["none", "4bit", "8bit"],
        horizontal=True,
        help="4bit/8bit reduces memory usage",
    )
    if st.button("🚀 Load Model"):
        with st.spinner(f"Loading {model_id}..."):
            ok = extractor.load_model(model_id, quant)
            if ok:
                st.success("✅ Model loaded successfully!")
                st.balloons()
                st.rerun()
            else:
                st.error("Failed to load model. Install: pip install -r requirements_hf.txt")
    if extractor.model_loaded:
        st.success(f"✅ **Active Model:** {extractor.model_name}")
        st.caption(f"Loaded on: {extractor.device}")


# --- Prompt 9: Display results ---
def display_results(extractor: HFCASExtractor, results: dict, filename: str) -> None:
    st.markdown(f"### 📄 Results: {filename}")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("CAS Numbers Found", len(results.get("cas_numbers", [])))
    with col2:
        st.metric("Method", (results.get("method_used") or "rule_based").replace("_", " ").title())
    with col3:
        if results.get("llm_used"):
            st.success("✅ LLM Enhanced")
        else:
            st.info("📏 Rule Based")

    details = results.get("details", [])
    if details:
        st.markdown("#### ✅ Extracted CAS Numbers")
        for i, d in enumerate(details):
            cols = st.columns([1, 2, 3, 1])
            with cols[0]:
                st.markdown(f"**{i+1}.**")
            with cols[1]:
                st.code(d.get("cas", ""), language=None)
            with cols[2]:
                st.markdown(f"*{d.get('component', 'Unknown')}*")
            with cols[3]:
                conf = d.get("confidence", "")
                badge = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(conf, "⚪")
                st.markdown(f"{badge} {conf}")
            with st.expander("Details"):
                st.json(d)
        st.markdown("#### 🔍 Assess These Chemicals")
        if len(results["cas_numbers"]) == 1:
            if st.button("📊 Assess Single Component", type="primary", use_container_width=True):
                st.session_state["selected_cas"] = results["cas_numbers"][0]
                st.session_state["assessment_mode"] = "sds_single"
                st.rerun()
        else:
            c1, c2 = st.columns(2)
            with c1:
                if st.button("📊 Assess All Components", type="primary", use_container_width=True):
                    st.session_state["selected_cas_list"] = results["cas_numbers"]
                    st.session_state["assessment_mode"] = "sds_multi"
                    st.rerun()
            with c2:
                opts = [f"{d['cas']} - {d.get('component', 'Unknown')}" for d in details]
                sel = st.selectbox("Or select one:", options=opts)
                if sel and st.button("Assess selected", use_container_width=True):
                    cas = sel.split(" - ")[0]
                    st.session_state["selected_cas"] = cas
                    st.session_state["assessment_mode"] = "sds_single"
                    st.rerun()
    else:
        st.warning("No CAS numbers found in this SDS.")
        st.markdown("#### ✏️ Manual Entry")
        manual = st.text_input("Enter CAS number manually:")
        if manual and validate_cas(manual):
            if st.button("Assess Manual CAS"):
                st.session_state["selected_cas"] = manual
                st.session_state["assessment_mode"] = "manual"
                st.rerun()


# --- Prompt 9 + 11: Main interface and entry ---
def render_main_interface(extractor: HFCASExtractor) -> None:
    st.markdown("## 🔬 SDS Chemical Identification")
    st.markdown("### 🏠 Local Processing with Hugging Face Transformers")

    with st.sidebar:
        st.markdown("## 🤖 Model Settings")
        if extractor.model_loaded:
            st.success(f"✅ Model: {extractor.model_name}")
            if st.button("🔄 Unload Model"):
                extractor.model = None
                extractor.tokenizer = None
                extractor.pipeline = None
                extractor.model_loaded = False
                extractor.model_name = None
                st.rerun()
        else:
            st.warning("⚠️ No model loaded")
            if st.button("⚙️ Open Model Manager"):
                st.session_state.show_hf_model_manager = True
        st.divider()
        st.markdown("### ⚙️ Settings")
        use_llm = st.checkbox("Use LLM verification", value=extractor.model_loaded)
        st.caption("LLM verification improves accuracy when a model is loaded.")
        st.divider()
        st.markdown("### 📊 Session Stats")
        _ensure_extractions_count()
        st.metric("PDFs Processed", st.session_state.hf_extractions)

    if st.session_state.get("show_hf_model_manager", False):
        with st.expander("Model Manager", expanded=True):
            render_model_manager(extractor)
            if st.button("Close Manager"):
                st.session_state.show_hf_model_manager = False
                st.rerun()

    uploaded = st.file_uploader(
        "Upload Safety Data Sheet (PDF)",
        type=["pdf"],
        help="PDF processed locally — no data leaves your computer",
    )
    if uploaded:
        col1, _ = st.columns([1, 3])
        with col1:
            process = st.button("🔍 Extract CAS Numbers", type="primary", use_container_width=True)
        if process:
            pdf_bytes = uploaded.getvalue()
            # Prompt 8: cache by hash
            pdf_key = "hf_pdf_cache_" + extractor.pdf_hash(pdf_bytes)
            if pdf_key not in st.session_state:
                with st.spinner("Processing PDF..."):
                    from io import BytesIO
                    results = extractor.extract_cas_from_pdf(BytesIO(pdf_bytes), use_llm=use_llm)
                    st.session_state[pdf_key] = results
                    _ensure_extractions_count()
                    st.session_state.hf_extractions += 1
            results = st.session_state[pdf_key]
            display_results(extractor, results, uploaded.name)
            st.info(f"Extraction method: {results.get('method_used', 'rule_based')}")


def main() -> None:
    st.set_page_config(
        page_title="Local SDS CAS Extractor",
        page_icon="🔬",
        layout="wide",
    )
    st.markdown("""
        <style>
        .main-header { font-size: 2rem; font-weight: 600; color: #1E3A8A; margin-bottom: 1rem; }
        .sub-header { font-size: 1.2rem; color: #4B5563; margin-bottom: 2rem; }
        </style>
    """, unsafe_allow_html=True)
    st.markdown('<p class="main-header">🔬 Local Chemical Intelligence Platform</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Powered by Hugging Face Transformers — 100% Local Processing</p>', unsafe_allow_html=True)

    if "show_hf_model_manager" not in st.session_state:
        st.session_state.show_hf_model_manager = True
    extractor = _ensure_extractor()
    render_main_interface(extractor)
    st.divider()
    st.caption("🔒 All processing done locally — no data leaves your computer.")
    st.caption(f"📅 Session: {datetime.now().strftime('%Y-%m-%d %H:%M')}")


if __name__ == "__main__":
    main()
