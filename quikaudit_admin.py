import streamlit as st
from pymongo import MongoClient
from bson import ObjectId

# ─── CONFIG ───────────────────────────────────────────────────────────────────

MONGODB_URI = st.secrets["MONGODB_URI"]

ORGANIZATIONS = {
    "Thrive Fashion Pvt. Ltd": "org_7779bed0-0aab-4508-904f-eafc3d22c8ff",
    "Caesar Industries LLP": "org_02206976-71a2-4378-b458-9139c7a124a6"
}

COLLECTIONS = {
    "vendors":      {"field": "vendor",     "label": "Vendor Name",     "extra_fields": [{"key": "initials", "label": "Initials"}]},
    "colors":       {"field": "color",      "label": "Color Name",      "extra_fields": []},
    "fabric_types": {"field": "fabric_type","label": "Fabric Type",     "extra_fields": []},
    "parties":      {"field": "party",      "label": "Party Name",      "extra_fields": [{"key": "initials", "label": "Initials"}]},
    "job_workers":  {"field": "job_worker", "label": "Job Worker Name", "extra_fields": []},
    "merchants":    {"field": "merchant",   "label": "Merchant Name",   "extra_fields": []},
    "plans":        {"field": "plan",       "label": "Plan",            "extra_fields": []},
    "receivers":    {"field": "name",       "label": "Receiver Name",   "extra_fields": [{"key": "type", "label": "Type", "default": "printer"}]},
    "designs":      {"field": "design_no",  "label": "Design Number",   "extra_fields": []},
}

@st.cache_resource
def get_db():
    client = MongoClient(MONGODB_URI)
    return client["organization_db"]

st.set_page_config(page_title="QuikAudit Admin", page_icon="🧵", layout="wide")
st.title("🧵 QuikAudit Admin Panel")
st.caption("Manage master data for Thrive Fashion and Caesar Industries")

st.sidebar.header("Select Organisation & Collection")
org_name = st.sidebar.selectbox("Organisation", list(ORGANIZATIONS.keys()))
org_id = ORGANIZATIONS[org_name]
collection_name = st.sidebar.selectbox("Collection", list(COLLECTIONS.keys()))
col_config = COLLECTIONS[collection_name]
main_field = col_config["field"]
main_label = col_config["label"]
extra_fields = col_config["extra_fields"]
st.sidebar.markdown("---")
st.sidebar.markdown(f"**Org ID:**\n`{org_id}`")

db = get_db()
collection = db[collection_name]

tab1, tab2, tab3 = st.tabs(["Add Entry", "View & Delete", "Bulk Add"])

with tab1:
    st.subheader(f"Add {main_label} for {org_name}")
    main_value = st.text_input(main_label, key="add_main")
    extra_values = {}
    for ef in extra_fields:
        extra_values[ef["key"]] = st.text_input(ef["label"], value=ef.get("default", ""), key=f"add_{ef['key']}")
    if st.button("Add Entry", type="primary"):
        if not main_value.strip():
            st.error(f"{main_label} cannot be empty.")
        else:
            doc = {"organization_id": org_id, main_field: main_value.strip().lower()}
            for key, val in extra_values.items():
                doc[key] = val.strip().lower() if val.strip() else ""
            collection.insert_one(doc)
            st.success(f"Added '{main_value}' to {collection_name} for {org_name}.")

with tab2:
    st.subheader(f"All {collection_name} for {org_name}")
    docs = list(collection.find({"organization_id": org_id}, {"_id": 1, main_field: 1, **{ef["key"]: 1 for ef in extra_fields}}))
    if not docs:
        st.info("No entries found.")
    else:
        search = st.text_input("Search", placeholder=f"Filter by {main_label.lower()}...", key="search")
        filtered = docs
        if search:
            filtered = [d for d in docs if search.lower() in str(d.get(main_field, "")).lower()]
        st.caption(f"Showing {len(filtered)} of {len(docs)} entries")
        options = []
        id_map = {}
        for doc in filtered:
            display = doc.get(main_field, "")
            extras = " | ".join([f"{ef['key']}: {doc.get(ef['key'], '')}" for ef in extra_fields if doc.get(ef['key'])])
            label = f"{display}" + (f" [{extras}]" if extras else "")
            options.append(label)
            id_map[label] = doc["_id"]
        if options:
            selected = st.selectbox("Select entry to delete", options, key="delete_select")
            if st.button("Delete Selected", type="primary"):
                collection.delete_one({"_id": id_map[selected]})
                st.success(f"Deleted '{selected}'")
                st.rerun()

with tab3:
    st.subheader(f"Bulk Add {main_label} for {org_name}")
    if extra_fields:
        field_hint = ", ".join([ef["key"] for ef in extra_fields])
        st.info(f"Format: value,{field_hint} per line.")
    else:
        st.info(f"Paste one {main_label.lower()} per line.")
    bulk_text = st.text_area("Paste entries here", height=300, key="bulk_text")
    if st.button("Bulk Insert", type="primary"):
        if not bulk_text.strip():
            st.error("Nothing to insert.")
        else:
            lines = [l.strip() for l in bulk_text.strip().splitlines() if l.strip()]
            existing_docs = collection.find({"organization_id": org_id}, {main_field: 1, "_id": 0})
            existing_values = set(d.get(main_field, "").lower() for d in existing_docs)
            docs_to_insert = []
            skipped = 0
            for line in lines:
                parts = [p.strip() for p in line.split(",")]
                main_val = parts[0].lower() if parts[0] else ""
                if not main_val or main_val in existing_values:
                    skipped += 1
                    continue
                doc = {"organization_id": org_id, main_field: main_val}
                for i, ef in enumerate(extra_fields):
                    if len(parts) > i + 1 and parts[i + 1]:
                        doc[ef["key"]] = parts[i + 1].lower()
                    else:
                        doc[ef["key"]] = ef.get("default", "").lower()
                docs_to_insert.append(doc)
                existing_values.add(main_val)
            if docs_to_insert:
                try:
                    collection.insert_many(docs_to_insert, ordered=False)
                    st.success(f"Inserted: {len(docs_to_insert)} | Skipped: {skipped}")
                except Exception as e:
                    st.error(f"Error: {str(e)}")
            else:
                st.warning(f"Nothing new to insert. All {skipped} were duplicates or empty.")
