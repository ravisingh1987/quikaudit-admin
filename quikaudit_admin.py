import streamlit as st
from pymongo import MongoClient
import pymysql
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

MONGODB_URI = st.secrets["MONGODB_URI"]
MARIADB_URI = st.secrets["MARIADB_URI"]

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

JOB_WORKER_TYPES = {"Outsource": 1, "In_House": 2}
JOB_WORKER_ROLES = {"Cutting": 1, "Printing": 2, "Embroidery": 3, "Stitching": 4, "Checking": 5, "Ironing": 6, "Packing": 7}

# ─── DB CONNECTIONS ───────────────────────────────────────────────────────────

@st.cache_resource
def get_mongo_db():
    client = MongoClient(MONGODB_URI)
    return client["organization_db"]

def get_mariadb_conn():
    uri = MARIADB_URI
    # Parse mysql+pymysql://user:pass@host/db
    uri = uri.replace("mysql+pymysql://", "")
    user_pass, rest = uri.split("@")
    user, password = user_pass.split(":")
    host_db = rest.split("/")
    host = host_db[0]
    db = host_db[1]
    return pymysql.connect(host=host, user=user, password=password, database=db, cursorclass=pymysql.cursors.DictCursor)

# ─── PAGE ─────────────────────────────────────────────────────────────────────

st.set_page_config(page_title="QuikAudit Admin", page_icon="🧵", layout="wide")
st.title("🧵 QuikAudit Admin Panel")
st.caption("Manage master data for Thrive Fashion and Caesar Industries")

# ─── SIDEBAR ──────────────────────────────────────────────────────────────────

st.sidebar.header("Select Organisation")
org_name = st.sidebar.selectbox("Organisation", list(ORGANIZATIONS.keys()))
org_id = ORGANIZATIONS[org_name]
st.sidebar.markdown("---")
st.sidebar.markdown(f"**Org ID:**\n`{org_id}`")

# ─── MAIN TABS ────────────────────────────────────────────────────────────────

main_tab1, main_tab2 = st.tabs(["📦 MongoDB Master Data", "👷 Job Workers (MariaDB)"])

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1: MONGODB
# ═══════════════════════════════════════════════════════════════════════════════

with main_tab1:
    collection_name = st.selectbox("Collection", list(COLLECTIONS.keys()))
    col_config = COLLECTIONS[collection_name]
    main_field = col_config["field"]
    main_label = col_config["label"]
    extra_fields = col_config["extra_fields"]

    db = get_mongo_db()
    collection = db[collection_name]

    tab1, tab2, tab3 = st.tabs(["➕ Add Entry", "📋 View & Delete", "📥 Bulk Add"])

    with tab1:
        st.subheader(f"Add {main_label} for {org_name}")
        main_value = st.text_input(main_label, key="add_main")
        extra_values = {}
        for ef in extra_fields:
            extra_values[ef["key"]] = st.text_input(ef["label"], value=ef.get("default", ""), key=f"add_{ef['key']}")
        if st.button("Add Entry", type="primary", key="add_btn"):
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
            filtered = [d for d in docs if not search or search.lower() in str(d.get(main_field, "")).lower()]
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
                if st.button("Delete Selected", type="primary", key="delete_btn"):
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
        if st.button("Bulk Insert", type="primary", key="bulk_btn"):
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
                        doc[ef["key"]] = parts[i+1].lower() if len(parts) > i+1 and parts[i+1] else ef.get("default", "").lower()
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

# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2: JOB WORKERS (MARIADB)
# ═══════════════════════════════════════════════════════════════════════════════

with main_tab2:
    st.subheader(f"Job Workers for {org_name}")

    jw_tab1, jw_tab2 = st.tabs(["➕ Add Job Worker", "📋 View Job Workers"])

    with jw_tab1:
        st.markdown("Fill in the details to add a new job worker.")

        jw_name = st.text_input("Job Worker Name", key="jw_name")
        jw_custom_id = st.text_input("Custom ID (short unique code, e.g. MPNA)", key="jw_custom_id")
        jw_type = st.selectbox("Type", list(JOB_WORKER_TYPES.keys()), key="jw_type")
        jw_role = st.selectbox("Role", list(JOB_WORKER_ROLES.keys()), key="jw_role")
        jw_capacity = st.number_input("Capacity", min_value=0, value=0, key="jw_capacity")
        jw_gst = st.text_input("GST Number (optional)", key="jw_gst")

        if st.button("Add Job Worker", type="primary", key="jw_add_btn"):
            if not jw_name.strip():
                st.error("Name cannot be empty.")
            elif not jw_custom_id.strip():
                st.error("Custom ID cannot be empty.")
            else:
                try:
                    conn = get_mariadb_conn()
                    with conn.cursor() as cursor:
                        # Check if custom_id already exists
                        cursor.execute("SELECT job_worker_id FROM job_workers WHERE custom_id = %s", (jw_custom_id.strip(),))
                        if cursor.fetchone():
                            st.error(f"Custom ID '{jw_custom_id}' already exists. Choose a different one.")
                        else:
                            # Insert into job_workers
                            cursor.execute("""
                                INSERT INTO job_workers (name, custom_id, type_id, role_id, capacity, gst, created_at, updated_at)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                jw_name.strip(),
                                jw_custom_id.strip(),
                                JOB_WORKER_TYPES[jw_type],
                                JOB_WORKER_ROLES[jw_role],
                                jw_capacity,
                                jw_gst.strip() if jw_gst.strip() else None,
                                datetime.utcnow(),
                                datetime.utcnow()
                            ))
                            new_id = cursor.lastrowid

                            # Link to organisation
                            cursor.execute("""
                                INSERT INTO job_worker_organization (job_worker_id, organization_id, assigned_at, is_active)
                                VALUES (%s, %s, %s, %s)
                            """, (new_id, org_id, datetime.utcnow(), 1))

                            conn.commit()
                            st.success(f"Job worker '{jw_name}' added successfully with ID {new_id} and linked to {org_name}.")
                except Exception as e:
                    st.error(f"Database error: {str(e)}")
                finally:
                    conn.close()

    with jw_tab2:
        try:
            conn = get_mariadb_conn()
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT jw.job_worker_id, jw.name, jw.custom_id, 
                           jwt.type_name, jwr.role_name, jw.capacity,
                           jwo.is_active
                    FROM job_workers jw
                    JOIN job_worker_organization jwo ON jw.job_worker_id = jwo.job_worker_id
                    JOIN job_worker_type jwt ON jw.type_id = jwt.id
                    JOIN job_worker_role jwr ON jw.role_id = jwr.id
                    WHERE jwo.organization_id = %s
                    ORDER BY jw.job_worker_id DESC
                """, (org_id,))
                rows = cursor.fetchall()

            if not rows:
                st.info(f"No job workers found for {org_name}.")
            else:
                st.caption(f"{len(rows)} job workers found")
                import pandas as pd
                df = pd.DataFrame(rows)
                df.columns = ["ID", "Name", "Custom ID", "Type", "Role", "Capacity", "Active"]
                df["Active"] = df["Active"].map({1: "✅ Yes", 0: "❌ No"})
                st.dataframe(df, use_container_width=True)
        except Exception as e:
            st.error(f"Database error: {str(e)}")
        finally:
            conn.close()
